//! The journal: source of truth for an execution. Append-only, never
//! rewritten.

use thiserror::Error;

use crate::command::CommandKind;
use crate::execution::ExecutionId;
use crate::execution::WorkflowErrorRecord;
use crate::execution::WorkflowName;
use crate::execution::WorkflowVersion;
use crate::random::RandomBytes;
use crate::step::Attempt;
use crate::step::StepErrorRecord;
use crate::step::StepName;
use crate::time::Deadline;
use crate::time::Timestamp;

/// 0-based position of a command in an execution's command sequence.
/// `Seq` increments per command (step, now, random, timer), giving replay a
/// stable spine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

/// A command position has no representable successor.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SeqError {
    /// The current position is `u64::MAX`.
    #[error("command position overflow")]
    Overflow,
}

impl Seq {
    pub fn zero() -> Self {
        Self(0)
    }

    /// Returns the successor, or `SeqError::Overflow` at `u64::MAX`.
    pub fn next(self) -> Result<Self, SeqError> {
        self.0.checked_add(1).map(Self).ok_or(SeqError::Overflow)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    /// Reads a stored command position; history parsing checks its ordering.
    pub(crate) fn from_record(position: u64) -> Self {
        Self(position)
    }
}

/// Zero-based event offset, independent of a command's position.
///
/// ```compile_fail
/// use yaoki::journal::EventOffset;
/// use yaoki::journal::Seq;
///
/// fn command_position(offset: EventOffset) -> Seq {
///     offset
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventOffset(usize);

impl EventOffset {
    /// Returns the event's index in append order.
    pub fn get(self) -> usize {
        self.0
    }

    /// Constructs an append offset for storage implementations.
    /// Every `usize` index is representable; this does not validate history order.
    ///
    /// ```
    /// use yaoki::journal::EventOffset;
    ///
    /// let completion_offset = EventOffset::from_index(3);
    /// assert_eq!(completion_offset.get(), 3);
    /// ```
    pub fn from_index(index: usize) -> Self {
        Self(index)
    }
}

/// Opaque serialized payload (step result, workflow input/output). The
/// engine never inspects it; the codec lives at the caller's boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPayload(Vec<u8>);

impl EventPayload {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// One durable fact about an execution. Append-only, never rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalEvent {
    ExecutionStarted {
        workflow: WorkflowName,
        version: WorkflowVersion,
        input: EventPayload,
    },
    StepScheduled {
        seq: Seq,
        name: StepName,
    },
    StepStarted {
        seq: Seq,
        attempt: Attempt,
    },
    StepCompleted {
        seq: Seq,
        result: EventPayload,
    },
    StepFailed {
        seq: Seq,
        attempt: Attempt,
        error: StepErrorRecord,
    },
    NowRecorded {
        seq: Seq,
        value: Timestamp,
    },
    RandomRecorded {
        seq: Seq,
        value: RandomBytes,
    },
    TimerScheduled {
        seq: Seq,
        deadline: Deadline,
    },
    TimerFired {
        seq: Seq,
    },
    ExecutionCompleted {
        output: EventPayload,
    },
    ExecutionFailed {
        error: WorkflowErrorRecord,
    },
}

impl JournalEvent {
    /// The `CommandKind` this event represents having been commanded, if
    /// any. `None` for events that only ever follow a scheduling event
    /// (`StepStarted`, `StepCompleted`, `StepFailed`, `TimerFired`) or that
    /// bookend the whole execution rather than a single command.
    pub fn command_kind(&self) -> Option<CommandKind> {
        match self {
            JournalEvent::StepScheduled { .. } => Some(CommandKind::RunStep),
            JournalEvent::NowRecorded { .. } => Some(CommandKind::ReadNow),
            JournalEvent::RandomRecorded { .. } => Some(CommandKind::DrawRandom),
            JournalEvent::TimerScheduled { .. } => Some(CommandKind::Sleep),
            JournalEvent::ExecutionStarted { .. }
            | JournalEvent::StepStarted { .. }
            | JournalEvent::StepCompleted { .. }
            | JournalEvent::StepFailed { .. }
            | JournalEvent::TimerFired { .. }
            | JournalEvent::ExecutionCompleted { .. }
            | JournalEvent::ExecutionFailed { .. } => None,
        }
    }
}

/// One execution's full event history, in append order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Journal(Vec<JournalEvent>);

impl Journal {
    pub fn new(events: Vec<JournalEvent>) -> Self {
        Self(events)
    }

    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn events(&self) -> &[JournalEvent] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Failures a `JournalStore` can report.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum JournalError {
    /// Another cooperating execution handle currently owns this stream.
    #[error("execution {id:?} is already owned")]
    ExecutionOwned { id: ExecutionId },

    #[error("journal store lock poisoned")]
    Poisoned,

    /// Environmental I/O failure (open, read, write, fsync). Captured as a
    /// message, not `#[from] std::io::Error`: `io::Error` has no `PartialEq`,
    /// and this crate's own error types are compared with `assert_eq!`
    /// throughout.
    #[error("journal I/O error: {message}")]
    Io { message: String },

    /// A frame passed its CRC/length check but decoded into a domain value
    /// a validated constructor rejects (e.g. an empty `StepName`). Distinct
    /// from a torn write: those are truncated silently, never surfaced here.
    #[error("journal record is malformed: {message}")]
    Codec { message: String },
}

/// Append-only event log, one logical stream per execution.
/// Engines acquire ownership before accessing a stream. Direct callers must
/// hold a lease across related loads and appends when access can compete.
/// An implementation must not offer a no-op lease for shared storage.
pub trait JournalStore {
    /// Exclusive ownership released when the lease is dropped.
    type Lease<'a>
    where
        Self: 'a;

    /// Acquires this stream without reading, healing, or appending its journal.
    /// Refuses a competing owner rather than waiting. File implementations must
    /// exclude cooperating owners in other processes as well as this one.
    fn acquire(&self, id: &ExecutionId) -> Result<Self::Lease<'_>, JournalError>;

    /// Appends `event` to the execution's log. Returns the 0-based position
    /// the event was appended at.
    fn append(&self, id: &ExecutionId, event: JournalEvent) -> Result<EventOffset, JournalError>;

    /// Loads the full event history for `id`. An execution with no events
    /// yet (never started) loads as an empty `Journal`, not an error.
    fn load(&self, id: &ExecutionId) -> Result<Journal, JournalError>;
}

#[cfg(test)]
mod tests {
    use proptest::proptest;

    use super::EventOffset;
    use super::EventPayload;
    use super::Journal;
    use super::JournalEvent;
    use super::Seq;
    use super::SeqError;
    use crate::command::CommandKind;
    use crate::execution::WorkflowErrorRecord;
    use crate::execution::WorkflowName;
    use crate::execution::WorkflowVersion;
    use crate::random::RandomBytes;
    use crate::step::Attempt;
    use crate::step::StepErrorRecord;
    use crate::step::StepName;
    use crate::time::Deadline;
    use crate::time::Timestamp;

    proptest! {
        #[test]
        fn seq_next_matches_checked_arithmetic(raw in proptest::prelude::any::<u64>()) {
            let seq = Seq::from_record(raw);

            let result = seq.next();

            assert_eq!(result, raw.checked_add(1).map(Seq::from_record).ok_or(SeqError::Overflow));
        }
    }

    #[test]
    fn seq_next_before_the_maximum_returns_the_last_position() {
        let penultimate = Seq::from_record(u64::MAX - 1);

        let result = penultimate.next();

        assert_eq!(result, Ok(Seq::from_record(u64::MAX)));
    }

    #[test]
    fn seq_zero_starts_at_zero() {
        assert_eq!(Seq::zero().get(), 0);
    }

    #[test]
    fn seq_next_increments_by_one() {
        let first = Seq::zero();

        let second = first.next().unwrap();

        assert_eq!(second.get(), 1);
    }

    #[test]
    fn seq_next_at_the_maximum_returns_a_typed_overflow_error() {
        let last = Seq::from_record(u64::MAX);

        let result = last.next();

        assert_eq!(result, Err(SeqError::Overflow));
    }

    #[test]
    fn event_offset_retains_an_event_position_independently_of_command_seq() {
        let offset = EventOffset::from_index(3);

        assert_eq!(offset.get(), 3);
    }

    #[test]
    fn event_payload_round_trips_bytes() {
        let charge_confirmation = br#"{"charge_id":"ch_2026_0718"}"#.to_vec();

        let payload = EventPayload::new(charge_confirmation.clone());

        assert_eq!(payload.as_bytes(), charge_confirmation.as_slice());
        assert_eq!(payload.into_bytes(), charge_confirmation);
    }

    #[test]
    fn step_completed_event_carries_seq_and_result() {
        let seq = Seq::zero();
        let result = EventPayload::new(b"charged".to_vec());

        let event = JournalEvent::StepCompleted {
            seq,
            result: result.clone(),
        };

        match event {
            JournalEvent::StepCompleted {
                seq: got_seq,
                result: got_result,
            } => {
                assert_eq!(got_seq, seq);
                assert_eq!(got_result, result);
            }
            other => panic!("expected StepCompleted, got {other:?}"),
        }
    }

    #[test]
    fn command_kind_maps_step_scheduled_to_run_step() {
        let event = JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new("charge-card").unwrap(),
        };

        assert_eq!(event.command_kind(), Some(CommandKind::RunStep));
    }

    #[test]
    fn command_kind_maps_now_recorded_to_read_now() {
        let event = JournalEvent::NowRecorded {
            seq: Seq::zero(),
            value: Timestamp::from_millis_since_epoch(1_753_401_600_000),
        };

        assert_eq!(event.command_kind(), Some(CommandKind::ReadNow));
    }

    #[test]
    fn command_kind_maps_random_recorded_to_draw_random() {
        let event = JournalEvent::RandomRecorded {
            seq: Seq::zero(),
            value: RandomBytes::new([0u8; 32]),
        };

        assert_eq!(event.command_kind(), Some(CommandKind::DrawRandom));
    }

    #[test]
    fn command_kind_maps_timer_scheduled_to_sleep() {
        let event = JournalEvent::TimerScheduled {
            seq: Seq::zero(),
            deadline: Deadline::at(Timestamp::from_millis_since_epoch(1_753_401_600_000)),
        };

        assert_eq!(event.command_kind(), Some(CommandKind::Sleep));
    }

    #[test]
    fn command_kind_is_none_for_non_scheduling_events() {
        let events = [
            JournalEvent::ExecutionStarted {
                workflow: WorkflowName::new("signup").unwrap(),
                version: WorkflowVersion::new("2026.07.18").unwrap(),
                input: EventPayload::new(b"{}".to_vec()),
            },
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first(),
            },
            JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: EventPayload::new(b"charged".to_vec()),
            },
            JournalEvent::StepFailed {
                seq: Seq::zero(),
                attempt: Attempt::first(),
                error: StepErrorRecord::new("payment gateway timed out"),
            },
            JournalEvent::TimerFired { seq: Seq::zero() },
            JournalEvent::ExecutionCompleted {
                output: EventPayload::new(b"done".to_vec()),
            },
            JournalEvent::ExecutionFailed {
                error: WorkflowErrorRecord::new("account creation rolled back"),
            },
        ];

        for event in events {
            assert_eq!(event.command_kind(), None, "unexpected kind for {event:?}");
        }
    }

    #[test]
    fn journal_empty_has_no_events() {
        let journal = Journal::empty();

        assert!(journal.is_empty());
        assert_eq!(journal.len(), 0);
        assert_eq!(journal.events(), &[]);
    }

    #[test]
    fn journal_new_preserves_append_order() {
        let scheduled = JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new("charge-card").unwrap(),
        };
        let started = JournalEvent::StepStarted {
            seq: Seq::zero(),
            attempt: Attempt::first(),
        };

        let journal = Journal::new(vec![scheduled.clone(), started.clone()]);

        assert!(!journal.is_empty());
        assert_eq!(journal.len(), 2);
        assert_eq!(journal.events(), &[scheduled, started]);
    }
}
