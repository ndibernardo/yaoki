//! The journal: source of truth for an execution. Append-only, never
//! rewritten.

use thiserror::Error;

use crate::execution::{ExecutionId, WorkflowErrorRecord, WorkflowName, WorkflowVersion};
use crate::random::RandomBytes;
use crate::step::{Attempt, StepErrorRecord, StepName};
use crate::time::{Deadline, Timestamp};

/// 0-based position of a command in an execution's command sequence.
/// `Seq` increments per command (step, now, random, timer), giving replay a
/// stable spine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

impl Seq {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    /// Builds a `Seq` from a store's raw append position. Store-internal:
    /// callers outside this crate never construct a `Seq` out of thin air.
    pub(crate) fn from_index(index: u64) -> Self {
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
    #[error("journal store lock poisoned")]
    Poisoned,
}

/// Append-only event log, one logical stream per execution.
pub trait JournalStore {
    /// Appends `event` to the execution's log. Returns the 0-based position
    /// the event was appended at.
    fn append(&self, id: &ExecutionId, event: JournalEvent) -> Result<Seq, JournalError>;

    /// Loads the full event history for `id`. An execution with no events
    /// yet (never started) loads as an empty `Journal`, not an error.
    fn load(&self, id: &ExecutionId) -> Result<Journal, JournalError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_zero_starts_at_zero() {
        assert_eq!(Seq::zero().get(), 0);
    }

    #[test]
    fn seq_next_increments_by_one() {
        let first = Seq::zero();

        let second = first.next();

        assert_eq!(second.get(), 1);
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
