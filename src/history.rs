//! Parses the event grammar independently of physical journal framing.

use thiserror::Error;

use crate::execution::WorkflowErrorRecord;
use crate::journal::EventOffset;
use crate::journal::EventPayload;
use crate::journal::Journal;
use crate::journal::JournalEvent;
use crate::journal::Seq;
use crate::journal::SeqError;
use crate::step::Attempt;
use crate::step::AttemptOverflow;

/// A structurally illegal journal, with the offending event offset.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HistoryError {
    /// No invocation was recorded.
    #[error("execution history is empty")]
    Empty,
    /// The event cannot follow the preceding lifecycle or command events.
    #[error("unexpected journal event at offset {offset:?}")]
    UnexpectedEvent { offset: EventOffset },
    /// A command or its follower used a different position.
    #[error("command position at offset {offset:?}: expected {expected:?}, recorded {recorded:?}")]
    PositionMismatch {
        offset: EventOffset,
        expected: Seq,
        recorded: Seq,
    },
    /// An attempt skipped, repeated, or failed a different attempt number.
    #[error("attempt at offset {offset:?}: expected {expected:?}, recorded {recorded:?}")]
    AttemptMismatch {
        offset: EventOffset,
        expected: Attempt,
        recorded: Attempt,
    },
    /// Advancing beyond the command would overflow its position.
    #[error("command position overflow at offset {offset:?}")]
    SequenceOverflow { offset: EventOffset },
    /// An interrupted attempt has no representable successor.
    #[error("attempt overflow at offset {offset:?}")]
    AttemptOverflow { offset: EventOffset },
}

/// A complete legal execution history or an admissible interrupted prefix.
/// Construction checks all events before making the history available for replay.
///
/// ```compile_fail
/// use yaoki::history::ValidatedHistory;
/// use yaoki::journal::Journal;
///
/// fn replace_events(history: &mut ValidatedHistory) {
///     history.journal = Journal::empty();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ValidatedHistory {
    journal: Journal,
    end: HistoryState,
}

impl ValidatedHistory {
    /// Parses command positions, attempt progression, timers, and terminal placement.
    /// Interrupted scheduling, attempts, and timers are accepted at the journal tail.
    ///
    /// # Errors
    /// Rejects an absent initial start, illegal transitions, mismatched positions or
    /// attempts, and counter overflow. Invocation selection is a separate contract.
    pub fn parse(journal: Journal) -> Result<Self, HistoryError> {
        match journal.events().first() {
            None => return Err(HistoryError::Empty),
            Some(JournalEvent::ExecutionStarted { .. }) => {}
            Some(
                JournalEvent::StepScheduled { .. }
                | JournalEvent::StepStarted { .. }
                | JournalEvent::StepCompleted { .. }
                | JournalEvent::StepFailed { .. }
                | JournalEvent::NowRecorded { .. }
                | JournalEvent::RandomRecorded { .. }
                | JournalEvent::TimerScheduled { .. }
                | JournalEvent::TimerFired { .. }
                | JournalEvent::ExecutionCompleted { .. }
                | JournalEvent::ExecutionFailed { .. },
            ) => {
                return unexpected(EventOffset::from_index(0));
            }
        }
        let end = journal.events().iter().enumerate().skip(1).try_fold(
            HistoryState::Ready { seq: Seq::zero() },
            |state, (index, event)| state.advance(event, EventOffset::from_index(index)),
        )?;
        end.ensure_resumable(EventOffset::from_index(journal.len() - 1))?;
        Ok(Self { journal, end })
    }

    /// Returns every validated event in append order, including followers.
    pub fn events(&self) -> &[JournalEvent] {
        self.journal.events()
    }

    /// Selects a terminal result or retains the proof for command replay.
    pub(crate) fn into_recovery(self) -> RecoveryHistory {
        match self.end {
            HistoryState::Completed(output) => RecoveryHistory::Completed(output),
            HistoryState::Failed(error) => RecoveryHistory::Failed(error),
            HistoryState::Ready { .. }
            | HistoryState::Scheduled { .. }
            | HistoryState::Started { .. }
            | HistoryState::Timer { .. } => RecoveryHistory::Running(ReplayHistory {
                journal: self.journal,
            }),
        }
    }
}

/// Recovery choices produced only after the complete grammar has been parsed.
pub(crate) enum RecoveryHistory {
    Running(ReplayHistory),
    Completed(EventPayload),
    Failed(WorkflowErrorRecord),
}

/// A validated nonterminal history. Only parsing can construct this value.
#[derive(Debug, Clone)]
pub(crate) struct ReplayHistory {
    journal: Journal,
}

impl ReplayHistory {
    /// Includes the initial invocation so cursor offsets match append offsets.
    pub(crate) fn events(&self) -> &[JournalEvent] {
        self.journal.events()
    }
}

#[derive(Debug, Clone)]
enum HistoryState {
    Ready { seq: Seq },
    Scheduled { seq: Seq },
    Started { seq: Seq, attempt: Attempt },
    Timer { seq: Seq },
    Completed(EventPayload),
    Failed(WorkflowErrorRecord),
}

impl HistoryState {
    fn advance(self, event: &JournalEvent, offset: EventOffset) -> Result<Self, HistoryError> {
        match event {
            JournalEvent::ExecutionStarted { .. } => unexpected(offset),
            JournalEvent::StepScheduled { seq, .. } => {
                position(self.ready(offset)?, *seq, offset)?;
                ready_after(*seq, offset)?;
                Ok(Self::Scheduled { seq: *seq })
            }
            JournalEvent::StepStarted { seq, attempt } => {
                self.start_attempt(*seq, *attempt, offset)
            }
            JournalEvent::StepCompleted { seq, .. } => {
                let (expected, _) = self.started(offset)?;
                position(expected, *seq, offset)?;
                ready_after(*seq, offset)
            }
            JournalEvent::StepFailed { seq, attempt, .. } => {
                let (expected_seq, expected_attempt) = self.started(offset)?;
                position(expected_seq, *seq, offset)?;
                attempt_number(expected_attempt, *attempt, offset)?;
                ready_after(*seq, offset)
            }
            JournalEvent::NowRecorded { seq, .. } | JournalEvent::RandomRecorded { seq, .. } => {
                position(self.ready(offset)?, *seq, offset)?;
                ready_after(*seq, offset)
            }
            JournalEvent::TimerScheduled { seq, .. } => {
                position(self.ready(offset)?, *seq, offset)?;
                ready_after(*seq, offset)?;
                Ok(Self::Timer { seq: *seq })
            }
            JournalEvent::TimerFired { seq } => match self {
                Self::Timer { seq: expected } => {
                    position(expected, *seq, offset)?;
                    ready_after(*seq, offset)
                }
                Self::Ready { .. }
                | Self::Scheduled { .. }
                | Self::Started { .. }
                | Self::Completed(_)
                | Self::Failed(_) => unexpected(offset),
            },
            JournalEvent::ExecutionCompleted { output } => {
                self.ready(offset)?;
                Ok(Self::Completed(output.clone()))
            }
            JournalEvent::ExecutionFailed { error } => {
                self.ready(offset)?;
                Ok(Self::Failed(error.clone()))
            }
        }
    }

    fn ensure_resumable(&self, offset: EventOffset) -> Result<(), HistoryError> {
        match self {
            Self::Started { attempt, .. } => attempt
                .next()
                .map(|_| ())
                .map_err(|AttemptOverflow| HistoryError::AttemptOverflow { offset }),
            Self::Ready { .. }
            | Self::Scheduled { .. }
            | Self::Timer { .. }
            | Self::Completed(_)
            | Self::Failed(_) => Ok(()),
        }
    }

    fn ready(self, offset: EventOffset) -> Result<Seq, HistoryError> {
        match self {
            Self::Ready { seq } => Ok(seq),
            Self::Scheduled { .. }
            | Self::Started { .. }
            | Self::Timer { .. }
            | Self::Completed(_)
            | Self::Failed(_) => unexpected(offset),
        }
    }

    fn started(self, offset: EventOffset) -> Result<(Seq, Attempt), HistoryError> {
        match self {
            Self::Started { seq, attempt } => Ok((seq, attempt)),
            Self::Ready { .. }
            | Self::Scheduled { .. }
            | Self::Timer { .. }
            | Self::Completed(_)
            | Self::Failed(_) => unexpected(offset),
        }
    }

    fn start_attempt(
        self,
        seq: Seq,
        attempt: Attempt,
        offset: EventOffset,
    ) -> Result<Self, HistoryError> {
        let (expected_seq, expected_attempt) = match self {
            Self::Scheduled { seq } => (seq, Attempt::first()),
            Self::Started { seq, attempt } => (
                seq,
                attempt
                    .next()
                    .map_err(|AttemptOverflow| HistoryError::AttemptOverflow { offset })?,
            ),
            Self::Ready { .. } | Self::Timer { .. } | Self::Completed(_) | Self::Failed(_) => {
                return unexpected(offset);
            }
        };
        position(expected_seq, seq, offset)?;
        attempt_number(expected_attempt, attempt, offset)?;
        Ok(Self::Started { seq, attempt })
    }
}

fn unexpected<T>(offset: EventOffset) -> Result<T, HistoryError> {
    Err(HistoryError::UnexpectedEvent { offset })
}

fn position(expected: Seq, recorded: Seq, offset: EventOffset) -> Result<(), HistoryError> {
    if expected != recorded {
        Err(HistoryError::PositionMismatch {
            offset,
            expected,
            recorded,
        })
    } else {
        Ok(())
    }
}

fn attempt_number(
    expected: Attempt,
    recorded: Attempt,
    offset: EventOffset,
) -> Result<(), HistoryError> {
    if expected != recorded {
        Err(HistoryError::AttemptMismatch {
            offset,
            expected,
            recorded,
        })
    } else {
        Ok(())
    }
}

fn ready_after(seq: Seq, offset: EventOffset) -> Result<HistoryState, HistoryError> {
    let seq = seq
        .next()
        .map_err(|SeqError::Overflow| HistoryError::SequenceOverflow { offset })?;
    Ok(HistoryState::Ready { seq })
}

#[cfg(test)]
mod tests {
    use super::HistoryError;
    use super::HistoryState;
    use super::RecoveryHistory;
    use super::ValidatedHistory;
    use crate::execution::WorkflowErrorRecord;
    use crate::execution::WorkflowName;
    use crate::execution::WorkflowVersion;
    use crate::journal::EventOffset;
    use crate::journal::EventPayload;
    use crate::journal::Journal;
    use crate::journal::JournalEvent;
    use crate::journal::Seq;
    use crate::random::RandomBytes;
    use crate::step::Attempt;
    use crate::step::StepErrorRecord;
    use crate::step::StepName;
    use crate::time::Deadline;
    use crate::time::Timestamp;

    fn signup_events() -> Vec<JournalEvent> {
        let seq = Seq::zero();
        vec![
            JournalEvent::ExecutionStarted {
                workflow: WorkflowName::new("signup").unwrap(),
                version: WorkflowVersion::new("2026.08.01").unwrap(),
                input: EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec()),
            },
            JournalEvent::StepScheduled {
                seq,
                name: StepName::new("charge-payment").unwrap(),
            },
            JournalEvent::StepStarted {
                seq,
                attempt: Attempt::first(),
            },
            JournalEvent::StepCompleted {
                seq,
                result: EventPayload::new(b"charged".to_vec()),
            },
            JournalEvent::StepFailed {
                seq,
                attempt: Attempt::first(),
                error: StepErrorRecord::new("payment gateway timed out"),
            },
            JournalEvent::NowRecorded {
                seq,
                value: Timestamp::from_millis_since_epoch(1_784_937_600_000),
            },
            JournalEvent::RandomRecorded {
                seq,
                value: RandomBytes::new([0x51; 32]),
            },
            JournalEvent::TimerScheduled {
                seq,
                deadline: Deadline::at(Timestamp::from_millis_since_epoch(1_784_937_600_000)),
            },
            JournalEvent::TimerFired { seq },
            JournalEvent::ExecutionCompleted {
                output: EventPayload::new(b"account-created".to_vec()),
            },
            JournalEvent::ExecutionFailed {
                error: WorkflowErrorRecord::new("signup refused"),
            },
        ]
    }

    #[test]
    fn parse_empty_history_returns_empty_error() {
        assert_eq!(
            ValidatedHistory::parse(Journal::empty()).unwrap_err(),
            HistoryError::Empty
        );
    }

    #[test]
    fn parse_every_non_start_header_rejects_the_initial_event() {
        for event in signup_events().into_iter().skip(1) {
            assert_eq!(
                ValidatedHistory::parse(Journal::new(vec![event])).unwrap_err(),
                HistoryError::UnexpectedEvent {
                    offset: EventOffset::from_index(0)
                }
            );
        }
    }

    #[test]
    fn transitions_accept_only_the_events_legal_in_each_state() {
        let seq = Seq::zero();
        let states = [
            (HistoryState::Ready { seq }, vec![1, 5, 6, 7, 9, 10]),
            (HistoryState::Scheduled { seq }, vec![2]),
            (
                HistoryState::Started {
                    seq,
                    attempt: Attempt::first(),
                },
                vec![3, 4],
            ),
            (HistoryState::Timer { seq }, vec![8]),
            (
                HistoryState::Completed(EventPayload::new(b"account-created".to_vec())),
                vec![],
            ),
            (
                HistoryState::Failed(WorkflowErrorRecord::new("signup refused")),
                vec![],
            ),
        ];
        for (state, accepted) in states {
            for (index, event) in signup_events().iter().enumerate() {
                let result = state.clone().advance(event, EventOffset::from_index(4));
                assert_eq!(
                    result.is_ok(),
                    accepted.contains(&index),
                    "state {state:?}, event {event:?}, result {result:?}"
                );
            }
        }
    }

    #[test]
    fn parse_preserves_the_full_history_and_classifies_both_terminal_outcomes() {
        let events = signup_events();
        for (index, outcome) in [(9, "completed"), (10, "failed")] {
            let journal = Journal::new(vec![events[0].clone(), events[index].clone()]);
            let history = ValidatedHistory::parse(journal.clone()).unwrap();
            assert_eq!(history.events(), journal.events());
            match history.into_recovery() {
                RecoveryHistory::Completed(output) => {
                    assert_eq!(outcome, "completed");
                    assert_eq!(output.as_bytes(), b"account-created");
                }
                RecoveryHistory::Failed(error) => {
                    assert_eq!(outcome, "failed");
                    assert_eq!(error.message(), "signup refused");
                }
                RecoveryHistory::Running(_) => panic!("terminal history must not run"),
            }
        }
    }

    #[test]
    fn parse_running_history_retains_all_command_followers() {
        let events = signup_events();
        let journal = Journal::new(events[..4].to_vec());
        let history = ValidatedHistory::parse(journal.clone()).unwrap();
        match history.into_recovery() {
            RecoveryHistory::Running(replay) => assert_eq!(replay.events(), journal.events()),
            RecoveryHistory::Completed(_) | RecoveryHistory::Failed(_) => {
                panic!("running history must not be terminal")
            }
        }
    }

    #[test]
    fn parse_position_mismatch_reports_both_command_positions_and_event_offset() {
        let mut events = signup_events()[..2].to_vec();
        let recorded = Seq::zero().next().unwrap();
        events[1] = JournalEvent::StepScheduled {
            seq: recorded,
            name: StepName::new("charge-payment").unwrap(),
        };

        let result = ValidatedHistory::parse(Journal::new(events));

        assert_eq!(
            result.unwrap_err(),
            HistoryError::PositionMismatch {
                offset: EventOffset::from_index(1),
                expected: Seq::zero(),
                recorded,
            }
        );
    }

    #[test]
    fn parse_attempt_mismatch_reports_both_attempts_and_event_offset() {
        let mut events = signup_events()[..3].to_vec();
        let recorded = Attempt::new(2).unwrap();
        events[2] = JournalEvent::StepStarted {
            seq: Seq::zero(),
            attempt: recorded,
        };

        let result = ValidatedHistory::parse(Journal::new(events));

        assert_eq!(
            result.unwrap_err(),
            HistoryError::AttemptMismatch {
                offset: EventOffset::from_index(2),
                expected: Attempt::first(),
                recorded,
            }
        );
    }

    #[test]
    fn scheduling_at_the_maximum_sequence_refuses_an_unresumable_pending_command() {
        let seq = Seq::from_record(u64::MAX);
        let offset = EventOffset::from_index(4);
        let events = [
            JournalEvent::StepScheduled {
                seq,
                name: StepName::new("charge-payment").unwrap(),
            },
            JournalEvent::TimerScheduled {
                seq,
                deadline: Deadline::at(Timestamp::from_millis_since_epoch(0)),
            },
        ];

        for event in events {
            let result = HistoryState::Ready { seq }.advance(&event, offset);

            assert_eq!(
                result.unwrap_err(),
                HistoryError::SequenceOverflow { offset }
            );
        }
    }

    #[test]
    fn an_interrupted_maximum_attempt_is_not_resumable() {
        let offset = EventOffset::from_index(4);
        let state = HistoryState::Started {
            seq: Seq::zero(),
            attempt: Attempt::new(u32::MAX).unwrap(),
        };

        let result = state.ensure_resumable(offset);

        assert_eq!(result, Err(HistoryError::AttemptOverflow { offset }));
    }

    #[test]
    fn a_settled_maximum_attempt_needs_no_attempt_successor() {
        let seq = Seq::zero();
        let offset = EventOffset::from_index(4);
        let attempt = Attempt::new(u32::MAX).unwrap();
        let state = HistoryState::Started { seq, attempt };
        let events = [
            JournalEvent::StepCompleted {
                seq,
                result: EventPayload::new(b"charged".to_vec()),
            },
            JournalEvent::StepFailed {
                seq,
                attempt,
                error: StepErrorRecord::new("payment gateway timed out"),
            },
        ];

        for event in events {
            let settled = state.clone().advance(&event, offset).unwrap();

            assert_eq!(settled.ensure_resumable(offset), Ok(()));
        }
    }

    #[test]
    fn transitions_at_the_maximum_sequence_return_typed_overflow() {
        let seq = Seq::from_record(u64::MAX);
        let offset = EventOffset::from_index(4);
        let state = HistoryState::Ready { seq };
        let event = JournalEvent::NowRecorded {
            seq,
            value: Timestamp::from_millis_since_epoch(0),
        };

        let result = state.advance(&event, offset);

        assert_eq!(
            result.unwrap_err(),
            HistoryError::SequenceOverflow { offset }
        );
    }

    #[test]
    fn transitions_after_the_maximum_attempt_return_typed_overflow() {
        let seq = Seq::zero();
        let offset = EventOffset::from_index(4);
        let attempt = Attempt::new(u32::MAX).unwrap();
        let state = HistoryState::Started { seq, attempt };
        let event = JournalEvent::StepStarted { seq, attempt };

        let result = state.advance(&event, offset);

        assert_eq!(
            result.unwrap_err(),
            HistoryError::AttemptOverflow { offset }
        );
    }
}
