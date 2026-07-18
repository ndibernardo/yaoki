//! Replay cursor and the engine's own error type. `Engine<S, E>` itself
//! (run/recover) arrives once `Execution<State>` exists to back it.

use thiserror::Error;

use crate::command::CommandKind;
use crate::journal::Journal;
use crate::journal::JournalError;
use crate::journal::JournalEvent;
use crate::journal::Seq;

/// Walks a loaded `Journal` command by command during replay.
#[derive(Debug, Clone)]
pub struct ReplayCursor {
    events: Vec<JournalEvent>,
    position: usize,
}

impl ReplayCursor {
    pub fn new(journal: Journal) -> Self {
        Self {
            events: journal.events().to_vec(),
            position: 0,
        }
    }

    /// True once every journaled event has been consumed.
    pub fn is_exhausted(&self) -> bool {
        self.position >= self.events.len()
    }

    /// The next unconsumed event, without advancing.
    pub fn peek(&self) -> Option<&JournalEvent> {
        self.events.get(self.position)
    }

    /// Consumes the event returned by the last `peek`.
    pub fn advance(&mut self) {
        self.position += 1;
    }
}

/// Errors from running or recovering an execution.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    #[error(
        "nondeterministic workflow at seq {seq:?}: journal has {expected:?}, code produced {got:?}"
    )]
    Nondeterminism {
        seq: Seq,
        expected: CommandKind,
        got: CommandKind,
    },

    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::WorkflowName;
    use crate::execution::WorkflowVersion;
    use crate::journal::EventPayload;
    use crate::step::StepName;

    fn signup_started() -> JournalEvent {
        JournalEvent::ExecutionStarted {
            workflow: WorkflowName::new("signup").unwrap(),
            version: WorkflowVersion::new("2026.07.18").unwrap(),
            input: EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec()),
        }
    }

    fn charge_card_scheduled() -> JournalEvent {
        JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new("charge-card").unwrap(),
        }
    }

    #[test]
    fn new_cursor_over_empty_journal_is_exhausted() {
        let cursor = ReplayCursor::new(Journal::empty());

        assert!(cursor.is_exhausted());
        assert_eq!(cursor.peek(), None);
    }

    #[test]
    fn new_cursor_over_nonempty_journal_is_not_exhausted() {
        let cursor = ReplayCursor::new(Journal::new(vec![signup_started()]));

        assert!(!cursor.is_exhausted());
    }

    #[test]
    fn peek_returns_the_event_at_the_current_position() {
        let cursor = ReplayCursor::new(Journal::new(vec![
            signup_started(),
            charge_card_scheduled(),
        ]));

        assert_eq!(cursor.peek(), Some(&signup_started()));
    }

    #[test]
    fn advance_moves_to_the_next_event() {
        let mut cursor = ReplayCursor::new(Journal::new(vec![
            signup_started(),
            charge_card_scheduled(),
        ]));

        cursor.advance();

        assert_eq!(cursor.peek(), Some(&charge_card_scheduled()));
    }

    #[test]
    fn advance_past_the_last_event_exhausts_the_cursor() {
        let mut cursor = ReplayCursor::new(Journal::new(vec![signup_started()]));

        cursor.advance();

        assert!(cursor.is_exhausted());
        assert_eq!(cursor.peek(), None);
    }
}
