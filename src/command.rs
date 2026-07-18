//! The closed vocabulary of effects a workflow may request. Divergence
//! detection compares `Command` at position n against the journaled event
//! kind at position n.

use crate::step::StepName;
use crate::time::Deadline;

/// Everything a workflow can do besides pure computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    RunStep { name: StepName },
    ReadNow,
    DrawRandom,
    Sleep { deadline: Deadline },
}

/// A `Command`'s variant, stripped of arguments. Reported by
/// `EngineError::Nondeterminism` when replay diverges from the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    RunStep,
    ReadNow,
    DrawRandom,
    Sleep,
}

impl Command {
    pub fn kind(&self) -> CommandKind {
        match self {
            Command::RunStep { .. } => CommandKind::RunStep,
            Command::ReadNow => CommandKind::ReadNow,
            Command::DrawRandom => CommandKind::DrawRandom,
            Command::Sleep { .. } => CommandKind::Sleep,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Timestamp;

    #[test]
    fn run_step_command_carries_the_step_name() {
        let name = StepName::new("charge-card").unwrap();

        let command = Command::RunStep { name: name.clone() };

        assert_eq!(command, Command::RunStep { name });
    }

    #[test]
    fn read_now_and_draw_random_are_distinct_commands() {
        assert_ne!(Command::ReadNow, Command::DrawRandom);
    }

    #[test]
    fn sleep_command_carries_the_deadline() {
        let deadline = Deadline::at(Timestamp::from_millis_since_epoch(1_753_401_600_000));

        let command = Command::Sleep { deadline };

        assert_eq!(command, Command::Sleep { deadline });
    }

    #[test]
    fn kind_maps_run_step_to_run_step_kind() {
        let name = StepName::new("charge-card").unwrap();

        assert_eq!(Command::RunStep { name }.kind(), CommandKind::RunStep);
    }

    #[test]
    fn kind_maps_read_now_to_read_now_kind() {
        assert_eq!(Command::ReadNow.kind(), CommandKind::ReadNow);
    }

    #[test]
    fn kind_maps_draw_random_to_draw_random_kind() {
        assert_eq!(Command::DrawRandom.kind(), CommandKind::DrawRandom);
    }

    #[test]
    fn kind_maps_sleep_to_sleep_kind() {
        let deadline = Deadline::at(Timestamp::from_millis_since_epoch(1_753_401_600_000));

        assert_eq!(Command::Sleep { deadline }.kind(), CommandKind::Sleep);
    }
}
