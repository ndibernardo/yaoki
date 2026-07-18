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
}
