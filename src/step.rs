//! Step identity, retry attempts, and per-attempt outcomes.

use thiserror::Error;

use crate::execution::ExecutionId;
use crate::journal::{EventPayload, Seq};

/// Non-empty, trimmed, `[a-z0-9-]` step name. Position-stable across replays.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StepName(String);

/// Reasons a `StepName` constructor rejects its input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum StepNameError {
    #[error("step name is empty")]
    Empty,
    #[error("step name {raw:?} contains characters outside [a-z0-9-]")]
    InvalidCharacters { raw: String },
}

impl StepName {
    /// Returns `Err` if `raw` is empty after trimming, or contains characters
    /// outside `[a-z0-9-]`.
    pub fn new(raw: impl Into<String>) -> Result<Self, StepNameError> {
        let trimmed = raw.into().trim().to_owned();
        if trimmed.is_empty() {
            return Err(StepNameError::Empty);
        }
        let is_valid = trimmed
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if !is_valid {
            return Err(StepNameError::InvalidCharacters { raw: trimmed });
        }
        Ok(Self(trimmed))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 1-based retry attempt counter for a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Attempt(u32);

/// Reasons an `Attempt` constructor rejects its input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AttemptError {
    #[error("attempt number must be >= 1, got 0")]
    Zero,
}

impl Attempt {
    /// The first attempt of a step.
    pub fn first() -> Self {
        Self(1)
    }

    /// Returns `Err` if `n` is zero. Attempts are 1-based.
    pub fn new(n: u32) -> Result<Self, AttemptError> {
        if n == 0 {
            return Err(AttemptError::Zero);
        }
        Ok(Self(n))
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

/// Captured failure from a step attempt, durable across replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepErrorRecord {
    message: String,
}

impl StepErrorRecord {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Outcome of running a step's closure once, before it is journaled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    Completed(EventPayload),
    Failed(StepErrorRecord),
}

/// `(ExecutionId, Seq)`, handed to every step closure so external calls can
/// deduplicate. The honest answer to non-atomic journal+effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    execution: ExecutionId,
    seq: Seq,
}

impl IdempotencyKey {
    pub fn new(execution: ExecutionId, seq: Seq) -> Self {
        Self { execution, seq }
    }

    pub fn execution(&self) -> ExecutionId {
        self.execution
    }

    pub fn seq(&self) -> Seq {
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::random::{RandomBytes, RngSource};

    fn charge_card_bytes() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xC4;
        bytes
    }

    /// Test double returning the same bytes every draw.
    struct FixedRng {
        bytes: [u8; 32],
    }

    impl RngSource for FixedRng {
        fn next_bytes(&mut self) -> RandomBytes {
            RandomBytes::new(self.bytes)
        }
    }

    #[test]
    fn step_name_new_accepts_trimmed_lowercase_hyphenated_name() {
        let name = StepName::new("  charge-card  ").unwrap();

        assert_eq!(name.as_str(), "charge-card");
    }

    #[test]
    fn step_name_new_rejects_empty_after_trim() {
        let result = StepName::new("   ");

        assert_eq!(result, Err(StepNameError::Empty));
    }

    #[test]
    fn step_name_new_rejects_uppercase_characters() {
        let result = StepName::new("ChargeCard");

        assert_eq!(
            result,
            Err(StepNameError::InvalidCharacters {
                raw: "ChargeCard".to_owned()
            })
        );
    }

    #[test]
    fn attempt_first_is_one() {
        assert_eq!(Attempt::first().get(), 1);
    }

    #[test]
    fn attempt_new_rejects_zero() {
        let result = Attempt::new(0);

        assert_eq!(result, Err(AttemptError::Zero));
    }

    #[test]
    fn attempt_new_accepts_positive_value() {
        let attempt = Attempt::new(3).unwrap();

        assert_eq!(attempt.get(), 3);
    }

    #[test]
    fn attempt_next_increments() {
        let first = Attempt::first();

        let second = first.next();

        assert_eq!(second.get(), 2);
    }

    #[test]
    fn step_error_record_new_captures_message() {
        let error = StepErrorRecord::new("payment gateway timed out");

        assert_eq!(error.message(), "payment gateway timed out");
    }

    #[test]
    fn idempotency_key_new_pairs_execution_and_seq() {
        let mut rng = FixedRng {
            bytes: charge_card_bytes(),
        };
        let execution = ExecutionId::generate(&mut rng);
        let seq = Seq::zero().next();

        let key = IdempotencyKey::new(execution, seq);

        assert_eq!(key.execution(), execution);
        assert_eq!(key.seq(), seq);
    }
}
