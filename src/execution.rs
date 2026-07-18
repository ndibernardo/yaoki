//! Execution identity and versioning. The typestate lifecycle
//! (`Execution<State>`) lands once `JournalStore` exists to back its
//! transitions.

use thiserror::Error;

use crate::random::RngSource;

/// Unique identifier of one run of a workflow definition. 128 bits, drawn
/// through `RngSource`. No ambient randomness (`Uuid::new_v4` is banned by
/// `clippy.toml` `disallowed-methods`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecutionId([u8; 16]);

impl ExecutionId {
    /// Draws 16 bytes from `rng` to identify a new execution.
    pub fn generate(rng: &mut impl RngSource) -> Self {
        let drawn = rng.next_bytes();
        let mut id = [0u8; 16];
        id.copy_from_slice(&drawn.as_bytes()[..16]);
        Self(id)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Non-empty, trimmed name of a workflow definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowName(String);

/// Reasons a `WorkflowName` constructor rejects its input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkflowNameError {
    #[error("workflow name is empty")]
    Empty,
}

impl WorkflowName {
    pub fn new(raw: impl Into<String>) -> Result<Self, WorkflowNameError> {
        let trimmed = raw.into().trim().to_owned();
        if trimmed.is_empty() {
            return Err(WorkflowNameError::Empty);
        }
        Ok(Self(trimmed))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Version of the workflow definition recorded at start, checked at
/// recovery. A mismatch fails recovery with a typed error.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkflowVersion(String);

/// Reasons a `WorkflowVersion` constructor rejects its input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkflowVersionError {
    #[error("workflow version is empty")]
    Empty,
}

impl WorkflowVersion {
    pub fn new(raw: impl Into<String>) -> Result<Self, WorkflowVersionError> {
        let trimmed = raw.into().trim().to_owned();
        if trimmed.is_empty() {
            return Err(WorkflowVersionError::Empty);
        }
        Ok(Self(trimmed))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Captured failure that terminated an execution, durable across replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowErrorRecord {
    message: String,
}

impl WorkflowErrorRecord {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::random::RandomBytes;

    /// Test double returning the same bytes every draw.
    struct FixedRng {
        bytes: [u8; 32],
    }

    impl RngSource for FixedRng {
        fn next_bytes(&mut self) -> RandomBytes {
            RandomBytes::new(self.bytes)
        }
    }

    fn signup_execution_bytes() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x51; // 'Q', arbitrary deterministic marker
        bytes
    }

    #[test]
    fn execution_id_generate_uses_first_sixteen_bytes_of_the_draw() {
        let mut rng = FixedRng {
            bytes: signup_execution_bytes(),
        };

        let id = ExecutionId::generate(&mut rng);

        assert_eq!(id.as_bytes(), &signup_execution_bytes()[..16]);
    }

    #[test]
    fn execution_id_generate_is_deterministic_for_the_same_draw() {
        let mut rng_a = FixedRng {
            bytes: signup_execution_bytes(),
        };
        let mut rng_b = FixedRng {
            bytes: signup_execution_bytes(),
        };

        let id_a = ExecutionId::generate(&mut rng_a);
        let id_b = ExecutionId::generate(&mut rng_b);

        assert_eq!(id_a, id_b);
    }

    #[test]
    fn workflow_name_new_accepts_trimmed_name() {
        let name = WorkflowName::new("  signup  ").unwrap();

        assert_eq!(name.as_str(), "signup");
    }

    #[test]
    fn workflow_name_new_rejects_empty_after_trim() {
        let result = WorkflowName::new("   ");

        assert_eq!(result, Err(WorkflowNameError::Empty));
    }

    #[test]
    fn workflow_version_new_accepts_trimmed_version() {
        let version = WorkflowVersion::new("2026.07.18").unwrap();

        assert_eq!(version.as_str(), "2026.07.18");
    }

    #[test]
    fn workflow_version_new_rejects_empty_after_trim() {
        let result = WorkflowVersion::new("");

        assert_eq!(result, Err(WorkflowVersionError::Empty));
    }

    #[test]
    fn workflow_error_record_new_captures_message() {
        let error = WorkflowErrorRecord::new("account creation rolled back");

        assert_eq!(error.message(), "account creation rolled back");
    }
}
