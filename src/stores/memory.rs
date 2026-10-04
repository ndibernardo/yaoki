//! In-memory journal storage. Each append locks the journal map; step
//! effects run separately and are not atomic with their completion records.
//! Journal contents do not survive process termination.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;

use crate::execution::ExecutionId;
use crate::journal::EventOffset;
use crate::journal::Journal;
use crate::journal::JournalError;
use crate::journal::JournalEvent;
use crate::journal::JournalStore;

/// `Mutex<HashMap<ExecutionId, Vec<JournalEvent>>>`. Mutex is justified:
/// genuinely shared mutable state across engine tasks.
#[derive(Debug, Default)]
pub struct MemoryJournal {
    executions: Mutex<HashMap<ExecutionId, Vec<JournalEvent>>>,
    // Ownership spans workflow calls, not just individual journal operations.
    owners: Mutex<HashSet<ExecutionId>>,
}

/// Exclusive ownership of one in-memory stream. Dropping it releases the ID.
pub struct MemoryLease<'a> {
    store: &'a MemoryJournal,
    id: ExecutionId,
}

impl Drop for MemoryLease<'_> {
    fn drop(&mut self) {
        let mut owners = match self.store.owners.lock() {
            Ok(owners) => owners,
            // Release ownership during unwinding without clearing the poison.
            Err(poisoned) => poisoned.into_inner(),
        };
        owners.remove(&self.id);
    }
}

impl MemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl JournalStore for MemoryJournal {
    type Lease<'a> = MemoryLease<'a>;

    fn acquire(&self, id: &ExecutionId) -> Result<MemoryLease<'_>, JournalError> {
        let mut owners = self.owners.lock().map_err(|_| JournalError::Poisoned)?;
        if !owners.insert(*id) {
            return Err(JournalError::ExecutionOwned { id: *id });
        }
        Ok(MemoryLease {
            store: self,
            id: *id,
        })
    }

    fn append(&self, id: &ExecutionId, event: JournalEvent) -> Result<EventOffset, JournalError> {
        let mut executions = self.executions.lock().map_err(|_| JournalError::Poisoned)?;
        let events = executions.entry(*id).or_default();
        let position = EventOffset::from_index(events.len());
        events.push(event);
        Ok(position)
    }

    fn load(&self, id: &ExecutionId) -> Result<Journal, JournalError> {
        let executions = self.executions.lock().map_err(|_| JournalError::Poisoned)?;
        let events = executions.get(id).cloned().unwrap_or_default();
        Ok(Journal::new(events))
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryJournal;
    use crate::execution::ExecutionId;
    use crate::execution::WorkflowName;
    use crate::execution::WorkflowVersion;
    use crate::journal::EventPayload;
    use crate::journal::JournalError;
    use crate::journal::JournalEvent;
    use crate::journal::JournalStore;
    use crate::journal::Seq;
    use crate::random::RandomBytes;
    use crate::random::RngSource;
    use crate::step::StepName;

    /// Test double returning the same bytes every draw.
    struct FixedRng {
        bytes: [u8; 32],
    }

    impl RngSource for FixedRng {
        fn next_bytes(&mut self) -> RandomBytes {
            RandomBytes::new(self.bytes)
        }
    }

    fn signup_execution() -> ExecutionId {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x51; // 'Q', arbitrary deterministic marker
        let mut rng = FixedRng { bytes };
        ExecutionId::generate(&mut rng)
    }

    fn renewal_execution() -> ExecutionId {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x52; // 'R', arbitrary deterministic marker
        let mut rng = FixedRng { bytes };
        ExecutionId::generate(&mut rng)
    }

    fn execution_started() -> JournalEvent {
        JournalEvent::ExecutionStarted {
            workflow: WorkflowName::new("signup").unwrap(),
            version: WorkflowVersion::new("2026.07.18").unwrap(),
            input: EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec()),
        }
    }

    fn step_scheduled(name: &str) -> JournalEvent {
        JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new(name).unwrap(),
        }
    }

    #[test]
    fn acquire_for_different_execution_ids_keeps_their_ownership_independent() {
        let store = MemoryJournal::new();
        let signup = store.acquire(&signup_execution()).unwrap();
        let renewal = store.acquire(&renewal_execution()).unwrap();

        assert_eq!(
            store.acquire(&signup_execution()).err(),
            Some(JournalError::ExecutionOwned {
                id: signup_execution()
            })
        );
        assert_eq!(
            store.acquire(&renewal_execution()).err(),
            Some(JournalError::ExecutionOwned {
                id: renewal_execution()
            })
        );
        drop(signup);
        assert!(store.acquire(&signup_execution()).is_ok());
        drop(renewal);
        assert!(store.acquire(&renewal_execution()).is_ok());
    }

    #[test]
    fn acquire_returns_poisoned_error_when_the_ownership_lock_is_poisoned() {
        let store = MemoryJournal::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.owners.lock().unwrap();
            panic!("simulated poisoning while holding the ownership lock");
        }));

        assert_eq!(
            store.acquire(&signup_execution()).err(),
            Some(JournalError::Poisoned)
        );
    }

    #[test]
    fn dropping_a_lease_releases_its_id_even_if_the_ownership_lock_is_poisoned() {
        let store = MemoryJournal::new();
        let lease = store.acquire(&signup_execution()).unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.owners.lock().unwrap();
            panic!("simulated poisoning after ownership acquisition");
        }));

        drop(lease);

        assert!(store.owners.lock().unwrap_err().into_inner().is_empty());
        assert_eq!(
            store.acquire(&signup_execution()).err(),
            Some(JournalError::Poisoned)
        );
    }

    #[test]
    fn load_on_unknown_execution_returns_empty_journal() {
        let store = MemoryJournal::new();
        let execution = signup_execution();

        let journal = store.load(&execution).unwrap();

        assert!(journal.is_empty());
    }

    #[test]
    fn append_then_load_preserves_order() {
        let store = MemoryJournal::new();
        let execution = signup_execution();

        store.append(&execution, execution_started()).unwrap();
        store
            .append(&execution, step_scheduled("charge-card"))
            .unwrap();
        let journal = store.load(&execution).unwrap();

        assert_eq!(
            journal.events(),
            &[execution_started(), step_scheduled("charge-card")]
        );
    }

    #[test]
    fn append_returns_the_zero_based_position_of_the_appended_event() {
        let store = MemoryJournal::new();
        let execution = signup_execution();

        let first_position = store.append(&execution, execution_started()).unwrap();
        let second_position = store
            .append(&execution, step_scheduled("charge-card"))
            .unwrap();

        assert_eq!(first_position.get(), 0);
        assert_eq!(second_position.get(), 1);
    }

    #[test]
    fn different_executions_have_independent_logs() {
        let store = MemoryJournal::new();
        let signup = signup_execution();
        let renewal = renewal_execution();

        store.append(&signup, execution_started()).unwrap();
        let renewal_journal = store.load(&renewal).unwrap();
        let signup_journal = store.load(&signup).unwrap();

        assert!(renewal_journal.is_empty());
        assert_eq!(signup_journal.len(), 1);
    }

    #[test]
    fn append_returns_poisoned_error_when_lock_is_poisoned() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.executions.lock().unwrap();
            panic!("simulated poisoning while holding the journal lock");
        }));

        let result = store.append(&execution, execution_started());

        assert_eq!(result, Err(JournalError::Poisoned));
    }

    #[test]
    fn load_returns_poisoned_error_when_lock_is_poisoned() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.executions.lock().unwrap();
            panic!("simulated poisoning while holding the journal lock");
        }));

        let result = store.load(&execution);

        assert_eq!(result, Err(JournalError::Poisoned));
    }
}
