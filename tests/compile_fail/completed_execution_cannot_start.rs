//! Terminal states have no transitions: `Execution<Completed>` has no
//! `start` method (only `Execution<Created>` does). This must fail to
//! compile, not at runtime.

use yaoki::engine::Execution;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::stores::memory::MemoryJournal;

struct FixedRng {
    bytes: [u8; 32],
}

impl RngSource for FixedRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new(self.bytes)
    }
}

fn renewal_workflow_name() -> WorkflowName {
    WorkflowName::new("subscription-renewal").unwrap()
}

fn renewal_workflow_version() -> WorkflowVersion {
    WorkflowVersion::new("2026.07.24").unwrap()
}

fn renewal_input() -> EventPayload {
    EventPayload::new(br#"{"subscription_id":"sub_2026_0724"}"#.to_vec())
}

fn main() {
    let store = MemoryJournal::new();
    let mut rng = FixedRng { bytes: [0u8; 32] };
    let execution = ExecutionId::generate(&mut rng);

    let running = Execution::new(&store, execution)
        .start(renewal_workflow_name(), renewal_workflow_version(), renewal_input())
        .unwrap();
    let completed = running
        .complete(EventPayload::new(br#"{"charge_id":"ch_2026_0724"}"#.to_vec()))
        .unwrap();

    let _ = completed.start(renewal_workflow_name(), renewal_workflow_version(), renewal_input());
}
