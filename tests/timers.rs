//! Durable timer recovery, exercised end to end through `Engine` (unit-level
//! coverage for the cursor-level re-arm decision already lives in
//! `context.rs`).
//!
//! A process dies between `TimerScheduled` and `TimerFired`, mid-wait.
//! Recovery must not re-append `TimerScheduled`: it re-arms toward the
//! already-journaled deadline and journals `TimerFired` once the wait (a
//! no-op under `TestClock`-like clocks) returns, then continues the
//! workflow live.

use std::cell::RefCell;

use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::Execution;
use yaoki::engine::Workflow;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::StepError;
use yaoki::step::StepName;
use yaoki::stores::memory::MemoryJournal;
use yaoki::time::Clock;
use yaoki::time::Deadline;
use yaoki::time::Timestamp;

struct FixedRng {
    bytes: [u8; 32],
}

impl RngSource for FixedRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new(self.bytes)
    }
}

fn renewal_execution() -> ExecutionId {
    let mut bytes = [0u8; 32];
    bytes[0] = 0x52; // 'R', arbitrary deterministic marker
    let mut rng = FixedRng { bytes };
    ExecutionId::generate(&mut rng)
}

fn unused_rng() -> FixedRng {
    FixedRng { bytes: [0u8; 32] }
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

/// 2026-07-25T00:00:00Z, the subscription's renewal deadline.
fn renewal_deadline() -> Deadline {
    Deadline::at(Timestamp::from_millis_since_epoch(1_784_937_600_000))
}

fn charge_renewal_name() -> StepName {
    StepName::new("charge-renewal").unwrap()
}

fn charge_renewal_confirmation() -> EventPayload {
    EventPayload::new(br#"{"charge_id":"ch_2026_0724"}"#.to_vec())
}

/// Records the deadline it is told to wait toward, without ever blocking.
/// This lets the test assert recovery re-armed through the `Clock`, not by
/// re-scheduling a fresh timer.
struct RecordingClock {
    now: Timestamp,
    slept_until: RefCell<Option<Timestamp>>,
}

impl RecordingClock {
    fn at(now: Timestamp) -> Self {
        Self {
            now,
            slept_until: RefCell::new(None),
        }
    }
}

impl Clock for RecordingClock {
    fn now(&self) -> Timestamp {
        self.now
    }

    fn sleep_until(&self, deadline: Timestamp) {
        *self.slept_until.borrow_mut() = Some(deadline);
    }
}

/// Waits out `deadline` then charges the renewal. Neither call touches
/// ambient time or randomness directly, only through `ctx`.
struct RenewalTimerWorkflow {
    deadline: Deadline,
}

impl Workflow<MemoryJournal> for RenewalTimerWorkflow {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        renewal_workflow_name()
    }

    fn version(&self) -> WorkflowVersion {
        renewal_workflow_version()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, MemoryJournal>,
        _input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        ctx.sleep_until(self.deadline)?;
        ctx.step(charge_renewal_name(), |_key| {
            Ok(charge_renewal_confirmation())
        })
    }
}

#[test]
fn recovery_mid_timer_rearms_the_remainder_and_completes_the_workflow_live() {
    let store = MemoryJournal::new();
    let execution = renewal_execution();
    let deadline = renewal_deadline();

    // Simulate a crash between TimerScheduled and TimerFired: the prior
    // process died mid-wait, so the journal has the deadline but no fired
    // event.
    Execution::new(&store, execution)
        .start(
            renewal_workflow_name(),
            renewal_workflow_version(),
            renewal_input(),
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::TimerScheduled {
                seq: Seq::zero(),
                deadline,
            },
        )
        .unwrap();

    let recovery_clock = RecordingClock::at(Timestamp::from_millis_since_epoch(0));
    let engine = Engine::new(&store);
    let workflow = RenewalTimerWorkflow { deadline };

    let result = engine.recover_and_run(
        execution,
        &workflow,
        renewal_input(),
        &recovery_clock,
        &mut unused_rng(),
    );

    assert_eq!(result.unwrap(), charge_renewal_confirmation());
    assert_eq!(
        *recovery_clock.slept_until.borrow(),
        Some(deadline.timestamp())
    );

    let journal = store.load(&execution).unwrap();
    assert_eq!(
        journal.events(),
        &[
            JournalEvent::ExecutionStarted {
                workflow: renewal_workflow_name(),
                version: renewal_workflow_version(),
                input: renewal_input(),
            },
            JournalEvent::TimerScheduled {
                seq: Seq::zero(),
                deadline,
            },
            JournalEvent::TimerFired { seq: Seq::zero() },
            JournalEvent::StepScheduled {
                seq: Seq::zero().next(),
                name: charge_renewal_name(),
            },
            JournalEvent::StepStarted {
                seq: Seq::zero().next(),
                attempt: yaoki::step::Attempt::first(),
            },
            JournalEvent::StepCompleted {
                seq: Seq::zero().next(),
                result: charge_renewal_confirmation(),
            },
            JournalEvent::ExecutionCompleted {
                output: charge_renewal_confirmation(),
            },
        ]
    );
}
