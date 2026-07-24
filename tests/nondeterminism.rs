//! Nondeterminism detection, exercised end to end through `Engine` rather
//! than raw `WorkflowCtx` (unit-level coverage for that already lives in
//! `context.rs`).
//!
//! **Negative scenario:** a workflow that branches on ambient time obtained
//! outside `ctx`, from a captured field and never from `ctx.now()`, diverges
//! from its own journal on recovery once that ambient time crosses the
//! deadline between the crashed run and the recovering one.
//!
//! **Positive control:** the same branching, but reading time through
//! `ctx.now()`, replays the journaled value and takes the original branch
//! even when the live clock on recovery reads a different, post-deadline
//! instant.

use yaoki::command::CommandKind;
use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::Execution;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::equivalence::DuplicateLast;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::Attempt;
use yaoki::step::StepError;
use yaoki::step::StepName;
use yaoki::stores::memory::MemoryJournal;
use yaoki::time::TestClock;
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
fn renewal_deadline_timestamp() -> Timestamp {
    Timestamp::from_millis_since_epoch(1_784_937_600_000)
}

/// 2026-07-20T00:00:00Z, before the deadline.
fn before_deadline_timestamp() -> Timestamp {
    Timestamp::from_millis_since_epoch(1_784_505_600_000)
}

/// 2026-07-30T00:00:00Z, after the deadline.
fn after_deadline_timestamp() -> Timestamp {
    Timestamp::from_millis_since_epoch(1_785_369_600_000)
}

fn charge_renewal_name() -> StepName {
    StepName::new("charge-renewal").unwrap()
}

fn charge_renewal_confirmation() -> EventPayload {
    EventPayload::new(br#"{"charge_id":"ch_2026_0724"}"#.to_vec())
}

fn skip_renewal_name() -> StepName {
    StepName::new("skip-renewal").unwrap()
}

fn skip_renewal_confirmation() -> EventPayload {
    EventPayload::new(br#"{"skipped":true}"#.to_vec())
}

fn send_receipt_name() -> StepName {
    StepName::new("send-receipt").unwrap()
}

fn send_receipt_confirmation() -> EventPayload {
    EventPayload::new(br#"{"receipt_sent":true}"#.to_vec())
}

/// Seeds a store with a crashed run's prefix: `ExecutionStarted` followed by
/// one fully-recorded `charge-renewal` step at `seq` 0, as if a prior
/// process read ambient time before the deadline, ran branch A's step, and
/// died before doing anything else.
fn seed_crashed_run_after_charge_renewal(store: &MemoryJournal, execution: ExecutionId) {
    Execution::new(store, execution)
        .start(
            renewal_workflow_name(),
            renewal_workflow_version(),
            renewal_input(),
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::StepScheduled {
                seq: Seq::zero(),
                name: charge_renewal_name(),
            },
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first(),
            },
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: charge_renewal_confirmation(),
            },
        )
        .unwrap();
}

/// Which command branch B issues at `seq` 0 once ambient time (read from a
/// captured field, never `ctx`) has crossed the deadline. These are the two
/// divergence axes the recovering run can hit against the journaled
/// `charge-renewal` `StepScheduled`.
enum PastDeadlineCommand {
    DifferentStepName,
    ReadNowInstead,
}

/// Branches on `ambient_now`, a field captured outside `ctx`. That is the
/// bug this test simulates. A correct workflow would read time through
/// `ctx.now()`
/// (see `TimeAwareRenewalWorkflow` below) so replay controls the branch.
struct AmbientTimeBranchWorkflow {
    ambient_now: Timestamp,
    past_deadline_command: PastDeadlineCommand,
}

impl Workflow<MemoryJournal> for AmbientTimeBranchWorkflow {
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
        if self.ambient_now < renewal_deadline_timestamp() {
            ctx.step(charge_renewal_name(), |_key| {
                Ok(charge_renewal_confirmation())
            })
        } else {
            match self.past_deadline_command {
                PastDeadlineCommand::DifferentStepName => {
                    ctx.step(skip_renewal_name(), |_key| Ok(skip_renewal_confirmation()))
                }
                PastDeadlineCommand::ReadNowInstead => {
                    ctx.now()?;
                    Ok(EventPayload::new(b"read-now".to_vec()))
                }
            }
        }
    }
}

/// Same branching, but time comes from `ctx.now()`, which is journaled, so
/// replay (not the live clock) decides the branch on recovery.
struct TimeAwareRenewalWorkflow;

impl Workflow<MemoryJournal> for TimeAwareRenewalWorkflow {
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
        let now = ctx.now()?;
        let charge = if now < renewal_deadline_timestamp() {
            ctx.step(charge_renewal_name(), |_key| {
                Ok(charge_renewal_confirmation())
            })?
        } else {
            ctx.step(skip_renewal_name(), |_key| Ok(skip_renewal_confirmation()))?
        };
        ctx.step(send_receipt_name(), |_key| Ok(send_receipt_confirmation()))?;
        Ok(charge)
    }
}

#[test]
fn recovery_branching_on_ambient_time_with_a_different_step_name_is_nondeterminism() {
    let store = MemoryJournal::new();
    let execution = renewal_execution();
    seed_crashed_run_after_charge_renewal(&store, execution);

    let engine = Engine::<_, DuplicateLast>::new(&store);
    let workflow = AmbientTimeBranchWorkflow {
        ambient_now: after_deadline_timestamp(),
        past_deadline_command: PastDeadlineCommand::DifferentStepName,
    };
    let unused_clock = TestClock::at(after_deadline_timestamp());

    let result = engine.recover_and_run(
        execution,
        &workflow,
        renewal_input(),
        &unused_clock,
        &mut unused_rng(),
    );

    match result {
        Err(RunError::Workflow(StepError::Engine(EngineError::Nondeterminism {
            seq,
            expected,
            got,
        }))) => {
            assert_eq!(seq, Seq::zero());
            assert_eq!(expected, CommandKind::RunStep);
            assert_eq!(got, CommandKind::RunStep);
        }
        other => panic!("expected Nondeterminism error, got {other:?}"),
    }
}

#[test]
fn recovery_branching_on_ambient_time_reading_now_instead_of_a_step_is_nondeterminism() {
    let store = MemoryJournal::new();
    let execution = renewal_execution();
    seed_crashed_run_after_charge_renewal(&store, execution);

    let engine = Engine::<_, DuplicateLast>::new(&store);
    let workflow = AmbientTimeBranchWorkflow {
        ambient_now: after_deadline_timestamp(),
        past_deadline_command: PastDeadlineCommand::ReadNowInstead,
    };
    let unused_clock = TestClock::at(after_deadline_timestamp());

    let result = engine.recover_and_run(
        execution,
        &workflow,
        renewal_input(),
        &unused_clock,
        &mut unused_rng(),
    );

    match result {
        Err(RunError::Workflow(StepError::Engine(EngineError::Nondeterminism {
            seq,
            expected,
            got,
        }))) => {
            assert_eq!(seq, Seq::zero());
            assert_eq!(expected, CommandKind::RunStep);
            assert_eq!(got, CommandKind::ReadNow);
        }
        other => panic!("expected Nondeterminism error, got {other:?}"),
    }
}

#[test]
fn recovery_branching_on_ctx_now_replays_the_journaled_time_and_completes_under_a_different_live_clock()
 {
    let store = MemoryJournal::new();
    let execution = renewal_execution();
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
            JournalEvent::NowRecorded {
                seq: Seq::zero(),
                value: before_deadline_timestamp(),
            },
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::StepScheduled {
                seq: Seq::zero().next(),
                name: charge_renewal_name(),
            },
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::StepStarted {
                seq: Seq::zero().next(),
                attempt: Attempt::first(),
            },
        )
        .unwrap();
    store
        .append(
            &execution,
            JournalEvent::StepCompleted {
                seq: Seq::zero().next(),
                result: charge_renewal_confirmation(),
            },
        )
        .unwrap();

    // Recovery's live clock reads a time past the deadline. If the branch
    // read this instead of the journaled `NowRecorded`, it would diverge
    // against the recorded `charge-renewal` step.
    let recovery_clock = TestClock::at(after_deadline_timestamp());
    let engine = Engine::<_, DuplicateLast>::new(&store);

    let result = engine.recover_and_run(
        execution,
        &TimeAwareRenewalWorkflow,
        renewal_input(),
        &recovery_clock,
        &mut unused_rng(),
    );

    assert_eq!(result.unwrap(), charge_renewal_confirmation());
    let journal = store.load(&execution).unwrap();
    assert!(matches!(
        journal.events().last(),
        Some(JournalEvent::ExecutionCompleted { .. })
    ));
    assert!(matches!(
        journal.events()[journal.len() - 2],
        JournalEvent::StepCompleted { ref result, .. } if *result == send_receipt_confirmation()
    ));
}
