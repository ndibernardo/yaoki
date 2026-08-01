//! Replay acceptance: the engine is disposable, the journal is not. Each
//! test runs the signup workflow against a store, drops the engine, builds a
//! fresh one over the same store, and checks what the second engine did.
//!
//! The unit tests in `engine.rs` cover the same transitions one call at a
//! time. These assert the end-to-end claim: identical output, and every step
//! whose result is durable answers from the journal instead of running again.

use std::cell::RefCell;

use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::Workflow;
use yaoki::equivalence::DuplicateLast;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::failpoints::CrashOnce;
use yaoki::failpoints::CrashPoint;
use yaoki::journal::EventPayload;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
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

fn signup_execution() -> ExecutionId {
    let mut bytes = [0u8; 32];
    bytes[0] = 0x52; // 'R', arbitrary deterministic marker
    let mut rng = FixedRng { bytes };
    ExecutionId::generate(&mut rng)
}

/// `SignupWorkflow` reads neither `ctx.now()` nor `ctx.random()`; these
/// satisfy the signature of `run` and `recover_and_run`.
fn unused_rng() -> FixedRng {
    FixedRng { bytes: [0u8; 32] }
}

fn unused_clock() -> TestClock {
    TestClock::at(Timestamp::from_millis_since_epoch(1_784_937_600_000))
}

fn signup_name() -> WorkflowName {
    WorkflowName::new("signup").unwrap()
}

fn signup_version() -> WorkflowVersion {
    WorkflowVersion::new("2026.08.01").unwrap()
}

fn signup_input() -> EventPayload {
    EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
}

fn charge_payment() -> StepName {
    StepName::new("charge-payment").unwrap()
}

fn payment_receipt() -> EventPayload {
    EventPayload::new(br#"{"charge_id":"ch_2026_0801"}"#.to_vec())
}

fn create_account() -> StepName {
    StepName::new("create-account").unwrap()
}

fn account_created() -> EventPayload {
    EventPayload::new(br#"{"account_id":"acct_2026_0801"}"#.to_vec())
}

/// Names the steps whose closures actually ran, in order. A replayed step
/// answers from the journal and never appends here.
struct ExecutionLog(RefCell<Vec<StepName>>);

impl ExecutionLog {
    fn new() -> Self {
        Self(RefCell::new(Vec::new()))
    }

    fn record(&self, step: StepName) {
        self.0.borrow_mut().push(step);
    }

    fn into_names(self) -> Vec<StepName> {
        self.0.into_inner()
    }
}

/// `charge-payment` at seq 0, `create-account` at seq 1, output is the
/// account record.
struct SignupWorkflow<'a> {
    executed: &'a ExecutionLog,
}

impl Workflow<MemoryJournal> for SignupWorkflow<'_> {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        signup_name()
    }

    fn version(&self) -> WorkflowVersion {
        signup_version()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, MemoryJournal>,
        _input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        ctx.step(charge_payment(), |_key| {
            self.executed.record(charge_payment());
            Ok(payment_receipt())
        })?;
        ctx.step(create_account(), |_key| {
            self.executed.record(create_account());
            Ok(account_created())
        })
    }
}

/// Runs the workflow on a fresh engine over `store`, live or recovered, and
/// returns the output alongside the steps that really executed.
fn run_fresh_engine(store: &MemoryJournal, recover: bool) -> (EventPayload, Vec<StepName>) {
    let executed = ExecutionLog::new();
    let engine = Engine::<_, DuplicateLast>::new(store);
    let workflow = SignupWorkflow {
        executed: &executed,
    };
    let output = if recover {
        engine.recover_and_run(
            signup_execution(),
            &workflow,
            signup_input(),
            &unused_clock(),
            &mut unused_rng(),
        )
    } else {
        engine.run(
            signup_execution(),
            &workflow,
            signup_input(),
            &unused_clock(),
            &mut unused_rng(),
        )
    }
    .unwrap();
    (output, executed.into_names())
}

#[test]
fn a_fresh_engine_over_a_completed_journal_replays_every_step_and_reruns_none() {
    // One full run, then the engine is gone and only the store
    // survives.
    let store = MemoryJournal::new();
    let (first_output, first_executed) = run_fresh_engine(&store, false);
    assert_eq!(first_executed, vec![charge_payment(), create_account()]);

    let (second_output, second_executed) = run_fresh_engine(&store, true);

    assert_eq!(second_output, first_output);
    assert_eq!(second_executed, Vec::new());
}

#[test]
fn replaying_a_completed_journal_appends_no_further_events() {
    let store = MemoryJournal::new();
    let _first = run_fresh_engine(&store, false);
    let after_first: Vec<JournalEvent> = store.load(&signup_execution()).unwrap().events().to_vec();

    let _second = run_fresh_engine(&store, true);

    let after_second: Vec<JournalEvent> =
        store.load(&signup_execution()).unwrap().events().to_vec();
    assert_eq!(after_second, after_first);
    assert!(matches!(
        after_second.last(),
        Some(JournalEvent::ExecutionCompleted { .. })
    ));
}

#[test]
fn replay_is_idempotent_across_repeated_recoveries() {
    let store = MemoryJournal::new();
    let (first_output, _first_executed) = run_fresh_engine(&store, false);

    let outputs: Vec<EventPayload> = (0..3)
        .map(|_| {
            let (output, executed) = run_fresh_engine(&store, true);
            assert_eq!(executed, Vec::new());
            output
        })
        .collect();

    assert_eq!(outputs, vec![first_output; 3]);
}

#[test]
fn a_fresh_engine_over_a_partial_journal_replays_the_prefix_and_runs_the_rest_live() {
    // A journal holding charge-payment's durable result and nothing
    // more, the way a process death between the two steps would leave it.
    let store = MemoryJournal::new();
    let interrupted = ExecutionLog::new();
    let policy = CrashOnce::new(CrashPoint::AfterStepCompleted(Seq::zero()));
    let crashed = Engine::<_, DuplicateLast>::with_failpoints(&store, &policy).run(
        signup_execution(),
        &SignupWorkflow {
            executed: &interrupted,
        },
        signup_input(),
        &unused_clock(),
        &mut unused_rng(),
    );
    assert!(crashed.is_err());
    assert_eq!(interrupted.into_names(), vec![charge_payment()]);

    let (output, executed) = run_fresh_engine(&store, true);

    // Charge-payment came from the journal, only create-account ran.
    assert_eq!(executed, vec![create_account()]);
    assert_eq!(output, account_created());
    let events = store.load(&signup_execution()).unwrap().events().to_vec();
    let charge_completions = events
        .iter()
        .filter(
            |event| matches!(event, JournalEvent::StepCompleted { seq, .. } if *seq == Seq::zero()),
        )
        .count();
    assert_eq!(charge_completions, 1);
}
