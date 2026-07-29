//! Crash windows, end to end: one test per `CrashPoint`. Each test runs the
//! signup workflow under a `CrashOnce` policy, catches the injected crash,
//! builds a fresh engine over the same store, recovers, and compares the
//! effects the steps performed against a failure-free reference run.
//!
//! `DuplicateLast` holds for every window. `ExactlyOnce` holds for every
//! window except `AfterSideEffect`, where the effect outran its journal
//! record.

use std::cell::RefCell;

use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::equivalence::DuplicateLast;
use yaoki::equivalence::EffectTrace;
use yaoki::equivalence::Equivalence;
use yaoki::equivalence::ExactlyOnce;
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
use yaoki::time::Deadline;
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
    bytes[0] = 0x53; // 'S', arbitrary deterministic marker
    let mut rng = FixedRng { bytes };
    ExecutionId::generate(&mut rng)
}

/// Neither workflow here reads `ctx.random()`; this satisfies the signature.
fn unused_rng() -> FixedRng {
    FixedRng { bytes: [0u8; 32] }
}

fn signup_clock() -> TestClock {
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

fn charge_card() -> StepName {
    StepName::new("charge-card").unwrap()
}

fn charge_confirmation() -> EventPayload {
    EventPayload::new(br#"{"charge_id":"ch_2026_0801"}"#.to_vec())
}

fn create_account() -> StepName {
    StepName::new("create-account").unwrap()
}

fn account_created() -> EventPayload {
    EventPayload::new(br#"{"account_id":"acct_2026_0801"}"#.to_vec())
}

/// 2026-08-08T00:00:00Z. One week of trial before the card is charged.
fn trial_deadline() -> Deadline {
    Deadline::at(Timestamp::from_millis_since_epoch(1_785_542_400_000))
}

/// `charge-card` at seq 0, then `create-account` at seq 1. Each step appends
/// to the shared trace as it performs its effect, so the trace counts
/// executions rather than journal entries.
struct SignupWorkflow<'a> {
    effects: &'a RefCell<EffectTrace>,
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
        ctx.step(charge_card(), |_key| {
            self.effects
                .borrow_mut()
                .record(charge_card(), charge_confirmation());
            Ok(charge_confirmation())
        })?;
        ctx.step(create_account(), |_key| {
            self.effects
                .borrow_mut()
                .record(create_account(), account_created());
            Ok(account_created())
        })
    }
}

/// Waits out the trial, then charges the card. The timer windows need a
/// `sleep_until` ahead of a step.
struct TrialSignupWorkflow<'a> {
    effects: &'a RefCell<EffectTrace>,
}

impl Workflow<MemoryJournal> for TrialSignupWorkflow<'_> {
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
        ctx.sleep_until(trial_deadline())?;
        ctx.step(charge_card(), |_key| {
            self.effects
                .borrow_mut()
                .record(charge_card(), charge_confirmation());
            Ok(charge_confirmation())
        })
    }
}

/// The failure-free effect sequence every crashed run is compared against.
fn signup_reference_trace() -> EffectTrace {
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let engine = Engine::<_, DuplicateLast>::new(&store);
    engine
        .run(
            signup_execution(),
            &SignupWorkflow { effects: &effects },
            signup_input(),
            &signup_clock(),
            &mut unused_rng(),
        )
        .unwrap();
    effects.into_inner()
}

fn trial_reference_trace() -> EffectTrace {
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let engine = Engine::<_, DuplicateLast>::new(&store);
    engine
        .run(
            signup_execution(),
            &TrialSignupWorkflow { effects: &effects },
            signup_input(),
            &signup_clock(),
            &mut unused_rng(),
        )
        .unwrap();
    effects.into_inner()
}

/// What one crash-and-recover cycle produced: the effects both runs
/// performed, and the journal they left behind.
struct Recovered {
    effects: EffectTrace,
    journal: Vec<JournalEvent>,
    output: EventPayload,
}

/// Runs the signup workflow with a failpoint armed at `point`, asserts the
/// engine reported that exact crash, then recovers over the same store with
/// a fresh, never-crashing engine.
fn crash_then_recover(point: CrashPoint) -> Recovered {
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let execution = signup_execution();
    let policy = CrashOnce::new(point);

    let crashed = Engine::<_, DuplicateLast>::with_failpoints(&store, &policy).run(
        execution,
        &SignupWorkflow { effects: &effects },
        signup_input(),
        &signup_clock(),
        &mut unused_rng(),
    );
    assert!(
        matches!(
            crashed,
            Err(RunError::Engine(EngineError::InjectedCrash(crashed_at))) if crashed_at == point
        ),
        "expected InjectedCrash({point:?}), got {crashed:?}"
    );
    assert!(policy.has_fired());

    let output = Engine::<_, DuplicateLast>::new(&store)
        .recover_and_run(
            execution,
            &SignupWorkflow { effects: &effects },
            signup_input(),
            &signup_clock(),
            &mut unused_rng(),
        )
        .unwrap();

    Recovered {
        effects: effects.into_inner(),
        journal: store.load(&execution).unwrap().events().to_vec(),
        output,
    }
}

/// The timer-window twin of `crash_then_recover`, over `TrialSignupWorkflow`.
fn crash_then_recover_trial(point: CrashPoint) -> Recovered {
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let execution = signup_execution();
    let policy = CrashOnce::new(point);

    let crashed = Engine::<_, DuplicateLast>::with_failpoints(&store, &policy).run(
        execution,
        &TrialSignupWorkflow { effects: &effects },
        signup_input(),
        &signup_clock(),
        &mut unused_rng(),
    );
    assert!(
        matches!(
            crashed,
            Err(RunError::Engine(EngineError::InjectedCrash(crashed_at))) if crashed_at == point
        ),
        "expected InjectedCrash({point:?}), got {crashed:?}"
    );

    let output = Engine::<_, DuplicateLast>::new(&store)
        .recover_and_run(
            execution,
            &TrialSignupWorkflow { effects: &effects },
            signup_input(),
            &signup_clock(),
            &mut unused_rng(),
        )
        .unwrap();

    Recovered {
        effects: effects.into_inner(),
        journal: store.load(&execution).unwrap().events().to_vec(),
        output,
    }
}

fn count_events(journal: &[JournalEvent], name: &StepName) -> usize {
    journal
        .iter()
        .filter(|event| matches!(event, JournalEvent::StepScheduled { name: n, .. } if n == name))
        .count()
}

#[test]
fn crash_before_step_scheduled_reruns_the_step_with_no_extra_effect() {
    let reference = signup_reference_trace();

    let recovered = crash_then_recover(CrashPoint::BeforeStepScheduled(Seq::zero()));

    // Nothing was journaled for charge-card and nothing was charged, so
    // recovery is indistinguishable from a first run.
    assert_eq!(recovered.effects, reference);
    assert!(ExactlyOnce::equivalent(&recovered.effects, &reference));
    assert_eq!(recovered.output, account_created());
    assert!(matches!(
        recovered.journal.last(),
        Some(JournalEvent::ExecutionCompleted { .. })
    ));
}

#[test]
fn crash_after_step_scheduled_reruns_the_step_without_rescheduling_it() {
    let reference = signup_reference_trace();

    let recovered = crash_then_recover(CrashPoint::AfterStepScheduled(Seq::zero()));

    // The closure never ran before the crash: no duplicate effect.
    assert_eq!(recovered.effects, reference);
    assert!(ExactlyOnce::equivalent(&recovered.effects, &reference));
    // One StepScheduled per Seq, however many attempts follow.
    assert_eq!(count_events(&recovered.journal, &charge_card()), 1);
}

#[test]
fn crash_after_step_started_reruns_the_step_with_no_extra_effect() {
    let reference = signup_reference_trace();

    let recovered = crash_then_recover(CrashPoint::AfterStepStarted(Seq::zero()));

    // StepStarted was durable but the side effect had not landed yet.
    assert_eq!(recovered.effects, reference);
    assert!(ExactlyOnce::equivalent(&recovered.effects, &reference));
    assert_eq!(count_events(&recovered.journal, &charge_card()), 1);
    // The rerun is journaled as a second attempt of the same step.
    let started_attempts = recovered
        .journal
        .iter()
        .filter(
            |event| matches!(event, JournalEvent::StepStarted { seq, .. } if *seq == Seq::zero()),
        )
        .count();
    assert_eq!(started_attempts, 2);
}

#[test]
fn crash_after_the_side_effect_duplicates_exactly_that_effect() {
    // The card was charged, the journal did not record it, recovery charges
    // it again.
    let reference = signup_reference_trace();

    let recovered = crash_then_recover(CrashPoint::AfterSideEffect(Seq::zero()));

    assert_eq!(recovered.effects.records().len(), 3);
    assert!(DuplicateLast::equivalent(&recovered.effects, &reference));
    assert!(
        !ExactlyOnce::equivalent(&recovered.effects, &reference),
        "a non-atomic journal-plus-effect cannot deliver ExactlyOnce"
    );
}

#[test]
fn crash_after_step_completed_replays_the_step_without_rerunning_it() {
    let reference = signup_reference_trace();

    let recovered = crash_then_recover(CrashPoint::AfterStepCompleted(Seq::zero()));

    // charge-card's result was durable, so recovery answered from the
    // journal and only create-account ran.
    assert_eq!(recovered.effects, reference);
    assert!(ExactlyOnce::equivalent(&recovered.effects, &reference));
    assert_eq!(recovered.output, account_created());
}

#[test]
fn crash_after_the_side_effect_of_the_final_step_duplicates_the_trailing_effect() {
    let reference = signup_reference_trace();

    let recovered = crash_then_recover(CrashPoint::AfterSideEffect(Seq::zero().next()));

    assert_eq!(recovered.effects.records().len(), 3);
    assert!(DuplicateLast::equivalent(&recovered.effects, &reference));
    assert!(!ExactlyOnce::equivalent(&recovered.effects, &reference));
}

#[test]
fn crash_after_timer_scheduled_rearms_the_timer_without_rescheduling_it() {
    let reference = trial_reference_trace();

    let recovered = crash_then_recover_trial(CrashPoint::AfterTimerScheduled(Seq::zero()));

    // A durable deadline is not re-armed from scratch: exactly one
    // TimerScheduled survives, and the step after it still runs once.
    let scheduled = recovered
        .journal
        .iter()
        .filter(|event| matches!(event, JournalEvent::TimerScheduled { .. }))
        .count();
    assert_eq!(scheduled, 1);
    assert_eq!(recovered.effects, reference);
    assert!(ExactlyOnce::equivalent(&recovered.effects, &reference));
}

#[test]
fn crash_after_timer_fired_replays_the_timer_and_runs_the_remaining_step_once() {
    let reference = trial_reference_trace();

    let recovered = crash_then_recover_trial(CrashPoint::AfterTimerFired(Seq::zero()));

    let fired = recovered
        .journal
        .iter()
        .filter(|event| matches!(event, JournalEvent::TimerFired { .. }))
        .count();
    assert_eq!(fired, 1);
    assert_eq!(recovered.effects, reference);
    assert!(ExactlyOnce::equivalent(&recovered.effects, &reference));
    assert_eq!(recovered.output, charge_confirmation());
}

#[test]
fn a_crashed_run_journals_no_terminal_event() {
    // A crash is process death: the execution must stay recoverable, never
    // be sealed as completed or failed by the run that died.
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let execution = signup_execution();
    let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero()));

    let crashed = Engine::<_, DuplicateLast>::with_failpoints(&store, &policy).run(
        execution,
        &SignupWorkflow { effects: &effects },
        signup_input(),
        &signup_clock(),
        &mut unused_rng(),
    );

    assert!(matches!(
        crashed,
        Err(RunError::Engine(EngineError::InjectedCrash(_)))
    ));
    let journal = store.load(&execution).unwrap();
    assert!(
        !journal.events().iter().any(|event| matches!(
            event,
            JournalEvent::ExecutionCompleted { .. } | JournalEvent::ExecutionFailed { .. }
        )),
        "a crashed run sealed the execution: {:?}",
        journal.events()
    );
}

#[test]
fn two_crashes_in_the_same_step_accumulate_attempts_and_still_recover() {
    let reference = signup_reference_trace();
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let execution = signup_execution();

    // First crash: after StepStarted, before the charge.
    let first_policy = CrashOnce::new(CrashPoint::AfterStepStarted(Seq::zero()));
    let _first = Engine::<_, DuplicateLast>::with_failpoints(&store, &first_policy).run(
        execution,
        &SignupWorkflow { effects: &effects },
        signup_input(),
        &signup_clock(),
        &mut unused_rng(),
    );
    // Second crash: same window, on the recovery attempt.
    let second_policy = CrashOnce::new(CrashPoint::AfterStepStarted(Seq::zero()));
    let _second = Engine::<_, DuplicateLast>::with_failpoints(&store, &second_policy)
        .recover_and_run(
            execution,
            &SignupWorkflow { effects: &effects },
            signup_input(),
            &signup_clock(),
            &mut unused_rng(),
        );

    let output = Engine::<_, DuplicateLast>::new(&store)
        .recover_and_run(
            execution,
            &SignupWorkflow { effects: &effects },
            signup_input(),
            &signup_clock(),
            &mut unused_rng(),
        )
        .unwrap();

    assert_eq!(output, account_created());
    assert_eq!(effects.into_inner(), reference);
    let journal = store.load(&execution).unwrap();
    let started_attempts = journal
        .events()
        .iter()
        .filter(
            |event| matches!(event, JournalEvent::StepStarted { seq, .. } if *seq == Seq::zero()),
        )
        .count();
    assert_eq!(started_attempts, 3);
}
