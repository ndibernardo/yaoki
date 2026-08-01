//! Property tests for the recovery-equivalence ladder. Workflows of one to
//! eight steps are generated, crashed at a random window of a random step,
//! and recovered over the same store. Each property pins one rung of the
//! ladder: what the mode promises, and what it refuses to promise.

use std::cell::RefCell;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use proptest::prelude::*;
use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::equivalence::DuplicateLast;
use yaoki::equivalence::EffectTrace;
use yaoki::equivalence::Equivalence;
use yaoki::equivalence::ExactlyOnce;
use yaoki::equivalence::ReplayAll;
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
use yaoki::step::IdempotencyKey;
use yaoki::step::StepError;
use yaoki::step::StepName;
use yaoki::stores::file::FileJournal;
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

fn onboarding_execution() -> ExecutionId {
    let mut bytes = [0u8; 32];
    bytes[0] = 0x4F; // 'O', arbitrary deterministic marker
    let mut rng = FixedRng { bytes };
    ExecutionId::generate(&mut rng)
}

/// No generated workflow reads `ctx.random()`; this satisfies the signature.
fn unused_rng() -> FixedRng {
    FixedRng { bytes: [0u8; 32] }
}

/// 2026-08-01T00:00:00Z. No generated workflow reads `ctx.now()` either.
fn onboarding_clock() -> TestClock {
    TestClock::at(Timestamp::from_millis_since_epoch(1_784_937_600_000))
}

fn onboarding_name() -> WorkflowName {
    WorkflowName::new("onboarding").unwrap()
}

fn onboarding_version() -> WorkflowVersion {
    WorkflowVersion::new("2026.08.01").unwrap()
}

fn onboarding_input() -> EventPayload {
    EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
}

/// The steps generated workflows are drawn from. Names are distinct, so a
/// deduplicated trace can still be compared position by position.
fn step_pool() -> Vec<StepName> {
    [
        "reserve-inventory",
        "charge-card",
        "create-account",
        "provision-mailbox",
        "grant-trial",
        "send-welcome-email",
        "notify-crm",
        "issue-receipt",
    ]
    .into_iter()
    .map(|name| StepName::new(name).unwrap())
    .collect()
}

/// Each step's result payload, derived from its name so the effect record
/// identifies which step produced it.
fn confirmation_of(step: &StepName) -> EventPayload {
    EventPayload::new(format!(r#"{{"step":"{}","status":"done"}}"#, step.as_str()).into_bytes())
}

/// The `Seq` of the `index`-th command. `Seq::from_index` is crate-private,
/// so positions are reached by stepping from zero.
fn seq_at(index: usize) -> Seq {
    (0..index).fold(Seq::zero(), |seq, _| seq.next())
}

/// Runs its steps in order, recording every execution of every step body in
/// the shared trace. The trace therefore counts executions, not journal
/// entries: a step that reruns after a crash appears twice.
struct GeneratedWorkflow<'a> {
    steps: Vec<StepName>,
    effects: &'a RefCell<EffectTrace>,
}

impl<S: JournalStore> Workflow<S> for GeneratedWorkflow<'_> {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        onboarding_name()
    }

    fn version(&self) -> WorkflowVersion {
        onboarding_version()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, S>,
        _input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        self.steps
            .iter()
            .try_fold(EventPayload::new(Vec::new()), |_previous, step| {
                ctx.step(step.clone(), |_key| {
                    self.effects
                        .borrow_mut()
                        .record(step.clone(), confirmation_of(step));
                    Ok(confirmation_of(step))
                })
            })
    }
}

/// External system that applies an effect at most once per idempotency key,
/// the way a payment API deduplicates a retried charge. `ReplayAll` is only
/// legal for step bodies that end in a sink like this one.
struct IdempotentSink {
    applied: RefCell<HashSet<IdempotencyKey>>,
}

impl IdempotentSink {
    fn new() -> Self {
        Self {
            applied: RefCell::new(HashSet::new()),
        }
    }

    fn apply(&self, key: IdempotencyKey) {
        self.applied.borrow_mut().insert(key);
    }

    fn applied_count(&self) -> usize {
        self.applied.borrow().len()
    }
}

/// `GeneratedWorkflow` whose steps also hit an idempotent external system.
struct IdempotentWorkflow<'a> {
    steps: Vec<StepName>,
    effects: &'a RefCell<EffectTrace>,
    sink: &'a IdempotentSink,
}

impl<S: JournalStore> Workflow<S> for IdempotentWorkflow<'_> {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        onboarding_name()
    }

    fn version(&self) -> WorkflowVersion {
        onboarding_version()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, S>,
        _input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        self.steps
            .iter()
            .try_fold(EventPayload::new(Vec::new()), |_previous, step| {
                ctx.step(step.clone(), |key| {
                    self.effects
                        .borrow_mut()
                        .record(step.clone(), confirmation_of(step));
                    self.sink.apply(key);
                    Ok(confirmation_of(step))
                })
            })
    }
}

/// The failure-free effect sequence a crashed run is compared against, plus
/// the output that run produced.
struct Reference {
    effects: EffectTrace,
    output: EventPayload,
}

fn reference_run(steps: &[StepName]) -> Reference {
    let store = MemoryJournal::new();
    let effects = RefCell::new(EffectTrace::new());
    let output = Engine::<_, DuplicateLast>::new(&store)
        .run(
            onboarding_execution(),
            &GeneratedWorkflow {
                steps: steps.to_vec(),
                effects: &effects,
            },
            onboarding_input(),
            &onboarding_clock(),
            &mut unused_rng(),
        )
        .unwrap();
    Reference {
        effects: effects.into_inner(),
        output,
    }
}

/// Runs `steps` with a failpoint armed at `point`, then recovers over the
/// same store with a fresh, never-crashing engine. Returns every effect both
/// runs performed and the recovered output.
fn crash_then_recover<S: JournalStore>(
    store: &S,
    steps: &[StepName],
    point: CrashPoint,
) -> Result<(EffectTrace, EventPayload), TestCaseError> {
    let effects = RefCell::new(EffectTrace::new());
    let execution = onboarding_execution();
    let policy = CrashOnce::new(point);

    let crashed = Engine::<_, DuplicateLast>::with_failpoints(store, &policy).run(
        execution,
        &GeneratedWorkflow {
            steps: steps.to_vec(),
            effects: &effects,
        },
        onboarding_input(),
        &onboarding_clock(),
        &mut unused_rng(),
    );
    prop_assert!(
        matches!(
            crashed,
            Err(RunError::Engine(EngineError::InjectedCrash(crashed_at))) if crashed_at == point
        ),
        "expected InjectedCrash({point:?}), got {crashed:?}"
    );

    let output = Engine::<_, DuplicateLast>::new(store)
        .recover_and_run(
            execution,
            &GeneratedWorkflow {
                steps: steps.to_vec(),
                effects: &effects,
            },
            onboarding_input(),
            &onboarding_clock(),
            &mut unused_rng(),
        )
        .unwrap();

    Ok((effects.into_inner(), output))
}

/// A scratch directory for one `FileJournal` case, removed on drop so a
/// 32-case property run does not leave 32 journals behind.
struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let case = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "yaoki-equivalence-properties-{}-{case}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("scratch dir must be creatable");
        Self { path }
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn workflow_steps() -> impl Strategy<Value = Vec<StepName>> {
    proptest::sample::subsequence(step_pool(), 1..=step_pool().len())
}

/// Every crash window a stepping workflow can reach. The timer windows need
/// a `sleep_until`, which generated workflows do not issue.
fn crash_point_at(seq: Seq) -> impl Strategy<Value = CrashPoint> {
    prop_oneof![
        Just(CrashPoint::BeforeStepScheduled(seq)),
        Just(CrashPoint::AfterStepScheduled(seq)),
        Just(CrashPoint::AfterStepStarted(seq)),
        Just(CrashPoint::AfterSideEffect(seq)),
        Just(CrashPoint::AfterStepCompleted(seq)),
    ]
}

fn steps_and_crash_point() -> impl Strategy<Value = (Vec<StepName>, CrashPoint)> {
    workflow_steps().prop_flat_map(|steps| {
        let count = steps.len();
        (
            Just(steps),
            (0..count).prop_flat_map(|index| crash_point_at(seq_at(index))),
        )
    })
}

/// The side-effect window alone: the only window that costs a duplicate.
fn steps_and_side_effect_crash() -> impl Strategy<Value = (Vec<StepName>, CrashPoint)> {
    workflow_steps().prop_flat_map(|steps| {
        let count = steps.len();
        (
            Just(steps),
            (0..count).prop_map(|index| CrashPoint::AfterSideEffect(seq_at(index))),
        )
    })
}

proptest! {
    #[test]
    fn duplicate_last_holds_after_a_crash_at_any_window_of_any_step(
        (steps, point) in steps_and_crash_point()
    ) {
        let reference = reference_run(&steps);
        let store = MemoryJournal::new();

        let (observed, output) = crash_then_recover(&store, &steps, point)?;

        prop_assert!(
            DuplicateLast::equivalent(&observed, &reference.effects),
            "crash at {point:?} produced {observed:?}, not a DuplicateLast \
             extension of {:?}",
            reference.effects
        );
        prop_assert_eq!(output, reference.output);
    }

    #[test]
    fn every_window_except_the_side_effect_one_costs_no_duplicate(
        (steps, point) in steps_and_crash_point()
    ) {
        let reference = reference_run(&steps);
        let store = MemoryJournal::new();

        let (observed, _output) = crash_then_recover(&store, &steps, point)?;

        // The side effect landed without its StepCompleted only in that one
        // window; every other window either had not run the closure yet or
        // had already made its result durable.
        let expected_extra = usize::from(matches!(point, CrashPoint::AfterSideEffect(_)));
        prop_assert_eq!(
            observed.records().len(),
            reference.effects.records().len() + expected_extra
        );
    }

    #[test]
    fn replay_all_holds_when_recovery_reruns_the_whole_workflow(
        steps in workflow_steps()
    ) {
        // No journal survives between the two runs, so the second engine
        // replays nothing and re-executes every step: the ReplayAll rung.
        let reference = reference_run(&steps);
        let effects = RefCell::new(EffectTrace::new());
        let sink = IdempotentSink::new();
        let first_store = MemoryJournal::new();
        let second_store = MemoryJournal::new();

        for store in [&first_store, &second_store] {
            Engine::<_, ReplayAll>::new(store)
                .run(
                    onboarding_execution(),
                    &IdempotentWorkflow {
                        steps: steps.clone(),
                        effects: &effects,
                        sink: &sink,
                    },
                    onboarding_input(),
                    &onboarding_clock(),
                    &mut unused_rng(),
                )
                .unwrap();
        }

        let observed = effects.into_inner();
        prop_assert!(ReplayAll::equivalent(&observed, &reference.effects));
        prop_assert_eq!(observed.records().len(), 2 * steps.len());
        // Idempotency keys are (execution, seq): the rerun reuses them, so
        // the external system applied each effect once.
        prop_assert_eq!(sink.applied_count(), steps.len());
    }

    #[test]
    fn replay_all_accepts_reruns_that_duplicate_last_rejects(
        steps in workflow_steps().prop_filter(
            "a single-step rerun is indistinguishable from one duplicate",
            |steps| steps.len() >= 2,
        )
    ) {
        let reference = reference_run(&steps);
        let effects = RefCell::new(EffectTrace::new());
        let sink = IdempotentSink::new();
        let first_store = MemoryJournal::new();
        let second_store = MemoryJournal::new();

        for store in [&first_store, &second_store] {
            Engine::<_, ReplayAll>::new(store)
                .run(
                    onboarding_execution(),
                    &IdempotentWorkflow {
                        steps: steps.clone(),
                        effects: &effects,
                        sink: &sink,
                    },
                    onboarding_input(),
                    &onboarding_clock(),
                    &mut unused_rng(),
                )
                .unwrap();
        }

        // The ladder is strict: a full rerun is more than one duplicate.
        let observed = effects.into_inner();
        prop_assert!(ReplayAll::equivalent(&observed, &reference.effects));
        prop_assert!(!DuplicateLast::equivalent(&observed, &reference.effects));
    }

    #[test]
    fn replaying_a_completed_journal_reruns_nothing_and_changes_nothing(
        steps in workflow_steps()
    ) {
        let store = MemoryJournal::new();
        let execution = onboarding_execution();
        let first_effects = RefCell::new(EffectTrace::new());
        let first_output = Engine::<_, DuplicateLast>::new(&store)
            .run(
                execution,
                &GeneratedWorkflow { steps: steps.clone(), effects: &first_effects },
                onboarding_input(),
                &onboarding_clock(),
                &mut unused_rng(),
            )
            .unwrap();
        let journal_after_first: Vec<JournalEvent> =
            store.load(&execution).unwrap().events().to_vec();

        let replay_effects = RefCell::new(EffectTrace::new());
        let outputs: Vec<EventPayload> = (0..2)
            .map(|_| {
                Engine::<_, DuplicateLast>::new(&store)
                    .recover_and_run(
                        execution,
                        &GeneratedWorkflow {
                            steps: steps.clone(),
                            effects: &replay_effects,
                        },
                        onboarding_input(),
                        &onboarding_clock(),
                        &mut unused_rng(),
                    )
                    .unwrap()
            })
            .collect();

        prop_assert_eq!(&outputs, &vec![first_output.clone(), first_output]);
        prop_assert_eq!(replay_effects.into_inner(), EffectTrace::new());
        prop_assert_eq!(
            store.load(&execution).unwrap().events().to_vec(),
            journal_after_first
        );
        prop_assert_eq!(first_effects.into_inner().records().len(), steps.len());
    }

}

// Each case of the property below writes a journal to disk; 32 cases cover
// the windows without turning the suite into an I/O benchmark.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn exactly_once_fails_after_a_side_effect_crash_on_a_non_transactional_store(
        (steps, point) in steps_and_side_effect_crash()
    ) {
        // FileJournal is not a TransactionalBoundary: the effect and its
        // journal record commit separately, so this window duplicates the
        // effect and no comparison can undo it afterwards.
        let reference = reference_run(&steps);
        let scratch = ScratchDir::new();
        let store = FileJournal::new(&scratch.path).unwrap();

        let (observed, output) = crash_then_recover(&store, &steps, point)?;

        prop_assert!(
            !ExactlyOnce::equivalent(&observed, &reference.effects),
            "ExactlyOnce must not hold for a crash at {point:?}"
        );
        prop_assert!(DuplicateLast::equivalent(&observed, &reference.effects));
        prop_assert_eq!(output, reference.output);
    }
}
