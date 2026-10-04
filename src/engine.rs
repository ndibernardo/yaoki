//! Execution lifecycle typestate, workflow orchestration, and the engine's
//! own error type. `ReplayCursor` and `EngineError` live in `context.rs`
//! (see the note there); this module depends on `context.rs`, not the
//! other way around.

use std::marker::PhantomData;

use crate::context::EngineError;
use crate::context::ReplayCursor;
use crate::context::WorkflowCtx;
use crate::execution::ExecutionId;
use crate::execution::WorkflowErrorRecord;
use crate::execution::WorkflowName;
use crate::execution::WorkflowVersion;
use crate::failpoints::CrashStatus;
use crate::failpoints::FailpointPolicy;
use crate::failpoints::NeverCrash;
use crate::history::RecoveryHistory;
use crate::history::ValidatedHistory;
use crate::journal::EventPayload;
use crate::journal::Journal;
use crate::journal::JournalEvent;
use crate::journal::JournalStore;
use crate::random::RngSource;
use crate::time::Clock;

/// A handle that has not appended `ExecutionStarted`.
pub struct Created;
/// `ExecutionStarted` appended; workflow run in progress.
pub struct Running;
/// Terminal: `ExecutionCompleted` appended.
pub struct Completed;
/// Terminal: `ExecutionFailed` appended.
pub struct Failed;

/// One validated durable invocation. Fields cannot be constructed independently.
///
/// ```compile_fail
/// use yaoki::engine::Invocation;
/// use yaoki::journal::EventPayload;
///
/// fn replace_input(invocation: &mut Invocation) {
///     invocation.input = EventPayload::new(Vec::new());
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    id: ExecutionId,
    workflow: WorkflowName,
    version: WorkflowVersion,
    input: EventPayload,
}

impl Invocation {
    /// The execution whose start record supplied this invocation.
    pub fn id(&self) -> ExecutionId {
        self.id
    }

    /// The recorded workflow name.
    pub fn workflow(&self) -> &WorkflowName {
        &self.workflow
    }

    /// The recorded workflow version.
    pub fn version(&self) -> &WorkflowVersion {
        &self.version
    }

    /// The authoritative input bytes from the start record.
    pub fn input(&self) -> &EventPayload {
        &self.input
    }

    fn parse(id: ExecutionId, journal: &Journal) -> Result<Self, EngineError> {
        let invocation = match journal.events().first() {
            None => return Err(EngineError::MissingExecution { id }),
            Some(JournalEvent::ExecutionStarted {
                workflow,
                version,
                input,
            }) => Self {
                id,
                workflow: workflow.clone(),
                version: version.clone(),
                input: input.clone(),
            },
            Some(
                JournalEvent::StepScheduled { .. }
                | JournalEvent::StepStarted { .. }
                | JournalEvent::StepCompleted { .. }
                | JournalEvent::StepFailed { .. }
                | JournalEvent::NowRecorded { .. }
                | JournalEvent::RandomRecorded { .. }
                | JournalEvent::TimerScheduled { .. }
                | JournalEvent::TimerFired { .. }
                | JournalEvent::ExecutionCompleted { .. }
                | JournalEvent::ExecutionFailed { .. },
            ) => {
                return Err(EngineError::InvalidInvocation { id });
            }
        };
        if journal.events().iter().skip(1).any(is_execution_start) {
            return Err(EngineError::InvalidInvocation { id });
        }
        Ok(invocation)
    }

    fn select(&self, name: &WorkflowName, version: &WorkflowVersion) -> Result<(), EngineError> {
        if &self.workflow != name {
            return Err(EngineError::WorkflowMismatch {
                recorded: self.workflow.clone(),
                current: name.clone(),
            });
        }
        if &self.version != version {
            return Err(EngineError::VersionMismatch {
                recorded: self.version.clone(),
                current: version.clone(),
            });
        }
        Ok(())
    }
}

fn is_execution_start(event: &JournalEvent) -> bool {
    match event {
        JournalEvent::ExecutionStarted { .. } => true,
        JournalEvent::StepScheduled { .. }
        | JournalEvent::StepStarted { .. }
        | JournalEvent::StepCompleted { .. }
        | JournalEvent::StepFailed { .. }
        | JournalEvent::NowRecorded { .. }
        | JournalEvent::RandomRecorded { .. }
        | JournalEvent::TimerScheduled { .. }
        | JournalEvent::TimerFired { .. }
        | JournalEvent::ExecutionCompleted { .. }
        | JournalEvent::ExecutionFailed { .. } => false,
    }
}

/// An execution's identity, tagged with its lifecycle state at the type
/// level. Illegal transitions (completing a `Created` execution, resuming a
/// `Completed` one) do not compile. The handle owns the stream until dropped,
/// including while a terminal handle is retained.
pub struct Execution<'a, S: JournalStore, State> {
    store: &'a S,
    id: ExecutionId,
    lease: S::Lease<'a>,
    _state: PhantomData<State>,
}

impl<'a, S: JournalStore, State> Execution<'a, S, State> {
    pub fn id(&self) -> ExecutionId {
        self.id
    }
}

impl<'a, S: JournalStore> Execution<'a, S, Created> {
    /// Acquires ownership without assuming that the execution is absent.
    /// The subsequent start checks absence while the same lease is held.
    ///
    /// # Errors
    /// Returns a journal ownership conflict or acquisition I/O failure.
    pub fn new(store: &'a S, id: ExecutionId) -> Result<Self, EngineError> {
        let lease = store.acquire(&id)?;
        Ok(Self {
            store,
            id,
            lease,
            _state: PhantomData,
        })
    }

    /// Appends `ExecutionStarted`, transitions to `Running`. Consumes self.
    pub fn start(
        self,
        workflow: WorkflowName,
        version: WorkflowVersion,
        input: EventPayload,
    ) -> Result<Execution<'a, S, Running>, EngineError> {
        if !self.store.load(&self.id)?.is_empty() {
            return Err(EngineError::ExistingExecution { id: self.id });
        }
        self.store
            .append(
                &self.id,
                JournalEvent::ExecutionStarted {
                    workflow,
                    version,
                    input,
                },
            )
            .map_err(EngineError::from)?;
        Ok(Execution {
            store: self.store,
            id: self.id,
            lease: self.lease,
            _state: PhantomData,
        })
    }

    /// Validates the invocation and complete event grammar before recovery.
    ///
    /// # Errors
    /// Reports absent or invalid invocations, mismatched names or versions,
    /// and storage failures before returning an execution.
    pub fn recover(
        store: &'a S,
        id: ExecutionId,
        current_name: &WorkflowName,
        current_version: &WorkflowVersion,
    ) -> Result<RecoveredExecution<'a, S>, EngineError> {
        let lease = store.acquire(&id)?;
        let journal = store.load(&id).map_err(EngineError::from)?;
        let invocation = Invocation::parse(id, &journal)?;
        invocation.select(current_name, current_version)?;

        let history = ValidatedHistory::parse(journal)?;
        match history.into_recovery() {
            RecoveryHistory::Completed(output) => Ok(RecoveredExecution::AlreadyCompleted(
                Execution {
                    store,
                    id,
                    lease,
                    _state: PhantomData,
                },
                output,
            )),
            RecoveryHistory::Failed(error) => Ok(RecoveredExecution::AlreadyFailed(
                Execution {
                    store,
                    id,
                    lease,
                    _state: PhantomData,
                },
                error,
            )),
            RecoveryHistory::Running(history) => Ok(RecoveredExecution::StillRunning(
                Execution {
                    store,
                    id,
                    lease,
                    _state: PhantomData,
                },
                ReplayCursor::from_history(history),
                invocation,
            )),
        }
    }
}

impl<'a, S: JournalStore> Execution<'a, S, Running> {
    /// Appends `ExecutionCompleted`, transitions to `Completed`. Terminal:
    /// no further transitions exist for `Execution<Completed>`.
    pub fn complete(
        self,
        output: EventPayload,
    ) -> Result<Execution<'a, S, Completed>, EngineError> {
        self.store
            .append(&self.id, JournalEvent::ExecutionCompleted { output })
            .map_err(EngineError::from)?;
        Ok(Execution {
            store: self.store,
            id: self.id,
            lease: self.lease,
            _state: PhantomData,
        })
    }

    /// Appends `ExecutionFailed`, transitions to `Failed`. Terminal: no
    /// further transitions exist for `Execution<Failed>`.
    pub fn fail(self, error: WorkflowErrorRecord) -> Result<Execution<'a, S, Failed>, EngineError> {
        self.store
            .append(&self.id, JournalEvent::ExecutionFailed { error })
            .map_err(EngineError::from)?;
        Ok(Execution {
            store: self.store,
            id: self.id,
            lease: self.lease,
            _state: PhantomData,
        })
    }
}

/// Where `Execution::recover` found an execution, from the journal tail.
pub enum RecoveredExecution<'a, S: JournalStore> {
    StillRunning(Execution<'a, S, Running>, ReplayCursor, Invocation),
    AlreadyCompleted(Execution<'a, S, Completed>, EventPayload),
    AlreadyFailed(Execution<'a, S, Failed>, WorkflowErrorRecord),
}

/// A workflow definition. Pure except for effects requested through `ctx`.
/// Input and output cross the boundary as opaque payloads; a typed
/// wrapper's own encode/decode lives at the caller's boundary, not here.
pub trait Workflow<S: JournalStore> {
    type Error: std::fmt::Debug;

    fn name(&self) -> WorkflowName;
    fn version(&self) -> WorkflowVersion;
    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, S>,
        input: EventPayload,
    ) -> Result<EventPayload, Self::Error>;
}

/// Failures from `Engine::run` / `Engine::recover_and_run`. A recovered
/// failure surfaces only the journaled `WorkflowErrorRecord`: once an error
/// crosses the journal boundary as a record, the original typed `E` cannot
/// be reconstructed without a codec, so pretending otherwise would lie.
#[derive(Debug)]
pub enum RunError<E> {
    Engine(EngineError),
    Workflow(E),
    Recovered(WorkflowErrorRecord),
}

/// Runs workflows live or recovers them over a borrowed journal store.
/// Effects without a journaled result can repeat. Trace predicates neither
/// select recovery behavior nor enforce external-effect guarantees.
///
/// Observation predicates cannot be engine type parameters on either store.
///
/// ```compile_fail
/// use yaoki::engine::Engine;
/// use yaoki::equivalence::SingleAdjacentDuplicate;
/// use yaoki::stores::memory::MemoryJournal;
///
/// let store = MemoryJournal::new();
/// let _engine = Engine::<MemoryJournal, SingleAdjacentDuplicate>::new(&store);
/// ```
///
/// ```compile_fail
/// use yaoki::engine::Engine;
/// use yaoki::equivalence::SingleAdjacentDuplicate;
/// use yaoki::stores::file::FileJournal;
///
/// fn construct(store: &FileJournal) {
///     let _engine = Engine::<FileJournal, SingleAdjacentDuplicate>::new(store);
/// }
/// ```
pub struct Engine<'a, S: JournalStore> {
    store: &'a S,
    failpoints: &'a dyn FailpointPolicy,
}

impl<'a, S: JournalStore> Engine<'a, S> {
    /// An engine that never simulates its own death.
    pub fn new(store: &'a S) -> Self {
        Self::with_failpoints(store, &NeverCrash)
    }

    /// An engine whose live paths consult `failpoints` at every
    /// `CrashPoint`. A fired failpoint aborts the run with
    /// `EngineError::InjectedCrash` and journals no terminal event, so
    /// recovering over the same store sees exactly what a killed process
    /// would have left behind.
    pub fn with_failpoints(store: &'a S, failpoints: &'a dyn FailpointPolicy) -> Self {
        Self { store, failpoints }
    }

    pub fn store(&self) -> &'a S {
        self.store
    }

    /// Starts a brand-new execution and runs `workflow` to completion.
    pub fn run<W: Workflow<S>>(
        &self,
        id: ExecutionId,
        workflow: &W,
        input: EventPayload,
        clock: &dyn Clock,
        rng: &mut dyn RngSource,
    ) -> Result<EventPayload, RunError<W::Error>> {
        let execution = Execution::new(self.store, id)
            .map_err(RunError::Engine)?
            .start(workflow.name(), workflow.version(), input.clone())
            .map_err(RunError::Engine)?;
        let mut ctx = WorkflowCtx::with_failpoints(
            self.store,
            id,
            ReplayCursor::new(Journal::empty()),
            clock,
            rng,
            self.failpoints,
        );
        Self::finish(execution, &mut ctx, workflow, input)
    }

    /// Recovers `id` and continues it: replays journaled commands, then
    /// runs any unjournaled remainder live. An execution already terminal
    /// returns its recorded outcome without invoking `workflow.run` again.
    /// Input is always taken from the validated durable start record. The complete
    /// event grammar is checked before workflow code or terminal retrieval.
    /// Workflow success and failure require consuming all recorded events;
    /// otherwise recovery returns `EngineError::UnconsumedHistory` without
    /// appending a terminal event.
    ///
    /// ```compile_fail
    /// use yaoki::engine::Engine;
    /// use yaoki::engine::Workflow;
    /// use yaoki::execution::ExecutionId;
    /// use yaoki::journal::EventPayload;
    /// use yaoki::journal::JournalStore;
    /// use yaoki::random::RngSource;
    /// use yaoki::time::Clock;
    ///
    /// fn replay<S: JournalStore, W: Workflow<S>>(
    ///     engine: &Engine<'_, S>, id: ExecutionId, workflow: &W,
    ///     input: EventPayload, clock: &dyn Clock, rng: &mut dyn RngSource,
    /// ) {
    ///     let _ = engine.recover_and_run(id, workflow, input, clock, rng);
    /// }
    /// ```
    pub fn recover_and_run<W: Workflow<S>>(
        &self,
        id: ExecutionId,
        workflow: &W,
        clock: &dyn Clock,
        rng: &mut dyn RngSource,
    ) -> Result<EventPayload, RunError<W::Error>> {
        match Execution::recover(self.store, id, &workflow.name(), &workflow.version())
            .map_err(RunError::Engine)?
        {
            RecoveredExecution::AlreadyCompleted(_, output) => Ok(output),
            RecoveredExecution::AlreadyFailed(_, error) => Err(RunError::Recovered(error)),
            RecoveredExecution::StillRunning(execution, cursor, invocation) => {
                let mut ctx = WorkflowCtx::with_failpoints(
                    self.store,
                    id,
                    cursor,
                    clock,
                    rng,
                    self.failpoints,
                );
                Self::finish(execution, &mut ctx, workflow, invocation.input)
            }
        }
    }

    fn finish<W: Workflow<S>>(
        execution: Execution<'_, S, Running>,
        ctx: &mut WorkflowCtx<'_, S>,
        workflow: &W,
        input: EventPayload,
    ) -> Result<EventPayload, RunError<W::Error>> {
        let result = workflow.run(ctx, input);

        // A fired failpoint stands for process death: nothing terminal may be
        // journaled, whatever the workflow returned. A workflow that
        // swallowed the crash error does not get to complete.
        match ctx.crash_status() {
            CrashStatus::Crashed(point) => {
                return Err(RunError::Engine(EngineError::InjectedCrash(point)));
            }
            CrashStatus::Intact => {}
        }
        ctx.ensure_replay_complete().map_err(RunError::Engine)?;

        match result {
            Ok(output) => {
                execution
                    .complete(output.clone())
                    .map_err(RunError::Engine)?;
                Ok(output)
            }
            Err(error) => {
                let record = WorkflowErrorRecord::new(format!("{error:?}"));
                execution.fail(record).map_err(RunError::Engine)?;
                Err(RunError::Workflow(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Engine;
    use super::Execution;
    use super::RecoveredExecution;
    use super::RunError;
    use super::Workflow;
    use crate::context::EngineError;
    use crate::context::WorkflowCtx;
    use crate::execution::ExecutionId;
    use crate::execution::WorkflowErrorRecord;
    use crate::execution::WorkflowName;
    use crate::execution::WorkflowVersion;
    use crate::journal::EventPayload;
    use crate::journal::JournalEvent;
    use crate::journal::JournalStore;
    use crate::random::RandomBytes;
    use crate::random::RngSource;
    use crate::step::StepErrorRecord;
    use crate::step::StepName;
    use crate::stores::memory::MemoryJournal;
    use crate::time::TestClock;
    use crate::time::Timestamp;

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

    fn signup_name() -> WorkflowName {
        WorkflowName::new("signup").unwrap()
    }

    fn signup_version() -> WorkflowVersion {
        WorkflowVersion::new("2026.07.18").unwrap()
    }

    fn signup_input() -> EventPayload {
        EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
    }

    /// Neither `SignupWorkflow` nor `AlwaysFailsWorkflow` call `ctx.now()`
    /// or `ctx.random()`; these exist only to satisfy `Engine::run` /
    /// `Engine::recover_and_run`'s signature.
    fn unused_clock() -> TestClock {
        TestClock::at(Timestamp::from_millis_since_epoch(1_753_401_600_000))
    }

    fn unused_rng() -> FixedRng {
        FixedRng { bytes: [0u8; 32] }
    }

    /// Two steps: `charge-card` then `create-account`, each returning its
    /// input back out as its result so the test can assert on it.
    struct SignupWorkflow;

    impl Workflow<MemoryJournal> for SignupWorkflow {
        type Error = String;

        fn name(&self) -> WorkflowName {
            signup_name()
        }

        fn version(&self) -> WorkflowVersion {
            signup_version()
        }

        fn run(
            &self,
            ctx: &mut WorkflowCtx<'_, MemoryJournal>,
            input: EventPayload,
        ) -> Result<EventPayload, String> {
            let charge_card = StepName::new("charge-card").unwrap();
            let charge_confirmation = ctx
                .step(charge_card, |_key| {
                    Ok(EventPayload::new(
                        br#"{"charge_id":"ch_2026_0718"}"#.to_vec(),
                    ))
                })
                .map_err(|error| format!("{error:?}"))?;

            let create_account = StepName::new("create-account").unwrap();
            let _account_created = ctx
                .step(create_account, |_key| {
                    Ok(EventPayload::new(
                        br#"{"account_id":"acct_2026_0718"}"#.to_vec(),
                    ))
                })
                .map_err(|error| format!("{error:?}"))?;

            let _ = input;
            Ok(charge_confirmation)
        }
    }

    /// Fails on its one step, every time, to exercise the `fail` transition.
    struct AlwaysFailsWorkflow;

    impl Workflow<MemoryJournal> for AlwaysFailsWorkflow {
        type Error = String;

        fn name(&self) -> WorkflowName {
            WorkflowName::new("renewal").unwrap()
        }

        fn version(&self) -> WorkflowVersion {
            signup_version()
        }

        fn run(
            &self,
            ctx: &mut WorkflowCtx<'_, MemoryJournal>,
            _input: EventPayload,
        ) -> Result<EventPayload, String> {
            let charge_card = StepName::new("charge-card").unwrap();
            ctx.step(charge_card, |_key| {
                Err(StepErrorRecord::new("payment gateway timed out"))
            })
            .map_err(|error| format!("{error:?}"))
        }
    }

    #[test]
    fn execution_start_appends_execution_started_and_transitions_to_running() {
        let store = MemoryJournal::new();
        let execution = signup_execution();

        let running = Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();

        assert_eq!(running.id(), execution);
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[JournalEvent::ExecutionStarted {
                workflow: signup_name(),
                version: signup_version(),
                input: signup_input(),
            }]
        );
    }

    #[test]
    fn execution_complete_appends_execution_completed_and_transitions_to_completed() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let running = Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();
        let output = EventPayload::new(b"done".to_vec());

        let _completed = running.complete(output.clone()).unwrap();

        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events().last(),
            Some(&JournalEvent::ExecutionCompleted { output })
        );
    }

    #[test]
    fn execution_fail_appends_execution_failed_and_transitions_to_failed() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let running = Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();
        let error = WorkflowErrorRecord::new("account creation rolled back");

        let _failed = running.fail(error.clone()).unwrap();

        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events().last(),
            Some(&JournalEvent::ExecutionFailed { error })
        );
    }

    #[test]
    fn recover_with_completed_tail_returns_already_completed_with_output() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let output = EventPayload::new(b"done".to_vec());
        Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap()
            .complete(output.clone())
            .unwrap();

        let recovered =
            Execution::recover(&store, execution, &signup_name(), &signup_version()).unwrap();

        match recovered {
            RecoveredExecution::AlreadyCompleted(_, recovered_output) => {
                assert_eq!(recovered_output, output);
            }
            _ => panic!("expected AlreadyCompleted"),
        }
    }

    #[test]
    fn recover_with_failed_tail_returns_already_failed_with_error() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let error = WorkflowErrorRecord::new("account creation rolled back");
        Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap()
            .fail(error.clone())
            .unwrap();

        let recovered =
            Execution::recover(&store, execution, &signup_name(), &signup_version()).unwrap();

        match recovered {
            RecoveredExecution::AlreadyFailed(_, recovered_error) => {
                assert_eq!(recovered_error, error);
            }
            _ => panic!("expected AlreadyFailed"),
        }
    }

    #[test]
    fn recover_with_no_terminal_tail_returns_still_running_with_a_cursor_over_the_remaining_commands()
     {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let charge_card = StepName::new("charge-card").unwrap();
        Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();
        store
            .append(
                &execution,
                JournalEvent::StepScheduled {
                    seq: crate::journal::Seq::zero(),
                    name: charge_card.clone(),
                },
            )
            .unwrap();

        let recovered =
            Execution::recover(&store, execution, &signup_name(), &signup_version()).unwrap();

        match recovered {
            RecoveredExecution::StillRunning(_, cursor, invocation) => {
                assert_eq!(invocation.id(), execution);
                assert_eq!(invocation.workflow(), &signup_name());
                assert_eq!(invocation.version(), &signup_version());
                assert_eq!(invocation.input(), &signup_input());
                assert_eq!(
                    cursor.peek(),
                    Some(&JournalEvent::StepScheduled {
                        seq: crate::journal::Seq::zero(),
                        name: charge_card,
                    })
                );
            }
            _ => panic!("expected StillRunning"),
        }
    }

    #[test]
    fn recover_with_mismatched_version_returns_version_mismatch_error() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();
        let newer_version = WorkflowVersion::new("2026.08.01").unwrap();

        let result = Execution::recover(&store, execution, &signup_name(), &newer_version);

        assert_eq!(
            result.err(),
            Some(EngineError::VersionMismatch {
                recorded: signup_version(),
                current: newer_version,
            })
        );
    }

    #[test]
    fn engine_run_starts_and_completes_a_fresh_execution() {
        let store = MemoryJournal::new();
        let engine = Engine::<_>::new(&store);
        let execution = signup_execution();

        let output = engine
            .run(
                execution,
                &SignupWorkflow,
                signup_input(),
                &unused_clock(),
                &mut unused_rng(),
            )
            .unwrap();

        assert_eq!(
            output,
            EventPayload::new(br#"{"charge_id":"ch_2026_0718"}"#.to_vec())
        );
        let journal = engine.store().load(&execution).unwrap();
        assert_eq!(
            journal.events().last(),
            Some(&JournalEvent::ExecutionCompleted {
                output: EventPayload::new(br#"{"charge_id":"ch_2026_0718"}"#.to_vec()),
            })
        );
    }

    #[test]
    fn engine_run_fails_and_journals_execution_failed_when_the_workflow_errs() {
        let store = MemoryJournal::new();
        let engine = Engine::<_>::new(&store);
        let execution = signup_execution();

        let result = engine.run(
            execution,
            &AlwaysFailsWorkflow,
            signup_input(),
            &unused_clock(),
            &mut unused_rng(),
        );

        assert!(matches!(result, Err(RunError::Workflow(_))));
        let journal = engine.store().load(&execution).unwrap();
        assert!(matches!(
            journal.events().last(),
            Some(JournalEvent::ExecutionFailed { .. })
        ));
    }

    #[test]
    fn engine_recover_and_run_on_a_completed_execution_returns_the_recorded_output_without_rerunning_steps()
     {
        // Run to completion once.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let first_output = {
            let engine = Engine::<_>::new(&store);
            engine
                .run(
                    execution,
                    &SignupWorkflow,
                    signup_input(),
                    &unused_clock(),
                    &mut unused_rng(),
                )
                .unwrap()
        };
        let events_after_first_run = store.load(&execution).unwrap().len();

        // "wipe the engine, keep the store". The first `engine` value
        // above is already gone (dropped at the end of its block); build a
        // fresh `Engine` over the same `store` binding and recover.
        let recovered_engine = Engine::<_>::new(&store);
        let second_output = recovered_engine
            .recover_and_run(
                execution,
                &SignupWorkflow,
                &unused_clock(),
                &mut unused_rng(),
            )
            .unwrap();

        // Identical output, and not a single event was re-appended
        // (both steps were answered from the journal, not re-executed).
        assert_eq!(first_output, second_output);
        assert_eq!(
            store.load(&execution).unwrap().len(),
            events_after_first_run
        );
    }

    #[test]
    fn engine_recover_and_run_on_a_still_running_execution_replays_then_continues_live() {
        // Journal a fully-recorded first step only (simulating a
        // crash between the first and second step of the signup workflow).
        let store = MemoryJournal::new();
        let execution = signup_execution();
        Execution::new(&store, execution)
            .unwrap()
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();
        let charge_card = StepName::new("charge-card").unwrap();
        store
            .append(
                &execution,
                JournalEvent::StepScheduled {
                    seq: crate::journal::Seq::zero(),
                    name: charge_card,
                },
            )
            .unwrap();
        store
            .append(
                &execution,
                JournalEvent::StepStarted {
                    seq: crate::journal::Seq::zero(),
                    attempt: crate::step::Attempt::first(),
                },
            )
            .unwrap();
        store
            .append(
                &execution,
                JournalEvent::StepCompleted {
                    seq: crate::journal::Seq::zero(),
                    result: EventPayload::new(br#"{"charge_id":"ch_2026_0718"}"#.to_vec()),
                },
            )
            .unwrap();

        let engine = Engine::<_>::new(&store);
        let output = engine
            .recover_and_run(
                execution,
                &SignupWorkflow,
                &unused_clock(),
                &mut unused_rng(),
            )
            .unwrap();

        // Charge-card was not re-run (its result came from the
        // journal); create-account ran live and was journaled.
        assert_eq!(
            output,
            EventPayload::new(br#"{"charge_id":"ch_2026_0718"}"#.to_vec())
        );
        let journal = engine.store().load(&execution).unwrap();
        // ExecutionStarted + 3 (charge-card, pre-existing) + 3 (create-account,
        // run live) + ExecutionCompleted.
        assert_eq!(journal.len(), 8);
        assert!(matches!(
            journal.events().last(),
            Some(JournalEvent::ExecutionCompleted { .. })
        ));
    }
}
