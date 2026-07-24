//! Execution lifecycle typestate, workflow orchestration, and the engine's
//! own error type. `ReplayCursor` and `EngineError` live in `context.rs`
//! (see the note there); this module depends on `context.rs`, not the
//! other way around.

use std::marker::PhantomData;

use crate::context::EngineError;
use crate::context::ReplayCursor;
use crate::context::WorkflowCtx;
use crate::equivalence::SupportedOn;
use crate::execution::ExecutionId;
use crate::execution::WorkflowErrorRecord;
use crate::execution::WorkflowName;
use crate::execution::WorkflowVersion;
use crate::journal::EventPayload;
use crate::journal::Journal;
use crate::journal::JournalEvent;
use crate::journal::JournalStore;
use crate::random::RngSource;
use crate::time::Clock;

/// Journal empty: no `ExecutionStarted` yet.
pub struct Created;
/// `ExecutionStarted` appended; workflow run in progress.
pub struct Running;
/// Terminal: `ExecutionCompleted` appended.
pub struct Completed;
/// Terminal: `ExecutionFailed` appended.
pub struct Failed;

/// An execution's identity, tagged with its lifecycle state at the type
/// level. Illegal transitions (completing a `Created` execution, resuming a
/// `Completed` one) do not compile.
pub struct Execution<'a, S: JournalStore, State> {
    store: &'a S,
    id: ExecutionId,
    _state: PhantomData<State>,
}

impl<'a, S: JournalStore, State> Execution<'a, S, State> {
    pub fn id(&self) -> ExecutionId {
        self.id
    }
}

impl<'a, S: JournalStore> Execution<'a, S, Created> {
    pub fn new(store: &'a S, id: ExecutionId) -> Self {
        Self {
            store,
            id,
            _state: PhantomData,
        }
    }

    /// Appends `ExecutionStarted`, transitions to `Running`. Consumes self.
    pub fn start(
        self,
        workflow: WorkflowName,
        version: WorkflowVersion,
        input: EventPayload,
    ) -> Result<Execution<'a, S, Running>, EngineError> {
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
            _state: PhantomData,
        })
    }

    /// Inspects the journal tail and re-enters the correct state.
    ///
    /// # Errors
    /// `VersionMismatch` if the journal's recorded workflow version differs
    /// from `current_version`. `Journal` if the store cannot be reached.
    pub fn recover(
        store: &'a S,
        id: ExecutionId,
        current_version: &WorkflowVersion,
    ) -> Result<RecoveredExecution<'a, S>, EngineError> {
        let journal = store.load(&id).map_err(EngineError::from)?;

        if let Some(JournalEvent::ExecutionStarted {
            version: recorded, ..
        }) = journal.events().first()
            && recorded != current_version
        {
            return Err(EngineError::VersionMismatch {
                recorded: recorded.clone(),
                current: current_version.clone(),
            });
        }

        if let Some(JournalEvent::ExecutionCompleted { output }) = journal.events().last() {
            let execution = Execution {
                store,
                id,
                _state: PhantomData,
            };
            return Ok(RecoveredExecution::AlreadyCompleted(
                execution,
                output.clone(),
            ));
        }
        if let Some(JournalEvent::ExecutionFailed { error }) = journal.events().last() {
            let execution = Execution {
                store,
                id,
                _state: PhantomData,
            };
            return Ok(RecoveredExecution::AlreadyFailed(execution, error.clone()));
        }

        // `ExecutionStarted` is the execution-level bookmark, not a
        // per-command event; the replay cursor walks commands only.
        let remaining: Vec<JournalEvent> = journal.events().iter().skip(1).cloned().collect();
        let cursor = ReplayCursor::new(Journal::new(remaining));
        let execution = Execution {
            store,
            id,
            _state: PhantomData,
        };
        Ok(RecoveredExecution::StillRunning(execution, cursor))
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
            _state: PhantomData,
        })
    }
}

/// Where `Execution::recover` found an execution, from the journal tail.
pub enum RecoveredExecution<'a, S: JournalStore> {
    StillRunning(Execution<'a, S, Running>, ReplayCursor),
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

/// Runs workflows against a `JournalStore`, live or recovered, under
/// recovery-equivalence mode `E`. Borrows the store rather than owning it,
/// so "wipe the engine, keep the store" is just: drop this `Engine`, build a
/// fresh one over the same store binding.
///
/// `E` is a compile-time contract, not yet a runtime behavior switch:
/// `Engine::<FileJournal, ExactlyOnce>::new(..)` fails to compile because
/// `FileJournal` is not a `TransactionalBoundary`. The API teaches the
/// impossibility before any workflow runs.
pub struct Engine<'a, S: JournalStore, E: SupportedOn<S>> {
    store: &'a S,
    _mode: PhantomData<E>,
}

impl<'a, S: JournalStore, E: SupportedOn<S>> Engine<'a, S, E> {
    pub fn new(store: &'a S) -> Self {
        Self {
            store,
            _mode: PhantomData,
        }
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
            .start(workflow.name(), workflow.version(), input.clone())
            .map_err(RunError::Engine)?;
        let mut ctx = WorkflowCtx::new(
            self.store,
            id,
            ReplayCursor::new(Journal::empty()),
            clock,
            rng,
        );
        Self::finish(execution, &mut ctx, workflow, input)
    }

    /// Recovers `id` and continues it: replays journaled commands, then
    /// runs any unjournaled remainder live. An execution already terminal
    /// returns its recorded outcome without invoking `workflow.run` again.
    pub fn recover_and_run<W: Workflow<S>>(
        &self,
        id: ExecutionId,
        workflow: &W,
        input: EventPayload,
        clock: &dyn Clock,
        rng: &mut dyn RngSource,
    ) -> Result<EventPayload, RunError<W::Error>> {
        match Execution::recover(self.store, id, &workflow.version()).map_err(RunError::Engine)? {
            RecoveredExecution::AlreadyCompleted(_, output) => Ok(output),
            RecoveredExecution::AlreadyFailed(_, error) => Err(RunError::Recovered(error)),
            RecoveredExecution::StillRunning(execution, cursor) => {
                let mut ctx = WorkflowCtx::new(self.store, id, cursor, clock, rng);
                Self::finish(execution, &mut ctx, workflow, input)
            }
        }
    }

    fn finish<W: Workflow<S>>(
        execution: Execution<'_, S, Running>,
        ctx: &mut WorkflowCtx<'_, S>,
        workflow: &W,
        input: EventPayload,
    ) -> Result<EventPayload, RunError<W::Error>> {
        match workflow.run(ctx, input) {
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
    use super::*;
    use crate::equivalence::DuplicateLast;
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
            .start(signup_name(), signup_version(), signup_input())
            .unwrap()
            .complete(output.clone())
            .unwrap();

        let recovered = Execution::recover(&store, execution, &signup_version()).unwrap();

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
            .start(signup_name(), signup_version(), signup_input())
            .unwrap()
            .fail(error.clone())
            .unwrap();

        let recovered = Execution::recover(&store, execution, &signup_version()).unwrap();

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

        let recovered = Execution::recover(&store, execution, &signup_version()).unwrap();

        match recovered {
            RecoveredExecution::StillRunning(_, cursor) => {
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
            .start(signup_name(), signup_version(), signup_input())
            .unwrap();
        let newer_version = WorkflowVersion::new("2026.08.01").unwrap();

        let result = Execution::recover(&store, execution, &newer_version);

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
        let engine = Engine::<_, DuplicateLast>::new(&store);
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
        let engine = Engine::<_, DuplicateLast>::new(&store);
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
            let engine = Engine::<_, DuplicateLast>::new(&store);
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
        let recovered_engine = Engine::<_, DuplicateLast>::new(&store);
        let second_output = recovered_engine
            .recover_and_run(
                execution,
                &SignupWorkflow,
                signup_input(),
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

        let engine = Engine::<_, DuplicateLast>::new(&store);
        let output = engine
            .recover_and_run(
                execution,
                &SignupWorkflow,
                signup_input(),
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
