//! `WorkflowCtx`: the sole capability a workflow receives. Every effect a
//! workflow requests goes through here, so replay can intercept it.

use crate::command::CommandKind;
use crate::engine::EngineError;
use crate::engine::ReplayCursor;
use crate::execution::ExecutionId;
use crate::journal::EventPayload;
use crate::journal::JournalEvent;
use crate::journal::JournalStore;
use crate::journal::Seq;
use crate::step::Attempt;
use crate::step::IdempotencyKey;
use crate::step::StepError;
use crate::step::StepErrorRecord;
use crate::step::StepName;

/// Replay-vs-live execution mode. Not a typestate: the transition happens
/// mid-run at journal exhaustion, inside one `&mut` borrow of `WorkflowCtx`,
/// so it is a runtime enum with an exhaustive match instead.
enum Mode {
    Replaying(ReplayCursor),
    Live,
}

/// A decision made while resolving one `step()` call: either the recorded
/// journal already answers it, or replay has run out and this step must be
/// executed live.
enum StepDecision {
    UseRecorded(Result<EventPayload, StepError>),
    RunLive,
}

/// The only capability a workflow receives. Every effect goes through here
/// so replay can intercept it.
pub struct WorkflowCtx<'a, S: JournalStore> {
    store: &'a S,
    execution: ExecutionId,
    seq: Seq,
    mode: Mode,
}

impl<'a, S: JournalStore> WorkflowCtx<'a, S> {
    /// Builds a context over `cursor`. A cursor already exhausted (fresh
    /// execution, or one whose journal has just `ExecutionStarted`) starts
    /// live; otherwise replay begins from the cursor's first event.
    pub fn new(store: &'a S, execution: ExecutionId, cursor: ReplayCursor) -> Self {
        let mode = if cursor.is_exhausted() {
            Mode::Live
        } else {
            Mode::Replaying(cursor)
        };
        Self {
            store,
            execution,
            seq: Seq::zero(),
            mode,
        }
    }

    /// Runs (or replays) a step. `f` receives an `IdempotencyKey` and may do
    /// arbitrary I/O; it is the unit of atomicity and recovery.
    pub fn step<F>(&mut self, name: StepName, f: F) -> Result<EventPayload, StepError>
    where
        F: FnOnce(IdempotencyKey) -> Result<EventPayload, StepErrorRecord>,
    {
        let seq = self.seq;
        self.seq = self.seq.next();

        let decision = match &mut self.mode {
            Mode::Replaying(cursor) => match cursor.peek().cloned() {
                Some(JournalEvent::StepScheduled {
                    name: journaled, ..
                }) if journaled == name => {
                    cursor.advance();
                    StepDecision::UseRecorded(Self::replay_step_outcome(cursor, seq))
                }
                Some(event) => {
                    let expected = event.command_kind().unwrap_or(CommandKind::RunStep);
                    StepDecision::UseRecorded(Err(StepError::Engine(EngineError::Nondeterminism {
                        seq,
                        expected,
                        got: CommandKind::RunStep,
                    })))
                }
                None => StepDecision::RunLive,
            },
            Mode::Live => StepDecision::RunLive,
        };

        match decision {
            StepDecision::UseRecorded(result) => result,
            StepDecision::RunLive => {
                self.mode = Mode::Live;
                self.run_live(seq, name, f)
            }
        }
    }

    /// Resolves a step already matched against a journaled `StepScheduled`:
    /// consumes the following `StepStarted` (if present) and returns the
    /// recorded `StepCompleted`/`StepFailed` outcome without running `f`.
    fn replay_step_outcome(cursor: &mut ReplayCursor, seq: Seq) -> Result<EventPayload, StepError> {
        if let Some(JournalEvent::StepStarted { .. }) = cursor.peek() {
            cursor.advance();
        }
        match cursor.peek().cloned() {
            Some(JournalEvent::StepCompleted { result, .. }) => {
                cursor.advance();
                Ok(result)
            }
            Some(JournalEvent::StepFailed { error, .. }) => {
                cursor.advance();
                Err(StepError::Failed(error))
            }
            Some(event) => {
                let expected = event.command_kind().unwrap_or(CommandKind::RunStep);
                Err(StepError::Engine(EngineError::Nondeterminism {
                    seq,
                    expected,
                    got: CommandKind::RunStep,
                }))
            }
            None => Err(StepError::Engine(EngineError::Nondeterminism {
                seq,
                expected: CommandKind::RunStep,
                got: CommandKind::RunStep,
            })),
        }
    }

    /// Executes `f` for real: schedules, starts, runs, and journals the
    /// outcome.
    fn run_live<F>(&mut self, seq: Seq, name: StepName, f: F) -> Result<EventPayload, StepError>
    where
        F: FnOnce(IdempotencyKey) -> Result<EventPayload, StepErrorRecord>,
    {
        self.store
            .append(&self.execution, JournalEvent::StepScheduled { seq, name })
            .map_err(EngineError::from)?;
        self.store
            .append(
                &self.execution,
                JournalEvent::StepStarted {
                    seq,
                    attempt: Attempt::first(),
                },
            )
            .map_err(EngineError::from)?;

        let key = IdempotencyKey::new(self.execution, seq);
        match f(key) {
            Ok(result) => {
                self.store
                    .append(
                        &self.execution,
                        JournalEvent::StepCompleted {
                            seq,
                            result: result.clone(),
                        },
                    )
                    .map_err(EngineError::from)?;
                Ok(result)
            }
            Err(error) => {
                self.store
                    .append(
                        &self.execution,
                        JournalEvent::StepFailed {
                            seq,
                            attempt: Attempt::first(),
                            error: error.clone(),
                        },
                    )
                    .map_err(EngineError::from)?;
                Err(StepError::Failed(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::Journal;
    use crate::journal::JournalError;
    use crate::random::RandomBytes;
    use crate::random::RngSource;
    use crate::stores::memory::MemoryJournal;
    use crate::time::Deadline;
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

    fn charge_card() -> StepName {
        StepName::new("charge-card").unwrap()
    }

    fn charge_confirmation() -> EventPayload {
        EventPayload::new(br#"{"charge_id":"ch_2026_0718"}"#.to_vec())
    }

    fn gateway_timeout() -> StepErrorRecord {
        StepErrorRecord::new("payment gateway timed out")
    }

    /// Always fails every append and load, to exercise the journal-error
    /// path without needing to poison a real lock.
    struct AlwaysFailingJournal;

    impl JournalStore for AlwaysFailingJournal {
        fn append(&self, _id: &ExecutionId, _event: JournalEvent) -> Result<Seq, JournalError> {
            Err(JournalError::Poisoned)
        }

        fn load(&self, _id: &ExecutionId) -> Result<Journal, JournalError> {
            Err(JournalError::Poisoned)
        }
    }

    #[test]
    fn live_step_with_ok_closure_journals_and_returns_the_result() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(Journal::empty()));

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        assert_eq!(result, Ok(charge_confirmation()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[
                JournalEvent::StepScheduled {
                    seq: Seq::zero(),
                    name: charge_card(),
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero(),
                    attempt: Attempt::first(),
                },
                JournalEvent::StepCompleted {
                    seq: Seq::zero(),
                    result: charge_confirmation(),
                },
            ]
        );
    }

    #[test]
    fn live_step_with_err_closure_journals_step_failed() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(Journal::empty()));

        let result = ctx.step(charge_card(), |_key| Err(gateway_timeout()));

        assert_eq!(result, Err(StepError::Failed(gateway_timeout())));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[
                JournalEvent::StepScheduled {
                    seq: Seq::zero(),
                    name: charge_card(),
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero(),
                    attempt: Attempt::first(),
                },
                JournalEvent::StepFailed {
                    seq: Seq::zero(),
                    attempt: Attempt::first(),
                    error: gateway_timeout(),
                },
            ]
        );
    }

    #[test]
    fn live_step_returns_journal_error_when_append_fails() {
        let store = AlwaysFailingJournal;
        let execution = signup_execution();
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(Journal::empty()));

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::Journal(
                JournalError::Poisoned
            )))
        );
    }

    #[test]
    fn replaying_step_completed_returns_recorded_result_without_running_closure() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![
            JournalEvent::StepScheduled {
                seq: Seq::zero(),
                name: charge_card(),
            },
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first(),
            },
            JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: charge_confirmation(),
            },
        ]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));

        let result = ctx.step(charge_card(), |_key| {
            panic!("closure must not run during replay of a completed step")
        });

        assert_eq!(result, Ok(charge_confirmation()));
    }

    #[test]
    fn replaying_step_failed_returns_recorded_error_without_running_closure() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![
            JournalEvent::StepScheduled {
                seq: Seq::zero(),
                name: charge_card(),
            },
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first(),
            },
            JournalEvent::StepFailed {
                seq: Seq::zero(),
                attempt: Attempt::first(),
                error: gateway_timeout(),
            },
        ]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));

        let result = ctx.step(charge_card(), |_key| {
            panic!("closure must not run during replay of a failed step")
        });

        assert_eq!(result, Err(StepError::Failed(gateway_timeout())));
    }

    #[test]
    fn replaying_kind_mismatch_returns_nondeterminism_error() {
        // Journal recorded a timer, code now asks to run a step.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::TimerScheduled {
            seq: Seq::zero(),
            deadline: Deadline::at(Timestamp::from_millis_since_epoch(1_753_401_600_000)),
        }]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));

        let result = ctx.step(charge_card(), |_key| {
            panic!("closure must not run when replay diverges")
        });

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::Sleep,
                got: CommandKind::RunStep,
            }))
        );
    }

    #[test]
    fn replaying_name_mismatch_returns_nondeterminism_error() {
        // Journal recorded a different step name at this position.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new("create-account").unwrap(),
        }]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));

        let result = ctx.step(charge_card(), |_key| {
            panic!("closure must not run when replay diverges")
        });

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got: CommandKind::RunStep,
            }))
        );
    }

    #[test]
    fn replaying_cursor_exhausted_mid_run_switches_to_live() {
        // Journal has exactly one recorded step; a second step()
        // call during the same run must find replay exhausted and go live.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![
            JournalEvent::StepScheduled {
                seq: Seq::zero(),
                name: charge_card(),
            },
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first(),
            },
            JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: charge_confirmation(),
            },
        ]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));
        let replayed = ctx.step(charge_card(), |_key| {
            panic!("closure must not run during replay")
        });
        assert_eq!(replayed, Ok(charge_confirmation()));

        let create_account = StepName::new("create-account").unwrap();
        let account_created = EventPayload::new(br#"{"account_id":"acct_2026_0718"}"#.to_vec());
        let live_result = ctx.step(create_account.clone(), |_key| Ok(account_created.clone()));

        assert_eq!(live_result, Ok(account_created.clone()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[
                JournalEvent::StepScheduled {
                    seq: Seq::zero().next(),
                    name: create_account,
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero().next(),
                    attempt: Attempt::first(),
                },
                JournalEvent::StepCompleted {
                    seq: Seq::zero().next(),
                    result: account_created,
                },
            ]
        );
    }

    #[test]
    fn replaying_step_scheduled_with_no_further_events_is_nondeterminism() {
        // Malformed journal, StepScheduled with nothing after it.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        }]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got: CommandKind::RunStep,
            }))
        );
    }

    #[test]
    fn replaying_step_scheduled_followed_by_wrong_event_is_nondeterminism() {
        // Malformed journal, StepScheduled directly followed by
        // ExecutionCompleted, skipping StepStarted/StepCompleted entirely.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![
            JournalEvent::StepScheduled {
                seq: Seq::zero(),
                name: charge_card(),
            },
            JournalEvent::ExecutionCompleted {
                output: EventPayload::new(b"done".to_vec()),
            },
        ]);
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(journal));

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got: CommandKind::RunStep,
            }))
        );
    }

    #[test]
    fn idempotency_key_passed_to_the_closure_pairs_execution_and_seq() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let mut ctx = WorkflowCtx::new(&store, execution, ReplayCursor::new(Journal::empty()));

        let mut observed_key = None;
        let _ = ctx.step(charge_card(), |key| {
            observed_key = Some(key);
            Ok(charge_confirmation())
        });

        let key = observed_key.unwrap();
        assert_eq!(key.execution(), execution);
        assert_eq!(key.seq(), Seq::zero());
    }
}
