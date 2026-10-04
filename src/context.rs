//! `WorkflowCtx`: the sole capability a workflow receives. Every effect a
//! workflow requests goes through here, so replay can intercept it.
//!
//! `ReplayCursor` and `EngineError` live here rather than in `engine.rs`:
//! `WorkflowCtx::step` is their primary consumer, and `Engine` (in
//! `engine.rs`) depends on `WorkflowCtx` to drive a workflow, so putting
//! them in `engine.rs` instead would make the two modules depend on each
//! other.

use thiserror::Error;

use crate::command::CommandKind;
use crate::execution::ExecutionId;
use crate::execution::WorkflowName;
use crate::execution::WorkflowVersion;
use crate::failpoints::CrashPoint;
use crate::failpoints::CrashStatus;
use crate::failpoints::FailpointDecision;
use crate::failpoints::FailpointPolicy;
use crate::failpoints::NeverCrash;
use crate::history::HistoryError;
use crate::history::ReplayHistory;
use crate::journal::EventOffset;
use crate::journal::EventPayload;
use crate::journal::Journal;
use crate::journal::JournalError;
use crate::journal::JournalEvent;
use crate::journal::JournalStore;
use crate::journal::Seq;
use crate::journal::SeqError;
use crate::random::RandomBytes;
use crate::random::RngSource;
use crate::step::Attempt;
use crate::step::AttemptOverflow;
use crate::step::IdempotencyKey;
use crate::step::StepError;
use crate::step::StepErrorRecord;
use crate::step::StepName;
use crate::time::Clock;
use crate::time::Deadline;
use crate::time::Timestamp;

/// Walks a loaded `Journal` command by command during replay.
#[derive(Debug, Clone)]
pub struct ReplayCursor {
    source: ReplaySource,
    position: EventOffset,
}

#[derive(Debug, Clone)]
enum ReplaySource {
    Raw(Journal),
    Validated(ReplayHistory),
}

impl ReplaySource {
    fn events(&self) -> &[JournalEvent] {
        match self {
            Self::Raw(journal) => journal.events(),
            Self::Validated(history) => history.events(),
        }
    }
}

impl ReplayCursor {
    /// Walks raw command events without validating their grammar.
    /// Engine recovery uses a parsed history instead of this low-level constructor.
    pub fn new(journal: Journal) -> Self {
        Self {
            source: ReplaySource::Raw(journal),
            position: EventOffset::from_index(0),
        }
    }

    /// Retains a parsed history and begins immediately after its invocation.
    pub(crate) fn from_history(history: ReplayHistory) -> Self {
        Self {
            source: ReplaySource::Validated(history),
            position: EventOffset::from_index(1),
        }
    }

    /// True once every journaled event has been consumed.
    pub fn is_exhausted(&self) -> bool {
        self.position.get() >= self.source.events().len()
    }

    /// The next unconsumed event, without advancing.
    pub fn peek(&self) -> Option<&JournalEvent> {
        self.source.events().get(self.position.get())
    }

    /// Consumes the current event. An exhausted cursor remains exhausted.
    pub fn advance(&mut self) {
        if !self.is_exhausted() {
            self.position = EventOffset::from_index(self.position.get() + 1);
        }
    }
}

/// Errors from running or recovering an execution.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    #[error(
        "nondeterministic workflow at seq {seq:?}: journal has {expected:?}, code produced {got:?}"
    )]
    Nondeterminism {
        seq: Seq,
        expected: CommandKind,
        got: CommandKind,
    },

    /// Workflow code returned while recorded events remained unread.
    #[error("workflow returned before consuming the recorded history")]
    UnconsumedHistory,

    #[error("workflow version mismatch: journal {recorded:?}, code {current:?}")]
    VersionMismatch {
        recorded: WorkflowVersion,
        current: WorkflowVersion,
    },

    #[error("workflow name mismatch: journal {recorded:?}, code {current:?}")]
    WorkflowMismatch {
        recorded: WorkflowName,
        current: WorkflowName,
    },

    #[error("execution {id:?} does not exist")]
    MissingExecution { id: ExecutionId },

    #[error("execution {id:?} already exists")]
    ExistingExecution { id: ExecutionId },

    #[error("execution {id:?} must have exactly one initial start record")]
    InvalidInvocation { id: ExecutionId },

    /// A command position has no successor.
    #[error("{0}")]
    Sequence(#[from] SeqError),

    /// An interrupted step has no next attempt.
    #[error("{0}")]
    Attempt(#[from] AttemptOverflow),

    /// The recorded event grammar is invalid.
    #[error("invalid execution history: {0}")]
    History(#[from] HistoryError),

    #[error("journal error: {0}")]
    Journal(#[from] JournalError),

    #[error("injected crash at {0:?}")]
    InjectedCrash(CrashPoint),
}

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
    RunLive(LiveEntry),
}

/// How a live step run enters the journal.
enum LiveEntry {
    /// Nothing is journaled at this position: schedule the step, then start
    /// its first attempt.
    Fresh,
    /// `StepScheduled` is already durable from an attempt a crash cut short.
    /// Start a new attempt without re-scheduling. A second `StepScheduled` at
    /// the same `Seq` would make every later replay ambiguous.
    Retry { attempt: Attempt },
}

/// What replay found after matching a journaled `StepScheduled`.
enum ReplayedStep {
    /// A terminal outcome (`StepCompleted`/`StepFailed`, or a divergence).
    /// Answer from the journal, do not run the closure.
    Recorded(Result<EventPayload, StepError>),
    /// The journal ends inside this step: the process died before any
    /// outcome was recorded, either before or after the side effect landed.
    /// The engine cannot tell which, so it reruns as `attempt`.
    /// The stable idempotency key only prevents duplicate effects when
    /// the recipient enforces deduplication.
    Rerun { attempt: Attempt },
}

/// What replay found at the cursor while resolving a `now()`/`random()`
/// call: either a recorded terminal outcome, or replay has run out and the
/// value must be drawn live.
enum ReplayedEffect<T> {
    Recorded(Result<T, EngineError>),
    Live,
}

/// A decision made while resolving one `sleep_until()` call.
enum SleepDecision {
    /// `TimerScheduled` + `TimerFired` both journaled, or a divergence.
    UseRecorded(Result<(), EngineError>),
    /// `TimerScheduled` journaled, `TimerFired` missing: the process died
    /// mid-wait. Re-arm toward the already-journaled deadline without
    /// re-appending `TimerScheduled`, and journal `TimerFired` once the wait
    /// (a no-op if the deadline already passed) returns.
    Rearm,
    /// Nothing journaled at this position: a brand-new timer.
    RunLive,
}

/// The only capability a workflow receives. Every effect goes through here
/// so replay can intercept it.
pub struct WorkflowCtx<'a, S: JournalStore> {
    store: &'a S,
    execution: ExecutionId,
    seq: Seq,
    mode: Mode,
    clock: &'a dyn Clock,
    rng: &'a mut dyn RngSource,
    failpoints: &'a dyn FailpointPolicy,
    crash: CrashStatus,
}

impl<'a, S: JournalStore> WorkflowCtx<'a, S> {
    /// Builds a context over `cursor` that never crashes. A cursor already
    /// exhausted (fresh execution, or one whose journal has just
    /// `ExecutionStarted`) starts live; otherwise replay begins from the
    /// cursor's first event.
    pub fn new(
        store: &'a S,
        execution: ExecutionId,
        cursor: ReplayCursor,
        clock: &'a dyn Clock,
        rng: &'a mut dyn RngSource,
    ) -> Self {
        Self::with_failpoints(store, execution, cursor, clock, rng, &NeverCrash)
    }

    /// Builds a context whose live paths consult `failpoints` at every
    /// `CrashPoint`.
    pub fn with_failpoints(
        store: &'a S,
        execution: ExecutionId,
        cursor: ReplayCursor,
        clock: &'a dyn Clock,
        rng: &'a mut dyn RngSource,
        failpoints: &'a dyn FailpointPolicy,
    ) -> Self {
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
            clock,
            rng,
            failpoints,
            crash: CrashStatus::Intact,
        }
    }

    /// Whether an injected crash fired during this run.
    pub fn crash_status(&self) -> CrashStatus {
        self.crash
    }

    /// Refuses terminal recording while replay still has unread events.
    pub(crate) fn ensure_replay_complete(&self) -> Result<(), EngineError> {
        match &self.mode {
            Mode::Live => Ok(()),
            Mode::Replaying(cursor) => {
                if cursor.is_exhausted() {
                    Ok(())
                } else {
                    Err(EngineError::UnconsumedHistory)
                }
            }
        }
    }

    /// Consults the policy at `point`. On `Crash`, records the crash and
    /// returns the error without journaling anything: the journal is left
    /// exactly as a process death at this window would leave it.
    fn checkpoint(&mut self, point: CrashPoint) -> Result<(), EngineError> {
        match self.failpoints.at(point) {
            FailpointDecision::Continue => Ok(()),
            FailpointDecision::Crash => {
                self.crash = CrashStatus::Crashed(point);
                Err(EngineError::InjectedCrash(point))
            }
        }
    }

    /// A crashed process runs no further commands. Once a failpoint has
    /// fired, every later `ctx` call fails with the same crash without
    /// consuming a `Seq` or touching the journal, so a workflow that swallows
    /// the first error cannot journal past its own death.
    fn refuse_after_crash(&self) -> Result<(), EngineError> {
        match self.crash {
            CrashStatus::Intact => Ok(()),
            CrashStatus::Crashed(point) => Err(EngineError::InjectedCrash(point)),
        }
    }

    /// Journaled clock read. Replay returns the original timestamp.
    ///
    /// # Errors
    /// `Nondeterminism` if the journal expected a different command at this
    /// position. `Journal` if the store cannot be reached.
    pub fn now(&mut self) -> Result<Timestamp, EngineError> {
        self.refuse_after_crash()?;
        let seq = self.seq;
        self.seq = self.seq.next().map_err(EngineError::from)?;

        let decision = match &mut self.mode {
            Mode::Replaying(cursor) => match cursor.peek().cloned() {
                Some(JournalEvent::NowRecorded { value, .. }) => {
                    cursor.advance();
                    ReplayedEffect::Recorded(Ok(value))
                }
                Some(event) => {
                    let expected = event.command_kind().unwrap_or(CommandKind::ReadNow);
                    ReplayedEffect::Recorded(Err(EngineError::Nondeterminism {
                        seq,
                        expected,
                        got: CommandKind::ReadNow,
                    }))
                }
                None => ReplayedEffect::Live,
            },
            Mode::Live => ReplayedEffect::Live,
        };

        match decision {
            ReplayedEffect::Recorded(result) => result,
            ReplayedEffect::Live => {
                self.mode = Mode::Live;
                let value = self.clock.now();
                self.store
                    .append(&self.execution, JournalEvent::NowRecorded { seq, value })
                    .map_err(EngineError::from)?;
                Ok(value)
            }
        }
    }

    /// Journaled randomness. Replay returns the original bytes.
    ///
    /// # Errors
    /// `Nondeterminism` if the journal expected a different command at this
    /// position. `Journal` if the store cannot be reached.
    pub fn random(&mut self) -> Result<RandomBytes, EngineError> {
        self.refuse_after_crash()?;
        let seq = self.seq;
        self.seq = self.seq.next().map_err(EngineError::from)?;

        let decision = match &mut self.mode {
            Mode::Replaying(cursor) => match cursor.peek().cloned() {
                Some(JournalEvent::RandomRecorded { value, .. }) => {
                    cursor.advance();
                    ReplayedEffect::Recorded(Ok(value))
                }
                Some(event) => {
                    let expected = event.command_kind().unwrap_or(CommandKind::DrawRandom);
                    ReplayedEffect::Recorded(Err(EngineError::Nondeterminism {
                        seq,
                        expected,
                        got: CommandKind::DrawRandom,
                    }))
                }
                None => ReplayedEffect::Live,
            },
            Mode::Live => ReplayedEffect::Live,
        };

        match decision {
            ReplayedEffect::Recorded(result) => result,
            ReplayedEffect::Live => {
                self.mode = Mode::Live;
                let value = self.rng.next_bytes();
                self.store
                    .append(&self.execution, JournalEvent::RandomRecorded { seq, value })
                    .map_err(EngineError::from)?;
                Ok(value)
            }
        }
    }

    /// Durable timer. Journals the wall-clock `deadline`; survives restart.
    /// Blocks through the `Clock` trait, so it is instant under `TestClock`.
    ///
    /// # Errors
    /// `Nondeterminism` if the journal expected a different command at this
    /// position. `Journal` if the store cannot be reached.
    pub fn sleep_until(&mut self, deadline: Deadline) -> Result<(), EngineError> {
        self.refuse_after_crash()?;
        let seq = self.seq;
        self.seq = self.seq.next().map_err(EngineError::from)?;

        let decision = match &mut self.mode {
            Mode::Replaying(cursor) => match cursor.peek().cloned() {
                Some(JournalEvent::TimerScheduled {
                    deadline: journaled,
                    ..
                }) if journaled == deadline => {
                    cursor.advance();
                    match cursor.peek().cloned() {
                        Some(JournalEvent::TimerFired { .. }) => {
                            cursor.advance();
                            SleepDecision::UseRecorded(Ok(()))
                        }
                        Some(event) => {
                            let expected = event.command_kind().unwrap_or(CommandKind::Sleep);
                            SleepDecision::UseRecorded(Err(EngineError::Nondeterminism {
                                seq,
                                expected,
                                got: CommandKind::Sleep,
                            }))
                        }
                        None => SleepDecision::Rearm,
                    }
                }
                Some(event) => {
                    let expected = event.command_kind().unwrap_or(CommandKind::Sleep);
                    SleepDecision::UseRecorded(Err(EngineError::Nondeterminism {
                        seq,
                        expected,
                        got: CommandKind::Sleep,
                    }))
                }
                None => SleepDecision::RunLive,
            },
            Mode::Live => SleepDecision::RunLive,
        };

        match decision {
            SleepDecision::UseRecorded(result) => result,
            SleepDecision::Rearm => {
                self.mode = Mode::Live;
                self.clock.sleep_until(deadline.timestamp());
                self.store
                    .append(&self.execution, JournalEvent::TimerFired { seq })
                    .map_err(EngineError::from)?;
                self.checkpoint(CrashPoint::AfterTimerFired(seq))
            }
            SleepDecision::RunLive => {
                self.mode = Mode::Live;
                self.store
                    .append(
                        &self.execution,
                        JournalEvent::TimerScheduled { seq, deadline },
                    )
                    .map_err(EngineError::from)?;
                self.checkpoint(CrashPoint::AfterTimerScheduled(seq))?;
                self.clock.sleep_until(deadline.timestamp());
                self.store
                    .append(&self.execution, JournalEvent::TimerFired { seq })
                    .map_err(EngineError::from)?;
                self.checkpoint(CrashPoint::AfterTimerFired(seq))
            }
        }
    }

    /// Replays a recorded step result or runs `f` with a stable idempotency key.
    /// The step is a replay and recovery boundary, not an atomic transaction.
    /// Body effects can partially execute or finish before their result is
    /// journaled and can repeat on recovery.
    pub fn step<F>(&mut self, name: StepName, f: F) -> Result<EventPayload, StepError>
    where
        F: FnOnce(IdempotencyKey) -> Result<EventPayload, StepErrorRecord>,
    {
        self.refuse_after_crash().map_err(StepError::Engine)?;
        let seq = self.seq;
        self.seq = self.seq.next().map_err(EngineError::from)?;

        let decision = match &mut self.mode {
            Mode::Replaying(cursor) => match cursor.peek().cloned() {
                Some(JournalEvent::StepScheduled {
                    name: journaled, ..
                }) if journaled == name => {
                    cursor.advance();
                    match Self::replay_step_outcome(cursor, seq) {
                        ReplayedStep::Recorded(result) => StepDecision::UseRecorded(result),
                        ReplayedStep::Rerun { attempt } => {
                            StepDecision::RunLive(LiveEntry::Retry { attempt })
                        }
                    }
                }
                Some(event) => {
                    let expected = event.command_kind().unwrap_or(CommandKind::RunStep);
                    StepDecision::UseRecorded(Err(StepError::Engine(EngineError::Nondeterminism {
                        seq,
                        expected,
                        got: CommandKind::RunStep,
                    })))
                }
                None => StepDecision::RunLive(LiveEntry::Fresh),
            },
            Mode::Live => StepDecision::RunLive(LiveEntry::Fresh),
        };

        match decision {
            StepDecision::UseRecorded(result) => result,
            StepDecision::RunLive(entry) => {
                self.mode = Mode::Live;
                self.run_live(seq, name, f, entry)
            }
        }
    }

    /// Resolves a step already matched against a journaled `StepScheduled`:
    /// consumes the attempts that follow and returns the recorded
    /// `StepCompleted`/`StepFailed` outcome, or signals a rerun.
    fn replay_step_outcome(cursor: &mut ReplayCursor, seq: Seq) -> ReplayedStep {
        // Every `StepStarted` at this position is one attempt; a crash-cut
        // attempt leaves its `StepStarted` behind with no outcome after it,
        // so several can accumulate before one completes.
        let mut next_attempt = Ok(Attempt::first());
        while let Some(JournalEvent::StepStarted { attempt, .. }) = cursor.peek() {
            next_attempt = attempt.next();
            cursor.advance();
        }
        match cursor.peek().cloned() {
            Some(JournalEvent::StepCompleted { result, .. }) => {
                cursor.advance();
                ReplayedStep::Recorded(Ok(result))
            }
            Some(JournalEvent::StepFailed { error, .. }) => {
                cursor.advance();
                ReplayedStep::Recorded(Err(StepError::Failed(error)))
            }
            Some(event) => {
                let expected = event.command_kind().unwrap_or(CommandKind::RunStep);
                ReplayedStep::Recorded(Err(StepError::Engine(EngineError::Nondeterminism {
                    seq,
                    expected,
                    got: CommandKind::RunStep,
                })))
            }
            // Journal ends here: the process died mid-step, after
            // `StepScheduled` (`CrashPoint::AfterStepScheduled`) or after
            // `StepStarted` (`AfterStepStarted` / `AfterSideEffect`). No
            // outcome was recorded either way, so rerun as the next attempt.
            None => match next_attempt {
                Ok(attempt) => ReplayedStep::Rerun { attempt },
                Err(error) => {
                    ReplayedStep::Recorded(Err(StepError::Engine(EngineError::Attempt(error))))
                }
            },
        }
    }

    /// Executes `f` for real: schedules (unless a crash-cut attempt already
    /// did), starts, runs, and journals the outcome.
    fn run_live<F>(
        &mut self,
        seq: Seq,
        name: StepName,
        f: F,
        entry: LiveEntry,
    ) -> Result<EventPayload, StepError>
    where
        F: FnOnce(IdempotencyKey) -> Result<EventPayload, StepErrorRecord>,
    {
        let attempt = match entry {
            LiveEntry::Fresh => {
                self.checkpoint(CrashPoint::BeforeStepScheduled(seq))?;
                self.store
                    .append(&self.execution, JournalEvent::StepScheduled { seq, name })
                    .map_err(EngineError::from)?;
                self.checkpoint(CrashPoint::AfterStepScheduled(seq))?;
                Attempt::first()
            }
            LiveEntry::Retry { attempt } => attempt,
        };
        self.store
            .append(&self.execution, JournalEvent::StepStarted { seq, attempt })
            .map_err(EngineError::from)?;
        self.checkpoint(CrashPoint::AfterStepStarted(seq))?;

        let key = IdempotencyKey::new(self.execution, seq);
        match f(key) {
            Ok(result) => {
                self.checkpoint(CrashPoint::AfterSideEffect(seq))?;
                self.store
                    .append(
                        &self.execution,
                        JournalEvent::StepCompleted {
                            seq,
                            result: result.clone(),
                        },
                    )
                    .map_err(EngineError::from)?;
                self.checkpoint(CrashPoint::AfterStepCompleted(seq))?;
                Ok(result)
            }
            Err(error) => {
                self.store
                    .append(
                        &self.execution,
                        JournalEvent::StepFailed {
                            seq,
                            attempt,
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
    use std::cell::Cell;
    use std::cell::RefCell;

    use super::*;
    use crate::failpoints::CrashOnce;
    use crate::random::RandomBytes;
    use crate::random::RngSource;
    use crate::stores::memory::MemoryJournal;
    use crate::time::Deadline;
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

    /// Records the deadline it was told to sleep toward, without ever
    /// blocking. This lets `sleep_until` tests assert what the engine asked
    /// the clock to wait for.
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

    fn signup_execution() -> ExecutionId {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x51; // 'Q', arbitrary deterministic marker
        let mut rng = FixedRng { bytes };
        ExecutionId::generate(&mut rng)
    }

    /// Fixed clock reading for `step()` tests, which never call `ctx.now()`.
    fn unused_clock() -> TestClock {
        TestClock::at(Timestamp::from_millis_since_epoch(1_753_401_600_000))
    }

    /// Fixed rng draw for `step()` tests, which never call `ctx.random()`.
    fn unused_rng() -> FixedRng {
        FixedRng { bytes: [0u8; 32] }
    }

    fn charge_renewal_deadline() -> Timestamp {
        Timestamp::from_millis_since_epoch(1_753_401_600_000)
    }

    fn charge_renewal_bytes() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x52; // 'R', arbitrary deterministic marker
        bytes
    }

    fn charge_card() -> StepName {
        StepName::new("charge-card").unwrap()
    }

    fn create_account() -> StepName {
        StepName::new("create-account").unwrap()
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
        type Lease<'a> = ();

        fn acquire(&self, _id: &ExecutionId) -> Result<(), JournalError> {
            Err(JournalError::Poisoned)
        }

        fn append(
            &self,
            _id: &ExecutionId,
            _event: JournalEvent,
        ) -> Result<EventOffset, JournalError> {
            Err(JournalError::Poisoned)
        }

        fn load(&self, _id: &ExecutionId) -> Result<Journal, JournalError> {
            Err(JournalError::Poisoned)
        }
    }

    struct ForbiddenEffects;

    impl Clock for ForbiddenEffects {
        fn now(&self) -> Timestamp {
            panic!("an exhausted command position must not read the clock")
        }

        fn sleep_until(&self, _deadline: Timestamp) {
            panic!("an exhausted command position must not wait")
        }
    }

    impl RngSource for ForbiddenEffects {
        fn next_bytes(&mut self) -> RandomBytes {
            panic!("an exhausted command position must not draw randomness")
        }
    }

    #[test]
    fn every_command_at_the_maximum_sequence_refuses_before_effects_or_appends() {
        for command in [
            CommandKind::RunStep,
            CommandKind::ReadNow,
            CommandKind::DrawRandom,
            CommandKind::Sleep,
        ] {
            let store = MemoryJournal::new();
            let execution = signup_execution();
            let mut rng = ForbiddenEffects;
            let mut ctx = WorkflowCtx::new(
                &store,
                execution,
                ReplayCursor::new(Journal::empty()),
                &ForbiddenEffects,
                &mut rng,
            );
            let last = Seq::from_record(u64::MAX);
            ctx.seq = last;

            let result = match command {
                CommandKind::RunStep => match ctx.step(charge_card(), |_key| {
                    panic!("an exhausted command position must not run a step")
                }) {
                    Ok(_) => Ok(()),
                    Err(StepError::Engine(error)) => Err(error),
                    Err(StepError::Failed(error)) => {
                        panic!("unexpected business failure: {error:?}")
                    }
                },
                CommandKind::ReadNow => ctx.now().map(|_| ()),
                CommandKind::DrawRandom => ctx.random().map(|_| ()),
                CommandKind::Sleep => ctx.sleep_until(Deadline::at(charge_renewal_deadline())),
            };

            assert_eq!(result, Err(EngineError::Sequence(SeqError::Overflow)));
            assert_eq!(ctx.seq, last);
            assert!(store.load(&execution).unwrap().is_empty());
        }
    }

    #[test]
    fn raw_cursor_with_an_interrupted_maximum_attempt_refuses_a_live_retry() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::new(vec![
                JournalEvent::StepScheduled {
                    seq: Seq::zero(),
                    name: charge_card(),
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero(),
                    attempt: Attempt::new(u32::MAX).unwrap(),
                },
            ])),
            &clock,
            &mut rng,
        );

        let result = ctx.step(charge_card(), |_key| {
            panic!("an exhausted attempt must not run again")
        });

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::Attempt(AttemptOverflow)))
        );
        assert!(store.load(&execution).unwrap().is_empty());
    }

    #[test]
    fn raw_cursor_with_a_settled_maximum_attempt_returns_its_recorded_result() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::new(vec![
                JournalEvent::StepScheduled {
                    seq: Seq::zero(),
                    name: charge_card(),
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero(),
                    attempt: Attempt::new(u32::MAX).unwrap(),
                },
                JournalEvent::StepCompleted {
                    seq: Seq::zero(),
                    result: charge_confirmation(),
                },
            ])),
            &clock,
            &mut rng,
        );

        let result = ctx.step(charge_card(), |_key| {
            panic!("a settled step must not run again")
        });

        assert_eq!(result, Ok(charge_confirmation()));
        assert!(store.load(&execution).unwrap().is_empty());
    }

    #[test]
    fn advance_on_an_exhausted_cursor_preserves_its_event_offset() {
        let mut cursor = ReplayCursor::new(Journal::empty());

        cursor.advance();

        assert_eq!(cursor.position.get(), 0);
        assert!(cursor.is_exhausted());
    }

    #[test]
    fn ensure_replay_complete_in_live_mode_accepts_terminal_recording() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

        let result = ctx.ensure_replay_complete();

        assert_eq!(result, Ok(()));
        assert!(store.load(&execution).unwrap().is_empty());
    }

    #[test]
    fn ensure_replay_complete_with_unread_events_refuses_without_advancing() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let recorded = charge_renewal_deadline();
        let clock = TestClock::at(Timestamp::from_millis_since_epoch(0));
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::new(vec![JournalEvent::NowRecorded {
                seq: Seq::zero(),
                value: recorded,
            }])),
            &clock,
            &mut rng,
        );

        let incomplete = ctx.ensure_replay_complete();

        assert_eq!(incomplete, Err(EngineError::UnconsumedHistory));
        assert_eq!(ctx.seq, Seq::zero());
        assert_eq!(ctx.now(), Ok(recorded));
        assert_eq!(ctx.ensure_replay_complete(), Ok(()));
        assert!(store.load(&execution).unwrap().is_empty());
    }

    #[test]
    fn live_step_with_ok_closure_journals_and_returns_the_result() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );
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
                    seq: Seq::zero().next().unwrap(),
                    name: create_account,
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero().next().unwrap(),
                    attempt: Attempt::first(),
                },
                JournalEvent::StepCompleted {
                    seq: Seq::zero().next().unwrap(),
                    result: account_created,
                },
            ]
        );
    }

    #[test]
    fn replaying_step_started_with_no_further_events_reruns_the_step_live() {
        // Crash between StepStarted and StepCompleted: the journal
        // ends right after StepStarted. The step must rerun live, not error.
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
        ]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        // The StepScheduled/StepStarted pair above seeds the replay cursor
        // only; like the other tests here, it is never written to `store`.
        // The rerun is attempt 2 and does not re-schedule, keeping one
        // StepScheduled per Seq.
        assert_eq!(result, Ok(charge_confirmation()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[
                JournalEvent::StepStarted {
                    seq: Seq::zero(),
                    attempt: Attempt::first().next().unwrap(),
                },
                JournalEvent::StepCompleted {
                    seq: Seq::zero(),
                    result: charge_confirmation(),
                },
            ]
        );
    }

    #[test]
    fn replaying_two_crash_cut_attempts_reruns_the_step_as_the_third_attempt() {
        // Two crashes in a row, each leaving a StepStarted with no outcome
        // after it. Replay consumes both and numbers the rerun accordingly.
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
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first().next().unwrap(),
            },
        ]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        assert_eq!(result, Ok(charge_confirmation()));
        assert_eq!(
            store.load(&execution).unwrap().events().first(),
            Some(&JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first().next().unwrap().next().unwrap(),
            })
        );
    }

    #[test]
    fn replaying_a_completed_step_after_a_crash_cut_attempt_returns_the_recorded_result() {
        // A crash-cut attempt followed by a successful one: replay must skip
        // past both StepStarted events and answer from StepCompleted.
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
            JournalEvent::StepStarted {
                seq: Seq::zero(),
                attempt: Attempt::first().next().unwrap(),
            },
            JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: charge_confirmation(),
            },
        ]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.step(charge_card(), |_key| {
            panic!("replayed step must not run its closure")
        });

        assert_eq!(result, Ok(charge_confirmation()));
        assert_eq!(store.load(&execution).unwrap().events(), &[]);
    }

    #[test]
    fn replaying_step_scheduled_with_no_further_events_reruns_the_step() {
        // The process died between StepScheduled and StepStarted
        // (`CrashPoint::AfterStepScheduled`): no outcome was recorded, so
        // the step runs live and journals a fresh attempt.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        }]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.step(charge_card(), |_key| Ok(charge_confirmation()));

        assert_eq!(result, Ok(charge_confirmation()));
        assert_eq!(
            store.load(&execution).unwrap().events().last(),
            Some(&JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: charge_confirmation(),
            })
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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

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
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

        let mut observed_key = None;
        let _ = ctx.step(charge_card(), |key| {
            observed_key = Some(key);
            Ok(charge_confirmation())
        });

        let key = observed_key.unwrap();
        assert_eq!(key.execution(), execution);
        assert_eq!(key.seq(), Seq::zero());
    }

    #[test]
    fn live_now_journals_the_clock_reading_and_returns_it() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = TestClock::at(charge_renewal_deadline());
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

        let result = ctx.now();

        assert_eq!(result, Ok(charge_renewal_deadline()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[JournalEvent::NowRecorded {
                seq: Seq::zero(),
                value: charge_renewal_deadline(),
            }]
        );
    }

    #[test]
    fn replaying_now_recorded_returns_the_recorded_timestamp_without_reading_the_clock() {
        // The clock is set to a different instant than the one recorded, so
        // a live read here would fail the assertion.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::NowRecorded {
            seq: Seq::zero(),
            value: charge_renewal_deadline(),
        }]);
        let clock = TestClock::at(Timestamp::from_millis_since_epoch(0));
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.now();

        assert_eq!(result, Ok(charge_renewal_deadline()));
    }

    #[test]
    fn replaying_now_with_mismatched_event_returns_nondeterminism_error() {
        // Journal recorded a step at this position, code now asks for the
        // clock.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        }]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.now();

        assert_eq!(
            result,
            Err(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got: CommandKind::ReadNow,
            })
        );
    }

    #[test]
    fn replaying_now_cursor_exhausted_switches_to_live() {
        // Journal has exactly one recorded step; a now() call after
        // replaying it must find replay exhausted and go live.
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
        let clock = TestClock::at(charge_renewal_deadline());
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );
        let replayed = ctx.step(charge_card(), |_key| {
            panic!("closure must not run during replay")
        });
        assert_eq!(replayed, Ok(charge_confirmation()));

        let result = ctx.now();

        assert_eq!(result, Ok(charge_renewal_deadline()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[JournalEvent::NowRecorded {
                seq: Seq::zero().next().unwrap(),
                value: charge_renewal_deadline(),
            }]
        );
    }

    #[test]
    fn live_random_journals_the_drawn_bytes_and_returns_them() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = FixedRng {
            bytes: charge_renewal_bytes(),
        };
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );

        let result = ctx.random();

        assert_eq!(result, Ok(RandomBytes::new(charge_renewal_bytes())));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[JournalEvent::RandomRecorded {
                seq: Seq::zero(),
                value: RandomBytes::new(charge_renewal_bytes()),
            }]
        );
    }

    #[test]
    fn replaying_random_recorded_returns_the_recorded_bytes_without_drawing() {
        // The rng is set to draw different bytes than the ones recorded, so
        // a live draw here would fail the assertion.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::RandomRecorded {
            seq: Seq::zero(),
            value: RandomBytes::new(charge_renewal_bytes()),
        }]);
        let clock = unused_clock();
        let mut rng = FixedRng { bytes: [0u8; 32] };
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.random();

        assert_eq!(result, Ok(RandomBytes::new(charge_renewal_bytes())));
    }

    #[test]
    fn replaying_random_with_mismatched_event_returns_nondeterminism_error() {
        // Journal recorded a step at this position, code now asks to draw
        // randomness.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        }]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.random();

        assert_eq!(
            result,
            Err(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got: CommandKind::DrawRandom,
            })
        );
    }

    #[test]
    fn live_sleep_until_journals_the_timer_and_waits_for_the_deadline() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = RecordingClock::at(Timestamp::from_millis_since_epoch(0));
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(Journal::empty()),
            &clock,
            &mut rng,
        );
        let deadline = Deadline::at(charge_renewal_deadline());

        let result = ctx.sleep_until(deadline);

        assert_eq!(result, Ok(()));
        assert_eq!(*clock.slept_until.borrow(), Some(charge_renewal_deadline()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[
                JournalEvent::TimerScheduled {
                    seq: Seq::zero(),
                    deadline,
                },
                JournalEvent::TimerFired { seq: Seq::zero() },
            ]
        );
    }

    #[test]
    fn replaying_timer_scheduled_and_fired_returns_ok_without_sleeping() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let deadline = Deadline::at(charge_renewal_deadline());
        let journal = Journal::new(vec![
            JournalEvent::TimerScheduled {
                seq: Seq::zero(),
                deadline,
            },
            JournalEvent::TimerFired { seq: Seq::zero() },
        ]);
        let clock = RecordingClock::at(Timestamp::from_millis_since_epoch(0));
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.sleep_until(deadline);

        assert_eq!(result, Ok(()));
        assert_eq!(*clock.slept_until.borrow(), None);
    }

    #[test]
    fn replaying_timer_deadline_mismatch_returns_nondeterminism_error() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journaled_deadline = Deadline::at(charge_renewal_deadline());
        let journal = Journal::new(vec![JournalEvent::TimerScheduled {
            seq: Seq::zero(),
            deadline: journaled_deadline,
        }]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );
        let different_deadline = Deadline::at(Timestamp::from_millis_since_epoch(0));

        let result = ctx.sleep_until(different_deadline);

        assert_eq!(
            result,
            Err(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::Sleep,
                got: CommandKind::Sleep,
            })
        );
    }

    #[test]
    fn replaying_timer_with_mismatched_event_returns_nondeterminism_error() {
        // Journal recorded a step at this position, code now asks to sleep.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        }]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.sleep_until(Deadline::at(charge_renewal_deadline()));

        assert_eq!(
            result,
            Err(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got: CommandKind::Sleep,
            })
        );
    }

    #[test]
    fn replaying_timer_scheduled_followed_by_wrong_event_is_nondeterminism() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let deadline = Deadline::at(charge_renewal_deadline());
        let journal = Journal::new(vec![
            JournalEvent::TimerScheduled {
                seq: Seq::zero(),
                deadline,
            },
            JournalEvent::ExecutionCompleted {
                output: EventPayload::new(b"done".to_vec()),
            },
        ]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.sleep_until(deadline);

        assert_eq!(
            result,
            Err(EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::Sleep,
                got: CommandKind::Sleep,
            })
        );
    }

    #[test]
    fn replaying_timer_scheduled_with_no_further_events_rearms_the_timer() {
        // The process died between TimerScheduled and TimerFired. Recovery
        // must not re-append TimerScheduled. It only waits out the
        // already-journaled deadline and journal TimerFired.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let deadline = Deadline::at(charge_renewal_deadline());
        let journal = Journal::new(vec![JournalEvent::TimerScheduled {
            seq: Seq::zero(),
            deadline,
        }]);
        let clock = RecordingClock::at(Timestamp::from_millis_since_epoch(0));
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );

        let result = ctx.sleep_until(deadline);

        assert_eq!(result, Ok(()));
        assert_eq!(*clock.slept_until.borrow(), Some(charge_renewal_deadline()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[JournalEvent::TimerFired { seq: Seq::zero() }]
        );
    }

    #[test]
    fn replaying_timer_cursor_exhausted_switches_to_live() {
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
        let clock = RecordingClock::at(Timestamp::from_millis_since_epoch(0));
        let mut rng = unused_rng();
        let mut ctx = WorkflowCtx::new(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
        );
        let replayed = ctx.step(charge_card(), |_key| {
            panic!("closure must not run during replay")
        });
        assert_eq!(replayed, Ok(charge_confirmation()));
        let deadline = Deadline::at(charge_renewal_deadline());

        let result = ctx.sleep_until(deadline);

        assert_eq!(result, Ok(()));
        assert_eq!(*clock.slept_until.borrow(), Some(charge_renewal_deadline()));
        let journal = store.load(&execution).unwrap();
        assert_eq!(
            journal.events(),
            &[
                JournalEvent::TimerScheduled {
                    seq: Seq::zero().next().unwrap(),
                    deadline,
                },
                JournalEvent::TimerFired {
                    seq: Seq::zero().next().unwrap(),
                },
            ]
        );
    }

    #[test]
    fn new_cursor_over_empty_journal_is_exhausted() {
        let cursor = ReplayCursor::new(Journal::empty());

        assert!(cursor.is_exhausted());
        assert_eq!(cursor.peek(), None);
    }

    #[test]
    fn new_cursor_over_nonempty_journal_is_not_exhausted() {
        let journal = Journal::new(vec![JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        }]);

        let cursor = ReplayCursor::new(journal);

        assert!(!cursor.is_exhausted());
    }

    #[test]
    fn peek_returns_the_event_at_the_current_position() {
        let scheduled = JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        };
        let cursor = ReplayCursor::new(Journal::new(vec![scheduled.clone()]));

        assert_eq!(cursor.peek(), Some(&scheduled));
    }

    #[test]
    fn advance_moves_to_the_next_event() {
        let scheduled = JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        };
        let started = JournalEvent::StepStarted {
            seq: Seq::zero(),
            attempt: Attempt::first(),
        };
        let mut cursor = ReplayCursor::new(Journal::new(vec![scheduled, started.clone()]));

        cursor.advance();

        assert_eq!(cursor.peek(), Some(&started));
    }

    #[test]
    fn advance_past_the_last_event_exhausts_the_cursor() {
        let scheduled = JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: charge_card(),
        };
        let mut cursor = ReplayCursor::new(Journal::new(vec![scheduled]));

        cursor.advance();

        assert!(cursor.is_exhausted());
        assert_eq!(cursor.peek(), None);
    }

    /// Counts closure executions, which is what the journal cannot be trusted
    /// to report across a crash.
    struct EffectCounter {
        runs: Cell<u32>,
    }

    impl EffectCounter {
        fn new() -> Self {
            Self { runs: Cell::new(0) }
        }

        fn charge(&self) -> EventPayload {
            self.runs.set(self.runs.get() + 1);
            charge_confirmation()
        }

        fn runs(&self) -> u32 {
            self.runs.get()
        }
    }

    /// Builds a live context whose failpoints crash once at `point`.
    fn crashing_ctx<'a>(
        store: &'a MemoryJournal,
        execution: ExecutionId,
        clock: &'a TestClock,
        rng: &'a mut FixedRng,
        policy: &'a CrashOnce,
    ) -> WorkflowCtx<'a, MemoryJournal> {
        WorkflowCtx::with_failpoints(
            store,
            execution,
            ReplayCursor::new(Journal::empty()),
            clock,
            rng,
            policy,
        )
    }

    #[test]
    fn crash_before_step_scheduled_journals_nothing_and_never_runs_the_closure() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::BeforeStepScheduled(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.step(charge_card(), |_key| Ok(effects.charge()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::InjectedCrash(
                CrashPoint::BeforeStepScheduled(Seq::zero())
            )))
        );
        assert_eq!(effects.runs(), 0);
        assert_eq!(store.load(&execution).unwrap().events(), &[]);
    }

    #[test]
    fn crash_after_step_scheduled_leaves_only_step_scheduled_and_never_runs_the_closure() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterStepScheduled(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.step(charge_card(), |_key| Ok(effects.charge()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::InjectedCrash(
                CrashPoint::AfterStepScheduled(Seq::zero())
            )))
        );
        assert_eq!(effects.runs(), 0);
        assert_eq!(
            store.load(&execution).unwrap().events(),
            &[JournalEvent::StepScheduled {
                seq: Seq::zero(),
                name: charge_card(),
            }]
        );
    }

    #[test]
    fn crash_after_step_started_leaves_the_step_unfinished_and_never_runs_the_closure() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterStepStarted(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.step(charge_card(), |_key| Ok(effects.charge()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::InjectedCrash(
                CrashPoint::AfterStepStarted(Seq::zero())
            )))
        );
        assert_eq!(effects.runs(), 0);
        assert_eq!(
            store.load(&execution).unwrap().events(),
            &[
                JournalEvent::StepScheduled {
                    seq: Seq::zero(),
                    name: charge_card(),
                },
                JournalEvent::StepStarted {
                    seq: Seq::zero(),
                    attempt: Attempt::first(),
                },
            ]
        );
    }

    #[test]
    fn crash_after_the_side_effect_runs_the_closure_but_journals_no_step_completed() {
        // The side effect landed, StepCompleted did not. Recovery cannot
        // tell, so it reruns the step and the effect happens twice.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.step(charge_card(), |_key| Ok(effects.charge()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::InjectedCrash(
                CrashPoint::AfterSideEffect(Seq::zero())
            )))
        );
        assert_eq!(effects.runs(), 1);
        let journal = store.load(&execution).unwrap();
        assert_eq!(journal.len(), 2);
        assert!(matches!(
            journal.events().last(),
            Some(JournalEvent::StepStarted { .. })
        ));
    }

    #[test]
    fn crash_after_step_completed_leaves_the_step_fully_journaled() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterStepCompleted(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.step(charge_card(), |_key| Ok(effects.charge()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::InjectedCrash(
                CrashPoint::AfterStepCompleted(Seq::zero())
            )))
        );
        assert_eq!(effects.runs(), 1);
        assert_eq!(
            store.load(&execution).unwrap().events().last(),
            Some(&JournalEvent::StepCompleted {
                seq: Seq::zero(),
                result: charge_confirmation(),
            })
        );
    }

    #[test]
    fn crash_after_timer_scheduled_leaves_the_timer_armed_and_unfired() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let deadline = Deadline::at(charge_renewal_deadline());
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterTimerScheduled(Seq::zero()));
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.sleep_until(deadline);

        assert_eq!(
            result,
            Err(EngineError::InjectedCrash(CrashPoint::AfterTimerScheduled(
                Seq::zero()
            )))
        );
        assert_eq!(
            store.load(&execution).unwrap().events(),
            &[JournalEvent::TimerScheduled {
                seq: Seq::zero(),
                deadline,
            }]
        );
    }

    #[test]
    fn crash_after_timer_fired_leaves_the_timer_complete() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let deadline = Deadline::at(charge_renewal_deadline());
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterTimerFired(Seq::zero()));
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.sleep_until(deadline);

        assert_eq!(
            result,
            Err(EngineError::InjectedCrash(CrashPoint::AfterTimerFired(
                Seq::zero()
            )))
        );
        assert_eq!(
            store.load(&execution).unwrap().events().last(),
            Some(&JournalEvent::TimerFired { seq: Seq::zero() })
        );
    }

    #[test]
    fn crash_during_a_rearmed_timer_leaves_the_journal_untouched_past_timer_fired() {
        // Recovery re-arms an already-journaled timer; the failpoint fires
        // after TimerFired lands, so TimerScheduled is never re-appended.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let deadline = Deadline::at(charge_renewal_deadline());
        let journal = Journal::new(vec![JournalEvent::TimerScheduled {
            seq: Seq::zero(),
            deadline,
        }]);
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterTimerFired(Seq::zero()));
        let mut ctx = WorkflowCtx::with_failpoints(
            &store,
            execution,
            ReplayCursor::new(journal),
            &clock,
            &mut rng,
            &policy,
        );

        let result = ctx.sleep_until(deadline);

        assert_eq!(
            result,
            Err(EngineError::InjectedCrash(CrashPoint::AfterTimerFired(
                Seq::zero()
            )))
        );
        assert_eq!(
            store.load(&execution).unwrap().events(),
            &[JournalEvent::TimerFired { seq: Seq::zero() }]
        );
    }

    #[test]
    fn a_step_after_a_crash_is_refused_without_touching_the_journal() {
        // A workflow that swallows the crash error and carries on must not
        // journal anything: the process it models is already dead.
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);
        let _crashed = ctx.step(charge_card(), |_key| Ok(effects.charge()));
        let events_at_crash = store.load(&execution).unwrap().len();

        let result = ctx.step(create_account(), |_key| Ok(effects.charge()));

        assert_eq!(
            result,
            Err(StepError::Engine(EngineError::InjectedCrash(
                CrashPoint::AfterSideEffect(Seq::zero())
            )))
        );
        assert_eq!(effects.runs(), 1);
        assert_eq!(store.load(&execution).unwrap().len(), events_at_crash);
    }

    #[test]
    fn now_after_a_crash_is_refused_without_touching_the_journal() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);
        let _crashed = ctx.step(charge_card(), |_key| Ok(effects.charge()));
        let events_at_crash = store.load(&execution).unwrap().len();

        let result = ctx.now();

        assert_eq!(
            result,
            Err(EngineError::InjectedCrash(CrashPoint::AfterSideEffect(
                Seq::zero()
            )))
        );
        assert_eq!(store.load(&execution).unwrap().len(), events_at_crash);
    }

    #[test]
    fn random_after_a_crash_is_refused_without_touching_the_journal() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);
        let _crashed = ctx.step(charge_card(), |_key| Ok(effects.charge()));
        let events_at_crash = store.load(&execution).unwrap().len();

        let result = ctx.random();

        assert_eq!(
            result,
            Err(EngineError::InjectedCrash(CrashPoint::AfterSideEffect(
                Seq::zero()
            )))
        );
        assert_eq!(store.load(&execution).unwrap().len(), events_at_crash);
    }

    #[test]
    fn sleep_until_after_a_crash_is_refused_without_touching_the_journal() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);
        let _crashed = ctx.step(charge_card(), |_key| Ok(effects.charge()));
        let events_at_crash = store.load(&execution).unwrap().len();

        let result = ctx.sleep_until(Deadline::at(charge_renewal_deadline()));

        assert_eq!(
            result,
            Err(EngineError::InjectedCrash(CrashPoint::AfterSideEffect(
                Seq::zero()
            )))
        );
        assert_eq!(store.load(&execution).unwrap().len(), events_at_crash);
    }

    #[test]
    fn crash_status_is_intact_on_a_run_where_no_failpoint_fires() {
        let store = MemoryJournal::new();
        let execution = signup_execution();
        let clock = unused_clock();
        let mut rng = unused_rng();
        let policy = CrashOnce::new(CrashPoint::AfterSideEffect(Seq::zero().next().unwrap()));
        let effects = EffectCounter::new();
        let mut ctx = crashing_ctx(&store, execution, &clock, &mut rng, &policy);

        let result = ctx.step(charge_card(), |_key| Ok(effects.charge()));

        assert_eq!(result, Ok(charge_confirmation()));
        assert_eq!(ctx.crash_status(), CrashStatus::Intact);
    }
}
