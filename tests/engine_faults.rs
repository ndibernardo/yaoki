//! Command faults stop the current run even when workflow code catches them.

use std::cell::Cell;
use std::cell::RefCell;

use proptest::proptest;
use yaoki::command::CommandKind;
use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventOffset;
use yaoki::journal::EventPayload;
use yaoki::journal::Journal;
use yaoki::journal::JournalError;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::Attempt;
use yaoki::step::StepError;
use yaoki::step::StepErrorRecord;
use yaoki::step::StepName;
use yaoki::stores::memory::MemoryJournal;
use yaoki::time::Clock;
use yaoki::time::Deadline;
use yaoki::time::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendSite {
    Invocation,
    StepSchedule,
    AttemptStart,
    StepCompletion,
    StepFailure,
    ClockRead,
    RandomDraw,
    TimerSchedule,
    TimerFiring,
    ExecutionCompletion,
    ExecutionFailure,
}

fn append_site(event: &JournalEvent) -> AppendSite {
    match event {
        JournalEvent::ExecutionStarted { .. } => AppendSite::Invocation,
        JournalEvent::StepScheduled { .. } => AppendSite::StepSchedule,
        JournalEvent::StepStarted { .. } => AppendSite::AttemptStart,
        JournalEvent::StepCompleted { .. } => AppendSite::StepCompletion,
        JournalEvent::StepFailed { .. } => AppendSite::StepFailure,
        JournalEvent::NowRecorded { .. } => AppendSite::ClockRead,
        JournalEvent::RandomRecorded { .. } => AppendSite::RandomDraw,
        JournalEvent::TimerScheduled { .. } => AppendSite::TimerSchedule,
        JournalEvent::TimerFired { .. } => AppendSite::TimerFiring,
        JournalEvent::ExecutionCompleted { .. } => AppendSite::ExecutionCompletion,
        JournalEvent::ExecutionFailed { .. } => AppendSite::ExecutionFailure,
    }
}

#[derive(Debug, Clone, Copy)]
enum CommitMode {
    BeforeCommit,
    AfterCommit,
}

#[derive(Clone, Copy)]
enum FaultSchedule {
    Armed { site: AppendSite, mode: CommitMode },
    Clear,
}

fn storage_failure() -> JournalError {
    JournalError::Io {
        message: "signup journal append acknowledgment failed".to_owned(),
    }
}

// Interior mutability records calls and injects one deterministic fault through &self.
struct FaultJournal {
    inner: MemoryJournal,
    schedule: Cell<FaultSchedule>,
    attempted: RefCell<Vec<JournalEvent>>,
    at_fault: RefCell<Journal>,
    fault_call_count: Cell<usize>,
}

impl FaultJournal {
    fn new(schedule: FaultSchedule) -> Self {
        Self {
            inner: MemoryJournal::new(),
            schedule: Cell::new(schedule),
            attempted: RefCell::new(Vec::new()),
            at_fault: RefCell::new(Journal::empty()),
            fault_call_count: Cell::new(0),
        }
    }
}

impl JournalStore for FaultJournal {
    type Lease<'a> = <MemoryJournal as JournalStore>::Lease<'a>;

    fn acquire(&self, id: &ExecutionId) -> Result<Self::Lease<'_>, JournalError> {
        self.inner.acquire(id)
    }

    fn load(&self, id: &ExecutionId) -> Result<Journal, JournalError> {
        self.inner.load(id)
    }

    fn append(&self, id: &ExecutionId, event: JournalEvent) -> Result<EventOffset, JournalError> {
        self.attempted.borrow_mut().push(event.clone());
        match self.schedule.get() {
            FaultSchedule::Armed { site, mode } if append_site(&event) == site => {
                self.schedule.set(FaultSchedule::Clear);
                match mode {
                    CommitMode::BeforeCommit => {}
                    CommitMode::AfterCommit => {
                        self.inner.append(id, event)?;
                    }
                }
                *self.at_fault.borrow_mut() = self.inner.load(id)?;
                self.fault_call_count.set(self.attempted.borrow().len());
                Err(storage_failure())
            }
            FaultSchedule::Armed { .. } | FaultSchedule::Clear => self.inner.append(id, event),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Request {
    Charge,
    DeclineCharge,
    CreateAccount,
    Now,
    Random,
    Sleep(Deadline),
}

#[derive(Debug, Clone, Copy)]
enum Reaction {
    Propagate,
    CatchAndSucceed,
    CatchAndFail,
}

struct SignupWorkflow {
    requests: Vec<Request>,
    reaction: Reaction,
    calls: Cell<usize>,
    effects: Cell<usize>,
    faults: RefCell<Vec<EngineError>>,
}

impl SignupWorkflow {
    fn new(requests: Vec<Request>, reaction: Reaction) -> Self {
        Self {
            requests,
            reaction,
            calls: Cell::new(0),
            effects: Cell::new(0),
            faults: RefCell::new(Vec::new()),
        }
    }

    fn request(
        &self,
        ctx: &mut WorkflowCtx<'_, FaultJournal>,
        request: Request,
    ) -> Result<(), EngineError> {
        match request {
            Request::Now => ctx.now().map(|_| ()),
            Request::Random => ctx.random().map(|_| ()),
            Request::Sleep(deadline) => ctx.sleep_until(deadline),
            Request::Charge => {
                self.request_step(ctx, StepName::new("charge-payment").unwrap(), Ok(input()))
            }
            Request::DeclineCharge => self.request_step(
                ctx,
                StepName::new("charge-payment").unwrap(),
                Err(StepErrorRecord::new("payment declined")),
            ),
            Request::CreateAccount => {
                self.request_step(ctx, StepName::new("create-account").unwrap(), Ok(input()))
            }
        }
    }

    fn request_step(
        &self,
        ctx: &mut WorkflowCtx<'_, FaultJournal>,
        name: StepName,
        outcome: Result<EventPayload, StepErrorRecord>,
    ) -> Result<(), EngineError> {
        match ctx.step(name, |_key| {
            self.effects.set(self.effects.get() + 1);
            outcome
        }) {
            Ok(_) | Err(StepError::Failed(_)) => Ok(()),
            Err(StepError::Engine(error)) => Err(error),
        }
    }
}

impl Workflow<FaultJournal> for SignupWorkflow {
    // A string-wrapped fault cannot be recovered by inspecting this error type.
    type Error = String;

    fn name(&self) -> WorkflowName {
        WorkflowName::new("signup").unwrap()
    }

    fn version(&self) -> WorkflowVersion {
        WorkflowVersion::new("2026.08.01").unwrap()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, FaultJournal>,
        input: EventPayload,
    ) -> Result<EventPayload, String> {
        self.calls.set(self.calls.get() + 1);
        for request in &self.requests {
            match self.request(ctx, *request) {
                Ok(()) => {}
                Err(error) => {
                    let message = format!("signup command failed: {error:?}");
                    self.faults.borrow_mut().push(error);
                    match self.reaction {
                        Reaction::Propagate => return Err(message),
                        Reaction::CatchAndSucceed | Reaction::CatchAndFail => {}
                    }
                }
            }
        }
        match self.reaction {
            Reaction::Propagate | Reaction::CatchAndSucceed => Ok(input),
            Reaction::CatchAndFail => Err("signup refused".to_owned()),
        }
    }
}

struct CountingRng {
    calls: usize,
}

impl RngSource for CountingRng {
    fn next_bytes(&mut self) -> RandomBytes {
        self.calls += 1;
        RandomBytes::new([0x51; 32])
    }
}

struct CountingClock {
    reads: Cell<usize>,
    sleeps: Cell<usize>,
}

impl CountingClock {
    fn new() -> Self {
        Self {
            reads: Cell::new(0),
            sleeps: Cell::new(0),
        }
    }
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.reads.set(self.reads.get() + 1);
        deadline().timestamp()
    }

    fn sleep_until(&self, requested: Timestamp) {
        assert_eq!(requested, deadline().timestamp());
        self.sleeps.set(self.sleeps.get() + 1);
    }
}

fn execution() -> ExecutionId {
    ExecutionId::generate(&mut CountingRng { calls: 0 })
}

fn input() -> EventPayload {
    EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
}

fn deadline() -> Deadline {
    Deadline::at(Timestamp::from_millis_since_epoch(1_784_937_600_000))
}

fn later_requests() -> Vec<Request> {
    vec![
        Request::Now,
        Request::Random,
        Request::Sleep(deadline()),
        Request::CreateAccount,
    ]
}

fn assert_engine_fault(result: Result<EventPayload, RunError<String>>, expected: &EngineError) {
    match result {
        Err(RunError::Engine(error)) => assert_eq!(&error, expected),
        other => panic!("expected engine fault {expected:?}, got {other:?}"),
    }
}

#[test]
fn command_append_faults_stop_later_effects_and_terminal_recording_for_every_reaction() {
    for (site, first) in [
        (AppendSite::StepSchedule, Request::Charge),
        (AppendSite::AttemptStart, Request::Charge),
        (AppendSite::StepCompletion, Request::Charge),
        (AppendSite::StepFailure, Request::DeclineCharge),
        (AppendSite::ClockRead, Request::Now),
        (AppendSite::RandomDraw, Request::Random),
        (AppendSite::TimerSchedule, Request::Sleep(deadline())),
        (AppendSite::TimerFiring, Request::Sleep(deadline())),
    ] {
        for mode in [CommitMode::BeforeCommit, CommitMode::AfterCommit] {
            for reaction in [
                Reaction::Propagate,
                Reaction::CatchAndSucceed,
                Reaction::CatchAndFail,
            ] {
                let store = FaultJournal::new(FaultSchedule::Armed { site, mode });
                let mut requests = vec![first];
                requests.extend(later_requests());
                let workflow = SignupWorkflow::new(requests, reaction);
                let clock = CountingClock::new();
                let mut rng = CountingRng { calls: 0 };

                let result =
                    Engine::new(&store).run(execution(), &workflow, input(), &clock, &mut rng);

                let expected = EngineError::Journal(storage_failure());
                assert_engine_fault(result, &expected);
                assert_eq!(store.load(&execution()).unwrap(), *store.at_fault.borrow());
                assert_eq!(store.attempted.borrow().len(), store.fault_call_count.get());
                let fault_count = match reaction {
                    Reaction::Propagate => 1,
                    Reaction::CatchAndSucceed | Reaction::CatchAndFail => 5,
                };
                assert_eq!(workflow.faults.borrow().len(), fault_count);
                assert!(
                    workflow
                        .faults
                        .borrow()
                        .iter()
                        .all(|error| error == &expected)
                );
                assert_eq!(
                    workflow.effects.get(),
                    usize::from(matches!(
                        site,
                        AppendSite::StepCompletion | AppendSite::StepFailure
                    ))
                );
                assert_eq!(
                    clock.reads.get(),
                    usize::from(site == AppendSite::ClockRead)
                );
                assert_eq!(
                    clock.sleeps.get(),
                    usize::from(site == AppendSite::TimerFiring)
                );
                assert_eq!(rng.calls, usize::from(site == AppendSite::RandomDraw));
                assert!(store.acquire(&execution()).is_ok());

                // The one-shot fault is gone; reload the actual committed prefix.
                let before = store.load(&execution()).unwrap();
                let settled = before.events().iter().any(|event| {
                    matches!(
                        event,
                        JournalEvent::StepCompleted { .. }
                            | JournalEvent::StepFailed { .. }
                            | JournalEvent::NowRecorded { .. }
                            | JournalEvent::RandomRecorded { .. }
                            | JournalEvent::TimerFired { .. }
                    )
                });
                let live_calls = usize::from(!settled);
                let reads_before = clock.reads.get();
                let sleeps_before = clock.sleeps.get();
                let draws_before = rng.calls;
                let resumed = SignupWorkflow::new(vec![first], Reaction::Propagate);
                let output = Engine::new(&store)
                    .recover_and_run(execution(), &resumed, &clock, &mut rng)
                    .unwrap();
                assert_eq!(output, input());
                assert_eq!(
                    resumed.effects.get(),
                    live_calls
                        * usize::from(matches!(
                            first,
                            Request::Charge | Request::DeclineCharge | Request::CreateAccount
                        ))
                );
                assert_eq!(
                    clock.reads.get(),
                    reads_before + live_calls * usize::from(matches!(first, Request::Now))
                );
                assert_eq!(
                    clock.sleeps.get(),
                    sleeps_before + live_calls * usize::from(matches!(first, Request::Sleep(_)))
                );
                assert_eq!(
                    rng.calls,
                    draws_before + live_calls * usize::from(matches!(first, Request::Random))
                );
                assert!(resumed.faults.borrow().is_empty());
                let after = store.load(&execution()).unwrap();
                assert_eq!(&after.events()[..before.len()], before.events());
            }
        }
    }
}

fn seed_completed_charge(store: &FaultJournal) -> Journal {
    let workflow = SignupWorkflow::new(vec![Request::Charge], Reaction::Propagate);
    let id = execution();
    let _lease = store.acquire(&id).unwrap();
    for event in [
        JournalEvent::ExecutionStarted {
            workflow: workflow.name(),
            version: workflow.version(),
            input: input(),
        },
        JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new("charge-payment").unwrap(),
        },
        JournalEvent::StepStarted {
            seq: Seq::zero(),
            attempt: Attempt::first(),
        },
        JournalEvent::StepCompleted {
            seq: Seq::zero(),
            result: input(),
        },
    ] {
        store.append(&id, event).unwrap();
    }
    store.load(&id).unwrap()
}

#[test]
fn caught_replay_mismatches_refuse_matching_history_and_all_later_commands() {
    for first in [
        Request::Now,
        Request::Random,
        Request::Sleep(deadline()),
        Request::CreateAccount,
    ] {
        for reaction in [
            Reaction::Propagate,
            Reaction::CatchAndSucceed,
            Reaction::CatchAndFail,
        ] {
            let store = FaultJournal::new(FaultSchedule::Clear);
            let before = seed_completed_charge(&store);
            let appended_before = store.attempted.borrow().len();
            let mut requests = vec![first, Request::Charge];
            requests.extend(later_requests());
            let workflow = SignupWorkflow::new(requests, reaction);
            let clock = CountingClock::new();
            let mut rng = CountingRng { calls: 0 };

            let result =
                Engine::new(&store).recover_and_run(execution(), &workflow, &clock, &mut rng);

            let got = match first {
                Request::Now => CommandKind::ReadNow,
                Request::Random => CommandKind::DrawRandom,
                Request::Sleep(_) => CommandKind::Sleep,
                Request::Charge | Request::DeclineCharge | Request::CreateAccount => {
                    CommandKind::RunStep
                }
            };
            let expected = EngineError::Nondeterminism {
                seq: Seq::zero(),
                expected: CommandKind::RunStep,
                got,
            };
            assert_engine_fault(result, &expected);
            assert_eq!(store.load(&execution()).unwrap(), before);
            assert_eq!(store.attempted.borrow().len(), appended_before);
            assert_eq!(workflow.effects.get(), 0);
            assert_eq!(clock.reads.get(), 0);
            assert_eq!(clock.sleeps.get(), 0);
            assert_eq!(rng.calls, 0);
            let fault_count = match reaction {
                Reaction::Propagate => 1,
                Reaction::CatchAndSucceed | Reaction::CatchAndFail => 6,
            };
            assert_eq!(workflow.faults.borrow().len(), fault_count);
            assert!(
                workflow
                    .faults
                    .borrow()
                    .iter()
                    .all(|error| error == &expected)
            );
        }
    }
}

#[test]
fn handled_business_step_failures_allow_later_commands_and_both_terminal_outcomes() {
    for reaction in [
        Reaction::Propagate,
        Reaction::CatchAndSucceed,
        Reaction::CatchAndFail,
    ] {
        let store = FaultJournal::new(FaultSchedule::Clear);
        let mut requests = vec![Request::DeclineCharge];
        requests.extend(later_requests());
        let workflow = SignupWorkflow::new(requests, reaction);
        let clock = CountingClock::new();
        let mut rng = CountingRng { calls: 0 };

        let result = Engine::new(&store).run(execution(), &workflow, input(), &clock, &mut rng);

        match reaction {
            Reaction::Propagate | Reaction::CatchAndSucceed => assert_eq!(result.unwrap(), input()),
            Reaction::CatchAndFail => assert!(matches!(result, Err(RunError::Workflow(_)))),
        }
        assert!(workflow.faults.borrow().is_empty());
        assert_eq!(workflow.effects.get(), 2);
        assert_eq!(clock.reads.get(), 1);
        assert_eq!(clock.sleeps.get(), 1);
        assert_eq!(rng.calls, 1);
        let before = store.load(&execution()).unwrap();
        let resumed = SignupWorkflow::new(vec![Request::DeclineCharge], reaction);
        let recovered =
            Engine::new(&store).recover_and_run(execution(), &resumed, &clock, &mut rng);
        match reaction {
            Reaction::Propagate | Reaction::CatchAndSucceed => {
                assert_eq!(recovered.unwrap(), input())
            }
            Reaction::CatchAndFail => assert!(matches!(recovered, Err(RunError::Recovered(_)))),
        }
        assert_eq!(resumed.calls.get(), 0);
        assert_eq!(store.load(&execution()).unwrap(), before);
    }
}

#[test]
fn caught_timer_deadline_divergence_refuses_the_matching_timer_and_later_commands() {
    for reaction in [
        Reaction::Propagate,
        Reaction::CatchAndSucceed,
        Reaction::CatchAndFail,
    ] {
        let store = FaultJournal::new(FaultSchedule::Clear);
        let changed = Deadline::at(Timestamp::from_millis_since_epoch(
            deadline().timestamp().as_millis_since_epoch() + 1_000,
        ));
        let mut requests = vec![Request::Sleep(changed), Request::Sleep(deadline())];
        requests.extend(later_requests());
        let workflow = SignupWorkflow::new(requests, reaction);
        {
            let id = execution();
            let _lease = store.acquire(&id).unwrap();
            for event in [
                JournalEvent::ExecutionStarted {
                    workflow: workflow.name(),
                    version: workflow.version(),
                    input: input(),
                },
                JournalEvent::TimerScheduled {
                    seq: Seq::zero(),
                    deadline: deadline(),
                },
                JournalEvent::TimerFired { seq: Seq::zero() },
            ] {
                store.append(&id, event).unwrap();
            }
        }
        let before = store.load(&execution()).unwrap();
        let calls_before = store.attempted.borrow().len();
        let clock = CountingClock::new();
        let mut rng = CountingRng { calls: 0 };

        let result = Engine::new(&store).recover_and_run(execution(), &workflow, &clock, &mut rng);

        let expected = EngineError::Nondeterminism {
            seq: Seq::zero(),
            expected: CommandKind::Sleep,
            got: CommandKind::Sleep,
        };
        assert_engine_fault(result, &expected);
        assert!(
            workflow
                .faults
                .borrow()
                .iter()
                .all(|error| error == &expected)
        );
        assert_eq!(store.load(&execution()).unwrap(), before);
        assert_eq!(store.attempted.borrow().len(), calls_before);
        assert_eq!(workflow.effects.get(), 0);
        assert_eq!(clock.reads.get(), 0);
        assert_eq!(clock.sleeps.get(), 0);
        assert_eq!(rng.calls, 0);
    }
}

proptest! {
    #[test]
    fn generated_commands_after_a_caught_mismatch_never_extend_history(
        followers in proptest::collection::vec(
            proptest::sample::select(vec![Request::Charge, Request::CreateAccount, Request::Now, Request::Random, Request::Sleep(deadline())]),
            0..16,
        ),
    ) {
        let store = FaultJournal::new(FaultSchedule::Clear);
        let before = seed_completed_charge(&store);
        let calls_before = store.attempted.borrow().len();
        let expected_faults = followers.len() + 1;
        let mut requests = vec![Request::Now];
        requests.extend(followers);
        let workflow = SignupWorkflow::new(requests, Reaction::CatchAndSucceed);
        let clock = CountingClock::new();
        let mut rng = CountingRng { calls: 0 };

        let result = Engine::new(&store).recover_and_run(execution(), &workflow, &clock, &mut rng);

        let expected = EngineError::Nondeterminism { seq: Seq::zero(), expected: CommandKind::RunStep, got: CommandKind::ReadNow };
        assert_engine_fault(result, &expected);
        assert_eq!(workflow.faults.borrow().len(), expected_faults);
        assert!(workflow.faults.borrow().iter().all(|error| error == &expected));
        assert_eq!(store.load(&execution()).unwrap(), before);
        assert_eq!(store.attempted.borrow().len(), calls_before);
        assert_eq!(workflow.effects.get(), 0);
        assert_eq!(clock.reads.get(), 0);
        assert_eq!(clock.sleeps.get(), 0);
        assert_eq!(rng.calls, 0);
    }
}

#[test]
fn terminal_append_errors_are_reconciled_from_actual_history_on_next_recovery() {
    #[derive(Clone, Copy)]
    enum TerminalOutcome {
        Success,
        Failure,
    }

    for outcome in [TerminalOutcome::Success, TerminalOutcome::Failure] {
        let (site, reaction) = match outcome {
            TerminalOutcome::Success => (AppendSite::ExecutionCompletion, Reaction::Propagate),
            TerminalOutcome::Failure => (AppendSite::ExecutionFailure, Reaction::CatchAndFail),
        };
        for mode in [CommitMode::BeforeCommit, CommitMode::AfterCommit] {
            let store = FaultJournal::new(FaultSchedule::Armed { site, mode });
            let workflow = SignupWorkflow::new(vec![Request::Charge], reaction);
            let clock = CountingClock::new();
            let mut rng = CountingRng { calls: 0 };

            let first = Engine::new(&store).run(execution(), &workflow, input(), &clock, &mut rng);

            assert_engine_fault(first, &EngineError::Journal(storage_failure()));
            let before = store.load(&execution()).unwrap();
            let resumed = SignupWorkflow::new(vec![Request::Charge], reaction);
            let result =
                Engine::new(&store).recover_and_run(execution(), &resumed, &clock, &mut rng);
            match (outcome, mode) {
                (TerminalOutcome::Success, CommitMode::BeforeCommit) => {
                    assert_eq!(result.unwrap(), input())
                }
                (TerminalOutcome::Success, CommitMode::AfterCommit) => {
                    assert_eq!(result.unwrap(), input())
                }
                (TerminalOutcome::Failure, CommitMode::BeforeCommit) => {
                    assert!(matches!(result, Err(RunError::Workflow(_))))
                }
                (TerminalOutcome::Failure, CommitMode::AfterCommit) => {
                    assert!(matches!(result, Err(RunError::Recovered(_))))
                }
            }
            let calls = match mode {
                CommitMode::BeforeCommit => 1,
                CommitMode::AfterCommit => 0,
            };
            assert_eq!(resumed.calls.get(), calls);
            assert_eq!(resumed.effects.get(), 0);
            let after = store.load(&execution()).unwrap();
            assert_eq!(&after.events()[..before.len()], before.events());
            assert_eq!(after.len(), before.len() + calls);
        }
    }
}
