use std::cell::Cell;
use std::env;
use std::fs;

use proptest::prelude::any;
use proptest::proptest;
use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowErrorRecord;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
use yaoki::journal::Journal;
use yaoki::journal::JournalError;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::Attempt;
use yaoki::step::StepErrorRecord;
use yaoki::step::StepName;
use yaoki::stores::file::FileJournal;
use yaoki::stores::memory::MemoryJournal;
use yaoki::time::Deadline;
use yaoki::time::TestClock;
use yaoki::time::Timestamp;

struct SignupRng;

impl RngSource for SignupRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new([0x51; 32])
    }
}

fn signup_id() -> ExecutionId {
    ExecutionId::generate(&mut SignupRng)
}

fn signup_input() -> EventPayload {
    EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
}

fn clock() -> TestClock {
    TestClock::at(Timestamp::from_millis_since_epoch(1_753_401_600_000))
}

enum SignupOutcome {
    ReturnInput,
    RefuseSignup,
}

struct SignupWorkflow {
    outcome: SignupOutcome,
    calls: Cell<usize>,
    name: WorkflowName,
    version: WorkflowVersion,
}

impl SignupWorkflow {
    fn recorded() -> Self {
        Self {
            outcome: SignupOutcome::ReturnInput,
            calls: Cell::new(0),
            name: WorkflowName::new("signup").unwrap(),
            version: WorkflowVersion::new("2026.07.18").unwrap(),
        }
    }
}

impl<S: JournalStore> Workflow<S> for SignupWorkflow {
    type Error = String;

    fn name(&self) -> WorkflowName {
        self.name.clone()
    }

    fn version(&self) -> WorkflowVersion {
        self.version.clone()
    }

    fn run(
        &self,
        _ctx: &mut WorkflowCtx<'_, S>,
        input: EventPayload,
    ) -> Result<EventPayload, Self::Error> {
        self.calls.set(self.calls.get() + 1);
        match self.outcome {
            SignupOutcome::ReturnInput => Ok(input),
            SignupOutcome::RefuseSignup => Err("signup refused".to_owned()),
        }
    }
}

fn start_record() -> JournalEvent {
    let workflow = SignupWorkflow::recorded();
    JournalEvent::ExecutionStarted {
        workflow: workflow.name,
        version: workflow.version,
        input: signup_input(),
    }
}

fn recover<S: JournalStore>(
    store: &S,
    workflow: &SignupWorkflow,
) -> Result<EventPayload, RunError<String>> {
    Engine::new(store).recover_and_run(signup_id(), workflow, &clock(), &mut SignupRng)
}

#[test]
fn recovery_of_an_unknown_execution_rejects_it_without_running_code_or_appending() {
    let store = MemoryJournal::new();
    let workflow = SignupWorkflow::recorded();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::MissingExecution { id })) if id == signup_id())
    );
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_of_a_partial_invocation_uses_the_recorded_email_input() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    let workflow = SignupWorkflow::recorded();

    let output = recover(&store, &workflow).unwrap();

    assert_eq!(output, signup_input());
    assert_eq!(workflow.calls.get(), 1);
    assert_eq!(
        store.load(&signup_id()).unwrap().events(),
        &[
            start_record(),
            JournalEvent::ExecutionCompleted {
                output: signup_input()
            },
        ]
    );
}

#[test]
fn recovery_with_a_different_workflow_name_rejects_it_before_user_code() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    let mut workflow = SignupWorkflow::recorded();
    workflow.name = WorkflowName::new("subscription-renewal").unwrap();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::WorkflowMismatch { recorded, current }))
        if recorded == SignupWorkflow::recorded().name && current == workflow.name)
    );
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_with_a_different_workflow_version_rejects_it_before_user_code() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    let mut workflow = SignupWorkflow::recorded();
    workflow.version = WorkflowVersion::new("2026.08.01").unwrap();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::VersionMismatch { recorded, current }))
        if recorded == SignupWorkflow::recorded().version && current == workflow.version)
    );
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn starting_an_existing_execution_rejects_it_and_preserves_its_entire_journal() {
    let store = MemoryJournal::new();
    let workflow = SignupWorkflow::recorded();
    let engine = Engine::new(&store);
    engine
        .run(
            signup_id(),
            &workflow,
            signup_input(),
            &clock(),
            &mut SignupRng,
        )
        .unwrap();
    let before = store.load(&signup_id()).unwrap();

    let result = engine.run(
        signup_id(),
        &workflow,
        signup_input(),
        &clock(),
        &mut SignupRng,
    );

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::ExistingExecution { id })) if id == signup_id())
    );
    assert_eq!(workflow.calls.get(), 1);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_of_a_history_without_a_start_rejects_it_without_appending() {
    let store = MemoryJournal::new();
    store
        .append(
            &signup_id(),
            JournalEvent::ExecutionCompleted {
                output: signup_input(),
            },
        )
        .unwrap();
    let workflow = SignupWorkflow::recorded();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::InvalidInvocation { id })) if id == signup_id())
    );
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_of_a_history_with_two_starts_rejects_it_without_appending() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    store.append(&signup_id(), start_record()).unwrap();
    let workflow = SignupWorkflow::recorded();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::InvalidInvocation { id })) if id == signup_id())
    );
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_of_a_completed_invocation_returns_output_without_user_code() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    let output = EventPayload::new(br#"{"account_id":"acct_2026_0718"}"#.to_vec());
    store
        .append(
            &signup_id(),
            JournalEvent::ExecutionCompleted {
                output: output.clone(),
            },
        )
        .unwrap();
    let workflow = SignupWorkflow::recorded();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow).unwrap();

    assert_eq!(result, output);
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_of_a_failed_invocation_returns_its_recorded_error_without_user_code() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    let error = WorkflowErrorRecord::new("payment gateway timed out");
    store
        .append(
            &signup_id(),
            JournalEvent::ExecutionFailed {
                error: error.clone(),
            },
        )
        .unwrap();
    let workflow = SignupWorkflow::recorded();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(matches!(result, Err(RunError::Recovered(record)) if record == error));
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_of_a_terminal_invocation_checks_the_name_before_returning_output() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    store
        .append(
            &signup_id(),
            JournalEvent::ExecutionCompleted {
                output: signup_input(),
            },
        )
        .unwrap();
    let mut workflow = SignupWorkflow::recorded();
    workflow.name = WorkflowName::new("subscription-renewal").unwrap();
    let before = store.load(&signup_id()).unwrap();

    let result = recover(&store, &workflow);

    assert!(matches!(
        result,
        Err(RunError::Engine(EngineError::WorkflowMismatch { .. }))
    ));
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn recovery_rejects_every_non_start_header_variant_without_running_code() {
    let events = [
        JournalEvent::StepScheduled {
            seq: Seq::zero(),
            name: StepName::new("charge-card").unwrap(),
        },
        JournalEvent::StepStarted {
            seq: Seq::zero(),
            attempt: Attempt::first(),
        },
        JournalEvent::StepCompleted {
            seq: Seq::zero(),
            result: signup_input(),
        },
        JournalEvent::StepFailed {
            seq: Seq::zero(),
            attempt: Attempt::first(),
            error: StepErrorRecord::new("payment gateway timed out"),
        },
        JournalEvent::NowRecorded {
            seq: Seq::zero(),
            value: Timestamp::from_millis_since_epoch(0),
        },
        JournalEvent::RandomRecorded {
            seq: Seq::zero(),
            value: RandomBytes::new([0x51; 32]),
        },
        JournalEvent::TimerScheduled {
            seq: Seq::zero(),
            deadline: Deadline::at(Timestamp::from_millis_since_epoch(0)),
        },
        JournalEvent::TimerFired { seq: Seq::zero() },
        JournalEvent::ExecutionCompleted {
            output: signup_input(),
        },
        JournalEvent::ExecutionFailed {
            error: WorkflowErrorRecord::new("signup refused"),
        },
    ];
    for event in events {
        let store = MemoryJournal::new();
        store.append(&signup_id(), event).unwrap();
        let before = store.load(&signup_id()).unwrap();
        let workflow = SignupWorkflow::recorded();

        assert!(matches!(
            recover(&store, &workflow),
            Err(RunError::Engine(EngineError::InvalidInvocation { .. }))
        ));
        assert_eq!(workflow.calls.get(), 0);
        assert_eq!(store.load(&signup_id()).unwrap(), before);
    }
}

#[test]
fn terminal_recovery_checks_both_name_and_version_for_success_and_failure() {
    let outcomes = [
        JournalEvent::ExecutionCompleted {
            output: signup_input(),
        },
        JournalEvent::ExecutionFailed {
            error: WorkflowErrorRecord::new("signup refused"),
        },
    ];
    for outcome in outcomes {
        let store = MemoryJournal::new();
        store.append(&signup_id(), start_record()).unwrap();
        store.append(&signup_id(), outcome).unwrap();
        let before = store.load(&signup_id()).unwrap();
        let mut workflow = SignupWorkflow::recorded();
        workflow.name = WorkflowName::new("subscription-renewal").unwrap();

        assert!(matches!(
            recover(&store, &workflow),
            Err(RunError::Engine(EngineError::WorkflowMismatch { .. }))
        ));
        workflow.name = SignupWorkflow::recorded().name;
        workflow.version = WorkflowVersion::new("2026.08.01").unwrap();
        assert!(matches!(
            recover(&store, &workflow),
            Err(RunError::Engine(EngineError::VersionMismatch { .. }))
        ));
        assert_eq!(workflow.calls.get(), 0);
        assert_eq!(store.load(&signup_id()).unwrap(), before);
    }
}

#[test]
fn terminal_recovery_rejects_an_extra_start_before_retrieving_the_result() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    store.append(&signup_id(), start_record()).unwrap();
    store
        .append(
            &signup_id(),
            JournalEvent::ExecutionFailed {
                error: WorkflowErrorRecord::new("signup refused"),
            },
        )
        .unwrap();
    let before = store.load(&signup_id()).unwrap();
    let workflow = SignupWorkflow::recorded();

    assert!(matches!(
        recover(&store, &workflow),
        Err(RunError::Engine(EngineError::InvalidInvocation { .. }))
    ));
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn starting_a_partial_invocation_rejects_it_without_running_or_appending() {
    let store = MemoryJournal::new();
    store.append(&signup_id(), start_record()).unwrap();
    let before = store.load(&signup_id()).unwrap();
    let workflow = SignupWorkflow::recorded();

    let result = Engine::new(&store).run(
        signup_id(),
        &workflow,
        signup_input(),
        &clock(),
        &mut SignupRng,
    );

    assert!(matches!(
        result,
        Err(RunError::Engine(EngineError::ExistingExecution { .. }))
    ));
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
}

#[test]
fn file_recovery_uses_persisted_input_and_rejects_duplicate_start_after_reopen() {
    let dir = env::temp_dir().join(format!("yaoki-invocation-authority-{}", std::process::id()));
    fs::create_dir(&dir).unwrap();
    {
        let store = FileJournal::new(&dir).unwrap();
        let lease = store.acquire(&signup_id()).unwrap();
        store.append(&signup_id(), start_record()).unwrap();
        drop(lease);
    }
    let store = FileJournal::new(&dir).unwrap();
    let workflow = SignupWorkflow::recorded();

    assert_eq!(recover(&store, &workflow).unwrap(), signup_input());
    let before = store.load(&signup_id()).unwrap();
    assert_eq!(recover(&store, &workflow).unwrap(), signup_input());
    let result = Engine::new(&store).run(
        signup_id(),
        &workflow,
        signup_input(),
        &clock(),
        &mut SignupRng,
    );
    assert!(matches!(
        result,
        Err(RunError::Engine(EngineError::ExistingExecution { .. }))
    ));
    assert_eq!(workflow.calls.get(), 1);
    assert_eq!(store.load(&signup_id()).unwrap(), before);
    fs::remove_dir_all(dir).unwrap();
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StorageFault {
    None,
    Acquire,
    Load,
    StartAppend,
    TerminalAppend,
}

struct ObservedJournal {
    inner: MemoryJournal,
    fault: StorageFault,
}

fn storage_failure() -> JournalError {
    JournalError::Io {
        message: "simulated journal I/O failure".to_owned(),
    }
}

impl JournalStore for ObservedJournal {
    type Lease<'a> = <MemoryJournal as JournalStore>::Lease<'a>;

    fn acquire(&self, id: &ExecutionId) -> Result<Self::Lease<'_>, JournalError> {
        if self.fault == StorageFault::Acquire {
            return Err(storage_failure());
        }
        self.inner.acquire(id)
    }

    fn load(&self, id: &ExecutionId) -> Result<Journal, JournalError> {
        assert_eq!(
            self.inner.acquire(id).err(),
            Some(JournalError::ExecutionOwned { id: *id })
        );
        if self.fault == StorageFault::Load {
            return Err(storage_failure());
        }
        self.inner.load(id)
    }

    fn append(&self, id: &ExecutionId, event: JournalEvent) -> Result<Seq, JournalError> {
        assert_eq!(
            self.inner.acquire(id).err(),
            Some(JournalError::ExecutionOwned { id: *id })
        );
        match (&event, self.fault) {
            (JournalEvent::ExecutionStarted { .. }, StorageFault::StartAppend)
            | (
                JournalEvent::ExecutionCompleted { .. } | JournalEvent::ExecutionFailed { .. },
                StorageFault::TerminalAppend,
            ) => Err(storage_failure()),
            _ => self.inner.append(id, event),
        }
    }
}

#[test]
fn creation_holds_ownership_across_the_absence_check_and_all_appends() {
    let store = ObservedJournal {
        inner: MemoryJournal::new(),
        fault: StorageFault::None,
    };
    let workflow = SignupWorkflow::recorded();

    assert_eq!(
        Engine::new(&store)
            .run(
                signup_id(),
                &workflow,
                signup_input(),
                &clock(),
                &mut SignupRng
            )
            .unwrap(),
        signup_input()
    );
    assert!(store.inner.acquire(&signup_id()).is_ok());
}

#[test]
fn creation_propagates_each_storage_failure_and_releases_ownership() {
    for (fault, calls, event_count) in [
        (StorageFault::Acquire, 0, 0),
        (StorageFault::Load, 0, 0),
        (StorageFault::StartAppend, 0, 0),
        (StorageFault::TerminalAppend, 1, 1),
    ] {
        let store = ObservedJournal {
            inner: MemoryJournal::new(),
            fault,
        };
        let workflow = SignupWorkflow::recorded();

        let result = Engine::new(&store).run(
            signup_id(),
            &workflow,
            signup_input(),
            &clock(),
            &mut SignupRng,
        );

        assert!(
            matches!(result, Err(RunError::Engine(EngineError::Journal(error))) if error == storage_failure())
        );
        assert_eq!(workflow.calls.get(), calls);
        assert_eq!(store.inner.load(&signup_id()).unwrap().len(), event_count);
        assert!(store.inner.acquire(&signup_id()).is_ok());
    }
}

#[test]
fn recovery_propagates_storage_failures_without_a_false_terminal_record() {
    for (fault, calls) in [
        (StorageFault::Acquire, 0),
        (StorageFault::Load, 0),
        (StorageFault::TerminalAppend, 1),
    ] {
        let store = ObservedJournal {
            inner: MemoryJournal::new(),
            fault,
        };
        store.inner.append(&signup_id(), start_record()).unwrap();
        let before = store.inner.load(&signup_id()).unwrap();
        let workflow = SignupWorkflow::recorded();

        let result = recover(&store, &workflow);

        assert!(
            matches!(result, Err(RunError::Engine(EngineError::Journal(error))) if error == storage_failure())
        );
        assert_eq!(workflow.calls.get(), calls);
        assert_eq!(store.inner.load(&signup_id()).unwrap(), before);
        assert!(store.inner.acquire(&signup_id()).is_ok());
    }
}

#[test]
fn business_failure_append_holds_ownership_and_propagates_a_storage_failure() {
    let store = ObservedJournal {
        inner: MemoryJournal::new(),
        fault: StorageFault::TerminalAppend,
    };
    let mut workflow = SignupWorkflow::recorded();
    workflow.outcome = SignupOutcome::RefuseSignup;

    let result = Engine::new(&store).run(
        signup_id(),
        &workflow,
        signup_input(),
        &clock(),
        &mut SignupRng,
    );

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::Journal(error))) if error == storage_failure())
    );
    assert_eq!(workflow.calls.get(), 1);
    assert_eq!(
        store.inner.load(&signup_id()).unwrap().events(),
        &[start_record()]
    );
    assert!(store.inner.acquire(&signup_id()).is_ok());
}

proptest! {
    #[test]
    fn recovery_preserves_all_opaque_recorded_input_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let store = MemoryJournal::new();
        let input = EventPayload::new(bytes);
        let mut event = start_record();
        if let JournalEvent::ExecutionStarted { input: recorded, .. } = &mut event {
            *recorded = input.clone();
        }
        store.append(&signup_id(), event).unwrap();
        let workflow = SignupWorkflow::recorded();

        assert_eq!(recover(&store, &workflow).unwrap(), input);
        assert_eq!(workflow.calls.get(), 1);
    }
}
