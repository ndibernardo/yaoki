//! Recovery validates acknowledged history before invoking workflow code.

use std::cell::Cell;
use std::env;
use std::fs;

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
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::Attempt;
use yaoki::step::StepError;
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

fn execution() -> ExecutionId {
    ExecutionId::generate(&mut SignupRng)
}

fn input() -> EventPayload {
    EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
}

fn start() -> JournalEvent {
    JournalEvent::ExecutionStarted {
        workflow: WorkflowName::new("signup").unwrap(),
        version: WorkflowVersion::new("2026.08.01").unwrap(),
        input: input(),
    }
}

fn deadline() -> Deadline {
    Deadline::at(Timestamp::from_millis_since_epoch(1_784_937_600_000))
}

fn scheduled(seq: Seq) -> JournalEvent {
    JournalEvent::StepScheduled {
        seq,
        name: StepName::new("charge-payment").unwrap(),
    }
}

fn started(seq: Seq, attempt: Attempt) -> JournalEvent {
    JournalEvent::StepStarted { seq, attempt }
}

fn completed(seq: Seq) -> JournalEvent {
    JournalEvent::StepCompleted {
        seq,
        result: input(),
    }
}

fn failed(seq: Seq, attempt: Attempt) -> JournalEvent {
    JournalEvent::StepFailed {
        seq,
        attempt,
        error: StepErrorRecord::new("payment gateway timed out"),
    }
}

fn seed<S: JournalStore>(store: &S, events: Vec<JournalEvent>) -> Journal {
    let id = execution();
    let _lease = store.acquire(&id).unwrap();
    for event in events {
        store.append(&id, event).unwrap();
    }
    store.load(&id).unwrap()
}

struct SignupWorkflow {
    calls: Cell<usize>,
    charges: Cell<usize>,
}

impl SignupWorkflow {
    fn new() -> Self {
        Self {
            calls: Cell::new(0),
            charges: Cell::new(0),
        }
    }
}

impl<S: JournalStore> Workflow<S> for SignupWorkflow {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        WorkflowName::new("signup").unwrap()
    }

    fn version(&self) -> WorkflowVersion {
        WorkflowVersion::new("2026.08.01").unwrap()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, S>,
        input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        self.calls.set(self.calls.get() + 1);
        ctx.step(StepName::new("charge-payment").unwrap(), |_key| {
            self.charges.set(self.charges.get() + 1);
            Ok(input.clone())
        })?;
        ctx.now()?;
        ctx.random()?;
        ctx.sleep_until(deadline())?;
        Ok(input)
    }
}

fn recover<S: JournalStore>(
    store: &S,
    workflow: &SignupWorkflow,
) -> Result<EventPayload, RunError<StepError>> {
    Engine::new(store).recover_and_run(
        execution(),
        workflow,
        &TestClock::at(deadline().timestamp()),
        &mut SignupRng,
    )
}

fn assert_rejected(events: Vec<JournalEvent>) {
    let store = MemoryJournal::new();
    let before = seed(&store, events);
    let workflow = SignupWorkflow::new();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Engine(EngineError::History(_)))),
        "got {result:?}"
    );
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(workflow.charges.get(), 0);
    assert_eq!(store.load(&execution()).unwrap(), before);
    assert!(store.acquire(&execution()).is_ok());
}

#[test]
fn recovery_with_wrong_command_positions_rejects_before_workflow_code() {
    let zero = Seq::zero();
    let wrong = zero.next().unwrap();
    let cases = vec![
        vec![start(), scheduled(wrong)],
        vec![start(), scheduled(zero), started(wrong, Attempt::first())],
        vec![
            start(),
            scheduled(zero),
            started(zero, Attempt::first()),
            completed(wrong),
        ],
        vec![
            start(),
            scheduled(zero),
            started(zero, Attempt::first()),
            failed(wrong, Attempt::first()),
        ],
        vec![
            start(),
            JournalEvent::NowRecorded {
                seq: wrong,
                value: deadline().timestamp(),
            },
        ],
        vec![
            start(),
            JournalEvent::RandomRecorded {
                seq: wrong,
                value: RandomBytes::new([0x41; 32]),
            },
        ],
        vec![
            start(),
            JournalEvent::TimerScheduled {
                seq: wrong,
                deadline: deadline(),
            },
        ],
        vec![
            start(),
            JournalEvent::TimerScheduled {
                seq: zero,
                deadline: deadline(),
            },
            JournalEvent::TimerFired { seq: wrong },
        ],
    ];

    for events in cases {
        assert_rejected(events);
    }
}

#[test]
fn recovery_with_invalid_attempt_progression_rejects_before_workflow_code() {
    let seq = Seq::zero();
    let cases = vec![
        vec![
            start(),
            scheduled(seq),
            started(seq, Attempt::new(2).unwrap()),
        ],
        vec![
            start(),
            scheduled(seq),
            started(seq, Attempt::first()),
            started(seq, Attempt::first()),
        ],
        vec![
            start(),
            scheduled(seq),
            started(seq, Attempt::first()),
            started(seq, Attempt::new(3).unwrap()),
        ],
        vec![
            start(),
            scheduled(seq),
            started(seq, Attempt::first()),
            failed(seq, Attempt::new(2).unwrap()),
        ],
    ];

    for events in cases {
        assert_rejected(events);
    }
}

#[test]
fn recovery_with_orphan_outcomes_rejects_before_workflow_code() {
    let seq = Seq::zero();
    for event in [
        started(seq, Attempt::first()),
        completed(seq),
        failed(seq, Attempt::first()),
        JournalEvent::TimerFired { seq },
    ] {
        assert_rejected(vec![start(), event]);
    }
}

#[test]
fn recovery_with_an_unrelated_event_inside_a_pending_operation_rejects_it() {
    let seq = Seq::zero();
    let followers = [
        scheduled(seq.next().unwrap()),
        JournalEvent::NowRecorded {
            seq: seq.next().unwrap(),
            value: deadline().timestamp(),
        },
        JournalEvent::RandomRecorded {
            seq: seq.next().unwrap(),
            value: RandomBytes::new([0x41; 32]),
        },
        JournalEvent::TimerScheduled {
            seq: seq.next().unwrap(),
            deadline: deadline(),
        },
        JournalEvent::ExecutionCompleted { output: input() },
        JournalEvent::ExecutionFailed {
            error: WorkflowErrorRecord::new("signup refused"),
        },
    ];
    for prefix in [
        vec![start(), scheduled(seq)],
        vec![start(), scheduled(seq), started(seq, Attempt::first())],
        vec![
            start(),
            JournalEvent::TimerScheduled {
                seq,
                deadline: deadline(),
            },
        ],
    ] {
        for follower in &followers {
            let mut events = prefix.clone();
            events.push(follower.clone());
            assert_rejected(events);
        }
    }
}

#[test]
fn recovery_with_events_after_either_terminal_outcome_rejects_them() {
    for terminal in [
        JournalEvent::ExecutionCompleted { output: input() },
        JournalEvent::ExecutionFailed {
            error: WorkflowErrorRecord::new("signup refused"),
        },
    ] {
        for follower in [
            scheduled(Seq::zero()),
            JournalEvent::ExecutionCompleted { output: input() },
            JournalEvent::ExecutionFailed {
                error: WorkflowErrorRecord::new("signup refused"),
            },
        ] {
            assert_rejected(vec![start(), terminal.clone(), follower]);
        }
    }
}

#[test]
fn recovery_of_an_illegal_history_with_either_terminal_tail_rejects_the_stored_result() {
    for terminal in [
        JournalEvent::ExecutionCompleted { output: input() },
        JournalEvent::ExecutionFailed {
            error: WorkflowErrorRecord::new("signup refused"),
        },
    ] {
        assert_rejected(vec![
            start(),
            JournalEvent::TimerFired { seq: Seq::zero() },
            terminal,
        ]);
    }
}

#[test]
fn recovery_of_a_recorded_step_failure_preserves_business_failure_without_retrying() {
    let store = MemoryJournal::new();
    let seq = Seq::zero();
    let before = seed(
        &store,
        vec![
            start(),
            scheduled(seq),
            started(seq, Attempt::first()),
            failed(seq, Attempt::first()),
        ],
    );
    let workflow = SignupWorkflow::new();

    let result = recover(&store, &workflow);

    assert!(
        matches!(result, Err(RunError::Workflow(StepError::Failed(error))) if error.message() == "payment gateway timed out")
    );
    assert_eq!(workflow.calls.get(), 1);
    assert_eq!(workflow.charges.get(), 0);
    let after = store.load(&execution()).unwrap();
    assert_eq!(&after.events()[..before.len()], before.events());
    assert_eq!(after.len(), before.len() + 1);
    assert!(matches!(
        after.events().last(),
        Some(JournalEvent::ExecutionFailed { .. })
    ));
    let terminal_workflow = SignupWorkflow::new();
    assert!(matches!(
        recover(&store, &terminal_workflow),
        Err(RunError::Recovered(_))
    ));
    assert_eq!(terminal_workflow.calls.get(), 0);
    assert_eq!(store.load(&execution()).unwrap(), after);
}

fn legal_history(interrupted_attempts: u32) -> Vec<JournalEvent> {
    let mut events = vec![start(), scheduled(Seq::zero())];
    for number in 1..=interrupted_attempts + 1 {
        events.push(started(Seq::zero(), Attempt::new(number).unwrap()));
    }
    events.extend([
        completed(Seq::zero()),
        JournalEvent::NowRecorded {
            seq: Seq::zero().next().unwrap(),
            value: deadline().timestamp(),
        },
        JournalEvent::RandomRecorded {
            seq: Seq::zero().next().unwrap().next().unwrap(),
            value: RandomBytes::new([0x41; 32]),
        },
        JournalEvent::TimerScheduled {
            seq: Seq::zero().next().unwrap().next().unwrap().next().unwrap(),
            deadline: deadline(),
        },
        JournalEvent::TimerFired {
            seq: Seq::zero().next().unwrap().next().unwrap().next().unwrap(),
        },
        JournalEvent::ExecutionCompleted { output: input() },
    ]);
    events
}

fn assert_prefix_recovers(events: &[JournalEvent]) {
    let store = MemoryJournal::new();
    let before = seed(&store, events.to_vec());
    let workflow = SignupWorkflow::new();

    let output = recover(&store, &workflow).unwrap();

    assert_eq!(output, input());
    let after = store.load(&execution()).unwrap();
    assert_eq!(&after.events()[..before.len()], before.events());
    let charge_settled = events
        .iter()
        .any(|event| matches!(event, JournalEvent::StepCompleted { .. }));
    assert_eq!(workflow.charges.get(), usize::from(!charge_settled));
    assert_eq!(
        after
            .events()
            .iter()
            .filter(|event| matches!(event, JournalEvent::StepScheduled { .. }))
            .count(),
        1
    );
    assert_eq!(
        after
            .events()
            .iter()
            .filter(|event| matches!(event, JournalEvent::TimerScheduled { .. }))
            .count(),
        1
    );
}

#[test]
fn recovery_accepts_and_resumes_every_legitimate_append_prefix() {
    let history = legal_history(2);

    for end in 1..=history.len() {
        assert_prefix_recovers(&history[..end]);
    }
}

proptest! {
    #[test]
    fn recovery_accepts_generated_interruption_boundaries(
        attempts in 0u32..8,
        cut in 0usize..24,
    ) {
        let history = legal_history(attempts);
        let end = 1 + cut % history.len();
        assert_prefix_recovers(&history[..end]);
    }
}

#[test]
fn file_recovery_of_a_complete_but_illegal_history_preserves_bytes() {
    let dir = env::temp_dir().join(format!("yaoki-illegal-history-{}", std::process::id()));
    fs::create_dir(&dir).unwrap();
    let store = FileJournal::new(&dir).unwrap();
    seed(
        &store,
        vec![start(), JournalEvent::TimerFired { seq: Seq::zero() }],
    );
    let path = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "journal")
        })
        .unwrap();
    let before = fs::read(&path).unwrap();
    let workflow = SignupWorkflow::new();

    let result = recover(&store, &workflow);

    let after = fs::read(path).unwrap();
    fs::remove_dir_all(dir).unwrap();
    assert!(matches!(
        result,
        Err(RunError::Engine(EngineError::History(_)))
    ));
    assert_eq!(workflow.calls.get(), 0);
    assert_eq!(after, before);
}
