//! Terminal outcomes require complete consumption of the recorded prefix.

use proptest::proptest;
use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
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
use yaoki::time::Deadline;
use yaoki::time::TestClock;
use yaoki::time::Timestamp;

struct SignupRng;

impl RngSource for SignupRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new([0x51; 32])
    }
}

fn signup_execution() -> ExecutionId {
    ExecutionId::generate(&mut SignupRng)
}

fn receipt() -> EventPayload {
    EventPayload::new(br#"{"charge_id":"ch_2026_0801"}"#.to_vec())
}

enum ReturnPoint {
    BeforeCharge,
    AfterCharge,
}

enum Outcome {
    Success,
    BusinessFailure,
}

struct ShortenedSignup {
    return_point: ReturnPoint,
    outcome: Outcome,
}

impl Workflow<MemoryJournal> for ShortenedSignup {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        WorkflowName::new("signup").unwrap()
    }

    fn version(&self) -> WorkflowVersion {
        WorkflowVersion::new("2026.08.01").unwrap()
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, MemoryJournal>,
        _input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        match self.return_point {
            ReturnPoint::BeforeCharge => {}
            ReturnPoint::AfterCharge => {
                ctx.step(StepName::new("charge-payment").unwrap(), |_key| {
                    panic!("a durable charge must not execute again")
                })?;
            }
        }
        match self.outcome {
            Outcome::Success => Ok(receipt()),
            Outcome::BusinessFailure => Err(StepError::Failed(StepErrorRecord::new(
                "account registration is closed",
            ))),
        }
    }
}

fn completed_charge() -> Vec<JournalEvent> {
    vec![
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
            result: receipt(),
        },
    ]
}

fn account_suffixes(seq: Seq) -> Vec<Vec<JournalEvent>> {
    let scheduled = JournalEvent::StepScheduled {
        seq,
        name: StepName::new("create-account").unwrap(),
    };
    let started = JournalEvent::StepStarted {
        seq,
        attempt: Attempt::first(),
    };
    let deadline = Deadline::at(Timestamp::from_millis_since_epoch(1_784_937_600_000));
    let timer = JournalEvent::TimerScheduled { seq, deadline };
    vec![
        vec![scheduled.clone()],
        vec![scheduled.clone(), started.clone()],
        vec![
            scheduled.clone(),
            started.clone(),
            JournalEvent::StepStarted {
                seq,
                attempt: Attempt::first().next(),
            },
        ],
        vec![
            scheduled.clone(),
            started.clone(),
            JournalEvent::StepCompleted {
                seq,
                result: EventPayload::new(br#"{"account_id":"acct_2026_0801"}"#.to_vec()),
            },
        ],
        vec![
            scheduled,
            started,
            JournalEvent::StepFailed {
                seq,
                attempt: Attempt::first(),
                error: StepErrorRecord::new("account registry is unavailable"),
            },
        ],
        vec![JournalEvent::NowRecorded {
            seq,
            value: deadline.timestamp(),
        }],
        vec![JournalEvent::RandomRecorded {
            seq,
            value: RandomBytes::new([0x41; 32]),
        }],
        vec![timer.clone()],
        vec![timer, JournalEvent::TimerFired { seq }],
    ]
}

fn seed_signup(store: &MemoryJournal, commands: Vec<JournalEvent>) {
    let id = signup_execution();
    let _lease = store.acquire(&id).unwrap();
    store
        .append(
            &id,
            JournalEvent::ExecutionStarted {
                workflow: WorkflowName::new("signup").unwrap(),
                version: WorkflowVersion::new("2026.08.01").unwrap(),
                input: EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec()),
            },
        )
        .unwrap();
    for event in commands {
        store.append(&id, event).unwrap();
    }
}

fn recover(
    store: &MemoryJournal,
    return_point: ReturnPoint,
    outcome: Outcome,
) -> Result<EventPayload, RunError<StepError>> {
    Engine::new(store).recover_and_run(
        signup_execution(),
        &ShortenedSignup {
            return_point,
            outcome,
        },
        &TestClock::at(Timestamp::from_millis_since_epoch(1_784_937_600_000)),
        &mut SignupRng,
    )
}

fn assert_unconsumed(result: Result<EventPayload, RunError<StepError>>) {
    assert!(
        matches!(
            result,
            Err(RunError::Engine(EngineError::UnconsumedHistory))
        ),
        "got {result:?}"
    );
}

proptest! {
    #[test]
    fn recovery_with_generated_unconsumed_suffixes_preserves_history(
        settled_commands in proptest::collection::vec(0usize..5, 0..16),
        final_prefix in 0usize..9,
    ) {
        let store = MemoryJournal::new();
        let mut commands = completed_charge();
        let mut seq = Seq::zero().next();
        for kind in settled_commands {
            // Only the last command may be interrupted.
            let settled_indices = [3, 4, 5, 6, 8];
            commands.extend(account_suffixes(seq).remove(settled_indices[kind]));
            seq = seq.next();
        }
        commands.extend(account_suffixes(seq).remove(final_prefix));
        seed_signup(&store, commands);
        let before = store.load(&signup_execution()).unwrap();

        let success = recover(&store, ReturnPoint::AfterCharge, Outcome::Success);
        assert_unconsumed(success);
        let failure = recover(&store, ReturnPoint::AfterCharge, Outcome::BusinessFailure);
        assert_unconsumed(failure);

        assert_eq!(store.load(&signup_execution()).unwrap(), before);
    }
}

#[test]
fn recovery_early_success_with_each_recorded_suffix_refuses_terminal_append() {
    for suffix in account_suffixes(Seq::zero().next()) {
        let store = MemoryJournal::new();
        seed_signup(
            &store,
            completed_charge().into_iter().chain(suffix).collect(),
        );
        let before = store.load(&signup_execution()).unwrap();

        let result = recover(&store, ReturnPoint::AfterCharge, Outcome::Success);

        assert_unconsumed(result);
        assert_eq!(store.load(&signup_execution()).unwrap(), before);
    }
}

#[test]
fn recovery_early_business_failure_with_each_recorded_suffix_refuses_terminal_append() {
    for suffix in account_suffixes(Seq::zero().next()) {
        let store = MemoryJournal::new();
        seed_signup(
            &store,
            completed_charge().into_iter().chain(suffix).collect(),
        );
        let before = store.load(&signup_execution()).unwrap();

        let result = recover(&store, ReturnPoint::AfterCharge, Outcome::BusinessFailure);

        assert_unconsumed(result);
        assert_eq!(store.load(&signup_execution()).unwrap(), before);
    }
}

#[test]
fn recovery_success_before_any_command_refuses_terminal_append() {
    let store = MemoryJournal::new();
    seed_signup(&store, completed_charge());
    let before = store.load(&signup_execution()).unwrap();

    let result = recover(&store, ReturnPoint::BeforeCharge, Outcome::Success);

    assert_unconsumed(result);
    assert_eq!(store.load(&signup_execution()).unwrap(), before);
}

#[test]
fn recovery_business_failure_before_any_command_refuses_terminal_append() {
    let store = MemoryJournal::new();
    seed_signup(&store, completed_charge());
    let before = store.load(&signup_execution()).unwrap();

    let result = recover(&store, ReturnPoint::BeforeCharge, Outcome::BusinessFailure);

    assert_unconsumed(result);
    assert_eq!(store.load(&signup_execution()).unwrap(), before);
}

#[test]
fn recovery_success_after_consuming_all_commands_appends_completion() {
    let store = MemoryJournal::new();
    seed_signup(&store, completed_charge());
    let before = store.load(&signup_execution()).unwrap();

    let result = recover(&store, ReturnPoint::AfterCharge, Outcome::Success);

    assert_eq!(result.unwrap(), receipt());
    let after = store.load(&signup_execution()).unwrap();
    assert_eq!(&after.events()[..before.len()], before.events());
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(
        after.events().last(),
        Some(&JournalEvent::ExecutionCompleted { output: receipt() })
    );
}

#[test]
fn recovery_business_failure_after_consuming_all_commands_appends_failure() {
    let store = MemoryJournal::new();
    seed_signup(&store, completed_charge());
    let before = store.load(&signup_execution()).unwrap();

    let result = recover(&store, ReturnPoint::AfterCharge, Outcome::BusinessFailure);

    assert!(matches!(
        result,
        Err(RunError::Workflow(StepError::Failed(_)))
    ));
    let after = store.load(&signup_execution()).unwrap();
    assert_eq!(&after.events()[..before.len()], before.events());
    assert_eq!(after.len(), before.len() + 1);
    assert!(matches!(
        after.events().last(),
        Some(JournalEvent::ExecutionFailed { .. })
    ));
}
