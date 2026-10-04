use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Barrier;
use std::thread;

use yaoki::context::EngineError;
use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::Execution;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
use yaoki::journal::JournalError;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::stores::file::FileJournal;
use yaoki::stores::memory::MemoryJournal;
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

fn scratch_dir(label: &str) -> PathBuf {
    let path = env::temp_dir().join(format!("yaoki-ownership-{label}-{}", std::process::id()));
    fs::create_dir(&path).unwrap();
    path
}

fn start<S: JournalStore>(store: &S) -> yaoki::engine::Execution<'_, S, yaoki::engine::Running> {
    Execution::new(store, signup_id())
        .unwrap()
        .start(
            WorkflowName::new("signup").unwrap(),
            WorkflowVersion::new("2026.07.18").unwrap(),
            EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec()),
        )
        .unwrap()
}

struct OwnershipProbe<'a, S> {
    store: &'a S,
}

impl<S: JournalStore> Workflow<S> for OwnershipProbe<'_, S> {
    type Error = String;

    fn name(&self) -> WorkflowName {
        WorkflowName::new("signup").unwrap()
    }

    fn version(&self) -> WorkflowVersion {
        WorkflowVersion::new("2026.07.18").unwrap()
    }

    fn run(
        &self,
        _ctx: &mut WorkflowCtx<'_, S>,
        input: EventPayload,
    ) -> Result<EventPayload, String> {
        assert_eq!(
            self.store.acquire(&signup_id()).err(),
            Some(JournalError::ExecutionOwned { id: signup_id() })
        );
        assert_eq!(
            Execution::new(self.store, signup_id()).err(),
            Some(EngineError::Journal(JournalError::ExecutionOwned {
                id: signup_id()
            }))
        );
        Ok(input)
    }
}

fn verify_workflow_ownership<S: JournalStore>(store: &S) {
    let workflow = OwnershipProbe { store };
    let clock = TestClock::at(Timestamp::from_millis_since_epoch(0));
    let output = Engine::new(store)
        .run(
            signup_id(),
            &workflow,
            EventPayload::new(Vec::new()),
            &clock,
            &mut SignupRng,
        )
        .unwrap();
    assert!(output.as_bytes().is_empty());
    assert!(store.acquire(&signup_id()).is_ok());
}

fn verify_concurrent_start<S: JournalStore + Sync>(store: &S) {
    let before = Barrier::new(2);
    let outcomes = thread::scope(|scope| {
        let attempts: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    let workflow = OwnershipProbe { store };
                    let clock = TestClock::at(Timestamp::from_millis_since_epoch(0));
                    before.wait();
                    Engine::new(store).run(
                        signup_id(),
                        &workflow,
                        EventPayload::new(Vec::new()),
                        &clock,
                        &mut SignupRng,
                    )
                })
            })
            .collect();
        attempts
            .into_iter()
            .map(|attempt| attempt.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    for outcome in outcomes {
        match outcome {
            Ok(output) => assert!(output.as_bytes().is_empty()),
            Err(RunError::Engine(
                EngineError::ExistingExecution { id }
                | EngineError::Journal(JournalError::ExecutionOwned { id }),
            )) => assert_eq!(id, signup_id()),
            Err(error) => panic!("unexpected concurrent-start result: {error:?}"),
        }
    }
    assert_eq!(
        store.load(&signup_id()).unwrap().events(),
        &[
            JournalEvent::ExecutionStarted {
                workflow: WorkflowName::new("signup").unwrap(),
                version: WorkflowVersion::new("2026.07.18").unwrap(),
                input: EventPayload::new(Vec::new()),
            },
            JournalEvent::ExecutionCompleted {
                output: EventPayload::new(Vec::new())
            },
        ]
    );
}

#[test]
fn concurrent_memory_starts_create_exactly_one_durable_invocation() {
    verify_concurrent_start(&MemoryJournal::new());
}

#[test]
fn concurrent_file_starts_create_exactly_one_durable_invocation() {
    let dir = scratch_dir("concurrent-start");
    verify_concurrent_start(&FileJournal::new(&dir).unwrap());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn memory_execution_holds_ownership_while_workflow_code_runs() {
    verify_workflow_ownership(&MemoryJournal::new());
}

#[test]
fn file_execution_holds_ownership_while_workflow_code_runs() {
    let dir = scratch_dir("workflow");
    verify_workflow_ownership(&FileJournal::new(&dir).unwrap());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn memory_ownership_excludes_a_second_owner_until_the_first_guard_is_dropped() {
    let store = MemoryJournal::new();
    let lease = store.acquire(&signup_id()).unwrap();

    assert_eq!(
        store.acquire(&signup_id()).err(),
        Some(JournalError::ExecutionOwned { id: signup_id() })
    );
    drop(lease);
    assert!(store.acquire(&signup_id()).is_ok());
    assert!(store.load(&signup_id()).unwrap().is_empty());
}

#[test]
fn concurrent_memory_acquisition_has_exactly_one_owner() {
    let store = MemoryJournal::new();
    let before = Barrier::new(2);
    let after = Barrier::new(2);

    let winners = thread::scope(|scope| {
        let attempts: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    before.wait();
                    let lease = store.acquire(&signup_id());
                    after.wait();
                    lease.is_ok()
                })
            })
            .collect();
        attempts
            .into_iter()
            .map(|attempt| attempt.join().unwrap())
            .filter(|won| *won)
            .count()
    });

    assert_eq!(winners, 1);
    assert!(store.acquire(&signup_id()).is_ok());
}

#[test]
fn a_running_handle_keeps_memory_ownership_until_it_is_dropped() {
    let store = MemoryJournal::new();
    let running = start(&store);

    assert_eq!(
        store.acquire(&signup_id()).err(),
        Some(JournalError::ExecutionOwned { id: signup_id() })
    );
    drop(running);
    assert!(store.acquire(&signup_id()).is_ok());
}

#[test]
fn a_running_handle_keeps_file_ownership_until_it_is_dropped() {
    let dir = scratch_dir("running");
    let store = FileJournal::new(&dir).unwrap();
    let contender = FileJournal::new(&dir).unwrap();
    let running = start(&store);

    assert_eq!(
        contender.acquire(&signup_id()).err(),
        Some(JournalError::ExecutionOwned { id: signup_id() })
    );
    drop(running);
    assert!(contender.acquire(&signup_id()).is_ok());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn file_ownership_excludes_other_handles_without_reading_or_healing_the_journal() {
    let dir = scratch_dir("no-healing");
    let store = FileJournal::new(&dir).unwrap();
    let contender = FileJournal::new(&dir).unwrap();
    let lease = store.acquire(&signup_id()).unwrap();
    let path = dir.join(format!("{}.journal", "51".repeat(16)));
    let incomplete = b"a torn frame must not be healed by a competing owner";
    fs::write(&path, incomplete).unwrap();

    assert_eq!(
        Execution::recover(
            &contender,
            signup_id(),
            &WorkflowName::new("signup").unwrap(),
            &WorkflowVersion::new("2026.07.18").unwrap(),
        )
        .err(),
        Some(yaoki::context::EngineError::Journal(
            JournalError::ExecutionOwned { id: signup_id() }
        ))
    );
    assert_eq!(fs::read(&path).unwrap(), incomplete);
    drop(lease);
    assert!(contender.acquire(&signup_id()).is_ok());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn another_process_cannot_acquire_an_owned_execution() {
    let dir = scratch_dir("process");
    let store = FileJournal::new(&dir).unwrap();
    let lease = store.acquire(&signup_id()).unwrap();

    let status = Command::new(env::current_exe().unwrap())
        .args(["--exact", "file_lock_contender_process", "--nocapture"])
        .env("YAOKI_LOCK_TEST_DIRECTORY", &dir)
        .status()
        .unwrap();

    assert!(status.success());
    drop(lease);
    assert!(store.acquire(&signup_id()).is_ok());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn file_lock_contender_process() {
    if let Some(dir) = env::var_os("YAOKI_LOCK_TEST_DIRECTORY") {
        let store = FileJournal::new(PathBuf::from(dir)).unwrap();
        assert_eq!(
            store.acquire(&signup_id()).err(),
            Some(JournalError::ExecutionOwned { id: signup_id() })
        );
    }
}

#[test]
fn file_lock_files_remain_after_release_so_future_owners_lock_the_same_inode() {
    let dir = scratch_dir("stable-lock");
    let store = FileJournal::new(&dir).unwrap();
    let lease = store.acquire(&signup_id()).unwrap();
    let files_before: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    drop(lease);

    let files_after: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files_before, files_after);
    assert_eq!(files_after.len(), 1);
    assert!(store.acquire(&signup_id()).is_ok());
    fs::remove_dir_all(dir).unwrap();
}
