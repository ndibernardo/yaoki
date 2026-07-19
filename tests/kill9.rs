//! `kill -9` integration test: spawns the demo binary
//! running the signup workflow, kills it in the window between
//! `create-account`'s side effect landing and `StepCompleted` being
//! journaled, restarts it against the same journal directory, and checks
//! the persisted effect trace satisfies `DuplicateLast` against a
//! failure-free reference run, with the journal ending in
//! `ExecutionCompleted`.

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;

use yaoki::execution::ExecutionId;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::stores::file::FileJournal;

const SIGNUP_SEED: &str = "signup-kill9-2026-07-19";

/// Mirrors `SeededRng` in `src/main.rs`: the test must derive the same
/// `ExecutionId` the spawned binary uses, to load its journal afterward.
struct SeededRng {
    seed: [u8; 32],
}

impl RngSource for SeededRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new(self.seed)
    }
}

fn execution_id_from_seed(label: &str) -> ExecutionId {
    let mut seed = [0u8; 32];
    for (slot, byte) in seed.iter_mut().zip(label.bytes()) {
        *slot = byte;
    }
    let mut rng = SeededRng { seed };
    ExecutionId::generate(&mut rng)
}

fn demo_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_yaoki"))
}

fn scratch_dir(label: &str) -> PathBuf {
    let dir = env::temp_dir().join(format!("yaoki-kill9-{label}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("scratch dir must be creatable");
    dir
}

fn read_trace_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

/// `DuplicateLast`: `observed` equals `reference`, or `observed` has
/// exactly one extra trailing element duplicating `reference`'s last.
fn is_duplicate_last(reference: &[String], observed: &[String]) -> bool {
    if observed == reference {
        return true;
    }
    match (reference.last(), observed.split_last()) {
        (Some(expected_last), Some((observed_last, observed_prefix))) => {
            observed_prefix == reference && observed_last == expected_last
        }
        _ => false,
    }
}

fn run_demo(journal_dir: &Path, trace_path: &Path, marker_path: &Path) -> bool {
    Command::new(demo_binary())
        .arg(journal_dir)
        .arg(SIGNUP_SEED)
        .arg(trace_path)
        .arg(marker_path)
        .status()
        .expect("demo binary must spawn")
        .success()
}

#[test]
fn kill9_mid_step_then_restart_satisfies_duplicate_last_and_completes() {
    // A failure-free reference run establishes the expected trace.
    let reference_dir = scratch_dir("reference");
    let reference_trace_path = reference_dir.join("trace.log");
    let reference_marker_path = reference_dir.join("marker");
    let reference_journal_dir = reference_dir.join("journal");
    assert!(run_demo(
        &reference_journal_dir,
        &reference_trace_path,
        &reference_marker_path,
    ));
    let reference_trace = read_trace_lines(&reference_trace_path);
    assert_eq!(reference_trace, vec!["charge-card", "create-account"]);

    // Spawn the same workflow again, but SIGKILL it right after
    // create-account's side effect lands (marker file appears) and before
    // StepCompleted is journaled. That is the duplicate-effect crash window.
    let crash_dir = scratch_dir("crash");
    let trace_path = crash_dir.join("trace.log");
    let marker_path = crash_dir.join("marker");
    let journal_dir = crash_dir.join("journal");
    let mut child = Command::new(demo_binary())
        .arg(&journal_dir)
        .arg(SIGNUP_SEED)
        .arg(&trace_path)
        .arg(&marker_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("crash run must spawn");

    let mut polls = 0;
    while !marker_path.exists() {
        polls += 1;
        assert!(polls < 500, "after-effect marker never appeared");
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().expect("SIGKILL must succeed");
    let _ = child.wait();

    // Restart against the same journal directory; recovery must rerun the
    // interrupted step and complete the execution.
    assert!(run_demo(&journal_dir, &trace_path, &marker_path));

    // The observed trace duplicates only the interrupted step's
    // last effect, and the journal ends with ExecutionCompleted.
    let observed_trace = read_trace_lines(&trace_path);
    assert!(
        is_duplicate_last(&reference_trace, &observed_trace),
        "observed trace {observed_trace:?} is not a DuplicateLast extension \
         of reference {reference_trace:?}"
    );

    let store = FileJournal::new(&journal_dir).expect("journal dir must open");
    let execution = execution_id_from_seed(SIGNUP_SEED);
    let journal = store.load(&execution).expect("journal must load");
    assert!(matches!(
        journal.events().last(),
        Some(JournalEvent::ExecutionCompleted { .. })
    ));
}
