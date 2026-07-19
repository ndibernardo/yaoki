//! Demo binary: runs the signup workflow against a `FileJournal`. Wiring
//! only. All engine logic lives in the library.
//!
//! Usage: `yaoki <journal-dir> <execution-seed> <effect-trace-path>
//! <after-effect-marker-path>`
//!
//! `create-account` writes `after-effect-marker` right after its side effect
//! lands and before returning, giving `tests/kill9.rs` a window to `SIGKILL`
//! the process between the side effect landing and `StepCompleted` being
//! journaled.

use std::env;
use std::error::Error;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::RunError;
use yaoki::engine::Workflow;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::journal::EventPayload;
use yaoki::journal::JournalStore;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::StepName;
use yaoki::stores::file::FileJournal;

/// Deterministic execution identity for demo/test runs: derives 32 seed
/// bytes from a CLI-supplied label, not from ambient randomness.
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

fn append_line(path: &Path, line: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("effect trace file must be writable");
    writeln!(file, "{line}").expect("effect trace write must succeed");
}

/// Two steps against fake external services: charge a card, then create an
/// account. `create-account` signals `after_effect_marker` after its side
/// effect lands. See the module docs.
struct SignupWorkflow {
    effect_trace: PathBuf,
    after_effect_marker: PathBuf,
}

impl Workflow<FileJournal> for SignupWorkflow {
    type Error = String;

    fn name(&self) -> WorkflowName {
        WorkflowName::new("signup").expect("literal workflow name is valid")
    }

    fn version(&self) -> WorkflowVersion {
        WorkflowVersion::new("2026.07.19").expect("literal workflow version is valid")
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, FileJournal>,
        input: EventPayload,
    ) -> Result<EventPayload, String> {
        let charge_card = StepName::new("charge-card").expect("literal step name is valid");
        let charge_confirmation = ctx
            .step(charge_card, |_key| {
                append_line(&self.effect_trace, "charge-card");
                Ok(EventPayload::new(
                    br#"{"charge_id":"ch_2026_0719"}"#.to_vec(),
                ))
            })
            .map_err(|error| format!("{error:?}"))?;

        let create_account = StepName::new("create-account").expect("literal step name is valid");
        ctx.step(create_account, |_key| {
            append_line(&self.effect_trace, "create-account");
            fs::write(&self.after_effect_marker, b"landed")
                .expect("after-effect marker write must succeed");
            thread::sleep(Duration::from_millis(200));
            Ok(EventPayload::new(
                br#"{"account_id":"acct_2026_0719"}"#.to_vec(),
            ))
        })
        .map_err(|error| format!("{error:?}"))?;

        let _ = input;
        Ok(charge_confirmation)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let usage = "usage: yaoki <journal-dir> <execution-seed> <effect-trace-path> <after-effect-marker-path>";
    let mut args = env::args().skip(1);
    let journal_dir = PathBuf::from(args.next().expect(usage));
    let seed = args.next().expect(usage);
    let effect_trace = PathBuf::from(args.next().expect(usage));
    let after_effect_marker = PathBuf::from(args.next().expect(usage));

    let store = FileJournal::new(&journal_dir)?;
    let execution = execution_id_from_seed(&seed);
    let workflow = SignupWorkflow {
        effect_trace,
        after_effect_marker,
    };
    let input = EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec());

    let engine = Engine::new(&store);
    let journal = store.load(&execution)?;
    let result = if journal.is_empty() {
        engine.run(execution, &workflow, input)
    } else {
        engine.recover_and_run(execution, &workflow, input)
    };

    match result {
        Ok(output) => {
            println!("completed: {}", String::from_utf8_lossy(output.as_bytes()));
            Ok(())
        }
        Err(RunError::Workflow(error)) => Err(format!("workflow error: {error}").into()),
        Err(RunError::Engine(error)) => Err(Box::new(error)),
        Err(RunError::Recovered(record)) => {
            Err(format!("execution previously failed: {}", record.message()).into())
        }
    }
}
