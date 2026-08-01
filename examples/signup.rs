//! Signup, crashed between the payment and the account: `charge-payment`
//! then `create-account` against two fake external services. The run dies
//! once `charge-payment`'s result is durable, a fresh engine recovers over
//! the same journal, and the card is not charged a second time.
//!
//! Run it with `cargo run --example signup`.
//!
//! The store here is a `MemoryJournal`, so "the process died" is simulated by
//! a failpoint and by dropping the engine. Swapping in a `FileJournal` is a
//! one-line change; `tests/kill9.rs` runs that version under a real
//! `SIGKILL`.

use std::cell::Cell;
use std::error::Error;

use yaoki::context::WorkflowCtx;
use yaoki::engine::Engine;
use yaoki::engine::Workflow;
use yaoki::equivalence::DuplicateLast;
use yaoki::execution::ExecutionId;
use yaoki::execution::WorkflowName;
use yaoki::execution::WorkflowVersion;
use yaoki::failpoints::CrashOnce;
use yaoki::failpoints::CrashPoint;
use yaoki::journal::EventPayload;
use yaoki::journal::JournalEvent;
use yaoki::journal::JournalStore;
use yaoki::journal::Seq;
use yaoki::random::RandomBytes;
use yaoki::random::RngSource;
use yaoki::step::StepError;
use yaoki::step::StepName;
use yaoki::stores::memory::MemoryJournal;
use yaoki::time::SystemClock;

/// Deterministic execution identity for the demo: 32 seed bytes derived from
/// a label instead of ambient randomness.
struct SeededRng {
    seed: [u8; 32],
}

impl SeededRng {
    fn from_label(label: &str) -> Self {
        let mut seed = [0u8; 32];
        for (slot, byte) in seed.iter_mut().zip(label.bytes()) {
            *slot = byte;
        }
        Self { seed }
    }
}

impl RngSource for SeededRng {
    fn next_bytes(&mut self) -> RandomBytes {
        RandomBytes::new(self.seed)
    }
}

/// Fake payment provider. Counts charges so the demo can show that recovery
/// did not bill the customer twice.
struct PaymentGateway {
    charges: Cell<usize>,
}

impl PaymentGateway {
    fn new() -> Self {
        Self {
            charges: Cell::new(0),
        }
    }

    fn charge(&self) -> EventPayload {
        self.charges.set(self.charges.get() + 1);
        println!("  [payment gateway] charging 49.00 EUR");
        EventPayload::new(br#"{"charge_id":"ch_2026_0801"}"#.to_vec())
    }

    fn charges(&self) -> usize {
        self.charges.get()
    }
}

/// Fake account directory, the second external service.
struct AccountDirectory {
    created: Cell<usize>,
}

impl AccountDirectory {
    fn new() -> Self {
        Self {
            created: Cell::new(0),
        }
    }

    fn create(&self) -> EventPayload {
        self.created.set(self.created.get() + 1);
        println!("  [account directory] creating account for john.smith@example.com");
        EventPayload::new(br#"{"account_id":"acct_2026_0801"}"#.to_vec())
    }

    fn created(&self) -> usize {
        self.created.get()
    }
}

struct SignupWorkflow<'a> {
    payments: &'a PaymentGateway,
    accounts: &'a AccountDirectory,
}

impl Workflow<MemoryJournal> for SignupWorkflow<'_> {
    type Error = StepError;

    fn name(&self) -> WorkflowName {
        WorkflowName::new("signup").expect("literal workflow name is valid")
    }

    fn version(&self) -> WorkflowVersion {
        WorkflowVersion::new("2026.08.01").expect("literal workflow version is valid")
    }

    fn run(
        &self,
        ctx: &mut WorkflowCtx<'_, MemoryJournal>,
        _input: EventPayload,
    ) -> Result<EventPayload, StepError> {
        let charge_payment = StepName::new("charge-payment").expect("literal step name is valid");
        ctx.step(charge_payment, |_key| Ok(self.payments.charge()))?;

        let create_account = StepName::new("create-account").expect("literal step name is valid");
        ctx.step(create_account, |_key| Ok(self.accounts.create()))
    }
}

fn signup_input() -> EventPayload {
    EventPayload::new(br#"{"email":"john.smith@example.com"}"#.to_vec())
}

fn describe(event: &JournalEvent) -> String {
    match event {
        JournalEvent::ExecutionStarted { workflow, .. } => {
            format!("ExecutionStarted({})", workflow.as_str())
        }
        JournalEvent::StepScheduled { seq, name } => {
            format!("StepScheduled({}, {})", seq.get(), name.as_str())
        }
        JournalEvent::StepStarted { seq, attempt } => {
            format!("StepStarted({}, attempt {})", seq.get(), attempt.get())
        }
        JournalEvent::StepCompleted { seq, .. } => format!("StepCompleted({})", seq.get()),
        JournalEvent::StepFailed { seq, .. } => format!("StepFailed({})", seq.get()),
        JournalEvent::TimerScheduled { seq, .. } => format!("TimerScheduled({})", seq.get()),
        JournalEvent::TimerFired { seq } => format!("TimerFired({})", seq.get()),
        JournalEvent::NowRecorded { seq, .. } => format!("NowRecorded({})", seq.get()),
        JournalEvent::RandomRecorded { seq, .. } => format!("RandomRecorded({})", seq.get()),
        JournalEvent::ExecutionCompleted { .. } => "ExecutionCompleted".to_owned(),
        JournalEvent::ExecutionFailed { .. } => "ExecutionFailed".to_owned(),
    }
}

fn print_journal(store: &MemoryJournal, execution: &ExecutionId) -> Result<(), Box<dyn Error>> {
    println!("journal:");
    for event in store.load(execution)?.events() {
        println!("  {}", describe(event));
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let store = MemoryJournal::new();
    let payments = PaymentGateway::new();
    let accounts = AccountDirectory::new();
    let workflow = SignupWorkflow {
        payments: &payments,
        accounts: &accounts,
    };
    let mut rng = SeededRng::from_label("signup-example-2026-08-01");
    let execution = ExecutionId::generate(&mut rng);
    let clock = SystemClock;

    // The process dies once charge-payment's result is durable and before
    // create-account is even scheduled.
    println!("run 1: charge the card, then die");
    let policy = CrashOnce::new(CrashPoint::AfterStepCompleted(Seq::zero()));
    let crashed = Engine::<_, DuplicateLast>::with_failpoints(&store, &policy).run(
        execution,
        &workflow,
        signup_input(),
        &clock,
        &mut rng,
    );
    println!("  run 1 ended with: {crashed:?}");
    print_journal(&store, &execution)?;

    // A fresh engine over the surviving journal: charge-payment is answered
    // from the journal, only create-account runs.
    println!("run 2: recover over the same journal");
    let output = Engine::<_, DuplicateLast>::new(&store).recover_and_run(
        execution,
        &workflow,
        signup_input(),
        &clock,
        &mut rng,
    );
    match output {
        Ok(payload) => println!(
            "  completed: {}",
            String::from_utf8_lossy(payload.as_bytes())
        ),
        Err(error) => return Err(format!("recovery failed: {error:?}").into()),
    }
    print_journal(&store, &execution)?;

    println!("charges: {}", payments.charges());
    println!("accounts created: {}", accounts.created());
    Ok(())
}
