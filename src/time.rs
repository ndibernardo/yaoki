//! Wall-clock types. Durable timers persist deadlines, never durations:
//! restart must not reset the clock.

use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Wall-clock instant, milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn from_millis_since_epoch(millis: u64) -> Self {
        Self(millis)
    }

    pub fn as_millis_since_epoch(&self) -> u64 {
        self.0
    }
}

/// Wall-clock deadline. Never a duration: recovery re-arms the remainder
/// against this absolute instant, so restart does not reset the clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Deadline(Timestamp);

impl Deadline {
    pub fn at(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    pub fn timestamp(&self) -> Timestamp {
        self.0
    }
}

/// Source of wall-clock time for a workflow run. `ctx.now()` is the only
/// door: `SystemTime::now` is banned elsewhere in this crate
/// (`clippy.toml` `disallowed-methods`), so `SystemClock` below is the sole
/// sanctioned caller.
pub trait Clock {
    fn now(&self) -> Timestamp;
}

/// Real wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[allow(
        clippy::disallowed_methods,
        reason = "the one sanctioned SystemTime::now call site; ctx.now() is the workflow-facing door"
    )]
    fn now(&self) -> Timestamp {
        // A clock set before the Unix epoch clamps to it rather than
        // panicking: unreachable in practice, not worth threading a
        // fallible `Clock::now` through every `ctx.now()` call.
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Timestamp::from_millis_since_epoch(millis as u64)
    }
}

/// Deterministic clock for tests: holds a fixed instant, advanced only by
/// explicit calls, never by reading the OS clock. "Charge in a week"
/// compresses to milliseconds by advancing this instead of sleeping.
#[derive(Debug)]
pub struct TestClock {
    now: Mutex<Timestamp>,
}

impl TestClock {
    pub fn at(timestamp: Timestamp) -> Self {
        Self {
            now: Mutex::new(timestamp),
        }
    }

    pub fn advance_millis(&self, millis: u64) {
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now = Timestamp::from_millis_since_epoch(now.as_millis_since_epoch() + millis);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_millis_since_epoch() {
        let timestamp = Timestamp::from_millis_since_epoch(1_752_838_800_000);

        assert_eq!(timestamp.as_millis_since_epoch(), 1_752_838_800_000);
    }

    #[test]
    fn deadline_at_preserves_the_absolute_timestamp() {
        let one_week_from_epoch = Timestamp::from_millis_since_epoch(604_800_000);

        let deadline = Deadline::at(one_week_from_epoch);

        assert_eq!(deadline.timestamp(), one_week_from_epoch);
    }

    #[test]
    fn system_clock_now_returns_a_time_after_the_epoch() {
        let clock = SystemClock;

        let now = clock.now();

        assert!(now.as_millis_since_epoch() > 0);
    }

    #[test]
    fn test_clock_now_returns_the_fixed_instant() {
        let charge_renewal = Timestamp::from_millis_since_epoch(1_753_401_600_000);
        let clock = TestClock::at(charge_renewal);

        assert_eq!(clock.now(), charge_renewal);
    }

    #[test]
    fn test_clock_advance_millis_moves_the_clock_forward() {
        let charge_renewal = Timestamp::from_millis_since_epoch(1_753_401_600_000);
        let clock = TestClock::at(charge_renewal);

        // "Charge in a week" compressed to milliseconds.
        clock.advance_millis(50);

        assert_eq!(
            clock.now(),
            Timestamp::from_millis_since_epoch(1_753_401_600_050)
        );
    }
}
