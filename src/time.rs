//! Wall-clock types. Durable timers persist deadlines, never durations:
//! restart must not reset the clock. `Clock` trait arrives alongside
//! `ctx.now()` / `ctx.sleep_until()`.

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
}
