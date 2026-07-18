//! Journaled randomness. `ctx.random()` records the drawn bytes so replay
//! returns the original answer instead of drawing again.

/// Fixed-width random bytes drawn through `RngSource`, journaled for replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RandomBytes([u8; 32]);

impl RandomBytes {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Source of randomness for a workflow run. Real impl draws from the OS;
/// test impls are deterministic. `rand::random` is banned inside this crate
/// (`clippy.toml` `disallowed-methods`); this trait is the only door.
pub trait RngSource {
    fn next_bytes(&mut self) -> RandomBytes;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_bytes_as_bytes_returns_constructed_value() {
        let mut drawn = [0u8; 32];
        drawn[0] = 0x7a;
        drawn[31] = 0x01;

        let random = RandomBytes::new(drawn);

        assert_eq!(random.as_bytes(), &drawn);
    }
}
