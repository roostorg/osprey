/// An AckId is basically an opaque type that is used to internally represent theAckId.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub(crate) struct AckId(u64);

impl AckId {
    /// Creates a new AckId, starting at the beginning of the sequence (0).
    pub(crate) fn new() -> Self {
        AckId(0)
    }

    /// Returns the "next" AckId. In the event that the internal sequence overflows, it simply wraps around.
    ///
    /// Practically, the wrap-around of a u64 is impossible.
    pub(crate) fn next(&self) -> AckId {
        Self(self.0.wrapping_add(1))
    }
}
