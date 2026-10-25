//! Deterministic simulation: a seeded PRNG, a simulated disk, and the VOPR
//! driver that checks the store against a model under crashes and faults.

pub mod rng;
pub mod storage;
pub mod perf;
pub mod vopr;

pub use rng::Rng;
pub use storage::{ClassStats, FaultConfig, IoStats, SimStorage};
