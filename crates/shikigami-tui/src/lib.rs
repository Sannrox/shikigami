//! Thin interactive process host: ACP client of the in-process session.
//!
//! Evolving surface, same rank as `shikigami acp`. Not freeze-core.
//! See [ADR 0014](../../../docs/decisions/0014-usable-guest-hosts.md).

mod clipboard;
mod look;
mod mention;
mod statusline;
mod usage;

mod host;
mod input;
mod protocol;
mod render;
mod session;
mod slash;

pub use host::{run, run_continuing};

#[cfg(test)]
mod tests;
