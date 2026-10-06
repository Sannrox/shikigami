//! Turn loop, ports, workspace jail, and in-process adapters for shikigami.
//!
//! Hosts inject `Arc<dyn *Port>` through [`run::Engine`]. Composition
//! (`Harness::from_config`) and the HTTP-callback adapter live in the
//! `shikigami` crate. The HTTP model adapter is `shikigami-http`; production
//! governance is `shikigami-governance-sekai`.

pub mod artifacts;
pub mod atomic;
pub mod checkpoint;
pub mod config;
pub mod content;
pub mod context;
pub mod digest;
pub mod events;
pub mod evidence_queue;
pub mod fallback;
pub mod governance;
pub mod hooks;
pub mod identity;
pub mod mcp;
pub mod metrics;
pub mod model;
pub mod prompts;
pub mod registry;
pub mod replay;
pub mod run;
pub mod sandbox;
pub mod state;
pub mod tools;
pub mod tracing_export;
pub mod transcript;
pub mod worker_lifecycle;
pub mod workspace;
