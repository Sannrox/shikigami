//! Host governance adapters selected by settings.
//!
//! In-process `none` and `local` live in `shikigami-engine`. This module
//! wires `http-callback` / `host-authz` onto the engine port. Rusui is the
//! plane caller ([ADR 0024](../../docs/decisions/0024-local-guest-rusui-plane-caller.md)).

pub(crate) mod http_callback;

pub use http_callback::HttpCallbackGovernance;
pub use shikigami_engine::governance::{
    ApprovalState, AvailableModel, GovernanceError, GovernancePort, LocalGovernance,
    NoneGovernance, RunHandle, RunOutcome, now_unix_ms, resolve_approval_wait,
};

use shikigami_engine::config::Config;
use shikigami_engine::governance as engine_governance;

pub fn from_config(config: &Config) -> Result<Box<dyn GovernancePort>, GovernanceError> {
    match config.governance.adapter.as_str() {
        "http-callback" | "host-authz" => {
            Ok(Box::new(HttpCallbackGovernance::from_config(config)?))
        }
        "sekai-chisei" => Err(GovernanceError::Unavailable(
            "governance adapter `sekai-chisei` was removed; rusui is the plane caller (ADR 0024)"
                .into(),
        )),
        _ => engine_governance::from_config(config),
    }
}
