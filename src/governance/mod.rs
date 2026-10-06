//! Host governance adapters selected by settings.
//!
//! In-process `none` and `local` live in `shikigami-engine`. This module
//! wires `http-callback` / `host-authz` and `sekai-chisei` onto the engine
//! port.

mod http_callback;
#[cfg(feature = "governance-sekai-chisei")]
pub use shikigami_governance_sekai as sekai_chisei;

pub use http_callback::HttpCallbackGovernance;
pub use shikigami_engine::governance::{
    ApprovalState, AvailableModel, ContentTurnContext, GovernanceError, GovernancePort,
    LocalGovernance, NoneGovernance, RunHandle, RunOutcome, now_unix_ms, resolve_approval_wait,
};

use shikigami_engine::config::Config;
use shikigami_engine::governance as engine_governance;

pub fn from_config(config: &Config) -> Result<Box<dyn GovernancePort>, GovernanceError> {
    match config.governance.adapter.as_str() {
        "http-callback" | "host-authz" => {
            Ok(Box::new(HttpCallbackGovernance::from_config(config)?))
        }
        "sekai-chisei" => {
            #[cfg(feature = "governance-sekai-chisei")]
            {
                Ok(Box::new(sekai_chisei::SekaiChiseiGovernance::from_config(
                    config,
                )?))
            }
            #[cfg(not(feature = "governance-sekai-chisei"))]
            {
                Err(GovernanceError::Unavailable(
                    "built without governance-sekai-chisei feature".into(),
                ))
            }
        }
        _ => engine_governance::from_config(config),
    }
}
