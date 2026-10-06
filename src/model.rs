//! Host model adapters selected by settings.
//!
//! In-process `scripted` and `plane` live in `shikigami-engine`. This module
//! wires the OpenAI-compatible HTTP adapter onto the engine port.

pub use shikigami_engine::model::{
    ChatMessage, CostEstimate, ModelError, ModelPort, ModelTurn, ScriptedModel, TokenUsage,
    ToolCall, effective_model_name, stable_tool_call_id,
};

#[cfg(feature = "model-http")]
pub use shikigami_http::HttpModel;

use shikigami_engine::config::Config;
use shikigami_engine::model as engine_model;

pub fn from_config(config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
    match config.model.adapter.as_str() {
        "http" => http_from_config(config),
        "plane"
            if config.model.fallback.enabled
                && config.model.fallback.adapter.as_deref() == Some("http") =>
        {
            http_from_config(config)
        }
        _ => engine_model::from_config(config),
    }
}

fn http_from_config(config: &Config) -> Result<Box<dyn ModelPort>, ModelError> {
    #[cfg(feature = "model-http")]
    {
        Ok(Box::new(HttpModel::from_config(config)?))
    }
    #[cfg(not(feature = "model-http"))]
    {
        let _ = config;
        Err(ModelError::HttpUnavailable)
    }
}
