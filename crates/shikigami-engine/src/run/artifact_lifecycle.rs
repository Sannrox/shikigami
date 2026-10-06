//! Run artifact lifecycle behind one private interface.
//!
//! The lifecycle owns best-effort baseline capture, terminal background-job
//! reaping, bounded manifest capture, warning publication, and Run Registry
//! linkage. Stable manifest and export compatibility remains in `artifacts`.

use std::path::{Path, PathBuf};

use crate::events::HarnessEvent;
use crate::tools::ToolRegistry;

use super::Engine;

pub(super) struct RunArtifactLifecycle<'a> {
    engine: &'a Engine,
}

impl<'a> RunArtifactLifecycle<'a> {
    pub(super) fn new(engine: &'a Engine) -> Self {
        Self { engine }
    }

    pub(super) fn begin(&self, run_id: &str, workspace: &Path) {
        if let Err(error) =
            crate::artifacts::capture_run_baseline(&self.engine.state_runs, run_id, workspace)
        {
            self.warn(run_id, format!("artifact baseline capture failed: {error}"));
        }
    }

    pub(super) async fn finalize(
        &self,
        run_id: &str,
        workspace: &Path,
        tools: &ToolRegistry,
    ) -> Option<PathBuf> {
        // Descendants must stop mutating the workspace before final inventory.
        tools.kill_background_jobs().await;
        match crate::artifacts::capture_run_artifacts(&self.engine.state_runs, run_id, workspace) {
            Ok(path) => {
                if let Err(error) = self.engine.registry.set_artifact_dir(run_id, &path) {
                    self.warn(run_id, format!("artifact registry linkage failed: {error}"));
                }
                Some(path)
            }
            Err(error) => {
                self.warn(run_id, format!("artifact capture failed: {error}"));
                None
            }
        }
    }

    fn warn(&self, run_id: &str, text: String) {
        self.engine.emit(
            run_id,
            HarnessEvent::Message {
                level: "warn".into(),
                text,
            },
        );
    }
}
