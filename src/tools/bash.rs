//! Background bash job bookkeeping and bash output caps.

use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) const MAX_BASH_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_BG_JOBS: usize = 4;
pub(crate) const MAX_BG_LOG_BYTES: usize = 256 * 1024;

pub(crate) struct BackgroundJobs {
    pub(crate) next_id: u64,
    pub(crate) jobs: HashMap<String, BgJob>,
}

pub(crate) struct BgJob {
    pub(crate) child: tokio::process::Child,
    pub(crate) log_path: PathBuf,
}

impl BackgroundJobs {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 1,
            jobs: HashMap::new(),
        }
    }
}
