//! Filesystem serve poll-and-drain loop behind the thin host entry.
//!
//! Owns concurrency, idle poll, `max_jobs`, graceful drain, health writes,
//! and control-task abort. Queue lifecycle and HTTP control stay in their
//! private modules.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

use super::control;
use super::queue::FilesystemQueue;
use super::{ControlOptions, ServeError, ServeOptions, ServeRuntimeOptions, wait_shutdown};
use crate::harness::Harness;

/// Drive claimed filesystem jobs until shutdown or `max_jobs` completed.
pub(super) async fn run_until_shutdown(
    harness: &Harness,
    queue: FilesystemQueue,
    options: ServeOptions,
    runtime: ServeRuntimeOptions,
    control: Option<ControlOptions>,
    shutdown: watch::Receiver<bool>,
) -> Result<u64, ServeError> {
    let running = Arc::new(AtomicBool::new(true));
    queue.write_health(&running, 0, None, &runtime)?;

    let control_task = if let Some(control) = control {
        let listener = TcpListener::bind(control.bind).await?;
        let control_harness = harness.clone();
        let control_queue = queue.clone();
        let control_shutdown = shutdown.clone();
        Some(tokio::spawn(async move {
            control::listen(
                listener,
                control_harness,
                control_queue,
                control,
                control_shutdown,
            )
            .await
        }))
    } else {
        None
    };

    let mut completed = 0u64;
    let mut active = 0usize;
    let mut last_run_id = None;
    let mut jobs = JoinSet::new();
    let mut stopping = false;
    loop {
        if *shutdown.borrow() {
            stopping = true;
            break;
        }
        if let Some(max) = options.max_jobs
            && completed + active as u64 >= max
            && active == 0
        {
            break;
        }

        while active < runtime.concurrency
            && options
                .max_jobs
                .map(|max| completed + (active as u64) < max)
                .unwrap_or(true)
        {
            let Some(claimed) = queue.claim_next()? else {
                break;
            };
            let worker = harness.clone();
            let worker_queue = queue.clone();
            let retry_limit = runtime.retry_limit;
            jobs.spawn(async move {
                worker_queue
                    .run_claimed(&worker, claimed, retry_limit)
                    .await
            });
            active += 1;
        }

        queue.write_health(&running, active, last_run_id.clone(), &runtime)?;
        if let Some(max) = options.max_jobs
            && completed >= max
            && active == 0
        {
            break;
        }
        if active == 0 {
            tokio::select! {
                _ = tokio::time::sleep(options.poll_interval) => {}
                _ = wait_shutdown(shutdown.clone()) => { stopping = true; break; }
            }
        } else {
            tokio::select! {
                joined = jobs.join_next() => {
                    active = active.saturating_sub(1);
                    if let Some(joined) = joined {
                        match joined {
                            Ok(Ok(Some(result))) => {
                                completed += 1;
                                last_run_id = Some(result.run_id);
                                queue.write_health(&running, active, last_run_id.clone(), &runtime)?;
                            }
                            Ok(Ok(None)) => {}
                            Ok(Err(_)) | Err(_) => {
                                completed += 1;
                                queue.write_health(&running, active, last_run_id.clone(), &runtime)?;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(options.poll_interval) => {}
                _ = wait_shutdown(shutdown.clone()) => { stopping = true; break; }
            }
        }
    }

    // Drain already claimed jobs on graceful shutdown; no new jobs are taken.
    if stopping {
        while let Some(joined) = jobs.join_next().await {
            active = active.saturating_sub(1);
            match joined {
                Ok(Ok(Some(result))) => {
                    completed += 1;
                    last_run_id = Some(result.run_id);
                }
                Ok(Ok(None)) => {}
                Ok(Err(_)) | Err(_) => {
                    completed += 1;
                }
            }
        }
    }

    running.store(false, Ordering::SeqCst);
    queue.write_health(&running, 0, last_run_id, &runtime)?;
    if let Some(task) = control_task {
        task.abort();
        let _ = task.await;
    }
    Ok(completed)
}
