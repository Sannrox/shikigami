//! One governed run's authoritative receipt-finalization protocol.

use super::{GovernanceError, RunHandle, RunOutcome, SekaiChiseiGovernance, harvest};

/// Reconcile required receipt surfaces, report the outcome, and release local state.
pub(super) async fn complete(
    governance: &SekaiChiseiGovernance,
    handle: &RunHandle,
    outcome: RunOutcome,
) -> Result<(), GovernanceError> {
    if let Err(error) = governance.retry_pending_harvest_event(handle).await {
        return governance.finish_best_effort(handle, error);
    }
    let receipt = match governance.harvest_receipt(handle).await {
        Ok(receipt) => receipt,
        Err(error) => return governance.finish_best_effort(handle, error),
    };
    if receipt.complete {
        governance.forget_harvest(&handle.run_id);
        return Ok(());
    }
    if missing_surface(&receipt.missing_surfaces, "attempt") {
        let attributes = harvest::attempt_attributes(&handle.run_id, &handle.operation_id);
        if let Err(error) = governance
            .report_harvest_event(handle, harvest::KIND_ATTEMPT, attributes, vec![])
            .await
        {
            return governance.finish_best_effort(handle, error);
        }
    }
    let receipt = match governance.harvest_receipt(handle).await {
        Ok(receipt) => receipt,
        Err(error) => return governance.finish_best_effort(handle, error),
    };
    if missing_surface(&receipt.missing_surfaces, "model_call") {
        let model_operation_id = governance
            .harvest
            .model_operation(&handle.run_id)?
            .0
            .filter(|operation_id| !operation_id.is_empty());
        let Some(model_operation_id) = model_operation_id else {
            return match governance
                .abort_uncheckpointed_receipt(handle, &outcome.summary)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => governance.finish_best_effort(handle, error),
            };
        };
        if let Err(error) = governance
            .report_harvest_event(
                handle,
                harvest::KIND_MODEL,
                harvest::model_attributes(&model_operation_id, false),
                vec![],
            )
            .await
        {
            return governance.finish_best_effort(handle, error);
        }
    }
    let response = match governance
        .report_harvest_event(
            handle,
            harvest::KIND_COMPLETE,
            harvest::complete_attributes(&outcome),
            harvest::complete_references(handle, &outcome),
        )
        .await
    {
        Ok(response) => response,
        Err(error) => return governance.finish_best_effort(handle, error),
    };
    if !response.complete && governance.fail_closed {
        return Err(GovernanceError::Message(format!(
            "operation receipt remains incomplete; missing surfaces: {}",
            response.missing_surfaces.join(", ")
        )));
    }
    governance.forget_harvest(&handle.run_id);
    Ok(())
}

fn missing_surface(surfaces: &[String], expected: &str) -> bool {
    surfaces.iter().any(|surface| surface == expected)
}

impl SekaiChiseiGovernance {
    fn finish_best_effort(
        &self,
        handle: &RunHandle,
        error: GovernanceError,
    ) -> Result<(), GovernanceError> {
        if self.fail_closed {
            return Err(error);
        }
        self.forget_harvest(&handle.run_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::missing_surface;

    #[test]
    fn identifies_only_the_requested_receipt_surface() {
        let surfaces = vec!["attempt".into(), "model_call".into()];
        assert!(missing_surface(&surfaces, "attempt"));
        assert!(missing_surface(&surfaces, "model_call"));
        assert!(!missing_surface(&surfaces, "outcome"));
    }
}
