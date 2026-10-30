//! Security-triage profile: a site-agent turn that turns findings into
//! evidence-backed exposure verdicts (`pkg:security.exposure-verdict/v1`, #465).

use serde::{Deserialize, Serialize};

/// Governed exposure-verdict package id.
pub const PACKAGE_ID: &str = "pkg:security.exposure-verdict/v1";

const SEVERITIES: [&str; 4] = ["low", "medium", "high", "critical"];
const DISPOSITION_STATUSES: [&str; 2] = ["proposed", "recorded"];

/// Numeric rank for comparing verdict severity. `None` if the token is unknown.
pub fn severity_rank(severity: &str) -> Option<u8> {
    SEVERITIES
        .iter()
        .position(|name| *name == severity)
        .map(|index| u8::try_from(index + 1).expect("severity rank fits u8"))
}

/// A proposed explanation. Never a measured fact and never mixed into evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hypothesis {
    pub statement: String,
}

/// A disposition the agent proposes (or records when autonomy was granted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedDisposition {
    pub kind: String,
    /// `proposed` waits for review. `recorded` is allowed only with autonomy.
    pub status: String,
    #[serde(default)]
    pub autonomy_granted: bool,
}

/// Final `report.summary` for a security-triage run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriageReport {
    pub package: String,
    pub finding_signature: String,
    pub component: String,
    pub verdict_severity: String,
    pub reachable_markings: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub environment_criticality: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub hypotheses: Vec<Hypothesis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_disposition: Option<ProposedDisposition>,
}

/// Stamp a recovered successful summary. Failed reports pass the stored text.
pub fn canonicalize_recovered_summary(summary: &str, success: bool) -> Result<String, String> {
    if success {
        canonicalize_report(summary)
    } else {
        Ok(summary.to_string())
    }
}

/// Parse a successful report and fail closed on an untyped or mixed record.
pub fn canonicalize_report(summary: &str) -> Result<String, String> {
    let mut report: TriageReport = serde_json::from_str(summary).map_err(|error| {
        format!("profile `security-triage`: report is not a typed exposure verdict ({error})")
    })?;
    if report.package != PACKAGE_ID {
        return Err(format!(
            "profile `security-triage`: package `{}` is not `{PACKAGE_ID}`",
            report.package
        ));
    }
    for (name, value) in [
        ("finding_signature", report.finding_signature.as_str()),
        ("component", report.component.as_str()),
        ("verdict_severity", report.verdict_severity.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("profile `security-triage`: {name} is required"));
        }
    }
    if severity_rank(&report.verdict_severity).is_none() {
        return Err(format!(
            "profile `security-triage`: verdict_severity `{}` is not low, medium, high, or critical",
            report.verdict_severity
        ));
    }
    report.reachable_markings = report
        .reachable_markings
        .into_iter()
        .map(|marking| marking.trim().to_string())
        .filter(|marking| !marking.is_empty())
        .collect();
    report.reachable_markings.sort();
    report.reachable_markings.dedup();
    if report.evidence.iter().any(|item| item.trim().is_empty()) {
        return Err("profile `security-triage`: evidence entries must not be empty".into());
    }
    for hypothesis in &report.hypotheses {
        if hypothesis.statement.trim().is_empty() {
            return Err("profile `security-triage`: hypothesis statement is required".into());
        }
    }
    if let Some(disposition) = &report.proposed_disposition {
        if disposition.kind.trim().is_empty() {
            return Err("profile `security-triage`: proposed_disposition.kind is required".into());
        }
        if !DISPOSITION_STATUSES.contains(&disposition.status.as_str()) {
            return Err(format!(
                "profile `security-triage`: proposed_disposition.status `{}` is not proposed or recorded",
                disposition.status
            ));
        }
        if disposition.status == "recorded" && !disposition.autonomy_granted {
            return Err(
                "profile `security-triage`: unreviewed disposition requires review; the attempt parks"
                    .into(),
            );
        }
    }
    serde_json::to_string(&report).map_err(|error| {
        format!("profile `security-triage`: failed to serialize typed report ({error})")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(markings: &[&str], severity: &str) -> String {
        serde_json::json!({
            "package": PACKAGE_ID,
            "finding_signature": "find-1",
            "component": "payments-api",
            "verdict_severity": severity,
            "reachable_markings": markings,
            "environment_criticality": "prod",
            "evidence": ["lineage:payments-api -> customer-pii"],
            "hypotheses": [{"statement": "default features expose the handler"}],
        })
        .to_string()
    }

    #[test]
    fn marked_data_ranks_higher_than_an_isolated_component() {
        let isolated = canonicalize_report(&report(&[], "medium")).unwrap();
        let marked = canonicalize_report(&report(&["customer-pii"], "high")).unwrap();
        let isolated: TriageReport = serde_json::from_str(&isolated).unwrap();
        let marked: TriageReport = serde_json::from_str(&marked).unwrap();
        assert!(
            severity_rank(&marked.verdict_severity).unwrap()
                > severity_rank(&isolated.verdict_severity).unwrap()
        );
        assert_eq!(marked.reachable_markings, vec!["customer-pii".to_string()]);
        assert!(isolated.reachable_markings.is_empty());
    }

    #[test]
    fn unreviewed_recorded_disposition_is_refused() {
        let summary = serde_json::json!({
            "package": PACKAGE_ID,
            "finding_signature": "find-1",
            "component": "payments-api",
            "verdict_severity": "high",
            "reachable_markings": ["customer-pii"],
            "evidence": ["lineage:payments-api -> customer-pii"],
            "proposed_disposition": {
                "kind": "false-positive",
                "status": "recorded",
                "autonomy_granted": false
            }
        })
        .to_string();
        let error = canonicalize_report(&summary).unwrap_err();
        assert!(error.contains("unreviewed disposition"), "{error}");
    }

    #[test]
    fn recovered_failure_is_not_reparsed() {
        assert_eq!(
            canonicalize_recovered_summary("not typed", false).unwrap(),
            "not typed"
        );
    }
}
