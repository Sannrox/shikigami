//! Security-finder profile: specialised read-only reviewers that emit typed
//! findings matching `pkg:security.finding-exposure/v1` (#464).

use serde::{Deserialize, Serialize};

use crate::digest::sha256_prefixed;

/// Governed findings package id (sekai-chisei ADR 0135).
pub const PACKAGE_ID: &str = "pkg:security.finding-exposure/v1";

pub const ROLE_DEPENDENCY_ADVISORY: &str = "dependency-advisory";
pub const ROLE_SOURCE_CHANGE: &str = "source-change";
pub const ROLE_DEPLOYMENT_MANIFEST: &str = "deployment-manifest";
pub const ROLE_AUTHORIZATION_BOUNDARY: &str = "authorization-boundary";

const READ_TOOLS: &[&str] = &["read_file", "glob", "grep", "report"];

/// One specialised reviewer: a read-only tool allowlist and a run budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Role {
    pub name: &'static str,
    pub tools: &'static [&'static str],
    pub max_turns: u32,
    pub timeout_secs: u64,
}

/// Roles the `security-finder` profile accepts. Names are capabilities, not
/// a model or provider.
pub const ROLES: &[Role] = &[
    Role {
        name: ROLE_DEPENDENCY_ADVISORY,
        tools: READ_TOOLS,
        max_turns: 12,
        timeout_secs: 600,
    },
    Role {
        name: ROLE_SOURCE_CHANGE,
        tools: READ_TOOLS,
        max_turns: 24,
        timeout_secs: 900,
    },
    Role {
        name: ROLE_DEPLOYMENT_MANIFEST,
        tools: READ_TOOLS,
        max_turns: 16,
        timeout_secs: 600,
    },
    Role {
        name: ROLE_AUTHORIZATION_BOUNDARY,
        tools: READ_TOOLS,
        max_turns: 16,
        timeout_secs: 600,
    },
];

const FINDING_SOURCES: [&str; 5] = ["probe", "review", "scanner", "agent", "advisory"];
const UNCLASSIFIED: &str = "unclassified";

/// Look up a reviewer role by name.
pub fn role(name: &str) -> Option<&'static Role> {
    ROLES.iter().find(|role| role.name == name)
}

/// Coverage of the review. A successful report must set `complete`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    pub complete: bool,
    pub summary: String,
}

/// One typed finding. Identity is `problem_signature` after canonicalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    pub source: String,
    pub component: String,
    pub version_range: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub problem_signature: String,
    pub reported_severity: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub evidence_refs: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub finding_class: String,
    /// Advisory, CVE, GHSA, or equivalent measured identifier. Empty when the
    /// workspace does not name one. Part of finding identity.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub advisory_id: String,
}

/// A proposed explanation. Never a measured fact and never mixed into a finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hypothesis {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub problem_signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finding_index: Option<usize>,
    pub statement: String,
}

/// Final `report.summary` for a security-finder run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinderReport {
    pub package: String,
    pub role: String,
    pub coverage: Coverage,
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default)]
    pub hypotheses: Vec<Hypothesis>,
}

/// Stable identity from measured facts: component, version range, finding
/// class, and advisory id. Distinct weaknesses use a distinct component or
/// advisory_id. Evidence paths, severity, and hypotheses are excluded so two
/// models reporting the same problem agree. The tuple is JSON so a newline
/// inside one field cannot collide with another field's delimiter.
pub fn problem_signature(
    component: &str,
    version_range: &str,
    finding_class: &str,
    advisory_id: &str,
) -> String {
    let class = if finding_class.is_empty() {
        UNCLASSIFIED
    } else {
        finding_class
    };
    let material = serde_json::json!([PACKAGE_ID, component, version_range, class, advisory_id]);
    sha256_prefixed(material.to_string().as_bytes())
}

/// Stamp a recovered successful summary against the configured role.
/// Failed reports and other profiles pass the stored text through.
pub fn canonicalize_recovered_summary(
    summary: &str,
    success: bool,
    expected_role: Option<&str>,
) -> Result<String, String> {
    match expected_role {
        Some(role) if success => canonicalize_report(summary, role),
        _ => Ok(summary.to_string()),
    }
}

/// Parse a successful report, stamp canonical signatures, and fail closed on
/// incomplete coverage or a finding/hypothesis mix-up.
pub fn canonicalize_report(summary: &str, expected_role: &str) -> Result<String, String> {
    let mut report: FinderReport = serde_json::from_str(summary).map_err(|error| {
        format!("profile `security-finder`: report is not a typed finding record ({error})")
    })?;
    if report.package != PACKAGE_ID {
        return Err(format!(
            "profile `security-finder`: package `{}` is not `{PACKAGE_ID}`",
            report.package
        ));
    }
    if report.role != expected_role {
        return Err(format!(
            "profile `security-finder`: report role `{}` does not match configured role `{expected_role}`",
            report.role
        ));
    }
    if role(&report.role).is_none() {
        return Err(format!(
            "profile `security-finder`: unknown role `{}`",
            report.role
        ));
    }
    if !report.coverage.complete {
        return Err(
            "profile `security-finder`: refuse a successful report when coverage is incomplete"
                .into(),
        );
    }
    if report.coverage.summary.trim().is_empty() {
        return Err("profile `security-finder`: coverage.summary is required".into());
    }
    for finding in &mut report.findings {
        validate_finding(finding)?;
        finding.problem_signature = problem_signature(
            &finding.component,
            &finding.version_range,
            &finding.finding_class,
            &finding.advisory_id,
        );
    }
    let mut signatures: Vec<&str> = report
        .findings
        .iter()
        .map(|finding| finding.problem_signature.as_str())
        .collect();
    signatures.sort_unstable();
    signatures.dedup();
    if signatures.len() != report.findings.len() {
        return Err("profile `security-finder`: duplicate problem_signature".into());
    }
    for hypothesis in &mut report.hypotheses {
        bind_hypothesis(hypothesis, &report.findings)?;
    }
    report
        .findings
        .sort_by(|left, right| left.problem_signature.cmp(&right.problem_signature));
    serde_json::to_string(&report).map_err(|error| {
        format!("profile `security-finder`: failed to serialize typed report ({error})")
    })
}

fn validate_finding(finding: &Finding) -> Result<(), String> {
    if !FINDING_SOURCES.contains(&finding.source.as_str()) {
        return Err(format!(
            "profile `security-finder`: finding source `{}` is not probe, review, scanner, agent, or advisory",
            finding.source
        ));
    }
    for (name, value) in [
        ("component", finding.component.as_str()),
        ("version_range", finding.version_range.as_str()),
        ("reported_severity", finding.reported_severity.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!(
                "profile `security-finder`: finding {name} is required"
            ));
        }
    }
    Ok(())
}

fn bind_hypothesis(hypothesis: &mut Hypothesis, findings: &[Finding]) -> Result<(), String> {
    if hypothesis.statement.trim().is_empty() {
        return Err("profile `security-finder`: hypothesis statement is required".into());
    }
    let signature = if !hypothesis.problem_signature.is_empty() {
        findings
            .iter()
            .map(|finding| finding.problem_signature.as_str())
            .find(|signature| *signature == hypothesis.problem_signature)
            .ok_or_else(|| {
                "profile `security-finder`: hypothesis problem_signature does not match a finding"
                    .to_string()
            })?
            .to_string()
    } else if let Some(index) = hypothesis.finding_index {
        findings
            .get(index)
            .ok_or_else(|| {
                format!(
                    "profile `security-finder`: hypothesis finding_index {index} is out of range"
                )
            })?
            .problem_signature
            .clone()
    } else {
        return Err(
            "profile `security-finder`: hypothesis needs problem_signature or finding_index".into(),
        );
    };
    hypothesis.problem_signature = signature;
    hypothesis.finding_index = None;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(component: &str, range: &str, class: &str) -> serde_json::Value {
        serde_json::json!({
            "source": "review",
            "component": component,
            "version_range": range,
            "reported_severity": "high",
            "evidence_refs": "Cargo.lock:left-pad@1.3.0",
            "finding_class": class,
        })
    }

    fn report(findings: serde_json::Value, hypotheses: serde_json::Value) -> String {
        serde_json::json!({
            "package": PACKAGE_ID,
            "role": ROLE_DEPENDENCY_ADVISORY,
            "coverage": {"complete": true, "summary": "Reviewed lockfile."},
            "findings": findings,
            "hypotheses": hypotheses,
        })
        .to_string()
    }

    #[test]
    fn same_facts_yield_the_same_signature_across_model_prose() {
        let left = report(
            serde_json::json!([finding("left-pad", "=1.3.0", "vulnerable-dependency")]),
            serde_json::json!([{"finding_index": 0, "statement": "reachable from default features"}]),
        );
        let right = report(
            serde_json::json!([{
                "source": "review",
                "component": "left-pad",
                "version_range": "=1.3.0",
                "problem_signature": "model-invented",
                "reported_severity": "medium",
                "evidence_refs": "other-path",
                "finding_class": "vulnerable-dependency",
            }]),
            serde_json::json!([{"finding_index": 0, "statement": "maybe exploited in production"}]),
        );
        let left: FinderReport =
            serde_json::from_str(&canonicalize_report(&left, ROLE_DEPENDENCY_ADVISORY).unwrap())
                .unwrap();
        let right: FinderReport =
            serde_json::from_str(&canonicalize_report(&right, ROLE_DEPENDENCY_ADVISORY).unwrap())
                .unwrap();
        assert_eq!(left.findings.len(), 1);
        assert_eq!(
            left.findings[0].problem_signature,
            right.findings[0].problem_signature
        );
        assert_eq!(
            left.findings[0].problem_signature,
            problem_signature("left-pad", "=1.3.0", "vulnerable-dependency", "")
        );
        assert_eq!(
            left.hypotheses[0].problem_signature,
            left.findings[0].problem_signature
        );
        assert!(left.hypotheses[0].finding_index.is_none());
        assert_ne!(left.hypotheses[0].statement, right.hypotheses[0].statement);
    }

    #[test]
    fn hypotheses_cannot_live_on_a_finding() {
        let mixed = serde_json::json!({
            "package": PACKAGE_ID,
            "role": ROLE_DEPENDENCY_ADVISORY,
            "coverage": {"complete": true, "summary": "Reviewed lockfile."},
            "findings": [{
                "source": "review",
                "component": "left-pad",
                "version_range": "=1.3.0",
                "reported_severity": "high",
                "statement": "this is a hypothesis"
            }],
        })
        .to_string();
        let error = canonicalize_report(&mixed, ROLE_DEPENDENCY_ADVISORY).unwrap_err();
        assert!(error.contains("typed finding record"), "{error}");
    }

    #[test]
    fn incomplete_coverage_is_not_a_successful_report() {
        let summary = serde_json::json!({
            "package": PACKAGE_ID,
            "role": ROLE_DEPENDENCY_ADVISORY,
            "coverage": {"complete": false, "summary": "ran out of files"},
            "findings": [],
        })
        .to_string();
        let error = canonicalize_report(&summary, ROLE_DEPENDENCY_ADVISORY).unwrap_err();
        assert!(error.contains("incomplete"), "{error}");
    }

    #[test]
    fn unknown_role_names_are_rejected() {
        assert!(role("triage").is_none());
        assert_eq!(ROLES.len(), 4);
    }

    #[test]
    fn distinct_advisories_on_the_same_component_keep_distinct_signatures() {
        let left = report(
            serde_json::json!([{
                "source": "advisory",
                "component": "left-pad",
                "version_range": "=1.3.0",
                "reported_severity": "high",
                "finding_class": "vulnerable-dependency",
                "advisory_id": "RUSTSEC-2020-0001",
            }]),
            serde_json::json!([]),
        );
        let right = report(
            serde_json::json!([{
                "source": "advisory",
                "component": "left-pad",
                "version_range": "=1.3.0",
                "reported_severity": "high",
                "finding_class": "vulnerable-dependency",
                "advisory_id": "RUSTSEC-2021-0002",
            }]),
            serde_json::json!([]),
        );
        let both = report(
            serde_json::json!([
                {
                    "source": "advisory",
                    "component": "left-pad",
                    "version_range": "=1.3.0",
                    "reported_severity": "high",
                    "finding_class": "vulnerable-dependency",
                    "advisory_id": "RUSTSEC-2020-0001",
                },
                {
                    "source": "advisory",
                    "component": "left-pad",
                    "version_range": "=1.3.0",
                    "reported_severity": "high",
                    "finding_class": "vulnerable-dependency",
                    "advisory_id": "RUSTSEC-2021-0002",
                }
            ]),
            serde_json::json!([]),
        );
        let left: FinderReport =
            serde_json::from_str(&canonicalize_report(&left, ROLE_DEPENDENCY_ADVISORY).unwrap())
                .unwrap();
        let right: FinderReport =
            serde_json::from_str(&canonicalize_report(&right, ROLE_DEPENDENCY_ADVISORY).unwrap())
                .unwrap();
        let both: FinderReport =
            serde_json::from_str(&canonicalize_report(&both, ROLE_DEPENDENCY_ADVISORY).unwrap())
                .unwrap();
        assert_ne!(
            left.findings[0].problem_signature,
            right.findings[0].problem_signature
        );
        assert_eq!(both.findings.len(), 2);
        assert_eq!(
            left.findings[0].problem_signature,
            problem_signature(
                "left-pad",
                "=1.3.0",
                "vulnerable-dependency",
                "RUSTSEC-2020-0001"
            )
        );
    }

    #[test]
    fn identity_fields_cannot_collide_across_delimiters() {
        let left = problem_signature("a\nversion_range=b", "c", "vulnerable-dependency", "");
        let right = problem_signature("a", "b\nversion_range=c", "vulnerable-dependency", "");
        assert_ne!(left, right);
    }

    #[test]
    fn recovered_successful_summary_must_match_the_configured_role() {
        let summary = report(
            serde_json::json!([finding("left-pad", "=1.3.0", "vulnerable-dependency")]),
            serde_json::json!([]),
        );
        let canonical =
            canonicalize_recovered_summary(&summary, true, Some(ROLE_DEPENDENCY_ADVISORY)).unwrap();
        assert_ne!(canonical, summary);
        let error =
            canonicalize_recovered_summary(&summary, true, Some(ROLE_AUTHORIZATION_BOUNDARY))
                .unwrap_err();
        assert!(error.contains("does not match configured role"), "{error}");
        assert_eq!(
            canonicalize_recovered_summary(&summary, true, None).unwrap(),
            summary
        );
        assert_eq!(
            canonicalize_recovered_summary("not typed", false, Some(ROLE_DEPENDENCY_ADVISORY))
                .unwrap(),
            "not typed"
        );
    }
}
