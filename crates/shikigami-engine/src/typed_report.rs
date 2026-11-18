//! Reports of the typed profiles (`security-triage`, `security-finder`).
//!
//! The typed result travels in `Report.data`, checked by the profile's
//! validator; `summary` is plain text. The records a resume reads back (the
//! `report:` tool-result text and the content terminal part) hold the whole
//! successful report as JSON, so recovery rebuilds summary and data together.

use serde_json::{Map, Value};

use crate::config::{Config, SECURITY_FINDER_PROFILE, SECURITY_TRIAGE_PROFILE};
use crate::tools::Report;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypedProfile<'a> {
    Triage,
    Finder { role: &'a str },
}

impl<'a> TypedProfile<'a> {
    /// The typed profile `config` runs, if any. A finder without a role is
    /// an error: its reports cannot be checked.
    pub(crate) fn of(config: &'a Config) -> Result<Option<Self>, String> {
        match config.profile.name.as_str() {
            SECURITY_TRIAGE_PROFILE => Ok(Some(Self::Triage)),
            SECURITY_FINDER_PROFILE => config
                .security_finder
                .role
                .as_deref()
                .map(|role| Some(Self::Finder { role }))
                .ok_or_else(|| "profile `security-finder`: missing security_finder.role".into()),
            _ => Ok(None),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Triage => SECURITY_TRIAGE_PROFILE,
            Self::Finder { .. } => SECURITY_FINDER_PROFILE,
        }
    }

    /// Replace a successful report's `data` with its canonical form; fail
    /// closed when it is missing or invalid. Failed reports pass unchanged.
    pub(crate) fn canonicalize(self, report: &mut Report) -> Result<(), String> {
        if !report.success {
            return Ok(());
        }
        let data = report
            .data
            .take()
            .ok_or_else(|| format!("profile `{}`: report data is required", self.name()))?;
        let text = Value::Object(data).to_string();
        let canonical = match self {
            Self::Triage => crate::security_triage::canonicalize_report(&text)?,
            Self::Finder { role } => crate::security_finder::canonicalize_report(&text, role)?,
        };
        let canonical: Map<String, Value> = serde_json::from_str(&canonical)
            .map_err(|error| format!("profile `{}`: {error}", self.name()))?;
        report.data = Some(canonical);
        Ok(())
    }

    /// Rebuild a report from its stored record (see [`stored_text`]).
    pub(crate) fn recover(self, stored: &str, success: bool) -> Result<Report, String> {
        if !success {
            return Ok(Report {
                summary: stored.into(),
                success,
                data: None,
            });
        }
        let mut report: Report = serde_json::from_str(stored).map_err(|error| {
            format!(
                "profile `{}`: stored report is not a typed record ({error})",
                self.name()
            )
        })?;
        report.success = true;
        self.canonicalize(&mut report)?;
        Ok(report)
    }
}

/// The text a resume reads back for `report`: the whole report as JSON for
/// a successful report of a typed profile, else the summary.
pub(crate) fn stored_text(config: &Config, report: &Report) -> String {
    if report.success && matches!(TypedProfile::of(config), Ok(Some(_))) {
        serde_json::to_string(report).unwrap_or_else(|_| report.summary.clone())
    } else {
        report.summary.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triage_verdict() -> Map<String, Value> {
        serde_json::json!({
            "package": crate::security_triage::PACKAGE_ID,
            "finding_signature": "sig-1",
            "component": "left-pad",
            "verdict_severity": "high",
            "reachable_markings": [" b ", "a", "a"],
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn report(success: bool, data: Option<Map<String, Value>>) -> Report {
        Report {
            summary: "reachable from the public API".into(),
            success,
            data,
        }
    }

    #[test]
    fn a_successful_report_needs_valid_data_and_gets_its_canonical_form() {
        let mut missing = report(true, None);
        let error = TypedProfile::Triage.canonicalize(&mut missing).unwrap_err();
        assert!(error.contains("report data is required"), "{error}");

        let mut invalid = report(true, Some(Map::new()));
        assert!(TypedProfile::Triage.canonicalize(&mut invalid).is_err());

        let mut valid = report(true, Some(triage_verdict()));
        TypedProfile::Triage.canonicalize(&mut valid).unwrap();
        assert_eq!(
            valid.data.unwrap()["reachable_markings"],
            serde_json::json!(["a", "b"])
        );

        let mut failed = report(false, None);
        TypedProfile::Triage.canonicalize(&mut failed).unwrap();
        assert_eq!(failed, report(false, None));
    }

    #[test]
    fn a_stored_record_recovers_summary_and_canonical_data() {
        let mut config = Config::default();
        config.profile.name = SECURITY_TRIAGE_PROFILE.into();
        let mut canonical = report(true, Some(triage_verdict()));
        TypedProfile::Triage.canonicalize(&mut canonical).unwrap();
        let stored = stored_text(&config, &canonical);
        assert_eq!(
            TypedProfile::Triage.recover(&stored, true).unwrap(),
            canonical
        );

        // Failed reports are stored and recovered as plain text.
        let failed = report(false, None);
        assert_eq!(stored_text(&config, &failed), failed.summary);
        assert_eq!(
            TypedProfile::Triage
                .recover(&failed.summary, false)
                .unwrap(),
            failed
        );

        // Other profiles store the summary alone.
        assert_eq!(
            stored_text(&Config::default(), &canonical),
            canonical.summary
        );
    }
}
