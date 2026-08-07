use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Instant;

const REPOSITORY_ROOT: &str = env!("CARGO_MANIFEST_DIR");
const REPORT_SCHEMA_VERSION: u32 = 1;
const MAX_CAPTURED_OUTPUT: usize = 4_000;

#[derive(Debug, Parser)]
#[command(
    name = "shikigami-project",
    about = "Deterministic project checks for shikigami",
    version
)]
struct Cli {
    /// Emit one stable JSON report to stdout.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: ProjectCommand,
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    /// Show the checks selected for the current change set.
    Plan(RunArgs),
    /// Run the selected checks and return non-zero if any check fails.
    Verify(RunArgs),
}

#[derive(Clone, Debug, Args)]
struct RunArgs {
    /// Use the complete project gate instead of change-based routing.
    #[arg(long, conflicts_with = "changed")]
    all: bool,

    /// Use the working tree and index as the change set (the default).
    #[arg(long, conflicts_with = "all")]
    changed: bool,

    /// Add the committed diff from REF to HEAD to the change set.
    #[arg(long, value_name = "REF")]
    base: Option<String>,

    /// Run only the named checks, overriding automatic routing.
    #[arg(long, value_delimiter = ',', value_name = "CHECK")]
    check: Vec<CheckName>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, ValueEnum)]
enum CheckName {
    Fmt,
    Docs,
    Build,
    Test,
    Clippy,
    Embed,
}

impl CheckName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Fmt => "fmt",
            Self::Docs => "docs",
            Self::Build => "build",
            Self::Test => "test",
            Self::Clippy => "clippy",
            Self::Embed => "embed",
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    command: &'static str,
    scope: &'static str,
    base: Option<String>,
    head: Option<String>,
    changed_files: Vec<String>,
    requested_checks: Vec<String>,
    checks: Vec<CheckResult>,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct CheckResult {
    name: String,
    command: Vec<String>,
    status: &'static str,
    exit_code: Option<i32>,
    duration_ms: Option<u128>,
    stdout: Option<String>,
    stderr: Option<String>,
}

#[derive(Debug)]
struct CheckSpec {
    command: &'static [&'static str],
}

fn main() {
    let cli = Cli::parse();
    let json = cli.json;
    let result = match cli.command {
        ProjectCommand::Plan(args) => execute(args, false, json),
        ProjectCommand::Verify(args) => execute(args, true, json),
    };

    match result {
        Ok(exit_code) => std::process::exit(exit_code),
        Err(error) => {
            if json {
                let report = serde_json::json!({
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "status": "error",
                    "error": error,
                });
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("static report serializes")
                );
            } else {
                eprintln!("shikigami-project: {error}");
            }
            std::process::exit(2);
        }
    }
}

fn execute(args: RunArgs, run_checks: bool, json: bool) -> Result<i32, String> {
    if args.all && args.base.is_some() {
        return Err("--base can only be used with change-based routing".into());
    }

    let scope = if args.all { "all" } else { "changed" };
    let changed_files = if args.all {
        Vec::new()
    } else {
        collect_changed_files(args.base.as_deref())?
    };
    let selected = select_checks(&args, &changed_files);
    let requested_checks = args
        .check
        .iter()
        .map(|check| check.as_str().to_string())
        .collect();
    let head = git_lines(["rev-parse", "HEAD"])
        .ok()
        .and_then(|lines| lines.into_iter().next());

    let checks: Vec<CheckResult> = if run_checks {
        selected
            .iter()
            .map(|check| run_check(*check, json))
            .collect()
    } else {
        selected.iter().map(|check| planned_check(*check)).collect()
    };

    let status = if run_checks {
        if checks.iter().any(|check| check.status == "failed") {
            "failed"
        } else if checks.is_empty() {
            "no_checks"
        } else {
            "passed"
        }
    } else {
        "planned"
    };

    let report = Report {
        schema_version: REPORT_SCHEMA_VERSION,
        command: if run_checks { "verify" } else { "plan" },
        scope,
        base: args.base,
        head,
        changed_files,
        requested_checks,
        checks,
        status,
    };

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
        );
    } else {
        print_human_report(&report);
    }

    Ok(i32::from(status == "failed"))
}

fn all_checks() -> Vec<CheckName> {
    vec![
        CheckName::Fmt,
        CheckName::Docs,
        CheckName::Build,
        CheckName::Test,
        CheckName::Clippy,
        CheckName::Embed,
    ]
}

fn select_checks(args: &RunArgs, changed_files: &[String]) -> Vec<CheckName> {
    if !args.check.is_empty() {
        return deduplicate(args.check.clone());
    }

    if args.all {
        return all_checks();
    }

    let mut selected = BTreeSet::new();
    for path in changed_files {
        if is_documentation(path) {
            selected.insert(CheckName::Docs);
        } else if is_project_configuration(path) || is_rust_source(path) {
            selected.insert(CheckName::Fmt);
            selected.insert(CheckName::Build);
            selected.insert(CheckName::Test);
            selected.insert(CheckName::Clippy);
        } else {
            // Unknown project files receive the conservative source gate.
            selected.insert(CheckName::Fmt);
            selected.insert(CheckName::Build);
            selected.insert(CheckName::Test);
            selected.insert(CheckName::Clippy);
        }
    }

    selected.into_iter().collect()
}

fn deduplicate(checks: Vec<CheckName>) -> Vec<CheckName> {
    checks
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn is_rust_source(path: &str) -> bool {
    path.ends_with(".rs")
        || path.starts_with("src/")
        || path.starts_with("tests/")
        || path.starts_with("examples/")
}

fn is_documentation(path: &str) -> bool {
    path == "AGENTS.md"
        || path == "CONTRIBUTING.md"
        || path.ends_with(".md")
        || path.ends_with(".mdx")
        || path.starts_with("docs/")
        || (path.starts_with(".agents/") && (path.ends_with(".md") || path.ends_with(".mdx")))
}

fn is_project_configuration(path: &str) -> bool {
    matches!(
        path,
        "Cargo.toml" | "Cargo.lock" | "rust-toolchain.toml" | "deny.toml" | "Dockerfile"
    ) || path.starts_with(".github/")
        || path.starts_with("scripts/")
}

fn check_spec(name: CheckName) -> Option<CheckSpec> {
    let command = match name {
        CheckName::Fmt => &["cargo", "fmt", "--all", "--", "--check"][..],
        CheckName::Build => &["cargo", "build", "--locked", "--all-targets"][..],
        CheckName::Test => &["cargo", "test", "--locked"][..],
        CheckName::Clippy => &[
            "cargo",
            "clippy",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ][..],
        CheckName::Embed => &["cargo", "run", "--locked", "--example", "embed_smoke"][..],
        CheckName::Docs => return None,
    };
    Some(CheckSpec { command })
}

fn planned_check(name: CheckName) -> CheckResult {
    let command = command_for(name);
    CheckResult {
        name: name.as_str().to_string(),
        command,
        status: "planned",
        exit_code: None,
        duration_ms: None,
        stdout: None,
        stderr: None,
    }
}

fn run_check(name: CheckName, json: bool) -> CheckResult {
    if name == CheckName::Docs {
        return run_docs_check(json);
    }

    let spec = check_spec(name).expect("non-doc check has a command");
    let command = command_for(name);
    if !json {
        println!("==> {}: {}", name.as_str(), command.join(" "));
    }

    let started = Instant::now();
    let mut process = ProcessCommand::new(spec.command[0]);
    process
        .args(&spec.command[1..])
        .current_dir(REPOSITORY_ROOT)
        .env("CARGO_TERM_COLOR", "never");

    if json {
        match process.output() {
            Ok(output) => CheckResult {
                name: name.as_str().to_string(),
                command,
                status: if output.status.success() {
                    "passed"
                } else {
                    "failed"
                },
                exit_code: output.status.code(),
                duration_ms: Some(started.elapsed().as_millis()),
                stdout: capture_output(&output.stdout),
                stderr: capture_output(&output.stderr),
            },
            Err(error) => CheckResult {
                name: name.as_str().to_string(),
                command,
                status: "failed",
                exit_code: None,
                duration_ms: Some(started.elapsed().as_millis()),
                stdout: None,
                stderr: Some(error.to_string()),
            },
        }
    } else {
        match process.status() {
            Ok(status) => {
                let passed = status.success();
                println!(
                    "<== {}: {}",
                    name.as_str(),
                    if passed { "passed" } else { "failed" }
                );
                CheckResult {
                    name: name.as_str().to_string(),
                    command,
                    status: if passed { "passed" } else { "failed" },
                    exit_code: status.code(),
                    duration_ms: Some(started.elapsed().as_millis()),
                    stdout: None,
                    stderr: None,
                }
            }
            Err(error) => {
                eprintln!("<== {}: failed to start: {error}", name.as_str());
                CheckResult {
                    name: name.as_str().to_string(),
                    command,
                    status: "failed",
                    exit_code: None,
                    duration_ms: Some(started.elapsed().as_millis()),
                    stdout: None,
                    stderr: Some(error.to_string()),
                }
            }
        }
    }
}

fn run_docs_check(json: bool) -> CheckResult {
    let name = CheckName::Docs;
    let command = command_for(name);
    if !json {
        println!("==> {}: {}", name.as_str(), command.join(" "));
    }
    let started = Instant::now();
    match validate_markdown_links(Path::new(REPOSITORY_ROOT)) {
        Ok(()) => {
            if !json {
                println!("<== docs: passed");
            }
            CheckResult {
                name: name.as_str().to_string(),
                command,
                status: "passed",
                exit_code: Some(0),
                duration_ms: Some(started.elapsed().as_millis()),
                stdout: None,
                stderr: None,
            }
        }
        Err(error) => {
            if !json {
                eprintln!("<== docs: failed: {error}");
            }
            CheckResult {
                name: name.as_str().to_string(),
                command,
                status: "failed",
                exit_code: Some(1),
                duration_ms: Some(started.elapsed().as_millis()),
                stdout: None,
                stderr: Some(error),
            }
        }
    }
}

fn command_for(name: CheckName) -> Vec<String> {
    check_spec(name)
        .map(|spec| {
            spec.command
                .iter()
                .map(|part| (*part).to_string())
                .collect()
        })
        .unwrap_or_else(|| vec!["internal".into(), "markdown-links".into()])
}

fn capture_output(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let output = String::from_utf8_lossy(bytes).into_owned();
    if output.len() <= MAX_CAPTURED_OUTPUT {
        Some(output)
    } else {
        let target_start = output.len() - MAX_CAPTURED_OUTPUT;
        let start = output
            .char_indices()
            .find(|(index, _)| *index >= target_start)
            .map_or(0, |(index, _)| index);
        Some(format!("…{}", &output[start..]))
    }
}

fn collect_changed_files(base: Option<&str>) -> Result<Vec<String>, String> {
    let mut paths = BTreeSet::new();
    for args in [
        vec!["diff", "--name-only", "--diff-filter=ACDMRTUXB", "-z"],
        vec![
            "diff",
            "--cached",
            "--name-only",
            "--diff-filter=ACDMRTUXB",
            "-z",
        ],
        vec!["ls-files", "--others", "--exclude-standard", "-z"],
    ] {
        paths.extend(git_paths(args)?);
    }

    if let Some(base) = base {
        let range = format!("{base}...HEAD");
        paths.extend(git_paths(vec![
            "diff",
            "--name-only",
            "--diff-filter=ACDMRTUXB",
            "-z",
            &range,
        ])?);
    }

    Ok(paths.into_iter().collect())
}

fn git_paths(args: Vec<&str>) -> Result<Vec<String>, String> {
    let output = ProcessCommand::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .current_dir(REPOSITORY_ROOT)
        .output()
        .map_err(|error| format!("failed to start git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect())
}

fn git_lines<const N: usize>(args: [&str; N]) -> Result<Vec<String>, String> {
    let output = ProcessCommand::new("git")
        .arg("--no-optional-locks")
        .args(args)
        .current_dir(REPOSITORY_ROOT)
        .output()
        .map_err(|error| format!("failed to start git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect())
}

fn validate_markdown_links(root: &Path) -> Result<(), String> {
    let mut markdown_files = Vec::new();
    collect_markdown_files(root, &mut markdown_files)?;
    markdown_files.sort();

    let mut errors = Vec::new();
    for file in markdown_files {
        let contents = fs::read_to_string(&file)
            .map_err(|error| format!("{}: {error}", display_path(root, &file)))?;
        for (line_number, line) in contents.lines().enumerate() {
            validate_line_links(root, &file, line, line_number + 1, &mut errors);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn collect_markdown_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let metadata = fs::metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if metadata.is_file() {
        if path
            .extension()
            .is_some_and(|extension| extension == "md" || extension == "mdx")
        {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }

    let entries = fs::read_dir(path).map_err(|error| format!("{}: {error}", path.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("{}: {error}", path.display()))?;
        let entry_path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("{}: {error}", entry_path.display()))?;
        if file_type.is_symlink()
            || (file_type.is_dir()
                && matches!(
                    entry.file_name().to_str(),
                    Some(".git" | "target" | ".shikigami-state")
                ))
        {
            continue;
        }
        collect_markdown_files(&entry_path, files)?;
    }
    Ok(())
}

fn validate_line_links(
    root: &Path,
    source: &Path,
    line: &str,
    line_number: usize,
    errors: &mut Vec<String>,
) {
    let mut offset = 0;
    while let Some(relative_start) = line[offset..].find("](") {
        let start = offset + relative_start + 2;
        let Some(relative_end) = line[start..].find(')') else {
            break;
        };
        let end = start + relative_end;
        let raw_target = line[start..end].trim();
        let target = raw_target
            .strip_prefix('<')
            .and_then(|target| target.strip_suffix('>'))
            .or_else(|| raw_target.split_whitespace().next())
            .unwrap_or_default();
        if !target.is_empty()
            && !is_external_link(target)
            && !local_target_exists(root, source, target)
        {
            errors.push(format!(
                "{}:{}: missing local link target `{target}`",
                display_path(root, source),
                line_number
            ));
        }
        offset = end + 1;
    }
}

fn is_external_link(target: &str) -> bool {
    target.starts_with("#")
        || target.starts_with("/")
        || target.starts_with("//")
        || target.starts_with("http://")
        || target.starts_with("https://")
        || target.starts_with("mailto:")
        || target.starts_with("tel:")
        || target.starts_with("data:")
}

fn local_target_exists(root: &Path, source: &Path, target: &str) -> bool {
    let path = target.split_once('#').map_or(target, |(path, _)| path);
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    if path.is_empty() {
        return true;
    }
    let candidate = source.parent().unwrap_or(root).join(path);
    candidate.exists()
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).map_or_else(
        |_| path.display().to_string(),
        |path| path.display().to_string(),
    )
}

fn print_human_report(report: &Report) {
    println!("scope: {}", report.scope);
    if let Some(base) = &report.base {
        println!("base: {base}");
    }
    if report.changed_files.is_empty() {
        println!("changed files: none");
    } else {
        println!("changed files:");
        for path in &report.changed_files {
            println!("  {path}");
        }
    }
    if report.checks.is_empty() {
        println!("checks: none");
    } else {
        println!("checks:");
        for check in &report.checks {
            println!("  {}: {}", check.name, check.status);
        }
    }
    println!("status: {}", report.status);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(all: bool, check: Vec<CheckName>) -> RunArgs {
        RunArgs {
            all,
            changed: !all,
            base: None,
            check,
        }
    }

    #[test]
    fn routes_documentation_changes_to_link_validation() {
        assert_eq!(
            select_checks(&args(false, Vec::new()), &["docs/settings.md".into()]),
            vec![CheckName::Docs]
        );
        assert!(is_documentation(".agents/skills/example/SKILL.md"));
        assert!(!is_documentation(".agents/skills/example/scripts/check"));
    }

    #[test]
    fn routes_source_changes_to_the_source_gate() {
        assert_eq!(
            select_checks(&args(false, Vec::new()), &["src/run.rs".into()]),
            vec![
                CheckName::Fmt,
                CheckName::Build,
                CheckName::Test,
                CheckName::Clippy
            ]
        );
    }

    #[test]
    fn unknown_changes_use_the_conservative_gate() {
        assert_eq!(
            select_checks(&args(false, Vec::new()), &["new-config-file".into()]),
            vec![
                CheckName::Fmt,
                CheckName::Build,
                CheckName::Test,
                CheckName::Clippy
            ]
        );
    }

    #[test]
    fn all_scope_selects_every_check() {
        assert_eq!(select_checks(&args(true, Vec::new()), &[]), all_checks());
    }

    #[test]
    fn explicit_checks_are_sorted_and_deduplicated() {
        assert_eq!(
            select_checks(
                &args(
                    false,
                    vec![CheckName::Clippy, CheckName::Fmt, CheckName::Fmt]
                ),
                &[]
            ),
            vec![CheckName::Fmt, CheckName::Clippy]
        );
    }

    #[test]
    fn captured_output_truncation_keeps_utf8_valid() {
        let output = capture_output("🙂".repeat(MAX_CAPTURED_OUTPUT).as_bytes())
            .expect("non-empty output is captured");
        assert!(output.starts_with('…'));
        assert!(output.is_char_boundary(output.len()));
    }
}
