//! Session token use for the footer and `/usage`. The host reports what each
//! prompt used in its result's `_meta.shikigami.usage`; a turn without it
//! (no usage from the model, a failed prompt, a turn replayed from before
//! this window) is unknown and shows as `?`, never as zero.

use std::collections::BTreeMap;

use serde_json::Value;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Tokens {
    input: u64,
    output: u64,
    cached: Option<u64>,
    created: Option<u64>,
}

/// Reported token use per turn (1-based, as the footer counts turns).
#[derive(Debug, Default)]
pub(crate) struct Usage {
    turns: BTreeMap<usize, Tokens>,
}

impl Usage {
    /// Record the usage a `session/prompt` result reports for `turn`.
    pub(crate) fn record(&mut self, turn: usize, result: &Value) {
        let field = |name| {
            result
                .pointer(&format!("/_meta/shikigami/usage/{name}"))
                .and_then(Value::as_u64)
        };
        if let (Some(input), Some(output)) = (field("inputTokens"), field("outputTokens")) {
            self.turns.insert(
                turn,
                Tokens {
                    input,
                    output,
                    cached: field("cachedInputTokens"),
                    created: field("cacheCreationInputTokens"),
                },
            );
        }
    }

    fn known(&self, turns: usize) -> impl Iterator<Item = (usize, Option<Tokens>)> + '_ {
        (1..=turns).map(|turn| (turn, self.turns.get(&turn).copied()))
    }

    fn total(&self, turns: usize) -> (Tokens, bool) {
        let mut sum = Tokens::default();
        let mut complete = true;
        for (_, tokens) in self.known(turns) {
            match tokens {
                Some(t) => {
                    sum.input += t.input;
                    sum.output += t.output;
                }
                None => complete = false,
            }
        }
        (sum, complete)
    }

    /// Last-turn prompt tokens as a percent of `window`, floored and capped
    /// at 100. Missing window, a zero window, or a last turn with no usage:
    /// `None`.
    fn fill_percent(&self, turns: usize, window: Option<u64>) -> Option<u32> {
        let window = window.filter(|w| *w > 0)?;
        let input = self.turns.get(&turns)?.input;
        let percent = u128::from(input).saturating_mul(100) / u128::from(window);
        u32::try_from(percent.min(100)).ok()
    }

    /// Footer part for a session with `turns` turns: `14.2k tok`, `≥1.2k tok`
    /// when some turns are unknown, `? tok` when all are; `None` before the
    /// first turn. When `window` is set and the last turn reported usage,
    /// appends that turn's fill (`14.2k tok  62%`).
    pub(crate) fn footer(&self, turns: usize, window: Option<u64>) -> Option<String> {
        if turns == 0 {
            return None;
        }
        let (sum, complete) = self.total(turns);
        let all = sum.input + sum.output;
        let mut text = match (all, complete) {
            (_, true) => format!("{} tok", compact(all)),
            (0, false) => "? tok".into(),
            (_, false) => format!("≥{} tok", compact(all)),
        };
        if let Some(percent) = self.fill_percent(turns, window) {
            text.push_str(&format!("  {percent}%"));
        }
        Some(text)
    }

    fn shows_cache(&self) -> bool {
        self.turns
            .values()
            .any(|tokens| tokens.cached.is_some() || tokens.created.is_some())
    }

    fn cache_total(&self, field: fn(&Tokens) -> Option<u64>) -> String {
        let mut n = 0;
        let mut any = false;
        for tokens in self.turns.values() {
            if let Some(value) = field(tokens) {
                n += value;
                any = true;
            }
        }
        if any { compact(n) } else { "?".into() }
    }

    /// The `/usage` table: one row per turn, then the total. When `window` is
    /// set and the last turn reported usage, one extra line names that fill.
    pub(crate) fn table(&self, turns: usize, window: Option<u64>) -> String {
        if turns == 0 {
            return "No token use yet. Send a prompt first.".into();
        }
        let cache = self.shows_cache();
        let mut out = String::from("Token use this session\n");
        let mut push = |turn: &str, input: &str, output: &str, cached: &str, created: &str| {
            if cache {
                out.push_str(&format!(
                    "{turn:<6} {input:>7} {output:>7} {cached:>7} {created:>7}\n"
                ));
            } else {
                out.push_str(&format!("{turn:<6} {input:>7} {output:>7}\n"));
            }
        };
        push("turn", "in", "out", "cached", "wrote");
        let cell =
            |t: Option<Tokens>, f: fn(Tokens) -> u64| t.map_or("?".into(), |t| compact(f(t)));
        let opt_cell = |t: Option<Tokens>, f: fn(Tokens) -> Option<u64>| {
            t.and_then(f).map_or("?".into(), compact)
        };
        for (turn, tokens) in self.known(turns) {
            push(
                &turn.to_string(),
                &cell(tokens, |t| t.input),
                &cell(tokens, |t| t.output),
                &opt_cell(tokens, |t| t.cached),
                &opt_cell(tokens, |t| t.created),
            );
        }
        let (sum, complete) = self.total(turns);
        let total = |n: u64| match (n, complete) {
            (_, true) => compact(n),
            (0, false) => "?".into(),
            (_, false) => format!("≥{}", compact(n)),
        };
        push(
            "total",
            &total(sum.input),
            &total(sum.output),
            &self.cache_total(|t| t.cached),
            &self.cache_total(|t| t.created),
        );
        if !complete {
            out.push_str("? means the model did not report usage for that turn.\n");
        }
        if let Some(percent) = self.fill_percent(turns, window) {
            out.push_str(&format!(
                "Last turn filled {percent}% of the context window."
            ));
        }
        out.trim_end().to_string()
    }
}

/// `950`, `12.4k`, `3.1M`; the unit is picked after rounding, so 999,960 is `1M`.
fn compact(n: u64) -> String {
    // Tenths of a unit, rounded half up.
    let tenths = |unit: u64| (n + unit / 20) / (unit / 10);
    let (tenths, unit) = match n {
        0..=999 => return n.to_string(),
        _ if tenths(1_000) < 10_000 => (tenths(1_000), "k"),
        _ => (tenths(1_000_000), "M"),
    };
    match tenths % 10 {
        0 => format!("{}{unit}", tenths / 10),
        frac => format!("{}.{frac}{unit}", tenths / 10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reported(input: u64, output: u64) -> Value {
        json!({ "_meta": { "shikigami": { "usage": { "inputTokens": input, "outputTokens": output } } } })
    }

    fn reported_cache(input: u64, output: u64, cached: u64, created: u64) -> Value {
        json!({ "_meta": { "shikigami": { "usage": {
            "inputTokens": input,
            "outputTokens": output,
            "cachedInputTokens": cached,
            "cacheCreationInputTokens": created,
        } } } })
    }

    #[test]
    fn compact_rounds_before_picking_the_unit() {
        let shown: Vec<String> = [950, 1_000, 12_449, 999_949, 999_960, 3_140_000]
            .map(compact)
            .into();
        assert_eq!(shown, ["950", "1k", "12.4k", "999.9k", "1M", "3.1M"]);
    }

    fn rows(table: &str) -> Vec<Vec<&str>> {
        table
            .lines()
            .map(|l| l.split_whitespace().collect())
            .collect()
    }

    #[test]
    fn totals_match_the_reported_turns() {
        let mut usage = Usage::default();
        usage.record(1, &reported(3_900, 410));
        usage.record(2, &reported(8_500, 1_390));
        assert_eq!(usage.footer(2, None).as_deref(), Some("14.2k tok"));
        let table = usage.table(2, None);
        assert!(rows(&table).contains(&vec!["1", "3.9k", "410"]), "{table}");
        assert!(
            rows(&table).contains(&vec!["total", "12.4k", "1.8k"]),
            "{table}"
        );
        assert!(!table.contains('?'), "{table}");
    }

    #[test]
    fn unknown_turns_show_as_unknown_not_zero() {
        let mut usage = Usage::default();
        assert_eq!(usage.footer(0, None), None);
        usage.record(1, &json!({ "stopReason": "end_turn" }));
        assert_eq!(usage.footer(1, None).as_deref(), Some("? tok"));
        assert!(rows(&usage.table(1, None)).contains(&vec!["total", "?", "?"]));
        usage.record(2, &reported(1_200, 300));
        assert_eq!(usage.footer(2, None).as_deref(), Some("≥1.5k tok"));
        let table = usage.table(2, None);
        assert!(rows(&table).contains(&vec!["1", "?", "?"]), "{table}");
        assert!(
            rows(&table).contains(&vec!["total", "≥1.2k", "≥300"]),
            "{table}"
        );
        assert!(table.contains("did not report usage"), "{table}");
    }

    #[test]
    fn cache_columns_appear_only_when_a_turn_reported_them() {
        let mut usage = Usage::default();
        usage.record(1, &reported_cache(1_200, 300, 900, 40));
        usage.record(2, &reported(800, 40));
        let table = usage.table(2, None);
        assert!(
            rows(&table).contains(&vec!["turn", "in", "out", "cached", "wrote"]),
            "{table}"
        );
        assert!(
            rows(&table).contains(&vec!["1", "1.2k", "300", "900", "40"]),
            "{table}"
        );
        assert!(
            rows(&table).contains(&vec!["2", "800", "40", "?", "?"]),
            "{table}"
        );
        assert!(
            rows(&table).contains(&vec!["total", "2k", "340", "900", "40"]),
            "{table}"
        );
        let mut plain = Usage::default();
        plain.record(1, &reported(1_200, 340));
        let table = plain.table(1, None);
        assert!(
            rows(&table).contains(&vec!["total", "1.2k", "340"]),
            "{table}"
        );
        assert!(!table.contains("cached"), "{table}");
    }

    #[test]
    fn last_turn_fill_is_a_percent_of_the_window() {
        let mut usage = Usage::default();
        usage.record(1, &reported(620, 10));
        assert_eq!(usage.footer(1, Some(1000)).as_deref(), Some("630 tok  62%"));
        assert_eq!(usage.footer(1, None).as_deref(), Some("630 tok"));
        let table = usage.table(1, Some(1000));
        assert!(
            table.contains("Last turn filled 62% of the context window."),
            "{table}"
        );
        assert!(!usage.table(1, None).contains('%'));
        usage.record(1, &reported(1_001, 1));
        assert_eq!(usage.footer(1, Some(1000)).as_deref(), Some("1k tok  100%"));
    }

    #[test]
    fn missing_last_turn_usage_adds_no_percent() {
        let mut usage = Usage::default();
        usage.record(1, &json!({ "stopReason": "end_turn" }));
        assert_eq!(usage.footer(1, Some(1000)).as_deref(), Some("? tok"));
        assert!(!usage.table(1, Some(1000)).contains('%'));
        usage.record(2, &reported(1_200, 300));
        assert_eq!(
            usage.footer(2, Some(1000)).as_deref(),
            Some("≥1.5k tok  100%")
        );
        usage.record(3, &json!({ "stopReason": "end_turn" }));
        assert_eq!(usage.footer(3, Some(1000)).as_deref(), Some("≥1.5k tok"));
    }
}
