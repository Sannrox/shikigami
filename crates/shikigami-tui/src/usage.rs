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
            self.turns.insert(turn, Tokens { input, output });
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

    /// Footer part for a session with `turns` turns: `14.2k tok`, `≥1.2k tok`
    /// when some turns are unknown, `? tok` when all are; `None` before the
    /// first turn.
    pub(crate) fn footer(&self, turns: usize) -> Option<String> {
        if turns == 0 {
            return None;
        }
        let (sum, complete) = self.total(turns);
        let all = sum.input + sum.output;
        Some(match (all, complete) {
            (_, true) => format!("{} tok", compact(all)),
            (0, false) => "? tok".into(),
            (_, false) => format!("≥{} tok", compact(all)),
        })
    }

    /// The `/usage` table: one row per turn, then the total.
    pub(crate) fn table(&self, turns: usize) -> String {
        if turns == 0 {
            return "No token use yet. Send a prompt first.".into();
        }
        let cell =
            |t: Option<Tokens>, f: fn(Tokens) -> u64| t.map_or("?".into(), |t| compact(f(t)));
        let mut rows = vec![("turn".to_string(), "in".to_string(), "out".to_string())];
        for (turn, tokens) in self.known(turns) {
            rows.push((
                turn.to_string(),
                cell(tokens, |t| t.input),
                cell(tokens, |t| t.output),
            ));
        }
        let (sum, complete) = self.total(turns);
        let total = |n: u64| match (n, complete) {
            (_, true) => compact(n),
            (0, false) => "?".into(),
            (_, false) => format!("≥{}", compact(n)),
        };
        rows.push(("total".into(), total(sum.input), total(sum.output)));
        let mut out = String::from("Token use this session\n");
        for (turn, input, output) in rows {
            out.push_str(&format!("{turn:<6} {input:>7} {output:>7}\n"));
        }
        if !complete {
            out.push_str("? means the model did not report usage for that turn.");
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
        assert_eq!(usage.footer(2).as_deref(), Some("14.2k tok"));
        let table = usage.table(2);
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
        assert_eq!(usage.footer(0), None);
        usage.record(1, &json!({ "stopReason": "end_turn" }));
        assert_eq!(usage.footer(1).as_deref(), Some("? tok"));
        assert!(rows(&usage.table(1)).contains(&vec!["total", "?", "?"]));
        usage.record(2, &reported(1_200, 300));
        assert_eq!(usage.footer(2).as_deref(), Some("≥1.5k tok"));
        let table = usage.table(2);
        assert!(rows(&table).contains(&vec!["1", "?", "?"]), "{table}");
        assert!(
            rows(&table).contains(&vec!["total", "≥1.2k", "≥300"]),
            "{table}"
        );
        assert!(table.contains("did not report usage"), "{table}");
    }
}
