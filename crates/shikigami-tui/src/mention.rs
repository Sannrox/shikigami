//! `@` file mentions in the composer. A word that starts with `@` at the
//! cursor opens the slash-style list of workspace files (the `glob` tool's
//! jail and ignore rules), filtered fuzzily on the path; picking one
//! replaces the word with `@path `.

/// Most rows the list ranks; the overlay shows a window of them.
const MAX_MATCHES: usize = 50;

/// The mention word under the cursor: its start (char index of `@`) and the
/// query typed after `@` up to the cursor. `None` when the word before the
/// cursor does not start with `@` or the `@` is inside a word (`a@b`).
pub(crate) fn mention_at(input: &str, cursor: usize) -> Option<(usize, String)> {
    let chars: Vec<char> = input.chars().collect();
    let cursor = cursor.min(chars.len());
    let start = chars[..cursor]
        .iter()
        .rposition(|c| c.is_whitespace())
        .map_or(0, |i| i + 1);
    (chars.get(start) == Some(&'@') && start < cursor)
        .then(|| (start, chars[start + 1..cursor].iter().collect()))
}

/// Files that contain `query` as a case-insensitive subsequence, best first:
/// a match inside the file name, then inside the path, then scattered; shorter
/// paths first within a tier.
pub(crate) fn rank(files: &[String], query: &str) -> Vec<String> {
    let query = query.to_lowercase();
    let mut scored: Vec<(u8, &String)> = files
        .iter()
        .filter_map(|path| {
            let lower = path.to_lowercase();
            let name = lower.rsplit('/').next().unwrap_or(&lower);
            let tier = if name.contains(&query) {
                0
            } else if lower.contains(&query) {
                1
            } else if is_subsequence(&query, &lower) {
                2
            } else {
                return None;
            };
            Some((tier, path))
        })
        .collect();
    scored.sort_by(|a, b| (a.0, a.1.len(), a.1).cmp(&(b.0, b.1.len(), b.1)));
    scored
        .into_iter()
        .take(MAX_MATCHES)
        .map(|(_, path)| path.clone())
        .collect()
}

fn is_subsequence(needle: &str, haystack: &str) -> bool {
    let mut rest = haystack.chars();
    needle.chars().all(|c| rest.any(|h| h == c))
}

/// Replace the mention word starting at `start` (through the end of that word)
/// with `@path ` and return the new input and cursor.
pub(crate) fn insert(input: &str, start: usize, path: &str) -> (String, usize) {
    let chars: Vec<char> = input.chars().collect();
    let end = chars[start..]
        .iter()
        .position(|c| c.is_whitespace())
        .map_or(chars.len(), |i| start + i);
    let head: String = chars[..start].iter().collect();
    let tail: String = chars[end..].iter().collect();
    let word = format!("@{path} ");
    let cursor = start + word.chars().count();
    let tail = tail.strip_prefix(' ').unwrap_or(&tail);
    (format!("{head}{word}{tail}"), cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mention_word_is_the_one_at_the_cursor() {
        assert_eq!(mention_at("@", 1), Some((0, String::new())));
        assert_eq!(mention_at("fix @src/ma", 11), Some((4, "src/ma".into())));
        assert_eq!(mention_at("fix @src now", 3), None);
        assert_eq!(mention_at("mail a@b.c", 10), None);
        assert_eq!(mention_at("@x done", 7), None);
    }

    #[test]
    fn rank_prefers_name_then_path_then_scattered() {
        let files: Vec<String> = [
            "docs/main.md",
            "src/main.rs",
            "src/domain/x.rs",
            "README.md",
        ]
        .map(String::from)
        .into();
        assert_eq!(
            rank(&files, "main"),
            ["src/main.rs", "docs/main.md", "src/domain/x.rs"]
        );
        assert_eq!(rank(&files, "DOMAIN"), ["src/domain/x.rs", "docs/main.md"]);
        assert_eq!(rank(&files, "srx"), ["src/domain/x.rs"]);
        assert_eq!(rank(&files, "").len(), 4);
    }

    #[test]
    fn insert_replaces_the_whole_word() {
        assert_eq!(
            insert("fix @ma please", 4, "src/main.rs"),
            ("fix @src/main.rs please".into(), 17)
        );
        assert_eq!(insert("@", 0, "a.rs"), ("@a.rs ".into(), 6));
    }
}
