/// Replace terminal controls in untrusted human-readable text, retaining LF and tab.
pub fn terminal_text(text: &str) -> String {
    text.chars()
        .map(|ch| {
            if ch.is_control() && ch != '\n' && ch != '\t' {
                '\u{fffd}'
            } else {
                ch
            }
        })
        .collect()
}
