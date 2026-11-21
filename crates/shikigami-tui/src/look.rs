//! `[tui]` settings: rebindable actions and color roles (ADR 0023).
//!
//! The engine keeps the tables as raw strings; this module checks them once at
//! launch so a typo refuses startup instead of silently doing nothing.

use std::collections::BTreeMap;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::{Color, Modifier, Style};

use shikigami::config::TuiSettings;

/// A TUI action a user can rebind in `[tui.keys]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Cancel the running prompt, or quit when idle.
    Interrupt,
    /// Cancel the in-flight model request and continue the same run.
    Steer,
    Quit,
    TogglePlan,
    ToggleTool,
    Copy,
    ScrollLineUp,
    ScrollLineDown,
    LineStart,
    LineEnd,
    ClearInput,
}

/// Every action with its settings name and default chord.
const ACTIONS: [(Action, &str, Chord); 11] = [
    (Action::Interrupt, "interrupt", Chord::Ctrl('c')),
    (Action::Steer, "steer", Chord::AltEnter),
    (Action::Quit, "quit", Chord::Ctrl('d')),
    (Action::TogglePlan, "toggle_plan", Chord::Ctrl('p')),
    (Action::ToggleTool, "toggle_tool", Chord::Ctrl('o')),
    (Action::Copy, "copy", Chord::Ctrl('y')),
    (Action::ScrollLineUp, "scroll_line_up", Chord::Ctrl('k')),
    (Action::ScrollLineDown, "scroll_line_down", Chord::Ctrl('j')),
    (Action::LineStart, "line_start", Chord::Ctrl('a')),
    (Action::LineEnd, "line_end", Chord::Ctrl('e')),
    (Action::ClearInput, "clear_input", Chord::Ctrl('u')),
];

const CHORD_HINT: &str = "use ctrl+<letter>, alt+<letter>, or alt+enter";

/// A modifier plus a lowercase ASCII letter, or Alt+Enter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Chord {
    Ctrl(char),
    Alt(char),
    AltEnter,
}

impl Chord {
    fn parse(text: &str) -> Result<Self, String> {
        let lower = text.trim().to_ascii_lowercase();
        if lower == "alt+enter" {
            return Ok(Chord::AltEnter);
        }
        let (make, rest): (fn(char) -> Chord, &str) =
            if let Some(rest) = lower.strip_prefix("ctrl+") {
                (Chord::Ctrl, rest)
            } else if let Some(rest) = lower.strip_prefix("alt+") {
                (Chord::Alt, rest)
            } else {
                return Err(CHORD_HINT.into());
            };
        let mut chars = rest.chars();
        let (Some(letter), None) = (chars.next(), chars.next()) else {
            return Err(CHORD_HINT.into());
        };
        if !letter.is_ascii_lowercase() {
            return Err(CHORD_HINT.into());
        }
        let chord = make(letter);
        // Terminals send these as Enter, Tab, and Backspace.
        if matches!(chord, Chord::Ctrl('m' | 'i' | 'h')) {
            return Err(format!(
                "ctrl+{letter} is not a separate key in most terminals"
            ));
        }
        Ok(chord)
    }

    fn of(key: &KeyEvent) -> Option<Self> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);
        let shift = key.modifiers.contains(KeyModifiers::SHIFT);
        match key.code {
            KeyCode::Enter if alt && !ctrl && !shift => Some(Chord::AltEnter),
            // Ctrl+J is LF; terminals report it as Ctrl+Enter or a bare '\n'.
            KeyCode::Enter if ctrl => Some(Chord::Ctrl('j')),
            KeyCode::Char('\n') => Some(Chord::Ctrl('j')),
            KeyCode::Char(ch) if ctrl => Some(Chord::Ctrl(ch.to_ascii_lowercase())),
            KeyCode::Char(ch) if alt => Some(Chord::Alt(ch.to_ascii_lowercase())),
            _ => None,
        }
    }

    /// Short form for the footer and help: `^c`, `alt+c`, `alt+enter`.
    fn label(self) -> String {
        match self {
            Chord::Ctrl(ch) => format!("^{ch}"),
            Chord::Alt(ch) => format!("alt+{ch}"),
            Chord::AltEnter => "alt+enter".into(),
        }
    }
}

/// The resolved key for every action.
#[derive(Clone, Debug)]
pub(crate) struct KeyMap {
    bindings: [(Action, Chord); ACTIONS.len()],
}

impl Default for KeyMap {
    fn default() -> Self {
        Self {
            bindings: ACTIONS.map(|(action, _, chord)| (action, chord)),
        }
    }
}

impl KeyMap {
    /// Applies `[tui.keys]` over the defaults. Errors name the entry.
    fn from_settings(keys: &BTreeMap<String, String>) -> Result<Self, String> {
        let mut map = Self::default();
        for (name, key) in keys {
            let Some(index) = ACTIONS.iter().position(|(_, n, _)| n == name) else {
                let names: Vec<_> = ACTIONS.iter().map(|(_, n, _)| *n).collect();
                return Err(format!(
                    "tui.keys.{name}: unknown action; use one of {}",
                    names.join(", ")
                ));
            };
            map.bindings[index].1 =
                Chord::parse(key).map_err(|e| format!("tui.keys.{name} = \"{key}\": {e}"))?;
        }
        for (i, (action, chord)) in map.bindings.iter().enumerate() {
            if let Some((other, _)) = map.bindings[..i].iter().find(|(_, c)| c == chord) {
                return Err(format!(
                    "tui.keys: {} is bound to both {} and {}",
                    chord.label(),
                    name_of(*other),
                    name_of(*action)
                ));
            }
        }
        Ok(map)
    }

    /// The action `key` triggers, if any.
    pub(crate) fn action(&self, key: &KeyEvent) -> Option<Action> {
        let chord = Chord::of(key)?;
        self.bindings
            .iter()
            .find(|(_, c)| *c == chord)
            .map(|(action, _)| *action)
    }

    /// The key for `action` as the footer and help show it.
    pub(crate) fn label(&self, action: Action) -> String {
        self.bindings
            .iter()
            .find(|(a, _)| *a == action)
            .map(|(_, chord)| chord.label())
            .unwrap_or_default()
    }
}

fn name_of(action: Action) -> &'static str {
    ACTIONS
        .iter()
        .find(|(a, _, _)| *a == action)
        .map(|(_, name, _)| *name)
        .unwrap_or_default()
}

/// Styles for the three kinds of text the TUI draws.
#[derive(Clone, Debug)]
pub(crate) struct Palette {
    /// Answers and the composer.
    pub(crate) text: Style,
    /// Prompts, tool and system lines, rules, lists, and the footer.
    pub(crate) muted: Style,
    /// The highlighted row in a list or ask.
    pub(crate) selected: Style,
}

impl Default for Palette {
    fn default() -> Self {
        Self {
            text: Style::new(),
            muted: Style::new().add_modifier(Modifier::DIM),
            selected: Style::new().add_modifier(Modifier::REVERSED),
        }
    }
}

const ROLES: [&str; 3] = ["text", "muted", "selected"];

impl Palette {
    /// Applies `[tui.colors]` over the defaults. With `no_color` (the
    /// `NO_COLOR` convention) names are still checked but colors are not used.
    fn from_settings(colors: &BTreeMap<String, String>, no_color: bool) -> Result<Self, String> {
        let mut palette = Self::default();
        for (role, value) in colors {
            let slot = match role.as_str() {
                "text" => &mut palette.text,
                "muted" => &mut palette.muted,
                "selected" => &mut palette.selected,
                _ => {
                    return Err(format!(
                        "tui.colors.{role}: unknown role; use one of {}",
                        ROLES.join(", ")
                    ));
                }
            };
            let color: Color = value.parse().map_err(|_| {
                format!("tui.colors.{role} = \"{value}\": use a color name or #rrggbb")
            })?;
            if !no_color {
                *slot = slot.fg(color);
            }
        }
        Ok(palette)
    }
}

/// Checked `[tui]` settings for one TUI process.
#[derive(Clone, Debug, Default)]
pub(crate) struct Look {
    pub(crate) keys: KeyMap,
    pub(crate) palette: Palette,
}

impl Look {
    pub(crate) fn from_settings(settings: &TuiSettings, no_color: bool) -> Result<Self, String> {
        Ok(Self {
            keys: KeyMap::from_settings(&settings.keys)?,
            palette: Palette::from_settings(&settings.colors, no_color)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(entries: &[(&str, &str)]) -> Result<KeyMap, String> {
        KeyMap::from_settings(
            &entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn ctrl(ch: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL)
    }

    #[test]
    fn rebinding_moves_the_action_and_frees_the_old_key() {
        let map = keys(&[("toggle_plan", "Alt+G")]).unwrap();
        let alt_g = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::ALT);
        assert_eq!(map.action(&alt_g), Some(Action::TogglePlan));
        assert_eq!(map.action(&ctrl('p')), None);
        assert_eq!(map.label(Action::TogglePlan), "alt+g");
        assert_eq!(map.action(&ctrl('c')), Some(Action::Interrupt));
        let alt_enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT);
        assert_eq!(map.action(&alt_enter), Some(Action::Steer));
        assert_eq!(map.label(Action::Steer), "alt+enter");
        let rebound = keys(&[("steer", "alt+s")]).unwrap();
        let alt_s = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::ALT);
        assert_eq!(rebound.action(&alt_s), Some(Action::Steer));
        assert_eq!(rebound.action(&alt_enter), None);
    }

    #[test]
    fn bad_entries_name_themselves() {
        let err = |entries| keys(entries).unwrap_err();
        assert!(
            err(&[("toggle_plna", "ctrl+g")]).starts_with("tui.keys.toggle_plna: unknown action")
        );
        assert!(err(&[("quit", "g")]).starts_with("tui.keys.quit = \"g\":"));
        assert!(err(&[("quit", "ctrl+m")]).contains("not a separate key"));
        assert_eq!(
            err(&[("quit", "ctrl+c")]),
            "tui.keys: ^c is bound to both interrupt and quit"
        );
        // Swapping two defaults is allowed.
        assert!(keys(&[("quit", "ctrl+c"), ("interrupt", "ctrl+d")]).is_ok());
    }

    #[test]
    fn colors_apply_unless_no_color() {
        let colors: BTreeMap<_, _> = [("muted".to_string(), "#808080".to_string())].into();
        let palette = Palette::from_settings(&colors, false).unwrap();
        assert_eq!(palette.muted.fg, Some(Color::Rgb(0x80, 0x80, 0x80)));
        assert!(palette.muted.add_modifier.contains(Modifier::DIM));
        assert_eq!(
            Palette::from_settings(&colors, true).unwrap().muted.fg,
            None
        );

        let bad: BTreeMap<_, _> = [("accent".to_string(), "red".to_string())].into();
        let err = Palette::from_settings(&bad, true).unwrap_err();
        assert!(err.starts_with("tui.colors.accent: unknown role"), "{err}");
        let bad: BTreeMap<_, _> = [("text".to_string(), "not-a-color".to_string())].into();
        let err = Palette::from_settings(&bad, false).unwrap_err();
        assert!(
            err.starts_with("tui.colors.text = \"not-a-color\""),
            "{err}"
        );
    }
}
