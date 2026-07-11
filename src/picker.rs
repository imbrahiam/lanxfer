use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Padding, Paragraph};
use std::io::IsTerminal;

// Keep the surface on the terminal's own background. Hard-coded RGB
// backgrounds render inconsistently in terminals without true-color support
// (and make light themes unreadable). Named accents are broadly supported.
const CYAN: Color = Color::Cyan;
const VIOLET: Color = Color::Magenta;
const MUTED: Color = Color::DarkGray;

#[derive(Clone, Copy)]
pub enum Tone {
    Info,
    Success,
    Warning,
    Error,
}

pub struct StatusScreen {
    terminal: Option<ratatui::DefaultTerminal>,
}

impl StatusScreen {
    pub fn new() -> Result<Self> {
        if !std::io::stdout().is_terminal() {
            return Ok(Self { terminal: None });
        }
        let mut terminal = ratatui::init();
        terminal.clear()?;
        Ok(Self {
            terminal: Some(terminal),
        })
    }

    pub fn render(
        &mut self,
        title: &str,
        message: &str,
        tone: Tone,
        details: &[(String, String)],
        footer: &str,
    ) -> Result<()> {
        if let Some(terminal) = &mut self.terminal {
            terminal.draw(|frame| draw_status(frame, title, message, tone, details, footer))?;
        } else {
            println!("{title}: {message}");
            for (key, value) in details {
                println!("{key}: {value}");
            }
        }
        Ok(())
    }

    pub fn choose(
        &mut self,
        title: &str,
        items: Vec<String>,
        start: usize,
        help: &str,
    ) -> Result<Option<usize>> {
        if let Some(terminal) = &mut self.terminal {
            run(terminal, title, &items, start, help)
        } else {
            Ok(items.get(start).map(|_| start))
        }
    }

    pub fn input(
        &mut self,
        title: &str,
        prompt: &str,
        help: &str,
        secret: bool,
    ) -> Result<Option<String>> {
        if self.terminal.is_none() {
            return Ok(None);
        }
        let terminal = self.terminal.as_mut().expect("terminal checked above");
        let mut value = String::new();
        loop {
            terminal.draw(|frame| draw_input(frame, title, prompt, &value, help, secret))?;
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Esc => return Ok(None),
                KeyCode::Enter if !value.trim().is_empty() => return Ok(Some(value)),
                KeyCode::Backspace => {
                    value.pop();
                }
                KeyCode::Char(c)
                    if !key
                        .modifiers
                        .contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    value.push(c);
                }
                _ => {}
            }
        }
    }

    pub fn wait_for_close(&mut self) -> Result<()> {
        if self.terminal.is_none() {
            return Ok(());
        }
        loop {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind == KeyEventKind::Press
                && matches!(key.code, KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q'))
            {
                return Ok(());
            }
        }
    }

    pub fn draw_list(
        &mut self,
        title: &str,
        items: &[String],
        visible: &[usize],
        selected: usize,
        query: &str,
        help: &str,
    ) -> Result<()> {
        if let Some(terminal) = &mut self.terminal {
            terminal.draw(|frame| draw(frame, title, items, visible, selected, query, help))?;
        }
        Ok(())
    }
}

impl Drop for StatusScreen {
    fn drop(&mut self) {
        if self.terminal.is_some() {
            ratatui::restore();
        }
    }
}

fn run(
    terminal: &mut ratatui::DefaultTerminal,
    title: &str,
    items: &[String],
    start: usize,
    help: &str,
) -> Result<Option<usize>> {
    let mut query = String::new();
    let mut selected = start.min(items.len().saturating_sub(1));
    loop {
        let visible = filtered(items, &query);
        selected = selected.min(visible.len().saturating_sub(1));
        terminal.draw(|frame| draw(frame, title, items, &visible, selected, &query, help))?;
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        match key.code {
            KeyCode::Esc => return Ok(None),
            KeyCode::Enter if !visible.is_empty() => return Ok(Some(visible[selected])),
            KeyCode::Up | KeyCode::Char('k') if query.is_empty() => {
                selected = selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') if query.is_empty() => {
                selected = (selected + 1).min(visible.len().saturating_sub(1));
            }
            KeyCode::Home => selected = 0,
            KeyCode::End => selected = visible.len().saturating_sub(1),
            KeyCode::Backspace => {
                query.pop();
                selected = 0;
            }
            KeyCode::Char(c)
                if !key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                query.push(c);
                selected = 0;
            }
            _ => {}
        }
    }
}

pub(crate) fn filtered(items: &[String], query: &str) -> Vec<usize> {
    let needle = query.to_lowercase();
    items
        .iter()
        .enumerate()
        .filter_map(|(i, item)| item.to_lowercase().contains(&needle).then_some(i))
        .collect()
}

fn draw_status(
    frame: &mut ratatui::Frame,
    title: &str,
    message: &str,
    tone: Tone,
    details: &[(String, String)],
    footer: &str,
) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::reset()), area);
    let height = (details.len() as u16 + 12).clamp(14, 28);
    let card = centered(area, area.width.min(82), area.height.min(height));
    frame.render_widget(Clear, card);
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(MUTED))
            .style(Style::reset())
            .padding(Padding::horizontal(2)),
        card,
    );
    let inner = Rect::new(
        card.x + 3,
        card.y + 2,
        card.width.saturating_sub(6),
        card.height.saturating_sub(4),
    );
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(2),
            Constraint::Length(2),
        ])
        .split(inner);
    let (symbol, color) = match tone {
        Tone::Info => ("◌", CYAN),
        Tone::Success => ("✓", Color::Green),
        Tone::Warning => ("⚠", Color::Yellow),
        Tone::Error => ("✗", Color::Red),
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "⇄  LANXFER",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("   {title}"),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ])),
        rows[0],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                format!("{symbol}  "),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(message, Style::default().add_modifier(Modifier::BOLD)),
        ]))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(MUTED)),
        ),
        rows[1],
    );
    let detail_lines: Vec<Line> = details
        .iter()
        .map(|(key, value)| {
            Line::from(vec![
                Span::styled(format!("{key:>14}  "), Style::default().fg(MUTED)),
                Span::raw(value),
            ])
        })
        .collect();
    frame.render_widget(Paragraph::new(detail_lines), rows[2]);
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(MUTED)),
        rows[3],
    );
}

fn draw_input(
    frame: &mut ratatui::Frame,
    title: &str,
    prompt: &str,
    value: &str,
    help: &str,
    secret: bool,
) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::reset()), area);
    let card = centered(area, area.width.min(82), area.height.clamp(14, 22));
    frame.render_widget(Clear, card);
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(MUTED))
            .style(Style::reset())
            .padding(Padding::horizontal(2)),
        card,
    );
    let inner = Rect::new(
        card.x + 3,
        card.y + 2,
        card.width.saturating_sub(6),
        card.height.saturating_sub(4),
    );
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "⇄  LANXFER",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("   {title}"),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ])),
        rows[0],
    );
    frame.render_widget(
        Paragraph::new(prompt).style(Style::default().fg(MUTED)),
        rows[1],
    );
    let shown = if secret {
        "•".repeat(value.chars().count())
    } else {
        value.to_string()
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("❯ ", Style::default().fg(CYAN)),
            Span::raw(shown),
            Span::styled("█", Style::default().fg(CYAN)),
        ]))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(CYAN)),
        ),
        rows[2],
    );
    frame.render_widget(
        Paragraph::new(help).style(Style::default().fg(MUTED)),
        rows[4],
    );
}

fn draw(
    frame: &mut ratatui::Frame,
    title: &str,
    items: &[String],
    visible: &[usize],
    selected: usize,
    query: &str,
    help: &str,
) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::reset()), area);
    let width = area.width.min(82);
    let height = area.height.clamp(12, 28);
    let card = centered(area, width, height);
    frame.render_widget(Clear, card);
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(MUTED))
            .style(Style::reset())
            .padding(Padding::horizontal(2)),
        card,
    );
    let inner = Rect::new(
        card.x + 3,
        card.y + 2,
        card.width.saturating_sub(6),
        card.height.saturating_sub(4),
    );
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(3),
            Constraint::Length(2),
        ])
        .split(inner);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "⇄  LANXFER",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("   {title}"),
                Style::default()
                    .fg(Color::Reset)
                    .add_modifier(Modifier::BOLD),
            ),
        ])),
        rows[0],
    );

    let search = if query.is_empty() {
        "Type to filter…".to_string()
    } else {
        query.to_string()
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("⌕  ", Style::default().fg(VIOLET)),
            Span::styled(
                search,
                Style::default().fg(if query.is_empty() {
                    MUTED
                } else {
                    Color::Reset
                }),
            ),
        ]))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(MUTED)),
        ),
        rows[1],
    );

    let list_items: Vec<ListItem> = if visible.is_empty() {
        vec![ListItem::new("  No matches").style(Style::default().fg(MUTED))]
    } else {
        visible
            .iter()
            .map(|&i| ListItem::new(format!("  {}", items[i])))
            .collect()
    };
    let list = List::new(list_items)
        .highlight_symbol("  ❯ ")
        .highlight_style(Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED));
    let mut state = ListState::default().with_selected((!visible.is_empty()).then_some(selected));
    frame.render_stateful_widget(list, rows[2], &mut state);

    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("↑↓", Style::default().fg(CYAN)),
            Span::styled(" move   ", Style::default().fg(MUTED)),
            Span::styled("enter", Style::default().fg(CYAN)),
            Span::styled(" select   ", Style::default().fg(MUTED)),
            Span::styled("esc", Style::default().fg(CYAN)),
            Span::styled(" back", Style::default().fg(MUTED)),
            Span::styled(
                format!("     {}/{}  {help}", visible.len(), items.len()),
                Style::default().fg(MUTED),
            ),
        ]))
        .alignment(Alignment::Left),
        rows[3],
    );
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

#[cfg(test)]
mod tests {
    use super::filtered;

    #[test]
    fn filtering_is_case_insensitive_and_preserves_source_indices() {
        let items = vec!["MacBook Pro".into(), "DESKTOP".into(), "phone".into()];
        assert_eq!(filtered(&items, "desk"), vec![1]);
        assert_eq!(filtered(&items, "book"), vec![0]);
        assert_eq!(filtered(&items, ""), vec![0, 1, 2]);
    }
}
