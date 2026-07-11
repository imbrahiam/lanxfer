use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Clear, Gauge, List, ListItem, ListState, Padding, Paragraph, Wrap,
};
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

    /// Drop the TUI and restore the terminal to normal mode. After this,
    /// plain println! works as expected. The StatusScreen becomes a no-op
    /// (renders nothing) until dropped.
    pub fn suspend(&mut self) {
        if self.terminal.is_some() {
            ratatui::restore();
            self.terminal = None;
        }
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
            choose_loop(terminal, title, &items, start, help)
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
            if is_ctrl_c(&key) {
                quit_app();
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
            if key.kind != KeyEventKind::Press {
                continue;
            }
            if is_ctrl_c(&key) {
                quit_app();
            }
            if matches!(key.code, KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q')) {
                return Ok(());
            }
        }
    }

    /// Blocking list with multi-select semantics: Space toggles the row
    /// under the cursor, Enter picks it, Esc cancels.
    pub fn choose_multi(
        &mut self,
        title: &str,
        items: Vec<String>,
        start: usize,
        help: &str,
    ) -> Result<MultiChoice> {
        let Some(terminal) = &mut self.terminal else {
            return Ok(items
                .get(start)
                .map_or(MultiChoice::Cancel, |_| MultiChoice::Pick(start)));
        };
        let mut query = String::new();
        let mut selected = start.min(items.len().saturating_sub(1));
        loop {
            let visible = filtered(&items, &query);
            selected = selected.min(visible.len().saturating_sub(1));
            terminal.draw(|frame| draw(frame, title, &items, &visible, selected, &query, help))?;
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            if is_ctrl_c(&key) {
                quit_app();
            }
            if key.code == KeyCode::Char(' ') && !visible.is_empty() {
                return Ok(MultiChoice::Toggle(visible[selected]));
            }
            match handle_list_key(&key, &items, &mut selected, &mut query) {
                KeyOutcome::Pick(index) => return Ok(MultiChoice::Pick(index)),
                KeyOutcome::Cancel => return Ok(MultiChoice::Cancel),
                KeyOutcome::Handled | KeyOutcome::Ignored => {}
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

    /// Wide, terminal-filling transfer view. Kept separate from the compact
    /// status/list cards so the initial peer picker retains its current size.
    #[allow(clippy::too_many_arguments)]
    pub fn render_transfer(
        &mut self,
        title: &str,
        source: &str,
        destination: &str,
        overall: &str,
        ratio: f64,
        rate: &str,
        units: &[String],
        footer: &str,
    ) -> Result<()> {
        if let Some(terminal) = &mut self.terminal {
            terminal.draw(|frame| {
                draw_transfer(
                    frame,
                    title,
                    source,
                    destination,
                    overall,
                    ratio,
                    rate,
                    units,
                    footer,
                )
            })?;
        } else {
            println!("{title}: {overall} · {rate}");
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

/// Ctrl+C arrives as a key event in raw mode, not SIGINT — honor it
/// anywhere the UI is reading keys, restoring the terminal first.
fn quit_app() -> ! {
    ratatui::restore();
    let _ = console::Term::stdout().show_cursor();
    std::process::exit(0)
}

fn is_ctrl_c(key: &crossterm::event::KeyEvent) -> bool {
    key.code == KeyCode::Char('c')
        && key
            .modifiers
            .contains(crossterm::event::KeyModifiers::CONTROL)
}

fn choose_loop(
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
        match handle_list_key(&key, items, &mut selected, &mut query) {
            KeyOutcome::Pick(index) => return Ok(Some(index)),
            KeyOutcome::Cancel => return Ok(None),
            KeyOutcome::Handled | KeyOutcome::Ignored => {}
        }
    }
}

pub(crate) enum MultiChoice {
    /// Enter — index into the full item list.
    Pick(usize),
    /// Space — toggle this index.
    Toggle(usize),
    /// Esc.
    Cancel,
}

/// Drain pending input during a busy screen (progress cards): Ctrl+C still
/// quits, everything else is discarded.
pub(crate) fn pump_quit_only() -> Result<()> {
    while event::poll(std::time::Duration::ZERO)? {
        if let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && is_ctrl_c(&key)
        {
            quit_app();
        }
    }
    Ok(())
}

pub(crate) enum KeyOutcome {
    /// Enter — index into the full item list.
    Pick(usize),
    /// Esc.
    Cancel,
    /// Navigation or filter state changed — redraw needed.
    Handled,
    Ignored,
}

/// Shared keyboard handling for every filterable list in the app.
pub(crate) fn handle_list_key(
    key: &crossterm::event::KeyEvent,
    items: &[String],
    selected: &mut usize,
    query: &mut String,
) -> KeyOutcome {
    if key.kind != KeyEventKind::Press {
        return KeyOutcome::Ignored;
    }
    if is_ctrl_c(key) {
        quit_app();
    }
    let visible = filtered(items, query);
    match key.code {
        KeyCode::Esc => KeyOutcome::Cancel,
        KeyCode::Enter if !visible.is_empty() => {
            KeyOutcome::Pick(visible[(*selected).min(visible.len() - 1)])
        }
        KeyCode::Up | KeyCode::Char('k') if query.is_empty() => {
            *selected = selected.saturating_sub(1);
            KeyOutcome::Handled
        }
        KeyCode::Down | KeyCode::Char('j') if query.is_empty() => {
            *selected = (*selected + 1).min(visible.len().saturating_sub(1));
            KeyOutcome::Handled
        }
        KeyCode::Home => {
            *selected = 0;
            KeyOutcome::Handled
        }
        KeyCode::End => {
            *selected = visible.len().saturating_sub(1);
            KeyOutcome::Handled
        }
        KeyCode::Backspace => {
            query.pop();
            *selected = 0;
            KeyOutcome::Handled
        }
        KeyCode::Char(c)
            if !key
                .modifiers
                .contains(crossterm::event::KeyModifiers::CONTROL) =>
        {
            query.push(c);
            *selected = 0;
            KeyOutcome::Handled
        }
        _ => KeyOutcome::Ignored,
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
    frame.render_widget(Clear, area);
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

#[allow(clippy::too_many_arguments)]
fn draw_transfer(
    frame: &mut ratatui::Frame,
    title: &str,
    source: &str,
    destination: &str,
    overall: &str,
    ratio: f64,
    rate: &str,
    units: &[String],
    footer: &str,
) {
    let area = frame.area();
    frame.render_widget(Clear, area);
    let width = area.width.saturating_sub(4).clamp(1, 160);
    let height = area.height.saturating_sub(2).clamp(1, 44);
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
        card.y + 1,
        card.width.saturating_sub(6),
        card.height.saturating_sub(2),
    );
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(4),
            Constraint::Length(4),
            Constraint::Length(3),
            Constraint::Length(2),
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
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ])),
        rows[0],
    );
    frame.render_widget(
        Paragraph::new(source)
            .wrap(Wrap { trim: false })
            .block(Block::default().title(" FROM ").borders(Borders::BOTTOM)),
        rows[1],
    );
    frame.render_widget(
        Paragraph::new(destination)
            .wrap(Wrap { trim: false })
            .block(Block::default().title(" TO ").borders(Borders::BOTTOM)),
        rows[2],
    );
    frame.render_widget(
        Gauge::default()
            .gauge_style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
            .ratio(ratio.clamp(0.0, 1.0))
            .label(overall),
        rows[3],
    );
    frame.render_widget(
        Paragraph::new(rate).style(Style::default().fg(VIOLET)),
        rows[4],
    );
    let unit_items: Vec<ListItem> = if units.is_empty() {
        vec![ListItem::new("  Preparing next file…").style(Style::default().fg(MUTED))]
    } else {
        units
            .iter()
            .map(|unit| ListItem::new(format!("  {unit}")))
            .collect()
    };
    frame.render_widget(
        List::new(unit_items).block(
            Block::default()
                .title(format!(" ACTIVE FILES ({}) ", units.len()))
                .borders(Borders::TOP),
        ),
        rows[5],
    );
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(MUTED)),
        rows[6],
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
    frame.render_widget(Clear, area);
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
    frame.render_widget(Clear, area);
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
    // Never exceed the frame — rendering outside the buffer panics ratatui
    // (tiny terminals, mid-resize).
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

#[cfg(test)]
mod tests {
    use super::{draw_transfer, filtered};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn filtering_is_case_insensitive_and_preserves_source_indices() {
        let items = vec!["MacBook Pro".into(), "DESKTOP".into(), "phone".into()];
        assert_eq!(filtered(&items, "desk"), vec![1]);
        assert_eq!(filtered(&items, "book"), vec![0]);
        assert_eq!(filtered(&items, ""), vec![0, 1, 2]);
    }

    #[test]
    fn transfer_view_handles_terminal_sizes() {
        for (width, height) in [(20, 10), (82, 28), (180, 50)] {
            let backend = TestBackend::new(width, height);
            let mut terminal = Terminal::new(backend).unwrap();
            terminal
                .draw(|frame| {
                    draw_transfer(
                        frame,
                        "Sending",
                        "/a/very/long/source/path",
                        "/a/very/long/destination/path",
                        "50% · 1/2 files · 5 MB / 10 MB",
                        0.5,
                        "Overall speed 5 MB/s · Overall folder ETA 0:01",
                        &["folder/file.bin · 50% · 5 MB / 10 MB".into()],
                        "ctrl+c quits",
                    )
                })
                .unwrap();
        }
    }
}
