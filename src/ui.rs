use console::{Term, style};
use inquire::ui::{Attributes, Color, RenderConfig, StyleSheet, Styled};

/// Clear the terminal and print the one-line banner. Every entrypoint calls
/// this so stale shell output never mixes with the session.
pub fn banner() {
    let term = Term::stdout();
    let _ = term.clear_screen();
    println!();
    println!(
        "  {} {}  {}",
        style("⇄").cyan().bold(),
        style("lanxfer").bold(),
        style(format!(
            "v{} · fast resumable LAN transfer",
            env!("CARGO_PKG_VERSION")
        ))
        .dim(),
    );
    println!();
}

/// Global inquire style — cyan accents, subtle chrome.
pub fn init_prompts() {
    let mut rc = RenderConfig::default_colored();
    rc.prompt_prefix = Styled::new("◆").with_fg(Color::LightCyan);
    rc.answered_prompt_prefix = Styled::new("◇").with_fg(Color::DarkGrey);
    rc.highlighted_option_prefix = Styled::new("❯").with_fg(Color::LightCyan);
    rc.selected_checkbox = Styled::new("●").with_fg(Color::LightGreen);
    rc.unselected_checkbox = Styled::new("○").with_fg(Color::DarkGrey);
    rc.help_message = StyleSheet::new().with_fg(Color::DarkGrey);
    rc.answer = StyleSheet::new().with_fg(Color::LightCyan);
    rc.selected_option = Some(
        StyleSheet::new()
            .with_fg(Color::LightCyan)
            .with_attr(Attributes::BOLD),
    );
    inquire::set_global_render_config(rc);
}

pub fn section(title: &str) {
    println!();
    println!("  {} {}", style("›").dim(), style(title).bold());
}

pub fn info(msg: &str) {
    println!("  {} {msg}", style("·").dim());
}

pub fn success(msg: &str) {
    println!("  {} {msg}", style("✓").green().bold());
}

pub fn warn(msg: &str) {
    println!("  {} {msg}", style("⚠").yellow().bold());
}

pub fn error(msg: &str) {
    eprintln!("  {} {msg}", style("✗").red().bold());
}

pub fn kv(key: &str, value: &str) {
    println!("  {}  {}", style(format!("{key:>13}")).dim(), value);
}

pub fn dim(msg: &str) -> String {
    style(msg).dim().to_string()
}

pub fn bold(msg: &str) -> String {
    style(msg).bold().to_string()
}

pub fn ok(msg: &str) -> String {
    style(msg).green().to_string()
}

pub fn yellow(msg: &str) -> String {
    style(msg).yellow().to_string()
}

fn legacy_console() -> bool {
    cfg!(windows)
        && std::env::var_os("WT_SESSION").is_none()
        && std::env::var_os("TERM_PROGRAM").is_none()
        && std::env::var_os("TERM").is_none()
}

/// Progress bar glyphs. Legacy conhost fonts lack the heavy-line glyphs —
/// ASCII there; Windows Terminal / VS Code / unix get the smooth bar.
pub fn progress_chars() -> &'static str {
    if legacy_console() { "=> " } else { "━╸━" }
}

/// indicatif style fragments for the smooth two-tone bar.
pub fn overall_bar_template() -> &'static str {
    "  {spinner:.cyan} {bar:38.cyan/238} {bytes:>10} / {total_bytes:<10} {binary_bytes_per_sec:>11} · eta {eta} {msg}"
}

pub fn unit_bar_template() -> &'static str {
    "    {bar:30.green/238} {prefix:.dim} {bytes:>9}"
}
