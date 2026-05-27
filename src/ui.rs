use console::{Term, measure_text_width, style};
use dialoguer::theme::ColorfulTheme;

const MIN_WIDTH: usize = 56;
const MAX_WIDTH: usize = 78;

fn width() -> usize {
    let cols = Term::stdout().size().1 as usize;
    cols.clamp(MIN_WIDTH, MAX_WIDTH)
}

pub fn banner() {
    let w = width();
    let inner = w.saturating_sub(2);
    let title = "  ⇄  L A N X F E R  ";
    let tagline = format!(
        " v{}  ·  fast resumable LAN transfer ",
        env!("CARGO_PKG_VERSION")
    );
    let title_w = measure_text_width(title);
    let tagline_w = measure_text_width(&tagline);
    let pad = inner.saturating_sub(title_w + tagline_w);

    println!();
    println!("  {}", style(format!("╭{}╮", "─".repeat(inner))).cyan());
    println!(
        "  {}{}{}{}{}",
        style("│").cyan(),
        style(title).bold().yellow(),
        " ".repeat(pad),
        style(&tagline).dim(),
        style("│").cyan(),
    );
    println!("  {}", style(format!("╰{}╯", "─".repeat(inner))).cyan());
    println!();
}

pub fn section(title: &str) {
    let w = width();
    let prefix = format!("─── {} ", title);
    let prefix_w = measure_text_width(&prefix);
    let fill = w.saturating_sub(prefix_w);
    println!();
    println!(
        "  {}{}",
        style(&prefix).cyan().bold(),
        style("─".repeat(fill)).dim(),
    );
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
    println!(
        "  {}  {}",
        style(format!("{key:>13}")).dim(),
        value,
    );
}

pub fn dim(msg: &str) -> String {
    style(msg).dim().to_string()
}

pub fn bold(msg: &str) -> String {
    style(msg).bold().to_string()
}

pub fn accent(msg: &str) -> String {
    style(msg).cyan().to_string()
}

pub fn ok(msg: &str) -> String {
    style(msg).green().to_string()
}

pub fn yellow(msg: &str) -> String {
    style(msg).yellow().to_string()
}

pub fn theme() -> ColorfulTheme {
    let mut t = ColorfulTheme::default();
    t.prompt_prefix = style("▶".to_string()).yellow().bold();
    t.prompt_suffix = style("·".to_string()).dim();
    t.success_prefix = style("✓".to_string()).green().bold();
    t.success_suffix = style("·".to_string()).dim();
    t.error_prefix = style("✗".to_string()).red().bold();
    t.active_item_prefix = style("▶".to_string()).yellow().bold();
    t.inactive_item_prefix = style("  ".to_string());
    t.active_item_style = console::Style::new().yellow().bold();
    t.inactive_item_style = console::Style::new();
    t.checked_item_prefix = style("✓".to_string()).green().bold();
    t.unchecked_item_prefix = style("·".to_string()).dim();
    t.hint_style = console::Style::new().black().bright();
    t
}
