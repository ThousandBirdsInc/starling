//! k9s-style terminal dashboard for Starling.
//!
//! Connects to the daemon, polls the aggregated state across all `starling up`
//! instances, and renders a navigable resource table with a live log pane.
//!
//! Keys:
//!   j/k, ↑/↓   move selection
//!   /          filter (Enter apply, Esc clear)
//!   Enter      detail view for the selected resource (Esc to exit)
//!   y          copy the visible log window to the clipboard
//!   t          trigger the selected resource
//!   Space      pause/resume the selected resource
//!   R          restart the selected resource's serve_cmd
//!   p          change the selected resource's preferred backend port
//!   PgUp/PgDn  page through logs (G/End follow tail, g/Home jump to oldest)
//!   r          refresh now
//!   q          quit

use std::io;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Cell, Paragraph, Row, Table, TableState};
use ratatui::{Frame, Terminal};

use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::{DashboardState, Request, Response};

/// A small, cohesive color palette (Tokyo Night-ish) used across the dashboard.
mod theme {
    use ratatui::style::Color;
    pub const ACCENT: Color = Color::Rgb(122, 162, 247); // soft blue
    pub const HEADER_BG: Color = Color::Rgb(36, 40, 59);
    pub const SEL_BG: Color = Color::Rgb(54, 60, 96);
    pub const MUTED: Color = Color::Rgb(132, 137, 165);
    pub const URL: Color = Color::Rgb(125, 207, 255);
    pub const OK: Color = Color::Rgb(158, 206, 106);
    pub const ERR: Color = Color::Rgb(247, 118, 142);
    pub const WARN: Color = Color::Rgb(224, 175, 104);
    pub const INFO: Color = Color::Rgb(125, 207, 255);
}

/// Braille spinner frames used for `in_progress` status.
const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Max log lines kept in the TUI scrollback per resource. Lines are fetched
/// incrementally and accumulated here, so this can exceed the daemon's own ring.
const LOG_HISTORY: usize = 2000;

/// Symbol + color for a status value. `frame` advances the in-progress spinner.
fn status_symbol(s: &str, frame: usize) -> (&'static str, Style) {
    match s {
        "ok" => ("●", Style::default().fg(theme::OK)),
        "error" => ("●", Style::default().fg(theme::ERR)),
        "in_progress" => (
            SPINNER[frame % SPINNER.len()],
            Style::default().fg(theme::WARN),
        ),
        "pending" => ("◌", Style::default().fg(theme::INFO)),
        "not_applicable" | "none" | "" => ("·", Style::default().fg(Color::DarkGray)),
        _ => ("•", Style::default()),
    }
}

/// Which status column a value belongs to. The same raw string reads
/// differently in each: an `in_progress` *update* is a build, while an
/// `in_progress` *runtime* is a serve_cmd being (re)started.
#[derive(Clone, Copy, PartialEq)]
enum StatusKind {
    Update,
    Runtime,
}

/// Human-friendly label for a raw status string, in the context of its column.
fn pretty_status(s: &str, kind: StatusKind) -> String {
    match s {
        "not_applicable" | "none" | "" => "—".into(),
        "in_progress" => match kind {
            StatusKind::Update => "building".into(),
            StatusKind::Runtime => "restarting".into(),
        },
        other => other.replace('_', " "),
    }
}

/// A status cell: colored symbol followed by its prettified label.
fn status_cell(s: &str, frame: usize, kind: StatusKind) -> Line<'static> {
    let (sym, style) = status_symbol(s, frame);
    Line::from(vec![
        Span::styled(sym, style),
        Span::raw(" "),
        Span::styled(pretty_status(s, kind), style),
    ])
}

/// Resource pause state, kept separate from build/runtime status so a paused
/// resource is obvious even when its update/runtime columns are idle.
fn state_cell(paused: bool) -> Line<'static> {
    if paused {
        Line::from(Span::styled("paused", Style::default().fg(theme::WARN)))
    } else {
        Line::from(Span::styled("active", Style::default().fg(theme::MUTED)))
    }
}

/// Build a styled footer line from `(key, label)` pairs: keys in accent, labels muted.
fn key_hints(pairs: &[(&str, &str)]) -> Line<'static> {
    let mut spans = vec![Span::raw(" ")];
    for (k, label) in pairs {
        spans.push(Span::styled(
            (*k).to_string(),
            Style::default()
                .fg(theme::ACCENT)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            format!(" {label}   "),
            Style::default().fg(theme::MUTED),
        ));
    }
    Line::from(spans)
}

/// One displayed row: a resource belonging to an instance.
#[derive(Clone)]
struct RowItem {
    instance_id: String,
    instance_name: String,
    name: String,
    kind: String,
    paused: bool,
    update: String,
    runtime: String,
    pod: String,
    url: String,
    route_port: Option<u16>,
    restart_count: Option<u32>,
    last_start: Option<String>,
}

#[derive(PartialEq)]
enum Mode {
    Normal,
    Filter,
    Detail,
    /// Full-screen logs for the selected resource.
    Logs,
    /// Typing a log-line filter (regex) while in full-screen logs.
    LogsFilter,
    /// Typing a new preferred backend port for the selected resource.
    PortEdit,
}

pub async fn run(proxy_port: u16, tld: &str, tls: bool) {
    let client = DaemonClient::new();
    if let Err(e) = client.ensure_running(proxy_port, tld, tls).await {
        eprintln!("starling: {e}");
        return;
    }
    if let Err(e) = run_ui(client).await {
        eprintln!("starling tui error: {e}");
    }
}

async fn run_ui(client: DaemonClient) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = event_loop(&mut terminal, &client).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    res
}

struct App {
    state: DashboardState,
    rows: Vec<RowItem>,
    table: TableState,
    logs: Vec<String>,
    /// Cached result of applying `log_filter` to `logs`. Rebuilt lazily only
    /// when `logs` or `log_filter` change — never on every frame.
    log_view: Vec<String>,
    /// Set when `logs`/`log_filter` change so `log_view` is rebuilt next draw.
    log_view_dirty: bool,
    /// Inner height (rows) of the log pane on the last frame. Drives page-sized
    /// scrolling and scroll clamping, which both need the visible height.
    log_view_h: usize,
    /// Cursor for incremental log fetches: the sequence past the last line in
    /// `logs`. Passed as `since` so each poll transfers only new lines.
    log_cursor: u64,
    /// The (instance, resource) `logs`/`log_cursor` belong to. When the
    /// selection moves to a different key, the buffer is refetched from scratch.
    log_key: Option<(String, String)>,
    mode: Mode,
    filter: String,
    /// Regex/substring filter applied to log lines (full-screen + log pane).
    log_filter: String,
    /// Lines scrolled up from the bottom; 0 = follow tail.
    log_scroll: usize,
    /// Port being edited in the footer.
    port_input: String,
    status_msg: String,
    /// When `status_msg` was set; it fades from the footer after a few seconds.
    status_at: Option<Instant>,
    /// Reference instant for time-based animation (the in-progress spinner).
    start: Instant,
}

impl App {
    fn selected(&self) -> Option<&RowItem> {
        self.table.selected().and_then(|i| self.rows.get(i))
    }

    /// Show a transient status message in the footer.
    fn note(&mut self, msg: String) {
        self.status_msg = msg;
        self.status_at = Some(Instant::now());
    }

    /// The current status message, if it hasn't yet faded out.
    fn active_status(&self) -> Option<&str> {
        match self.status_at {
            Some(t) if t.elapsed() < Duration::from_secs(4) => Some(self.status_msg.as_str()),
            _ => None,
        }
    }

    /// Current spinner frame, derived from elapsed time so it animates smoothly.
    fn spinner_frame(&self) -> usize {
        (self.start.elapsed().as_millis() / 90) as usize
    }

    /// Append newly-fetched lines for the current resource and advance the
    /// cursor. When nothing new arrived (the common idle case) this is a no-op
    /// that leaves the filtered view cache intact — no re-filter, no realloc.
    fn append_logs(&mut self, lines: Vec<String>, cursor: u64) {
        self.log_cursor = cursor;
        if lines.is_empty() {
            return;
        }
        self.logs.extend(lines);
        let overflow = self.logs.len().saturating_sub(LOG_HISTORY);
        if overflow > 0 {
            self.logs.drain(..overflow);
        }
        self.log_view_dirty = true;
    }

    /// Switch the log pane to a different resource (or none): replace the buffer
    /// wholesale and reset the cursor and scroll position.
    fn reset_logs(&mut self, key: Option<(String, String)>, lines: Vec<String>, cursor: u64) {
        self.log_key = key;
        self.log_cursor = cursor;
        self.logs = lines;
        self.log_scroll = 0;
        self.log_view_dirty = true;
    }

    /// Rebuild `log_view` from `logs`/`log_filter` if it went stale. Called once
    /// per draw so the regex filter runs on change, not on every frame.
    fn ensure_log_view(&mut self) {
        if self.log_view_dirty {
            self.log_view = filter_log_lines(&self.logs, &self.log_filter);
            self.log_view_dirty = false;
        }
    }

    /// Highest valid scroll offset: enough to bring the oldest line into view.
    fn max_log_scroll(&self) -> usize {
        self.log_view.len().saturating_sub(self.log_view_h)
    }

    /// One screenful for PageUp/PageDown, keeping a line of overlap for context.
    fn page_lines(&self) -> i32 {
        self.log_view_h.saturating_sub(1).max(1) as i32
    }

    /// Scroll by `delta` lines (positive = toward older lines), clamped to the
    /// valid range so paging past either end never leaves the offset stuck.
    fn scroll_logs(&mut self, delta: i32) {
        self.log_scroll = clamp_scroll(self.log_scroll, delta, self.max_log_scroll());
    }

    /// Copy the log lines currently visible in the pane to the system clipboard
    /// as plain text (ANSI styling stripped), and report the outcome.
    fn copy_visible_logs(&mut self) {
        let window = visible_window(&self.log_view, self.log_view_h, self.log_scroll);
        let text = window
            .iter()
            .map(|l| plain_log_text(l))
            .collect::<Vec<_>>()
            .join("\n");
        let n = window.len();
        // `window` borrows `self`; it is unused past here, so `note` can borrow.
        if n == 0 {
            self.note("no logs to copy".into());
            return;
        }
        match copy_to_clipboard(&text) {
            Ok(()) => self.note(format!("copied {n} log lines to clipboard")),
            Err(e) => self.note(format!("couldn't copy logs: {e}")),
        }
    }
}

/// Copy `text` to the system clipboard via the platform clipboard utility,
/// mirroring how `open_url` shells out (no extra dependency). On Linux it tries
/// the Wayland then X11 helpers in turn.
fn copy_to_clipboard(text: &str) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        pipe_to(&mut std::process::Command::new("pbcopy"), text)
    }
    #[cfg(target_os = "windows")]
    {
        pipe_to(&mut std::process::Command::new("clip"), text)
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        let candidates: [(&str, &[&str]); 3] = [
            ("wl-copy", &[]),
            ("xclip", &["-selection", "clipboard"]),
            ("xsel", &["--clipboard", "--input"]),
        ];
        let mut last = io::Error::new(
            io::ErrorKind::NotFound,
            "no clipboard tool found (install wl-clipboard, xclip, or xsel)",
        );
        for (bin, args) in candidates {
            let mut cmd = std::process::Command::new(bin);
            cmd.args(args);
            match pipe_to(&mut cmd, text) {
                Ok(()) => return Ok(()),
                // Helper isn't installed: fall through to the next candidate.
                Err(e) if e.kind() == io::ErrorKind::NotFound => last = e,
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }
}

/// Spawn `cmd`, write `text` to its stdin, and wait for it to finish.
fn pipe_to(cmd: &mut std::process::Command, text: &str) -> io::Result<()> {
    use std::io::Write;
    let mut child = cmd
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "clipboard stdin unavailable"))?
        .write_all(text.as_bytes())?;
    child.wait()?;
    Ok(())
}

/// Apply `delta` to a scroll offset and clamp it to `[0, max]`. Clamping (rather
/// than letting the offset run past `max`) is what keeps paging responsive: an
/// overshoot at the top doesn't have to be "unwound" before the view moves.
fn clamp_scroll(current: usize, delta: i32, max: usize) -> usize {
    (current as i64 + delta as i64).clamp(0, max as i64) as usize
}

/// Render a log line with ANSI SGR color/style codes translated into ratatui
/// spans. Other terminal control sequences are dropped so process output can't
/// move the cursor or corrupt the dashboard.
fn ansi_log_line(line: &str) -> Line<'static> {
    let mut spans = Vec::new();
    let mut buf = String::new();
    let mut style = Style::default();
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => {
                flush_log_span(&mut spans, &mut buf, style);
                consume_ansi_escape(&mut chars, Some(&mut style));
            }
            '\t' => buf.push_str("    "),
            // Drop carriage returns and every other control char (C0/C1, DEL,
            // and any stray newline within a line). Emoji and printable text
            // are left untouched.
            c if c.is_control() => {}
            c => buf.push(c),
        }
    }
    flush_log_span(&mut spans, &mut buf, style);
    Line::from(spans)
}

/// Strip terminal controls from a log line for filtering/search. This mirrors
/// `ansi_log_line` but keeps only visible text.
fn plain_log_text(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{1b}' => consume_ansi_escape(&mut chars, None),
            '\t' => out.push_str("    "),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

fn flush_log_span(spans: &mut Vec<Span<'static>>, buf: &mut String, style: Style) {
    if !buf.is_empty() {
        spans.push(Span::styled(std::mem::take(buf), style));
    }
}

fn consume_ansi_escape(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    mut style: Option<&mut Style>,
) {
    match chars.peek() {
        Some('[') => {
            chars.next();
            let mut params = String::new();
            while let Some(n) = chars.next() {
                if ('\u{40}'..='\u{7e}').contains(&n) {
                    if n == 'm' {
                        if let Some(style) = style.as_deref_mut() {
                            apply_sgr(&params, style);
                        }
                    }
                    break;
                }
                params.push(n);
            }
        }
        Some(']') => {
            chars.next();
            while let Some(n) = chars.next() {
                if n == '\u{7}' {
                    break;
                }
                if n == '\u{1b}' && chars.peek() == Some(&'\\') {
                    chars.next();
                    break;
                }
            }
        }
        Some(_) => {
            chars.next();
        }
        None => {}
    }
}

fn apply_sgr(params: &str, style: &mut Style) {
    let codes: Vec<u16> = if params.is_empty() {
        vec![0]
    } else {
        params
            .split([';', ':'])
            .map(|part| part.parse::<u16>().unwrap_or(0))
            .collect()
    };
    let mut i = 0;
    while i < codes.len() {
        match codes[i] {
            0 => *style = Style::default(),
            1 => *style = style.add_modifier(Modifier::BOLD),
            2 => *style = style.add_modifier(Modifier::DIM),
            3 => *style = style.add_modifier(Modifier::ITALIC),
            4 => *style = style.add_modifier(Modifier::UNDERLINED),
            22 => *style = style.remove_modifier(Modifier::BOLD | Modifier::DIM),
            23 => *style = style.remove_modifier(Modifier::ITALIC),
            24 => *style = style.remove_modifier(Modifier::UNDERLINED),
            30..=37 | 90..=97 => style.fg = ansi_color(codes[i]),
            39 => style.fg = None,
            40..=47 | 100..=107 => style.bg = ansi_bg_color(codes[i]),
            49 => style.bg = None,
            38 | 48 => {
                if let Some((color, consumed)) = extended_ansi_color(&codes[i + 1..]) {
                    if codes[i] == 38 {
                        style.fg = Some(color);
                    } else {
                        style.bg = Some(color);
                    }
                    i += consumed;
                }
            }
            _ => {}
        }
        i += 1;
    }
}

fn extended_ansi_color(codes: &[u16]) -> Option<(Color, usize)> {
    match codes {
        [5, index, ..] => Some((Color::Indexed((*index).min(255) as u8), 2)),
        [2, r, g, b, ..] => Some((
            Color::Rgb(
                (*r).min(255) as u8,
                (*g).min(255) as u8,
                (*b).min(255) as u8,
            ),
            4,
        )),
        _ => None,
    }
}

fn ansi_bg_color(code: u16) -> Option<Color> {
    ansi_color(match code {
        40..=47 => code - 10,
        100..=107 => code - 10,
        _ => return None,
    })
}

fn ansi_color(code: u16) -> Option<Color> {
    Some(match code {
        30 => Color::Black,
        31 => Color::Red,
        32 => Color::Green,
        33 => Color::Yellow,
        34 => Color::Blue,
        35 => Color::Magenta,
        36 => Color::Cyan,
        37 => Color::Gray,
        90 => Color::DarkGray,
        91 => Color::LightRed,
        92 => Color::LightGreen,
        93 => Color::LightYellow,
        94 => Color::LightBlue,
        95 => Color::LightMagenta,
        96 => Color::LightCyan,
        97 => Color::White,
        _ => return None,
    })
}

/// Filter log lines by `pattern` (a regex; falls back to case-insensitive
/// substring if the regex doesn't compile). Empty pattern = all lines.
fn filter_log_lines(logs: &[String], pattern: &str) -> Vec<String> {
    if pattern.is_empty() {
        return logs.to_vec();
    }
    match regex::RegexBuilder::new(pattern)
        .case_insensitive(true)
        .build()
    {
        Ok(re) => logs
            .iter()
            .filter(|l| re.is_match(&plain_log_text(l)))
            .cloned()
            .collect(),
        Err(_) => {
            let needle = pattern.to_ascii_lowercase();
            logs.iter()
                .filter(|l| plain_log_text(l).to_ascii_lowercase().contains(&needle))
                .cloned()
                .collect()
        }
    }
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: &DaemonClient,
) -> io::Result<()> {
    let mut app = App {
        state: DashboardState::default(),
        rows: vec![],
        table: TableState::default().with_selected(Some(0)),
        logs: vec![],
        log_view: vec![],
        log_view_dirty: false,
        log_view_h: 0,
        log_cursor: 0,
        log_key: None,
        mode: Mode::Normal,
        filter: String::new(),
        log_filter: String::new(),
        log_scroll: 0,
        port_input: String::new(),
        status_msg: String::new(),
        status_at: None,
        start: Instant::now(),
    };
    let mut last_refresh = Instant::now() - Duration::from_secs(1);

    loop {
        if last_refresh.elapsed() >= Duration::from_millis(500) {
            if let Ok(Response::State(s)) = client.call(&Request::GetState).await {
                app.state = s;
            }
            app.rows = filtered(&app.state, &app.filter);
            let sel = app
                .table
                .selected()
                .unwrap_or(0)
                .min(app.rows.len().saturating_sub(1));
            app.table
                .select(if app.rows.is_empty() { None } else { Some(sel) });
            match app
                .selected()
                .map(|r| (r.instance_id.clone(), r.name.clone()))
            {
                Some((instance, resource)) => {
                    // Same resource as last tick: fetch only lines past our
                    // cursor and append. Otherwise refetch the tail from scratch.
                    let same = app.log_key.as_ref() == Some(&(instance.clone(), resource.clone()));
                    let since = if same { app.log_cursor } else { 0 };
                    let (lines, cursor) = fetch_logs(client, &instance, &resource, since).await;
                    if same {
                        app.append_logs(lines, cursor);
                    } else {
                        app.reset_logs(Some((instance, resource)), lines, cursor);
                    }
                }
                None if app.log_key.is_some() => app.reset_logs(None, vec![], 0),
                None => {}
            }
            last_refresh = Instant::now();
        }

        terminal.draw(|f| draw(f, &mut app))?;

        // Poll at ~10fps so the in-progress spinner animates while idle.
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                let ctrl_c =
                    key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c');
                if ctrl_c {
                    break;
                }

                match app.mode {
                    Mode::Filter => {
                        match key.code {
                            KeyCode::Enter => app.mode = Mode::Normal,
                            KeyCode::Esc => {
                                app.filter.clear();
                                app.mode = Mode::Normal;
                            }
                            KeyCode::Backspace => {
                                app.filter.pop();
                            }
                            KeyCode::Char(c) => app.filter.push(c),
                            _ => {}
                        }
                        // Re-filter and refetch logs on the next tick so the
                        // table reacts to each keystroke instead of lagging.
                        last_refresh = Instant::now() - Duration::from_secs(1);
                    }
                    // Typing a log-line filter inside the full-screen log view.
                    Mode::LogsFilter => match key.code {
                        KeyCode::Enter => app.mode = Mode::Logs,
                        KeyCode::Esc => {
                            app.log_filter.clear();
                            app.log_view_dirty = true;
                            app.mode = Mode::Logs;
                        }
                        KeyCode::Backspace => {
                            app.log_filter.pop();
                            app.log_view_dirty = true;
                        }
                        KeyCode::Char(c) => {
                            app.log_filter.push(c);
                            app.log_view_dirty = true;
                            app.log_scroll = 0;
                        }
                        _ => {}
                    },
                    Mode::PortEdit => match key.code {
                        KeyCode::Enter => match parse_port(&app.port_input) {
                            Some(port) => {
                                if let Some(r) = app.selected().cloned() {
                                    let resp = client
                                        .call(&Request::SetPort {
                                            instance: r.instance_id.clone(),
                                            resource: r.name.clone(),
                                            port,
                                        })
                                        .await;
                                    let msg = match resp {
                                        Ok(Response::Ok) => {
                                            format!("changing {} to port {port}", r.name)
                                        }
                                        Ok(Response::Error(e)) => {
                                            format!("couldn't change port: {e}")
                                        }
                                        Ok(other) => {
                                            format!("unexpected daemon response: {other:?}")
                                        }
                                        Err(e) => format!("couldn't change port: {e}"),
                                    };
                                    app.note(msg);
                                }
                                app.mode = Mode::Normal;
                            }
                            None => {
                                app.note("port must be 1-65535".to_string());
                            }
                        },
                        KeyCode::Esc => {
                            app.port_input.clear();
                            app.mode = Mode::Normal;
                        }
                        KeyCode::Backspace => {
                            app.port_input.pop();
                        }
                        KeyCode::Char(c) if c.is_ascii_digit() => {
                            app.port_input.push(c);
                        }
                        _ => {}
                    },
                    // Full-screen logs for the selected resource.
                    Mode::Logs => match key.code {
                        KeyCode::Char('q') | KeyCode::Esc | KeyCode::Char('l') => {
                            app.mode = Mode::Normal;
                        }
                        KeyCode::Char('/') => app.mode = Mode::LogsFilter,
                        KeyCode::PageUp => app.scroll_logs(app.page_lines()),
                        KeyCode::PageDown => app.scroll_logs(-app.page_lines()),
                        KeyCode::Char('k') | KeyCode::Up => app.scroll_logs(1),
                        KeyCode::Char('j') | KeyCode::Down => app.scroll_logs(-1),
                        KeyCode::Char('g') | KeyCode::Home => app.log_scroll = app.max_log_scroll(),
                        KeyCode::Char('G') | KeyCode::End => app.log_scroll = 0,
                        KeyCode::Char('o') => {
                            if let Some(r) = app.selected() {
                                if !r.url.is_empty() {
                                    let _ = open_url(&r.url);
                                }
                            }
                        }
                        KeyCode::Char('y') => app.copy_visible_logs(),
                        _ => {}
                    },
                    Mode::Normal | Mode::Detail => match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Esc if app.mode == Mode::Detail => app.mode = Mode::Normal,
                        KeyCode::Esc => break,
                        KeyCode::Char('j') | KeyCode::Down => {
                            move_sel(&mut app, 1);
                            last_refresh = Instant::now() - Duration::from_secs(1);
                        }
                        KeyCode::Char('k') | KeyCode::Up => {
                            move_sel(&mut app, -1);
                            last_refresh = Instant::now() - Duration::from_secs(1);
                        }
                        KeyCode::Enter => {
                            app.mode = if app.mode == Mode::Detail {
                                Mode::Normal
                            } else {
                                Mode::Detail
                            };
                        }
                        KeyCode::Char('l') => {
                            app.log_scroll = 0;
                            app.mode = Mode::Logs;
                        }
                        KeyCode::Char('/') => {
                            app.mode = Mode::Filter;
                        }
                        KeyCode::Char('r') => {
                            last_refresh = Instant::now() - Duration::from_secs(1);
                        }
                        KeyCode::PageUp => app.scroll_logs(app.page_lines()),
                        KeyCode::PageDown => app.scroll_logs(-app.page_lines()),
                        KeyCode::Char('G') | KeyCode::End => app.log_scroll = 0,
                        KeyCode::Char('t') => {
                            if let Some(r) = app.selected() {
                                if r.paused {
                                    let name = r.name.clone();
                                    app.note(format!("{name} is paused"));
                                    continue;
                                }
                                let _ = client
                                    .call(&Request::Trigger {
                                        instance: r.instance_id.clone(),
                                        resource: r.name.clone(),
                                    })
                                    .await;
                                let name = r.name.clone();
                                app.note(format!("triggered {name}"));
                            }
                        }
                        KeyCode::Char(' ') => {
                            if let Some(r) = app.selected() {
                                let paused = !r.paused;
                                let _ = client
                                    .call(&Request::SetPaused {
                                        instance: r.instance_id.clone(),
                                        resource: r.name.clone(),
                                        paused,
                                    })
                                    .await;
                                let name = r.name.clone();
                                app.note(format!(
                                    "{} {name}",
                                    if paused { "paused" } else { "resumed" }
                                ));
                            }
                        }
                        KeyCode::Char('R') => {
                            if let Some(r) = app.selected() {
                                let _ = client
                                    .call(&Request::Restart {
                                        instance: r.instance_id.clone(),
                                        resource: r.name.clone(),
                                    })
                                    .await;
                                let name = r.name.clone();
                                app.note(format!("restarting {name}"));
                            }
                        }
                        KeyCode::Char('p') => {
                            if let Some(r) = app.selected() {
                                app.port_input =
                                    r.route_port.map(|p| p.to_string()).unwrap_or_default();
                                app.mode = Mode::PortEdit;
                            }
                        }
                        KeyCode::Char('o') => match app.selected() {
                            Some(r) if !r.url.is_empty() => {
                                let url = r.url.clone();
                                let msg = match open_url(&url) {
                                    Ok(()) => format!("opening {url}"),
                                    Err(e) => format!("couldn't open {url}: {e}"),
                                };
                                app.note(msg);
                            }
                            Some(r) => {
                                let name = r.name.clone();
                                app.note(format!("{name} has no URL"));
                            }
                            None => {}
                        },
                        KeyCode::Char('y') => app.copy_visible_logs(),
                        _ => {}
                    },
                }
            }
        }
    }
    Ok(())
}

/// Open a URL in the default browser (platform-specific opener).
fn open_url(url: &str) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    let mut cmd = std::process::Command::new("open");
    #[cfg(target_os = "windows")]
    let mut cmd = {
        let mut c = std::process::Command::new("cmd");
        c.args(["/C", "start", ""]);
        c
    };
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    let mut cmd = std::process::Command::new("xdg-open");

    cmd.arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
}

fn move_sel(app: &mut App, delta: i32) {
    if app.rows.is_empty() {
        return;
    }
    let cur = app.table.selected().unwrap_or(0) as i32;
    let next = (cur + delta).rem_euclid(app.rows.len() as i32);
    app.table.select(Some(next as usize));
    app.log_scroll = 0;
}

fn filtered(state: &DashboardState, filter: &str) -> Vec<RowItem> {
    let f = filter.to_ascii_lowercase();
    let mut rows = vec![];
    for inst in &state.instances {
        for r in &inst.resources {
            let item = RowItem {
                instance_id: inst.id.clone(),
                instance_name: inst.name.clone(),
                name: r.name.clone(),
                kind: r.kind.clone(),
                paused: r.paused,
                update: r.update_status.clone(),
                runtime: r.runtime_status.clone(),
                pod: r.pod.clone().unwrap_or_default(),
                url: r.url.clone().unwrap_or_default(),
                route_port: route_port_for_url(state, &inst.id, r.url.as_deref()),
                restart_count: r.restart_count,
                last_start: r.last_start.clone(),
            };
            if f.is_empty()
                || item.name.to_ascii_lowercase().contains(&f)
                || item.instance_name.to_ascii_lowercase().contains(&f)
            {
                rows.push(item);
            }
        }
    }
    rows
}

fn route_port_for_url(state: &DashboardState, instance: &str, url: Option<&str>) -> Option<u16> {
    let host = hostname_from_url(url?)?;
    state
        .routes
        .iter()
        .find(|r| r.instance == instance && r.hostname == host)
        .map(|r| r.port)
}

fn hostname_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    if authority.is_empty() {
        return None;
    }
    Some(authority.split(':').next().unwrap_or(authority).to_string())
}

fn format_start_time(timestamp: Option<&str>, full: bool) -> String {
    let Some(timestamp) = timestamp else {
        return String::new();
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(timestamp) else {
        return timestamp.to_string();
    };
    let local = parsed.with_timezone(&Local);
    if full {
        local.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        local.format("%H:%M:%S").to_string()
    }
}

fn parse_port(input: &str) -> Option<u16> {
    let port = input.parse::<u16>().ok()?;
    (port != 0).then_some(port)
}

/// Fetch log lines newer than `since` for a resource, returning the lines and
/// the cursor for the next call. On error the cursor is left unchanged so the
/// next poll retries from the same point rather than refetching the whole tail.
async fn fetch_logs(
    client: &DaemonClient,
    instance: &str,
    resource: &str,
    since: u64,
) -> (Vec<String>, u64) {
    match client
        .call(&Request::GetLogs {
            instance: instance.to_string(),
            resource: resource.to_string(),
            since,
        })
        .await
    {
        Ok(Response::Logs { lines, cursor }) => (lines, cursor),
        _ => (vec![], since),
    }
}

/// The slice of `logs` currently on screen in a pane of inner height `h` at the
/// given scroll offset (0 = tail). Shared by rendering and clipboard copy so
/// "the current window" means exactly what the user sees.
fn visible_window(logs: &[String], h: usize, scroll: usize) -> &[String] {
    if logs.is_empty() || h == 0 {
        return &[];
    }
    let max_scroll = logs.len().saturating_sub(h);
    let scroll = scroll.min(max_scroll);
    let end = logs.len() - scroll;
    let start = end.saturating_sub(h);
    &logs[start..end]
}

/// Render the on-screen window of `logs`, honoring scroll, with ANSI styling.
fn log_lines(logs: &[String], h: usize, scroll: usize) -> Vec<Line<'static>> {
    visible_window(logs, h, scroll)
        .iter()
        .map(|l| ansi_log_line(l))
        .collect()
}

fn draw(f: &mut Frame, app: &mut App) {
    if app.mode == Mode::Detail {
        draw_detail(f, app);
        return;
    }
    if app.mode == Mode::Logs || app.mode == Mode::LogsFilter {
        draw_logs_fullscreen(f, app);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(6),
            Constraint::Length(10),
            Constraint::Length(1),
        ])
        .split(f.area());

    f.render_widget(title_bar(app), chunks[0]);

    let frame = app.spinner_frame();
    let header = Row::new(
        [
            "INSTANCE",
            "RESOURCE",
            "STATE",
            "TYPE",
            "UPDATE",
            "RUNTIME",
            "RESTARTS",
            "LAST START",
            "PORT",
            "POD",
            "URL",
        ]
        .iter()
        .map(|h| {
            Cell::from(*h).style(
                Style::default()
                    .fg(theme::ACCENT)
                    .add_modifier(Modifier::BOLD),
            )
        }),
    )
    .height(1)
    .bottom_margin(1);
    let table_rows: Vec<Row> = app
        .rows
        .iter()
        .map(|r| {
            Row::new(vec![
                Cell::from(Span::styled(
                    r.instance_name.clone(),
                    Style::default().fg(theme::MUTED),
                )),
                Cell::from(Span::styled(
                    r.name.clone(),
                    Style::default().add_modifier(Modifier::BOLD),
                )),
                Cell::from(state_cell(r.paused)),
                Cell::from(r.kind.clone()),
                Cell::from(status_cell(&r.update, frame, StatusKind::Update)),
                Cell::from(status_cell(&r.runtime, frame, StatusKind::Runtime)),
                Cell::from(
                    r.restart_count
                        .map(|count| count.to_string())
                        .unwrap_or_default(),
                ),
                Cell::from(format_start_time(r.last_start.as_deref(), false)),
                Cell::from(r.route_port.map(|p| p.to_string()).unwrap_or_default()),
                Cell::from(r.pod.clone()),
                Cell::from(Span::styled(r.url.clone(), Style::default().fg(theme::URL))),
            ])
        })
        .collect();
    let widths = [
        Constraint::Length(14),
        Constraint::Length(18),
        Constraint::Length(8),
        Constraint::Length(6),
        Constraint::Length(13),
        Constraint::Length(13),
        Constraint::Length(8),
        Constraint::Length(10),
        Constraint::Length(7),
        Constraint::Length(16),
        Constraint::Min(18),
    ];
    let table = Table::new(table_rows, widths)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme::MUTED))
                .title(Span::styled(
                    " Resources ",
                    Style::default()
                        .fg(theme::ACCENT)
                        .add_modifier(Modifier::BOLD),
                )),
        )
        .highlight_style(
            Style::default()
                .bg(theme::SEL_BG)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(Span::styled("▌ ", Style::default().fg(theme::ACCENT)));
    f.render_stateful_widget(table, chunks[1], &mut app.table);

    let sel_name = app
        .selected()
        .map(|r| format!("{} / {}", r.instance_name, r.name))
        .unwrap_or_else(|| "—".into());
    let h = chunks[2].height.saturating_sub(2) as usize;
    app.log_view_h = h;
    app.ensure_log_view();
    let follow = if app.log_scroll == 0 {
        ""
    } else {
        " · scrolled"
    };
    let filt = if app.log_filter.is_empty() {
        String::new()
    } else {
        format!(" /{}", app.log_filter)
    };
    f.render_widget(
        Paragraph::new(log_body(&app.log_view, h, app.log_scroll)).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme::MUTED))
                .title(Span::styled(
                    format!(" Logs · {sel_name}{filt}{follow} "),
                    Style::default().fg(theme::ACCENT),
                )),
        ),
        chunks[2],
    );

    let footer: Line = if app.mode == Mode::Filter {
        prompt_line("filter", &app.filter, "Enter apply · Esc clear")
    } else if app.mode == Mode::PortEdit {
        prompt_line("port", &app.port_input, "Enter apply · Esc cancel")
    } else if app.rows.is_empty() {
        Line::from(vec![
            Span::styled(
                " No resources. ",
                Style::default()
                    .fg(theme::WARN)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "Run `starling up` in a project.",
                Style::default().fg(theme::MUTED),
            ),
        ])
    } else if let Some(msg) = app.active_status() {
        status_footer(msg)
    } else {
        key_hints(&[
            ("j/k", "move"),
            ("↵", "detail"),
            ("l", "logs"),
            ("o", "open"),
            ("y", "copy logs"),
            ("t", "trigger"),
            ("Space", "pause"),
            ("R", "restart"),
            ("p", "port"),
            ("/", "filter"),
            ("q", "quit"),
        ])
    };
    f.render_widget(Paragraph::new(footer), chunks[3]);
}

/// The top status bar: an accent badge plus muted instance/proxy details.
fn title_bar(app: &App) -> Paragraph<'static> {
    let line = Line::from(vec![
        Span::styled(
            " ✦ Starling ",
            Style::default()
                .fg(Color::Rgb(20, 22, 34))
                .bg(theme::ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(
                "  {} instances · {} resources · proxy :{} · .{} ",
                app.state.instances.len(),
                app.rows.len(),
                app.state.proxy_port,
                app.state.tld,
            ),
            Style::default().fg(theme::MUTED).bg(theme::HEADER_BG),
        ),
    ]);
    Paragraph::new(line).style(Style::default().bg(theme::HEADER_BG))
}

/// A footer input prompt with a blinking-style cursor block.
fn prompt_line(label: &str, value: &str, hint: &str) -> Line<'static> {
    Line::from(vec![
        Span::raw(" "),
        Span::styled(
            format!("{label} "),
            Style::default()
                .fg(theme::ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{value}\u{2588}"),
            Style::default().fg(Color::White),
        ),
        Span::styled(format!("   {hint} "), Style::default().fg(theme::MUTED)),
    ])
}

/// Footer line showing a transient status message (e.g. a copy confirmation).
fn status_footer(msg: &str) -> Line<'static> {
    Line::from(vec![
        Span::raw(" "),
        Span::styled("● ", Style::default().fg(theme::ACCENT)),
        Span::styled(msg.to_string(), Style::default().fg(Color::White)),
    ])
}

/// Log lines for a pane, or a muted placeholder when there is no output yet.
fn log_body(logs: &[String], h: usize, scroll: usize) -> Vec<Line<'static>> {
    if logs.is_empty() {
        return vec![Line::from(Span::styled(
            "  — no log output yet —",
            Style::default()
                .fg(theme::MUTED)
                .add_modifier(Modifier::ITALIC),
        ))];
    }
    log_lines(logs, h, scroll)
}

fn draw_detail(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(13),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(f.area());

    let r = app.selected().cloned().unwrap_or(RowItem {
        instance_id: String::new(),
        instance_name: "—".into(),
        name: "—".into(),
        kind: String::new(),
        paused: false,
        update: String::new(),
        runtime: String::new(),
        pod: String::new(),
        url: String::new(),
        route_port: None,
        restart_count: None,
        last_start: None,
    });

    let frame = app.spinner_frame();
    let banner = Line::from(vec![
        Span::styled(
            " ✦ Detail ",
            Style::default()
                .fg(Color::Rgb(20, 22, 34))
                .bg(theme::ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  {} / {} ", r.instance_name, r.name),
            Style::default().fg(theme::MUTED).bg(theme::HEADER_BG),
        ),
    ]);
    f.render_widget(
        Paragraph::new(banner).style(Style::default().bg(theme::HEADER_BG)),
        chunks[0],
    );

    let field = |k: &'static str| Span::styled(k, Style::default().fg(theme::MUTED));
    let info = vec![
        Line::from(vec![
            field("instance  "),
            Span::raw(r.instance_name.clone()),
        ]),
        Line::from(vec![
            field("resource  "),
            Span::styled(r.name.clone(), bold()),
        ]),
        Line::from({
            let mut v = vec![field("state     ")];
            v.extend(state_cell(r.paused).spans);
            v
        }),
        Line::from(vec![field("type      "), Span::raw(r.kind.clone())]),
        Line::from({
            let mut v = vec![field("update    ")];
            v.extend(status_cell(&r.update, frame, StatusKind::Update).spans);
            v
        }),
        Line::from({
            let mut v = vec![field("runtime   ")];
            v.extend(status_cell(&r.runtime, frame, StatusKind::Runtime).spans);
            v
        }),
        Line::from(vec![
            field("restarts "),
            Span::raw(
                r.restart_count
                    .map(|count| count.to_string())
                    .unwrap_or_default(),
            ),
        ]),
        Line::from(vec![
            field("started  "),
            Span::raw(format_start_time(r.last_start.as_deref(), true)),
        ]),
        Line::from(vec![
            field("port      "),
            Span::raw(r.route_port.map(|p| p.to_string()).unwrap_or_default()),
        ]),
        Line::from(vec![field("pod       "), Span::raw(r.pod.clone())]),
        Line::from(vec![
            field("url       "),
            Span::styled(
                r.url.clone(),
                Style::default()
                    .fg(theme::URL)
                    .add_modifier(Modifier::UNDERLINED),
            ),
        ]),
    ];
    f.render_widget(
        Paragraph::new(info).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme::MUTED))
                .title(Span::styled(
                    " Status ",
                    Style::default()
                        .fg(theme::ACCENT)
                        .add_modifier(Modifier::BOLD),
                )),
        ),
        chunks[1],
    );

    let h = chunks[2].height.saturating_sub(2) as usize;
    app.log_view_h = h;
    app.ensure_log_view();
    f.render_widget(
        Paragraph::new(log_body(&app.log_view, h, app.log_scroll)).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme::MUTED))
                .title(Span::styled(" Logs ", Style::default().fg(theme::ACCENT))),
        ),
        chunks[2],
    );

    let detail_footer = match app.active_status() {
        Some(msg) => status_footer(msg),
        None => key_hints(&[
            ("Esc/↵", "back"),
            ("l", "full logs"),
            ("o", "open"),
            ("y", "copy logs"),
            ("t", "trigger"),
            ("Space", "pause"),
            ("R", "restart"),
            ("p", "port"),
            ("PgUp/Dn", "scroll"),
            ("q", "quit"),
        ]),
    };
    f.render_widget(Paragraph::new(detail_footer), chunks[3]);
}

fn bold() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

#[cfg(test)]
mod tests {
    use super::{
        ansi_log_line, clamp_scroll, filter_log_lines, hostname_from_url, parse_port,
        plain_log_text, pretty_status, route_port_for_url, visible_window, StatusKind,
    };
    use crate::daemon::protocol::{DashboardState, RouteInfo};
    use ratatui::style::{Color, Modifier};

    #[test]
    fn plain_log_text_strips_controls_but_keeps_visible_text() {
        // ANSI SGR color codes are removed for searching, visible text kept.
        assert_eq!(plain_log_text("\u{1b}[32mready\u{1b}[0m"), "ready");
        // Cursor-move CSI sequences are removed.
        assert_eq!(plain_log_text("a\u{1b}[2Kb"), "ab");
        // OSC sequences (e.g. window title) terminated by BEL are removed.
        assert_eq!(plain_log_text("\u{1b}]0;title\u{7}done"), "done");
        // Carriage returns and other control bytes are dropped; tabs expand.
        assert_eq!(plain_log_text("a\rb\tc\u{0}"), "ab    c");
        // Emoji and other printable Unicode survive intact.
        assert_eq!(
            plain_log_text("\u{1b}[33m\u{2728} built \u{1f680}"),
            "\u{2728} built \u{1f680}"
        );
        // A lone trailing ESC doesn't panic and is dropped.
        assert_eq!(plain_log_text("hi\u{1b}"), "hi");
    }

    #[test]
    fn ansi_log_line_preserves_sgr_colors_as_spans() {
        let line = ansi_log_line("\u{1b}[32mready\u{1b}[0m plain \u{1b}[1;31merr");

        assert_eq!(line.spans[0].content.as_ref(), "ready");
        assert_eq!(line.spans[0].style.fg, Some(Color::Green));
        assert_eq!(line.spans[1].content.as_ref(), " plain ");
        assert_eq!(line.spans[1].style.fg, None);
        assert_eq!(line.spans[2].content.as_ref(), "err");
        assert_eq!(line.spans[2].style.fg, Some(Color::Red));
        assert!(line.spans[2].style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn log_filter_regex_substring_and_empty() {
        let logs = vec![
            "GET /healthz 200".to_string(),
            "\u{1b}[1mERROR\u{1b}[0m: connection refused".to_string(),
            "\u{1b}[31mGET /users 500\u{1b}[0m".to_string(),
        ];
        // empty => all lines
        assert_eq!(filter_log_lines(&logs, "").len(), 3);
        // case-insensitive substring / regex
        assert_eq!(
            plain_log_text(&filter_log_lines(&logs, "error")[0]),
            "ERROR: connection refused"
        );
        // regex: lines with a 5xx status
        assert_eq!(
            plain_log_text(&filter_log_lines(&logs, r"\b5\d\d\b")[0]),
            "GET /users 500"
        );
        // invalid regex falls back to substring (no panic)
        assert_eq!(filter_log_lines(&logs, "GET [").len(), 0);
    }

    #[test]
    fn visible_window_is_the_on_screen_slice() {
        let logs: Vec<String> = (0..10).map(|i| i.to_string()).collect();
        // Following the tail: the last `h` lines.
        assert_eq!(visible_window(&logs, 3, 0), &["7", "8", "9"]);
        // Scrolled up two lines from the tail.
        assert_eq!(visible_window(&logs, 3, 2), &["5", "6", "7"]);
        // Scroll past the top is clamped to the oldest lines.
        assert_eq!(visible_window(&logs, 3, 999), &["0", "1", "2"]);
        // Fewer lines than the pane height: all of them.
        assert_eq!(visible_window(&logs[..2], 5, 0), &["0", "1"]);
        // Degenerate cases yield nothing (and don't panic).
        assert!(visible_window(&logs, 0, 0).is_empty());
        assert!(visible_window(&[], 3, 0).is_empty());
    }

    #[test]
    fn scroll_stays_clamped_and_never_sticks_past_the_ends() {
        // A page up from the tail moves by the delta.
        assert_eq!(clamp_scroll(0, 9, 100), 9);
        // Paging past the oldest line pins to max instead of overshooting, so
        // the next page down immediately moves the view (no stuck counter).
        assert_eq!(clamp_scroll(95, 50, 100), 100);
        assert_eq!(clamp_scroll(100, -9, 100), 91);
        // Paging below the tail pins to 0 (follow mode).
        assert_eq!(clamp_scroll(5, -9, 100), 0);
        // When everything fits (max 0), scrolling is a no-op.
        assert_eq!(clamp_scroll(0, 9, 0), 0);
    }

    #[test]
    fn in_progress_reads_as_build_or_restart_by_column() {
        // The same raw status reads differently per column: a build vs. a
        // serve_cmd coming back up after a restart.
        assert_eq!(pretty_status("in_progress", StatusKind::Update), "building");
        assert_eq!(
            pretty_status("in_progress", StatusKind::Runtime),
            "restarting"
        );
        // Other values are column-independent.
        assert_eq!(pretty_status("ok", StatusKind::Runtime), "ok");
        assert_eq!(pretty_status("none", StatusKind::Runtime), "—");
        assert_eq!(pretty_status("not_applicable", StatusKind::Update), "—");
    }

    #[test]
    fn parses_valid_backend_ports() {
        assert_eq!(parse_port("1"), Some(1));
        assert_eq!(parse_port("65535"), Some(65535));
        assert_eq!(parse_port("0"), None);
        assert_eq!(parse_port("65536"), None);
        assert_eq!(parse_port("abc"), None);
    }

    #[test]
    fn extracts_hostname_from_named_url() {
        assert_eq!(
            hostname_from_url("http://web-app.localhost:1360/path"),
            Some("web-app.localhost".to_string())
        );
        assert_eq!(
            hostname_from_url("https://api.localhost"),
            Some("api.localhost".to_string())
        );
        assert_eq!(hostname_from_url(""), None);
    }

    #[test]
    fn finds_backend_port_for_selected_route() {
        let state = DashboardState {
            routes: vec![RouteInfo {
                hostname: "web.localhost".to_string(),
                port: 8080,
                instance: "inst".to_string(),
            }],
            ..Default::default()
        };

        assert_eq!(
            route_port_for_url(&state, "inst", Some("http://web.localhost:1360")),
            Some(8080)
        );
        assert_eq!(
            route_port_for_url(&state, "other", Some("http://web.localhost:1360")),
            None
        );
    }
}

/// Full-screen log view for the selected resource, with a regex filter.
fn draw_logs_fullscreen(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // title
            Constraint::Min(3),    // logs
            Constraint::Length(1), // footer
        ])
        .split(f.area());

    let h = chunks[1].height.saturating_sub(2) as usize;
    app.log_view_h = h;
    app.ensure_log_view();

    let sel = app
        .selected()
        .map(|r| format!("{} / {}", r.instance_name, r.name))
        .unwrap_or_else(|| "—".into());
    let matched = if app.log_filter.is_empty() {
        format!("{} lines", app.log_view.len())
    } else {
        format!("{} matching /{}", app.log_view.len(), app.log_filter)
    };
    let banner = Line::from(vec![
        Span::styled(
            " ✦ Logs ",
            Style::default()
                .fg(Color::Rgb(20, 22, 34))
                .bg(theme::ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  {sel} · {matched} "),
            Style::default().fg(theme::MUTED).bg(theme::HEADER_BG),
        ),
    ]);
    f.render_widget(
        Paragraph::new(banner).style(Style::default().bg(theme::HEADER_BG)),
        chunks[0],
    );

    f.render_widget(
        Paragraph::new(log_body(&app.log_view, h, app.log_scroll)).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme::MUTED)),
        ),
        chunks[1],
    );

    let footer: Line = if app.mode == Mode::LogsFilter {
        prompt_line("filter", &app.log_filter, "Enter apply · Esc clear")
    } else if let Some(msg) = app.active_status() {
        status_footer(msg)
    } else {
        key_hints(&[
            ("/", "filter"),
            ("PgUp/Dn", "page"),
            ("g/G", "top/tail"),
            ("y", "copy"),
            ("o", "open"),
            ("l/Esc", "back"),
            ("q", "quit"),
        ])
    };
    f.render_widget(Paragraph::new(footer), chunks[2]);
}
