//! k9s-style terminal dashboard for Starling.
//!
//! Connects to the daemon, polls the aggregated state across all `starling up`
//! instances, and renders a navigable resource table with a live log pane.
//!
//! Keys:
//!   j/k, ↑/↓   move selection
//!   /          filter (Enter apply, Esc clear)
//!   Enter      detail view for the selected resource (Esc to exit)
//!   t          trigger the selected resource
//!   R          restart the selected resource's serve_cmd
//!   p          change the selected resource's preferred backend port
//!   PgUp/PgDn  scroll logs (G / End jumps back to follow)
//!   r          refresh now
//!   q          quit

use std::io;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use ratatui::{Frame, Terminal};

use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::{DashboardState, Request, Response};

/// One displayed row: a resource belonging to an instance.
#[derive(Clone)]
struct RowItem {
    instance_id: String,
    instance_name: String,
    name: String,
    kind: String,
    update: String,
    runtime: String,
    pod: String,
    url: String,
    route_port: Option<u16>,
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
    mode: Mode,
    filter: String,
    /// Regex/substring filter applied to log lines (full-screen + log pane).
    log_filter: String,
    /// Lines scrolled up from the bottom; 0 = follow tail.
    log_scroll: usize,
    /// Port being edited in the footer.
    port_input: String,
    status_msg: String,
}

impl App {
    fn selected(&self) -> Option<&RowItem> {
        self.table.selected().and_then(|i| self.rows.get(i))
    }

    /// Log lines for the selected resource, filtered by `log_filter`.
    fn filtered_logs(&self) -> Vec<String> {
        filter_log_lines(&self.logs, &self.log_filter)
    }
}

/// Filter log lines by `pattern` (a regex; falls back to case-insensitive
/// substring if the regex doesn't compile). Empty pattern = all lines.
fn filter_log_lines(logs: &[String], pattern: &str) -> Vec<String> {
    if pattern.is_empty() {
        return logs.to_vec();
    }
    match regex::RegexBuilder::new(pattern).case_insensitive(true).build() {
        Ok(re) => logs.iter().filter(|l| re.is_match(l)).cloned().collect(),
        Err(_) => {
            let needle = pattern.to_ascii_lowercase();
            logs.iter()
                .filter(|l| l.to_ascii_lowercase().contains(&needle))
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
        mode: Mode::Normal,
        filter: String::new(),
        log_filter: String::new(),
        log_scroll: 0,
        port_input: String::new(),
        status_msg: String::new(),
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
            app.table.select(if app.rows.is_empty() { None } else { Some(sel) });
            app.logs = match app.selected() {
                Some(r) => fetch_logs(client, &r.instance_id, &r.name).await,
                None => vec![],
            };
            last_refresh = Instant::now();
        }

        terminal.draw(|f| draw(f, &mut app))?;

        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                let ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL)
                    && key.code == KeyCode::Char('c');
                if ctrl_c {
                    break;
                }

                match app.mode {
                    Mode::Filter => match key.code {
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
                    },
                    // Typing a log-line filter inside the full-screen log view.
                    Mode::LogsFilter => match key.code {
                        KeyCode::Enter => app.mode = Mode::Logs,
                        KeyCode::Esc => {
                            app.log_filter.clear();
                            app.mode = Mode::Logs;
                        }
                        KeyCode::Backspace => {
                            app.log_filter.pop();
                        }
                        KeyCode::Char(c) => {
                            app.log_filter.push(c);
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
                                    app.status_msg = match resp {
                                        Ok(Response::Ok) => {
                                            format!("changing {} to port {port}", r.name)
                                        }
                                        Ok(Response::Error(e)) => {
                                            format!("couldn't change port: {e}")
                                        }
                                        Ok(other) => format!("unexpected daemon response: {other:?}"),
                                        Err(e) => format!("couldn't change port: {e}"),
                                    };
                                }
                                app.mode = Mode::Normal;
                            }
                            None => {
                                app.status_msg = "port must be 1-65535".to_string();
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
                        KeyCode::PageUp | KeyCode::Char('k') | KeyCode::Up => app.log_scroll += 5,
                        KeyCode::PageDown | KeyCode::Char('j') | KeyCode::Down => {
                            app.log_scroll = app.log_scroll.saturating_sub(5)
                        }
                        KeyCode::Char('G') | KeyCode::End => app.log_scroll = 0,
                        KeyCode::Char('o') => {
                            if let Some(r) = app.selected() {
                                if !r.url.is_empty() {
                                    let _ = open_url(&r.url);
                                }
                            }
                        }
                        _ => {}
                    },
                    Mode::Normal | Mode::Detail => match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Esc if app.mode == Mode::Detail => app.mode = Mode::Normal,
                        KeyCode::Esc => break,
                        KeyCode::Char('j') | KeyCode::Down => {
                            move_sel(&mut app, 1);
                        }
                        KeyCode::Char('k') | KeyCode::Up => {
                            move_sel(&mut app, -1);
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
                        KeyCode::PageUp => app.log_scroll += 5,
                        KeyCode::PageDown => app.log_scroll = app.log_scroll.saturating_sub(5),
                        KeyCode::Char('G') | KeyCode::End => app.log_scroll = 0,
                        KeyCode::Char('t') => {
                            if let Some(r) = app.selected() {
                                let _ = client
                                    .call(&Request::Trigger {
                                        instance: r.instance_id.clone(),
                                        resource: r.name.clone(),
                                    })
                                    .await;
                                app.status_msg = format!("triggered {}", r.name);
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
                                app.status_msg = format!("restarting {}", r.name);
                            }
                        }
                        KeyCode::Char('p') => {
                            if let Some(r) = app.selected() {
                                app.port_input = r
                                    .route_port
                                    .map(|p| p.to_string())
                                    .unwrap_or_default();
                                app.mode = Mode::PortEdit;
                            }
                        }
                        KeyCode::Char('o') => {
                            match app.selected() {
                                Some(r) if !r.url.is_empty() => {
                                    let url = r.url.clone();
                                    app.status_msg = match open_url(&url) {
                                        Ok(()) => format!("opening {url}"),
                                        Err(e) => format!("couldn't open {url}: {e}"),
                                    };
                                }
                                Some(r) => app.status_msg = format!("{} has no URL", r.name),
                                None => {}
                            }
                        }
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
                update: r.update_status.clone(),
                runtime: r.runtime_status.clone(),
                pod: r.pod.clone().unwrap_or_default(),
                url: r.url.clone().unwrap_or_default(),
                route_port: route_port_for_url(state, &inst.id, r.url.as_deref()),
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

fn parse_port(input: &str) -> Option<u16> {
    let port = input.parse::<u16>().ok()?;
    (port != 0).then_some(port)
}

async fn fetch_logs(client: &DaemonClient, instance: &str, resource: &str) -> Vec<String> {
    match client
        .call(&Request::GetLogs {
            instance: instance.to_string(),
            resource: resource.to_string(),
        })
        .await
    {
        Ok(Response::Logs(l)) => l,
        _ => vec![],
    }
}

fn status_style(s: &str) -> Style {
    match s {
        "ok" => Style::default().fg(Color::Green),
        "error" => Style::default().fg(Color::Red),
        "in_progress" => Style::default().fg(Color::Yellow),
        "pending" => Style::default().fg(Color::Cyan),
        "not_applicable" | "none" | "" => Style::default().fg(Color::DarkGray),
        _ => Style::default(),
    }
}

/// Render the tail of `logs` into a pane of inner height `h`, honoring scroll.
fn log_lines(logs: &[String], h: usize, scroll: usize) -> Vec<Line<'static>> {
    if logs.is_empty() {
        return vec![];
    }
    let max_scroll = logs.len().saturating_sub(h);
    let scroll = scroll.min(max_scroll);
    let end = logs.len() - scroll;
    let start = end.saturating_sub(h);
    logs[start..end].iter().map(|l| Line::raw(l.clone())).collect()
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

    let title = format!(
        " Starling  ·  {} instance(s)  ·  {} resource(s)  ·  shared proxy :{}  ·  .{} ",
        app.state.instances.len(),
        app.rows.len(),
        app.state.proxy_port,
        app.state.tld,
    );
    f.render_widget(
        Paragraph::new(title).style(
            Style::default()
                .fg(Color::White)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        chunks[0],
    );

    let header = Row::new(
        ["INSTANCE", "RESOURCE", "TYPE", "UPDATE", "RUNTIME", "PORT", "POD", "URL"]
            .iter()
            .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD))),
    );
    let table_rows: Vec<Row> = app
        .rows
        .iter()
        .map(|r| {
            Row::new(vec![
                Cell::from(r.instance_name.clone()),
                Cell::from(r.name.clone()),
                Cell::from(r.kind.clone()),
                Cell::from(r.update.clone()).style(status_style(&r.update)),
                Cell::from(r.runtime.clone()).style(status_style(&r.runtime)),
                Cell::from(r.route_port.map(|p| p.to_string()).unwrap_or_default()),
                Cell::from(r.pod.clone()),
                Cell::from(r.url.clone()).style(Style::default().fg(Color::Blue)),
            ])
        })
        .collect();
    let widths = [
        Constraint::Length(14),
        Constraint::Length(18),
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(14),
        Constraint::Length(7),
        Constraint::Length(20),
        Constraint::Min(20),
    ];
    let table = Table::new(table_rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Resources "))
        .highlight_style(Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD))
        .highlight_symbol("▌ ");
    f.render_stateful_widget(table, chunks[1], &mut app.table);

    let sel_name = app
        .selected()
        .map(|r| format!("{} / {}", r.instance_name, r.name))
        .unwrap_or_else(|| "—".into());
    let h = chunks[2].height.saturating_sub(2) as usize;
    let follow = if app.log_scroll == 0 { "" } else { " (scrolled)" };
    let logs = app.filtered_logs();
    let filt = if app.log_filter.is_empty() {
        String::new()
    } else {
        format!(" /{}", app.log_filter)
    };
    f.render_widget(
        Paragraph::new(log_lines(&logs, h, app.log_scroll)).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Logs · {sel_name}{filt}{follow} ")),
        ),
        chunks[2],
    );

    let footer = if app.mode == Mode::Filter {
        format!(" /{}\u{2588}   (Enter apply · Esc clear) ", app.filter)
    } else if app.mode == Mode::PortEdit {
        format!(
            " port {}{}   (Enter apply · Esc cancel) ",
            app.port_input, "\u{2588}"
        )
    } else if app.rows.is_empty() {
        " No resources. Run `starling up` in a project.   [/] filter  [q] quit ".to_string()
    } else if !app.status_msg.is_empty() {
        format!(
            " {}   ·   [↵] detail [o] open [l] logs [t] trigger [R] restart [p] port [/] filter [q] quit ",
            app.status_msg
        )
    } else {
        " [j/k] move  [↵] detail  [l] logs  [o] open url  [t] trigger  [R] restart  [p] port  [/] filter  [q] quit "
            .to_string()
    };
    f.render_widget(
        Paragraph::new(Span::styled(footer, Style::default().fg(Color::Gray))),
        chunks[3],
    );
}

fn draw_detail(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(10),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(f.area());

    let r = app.selected().cloned().unwrap_or(RowItem {
        instance_id: String::new(),
        instance_name: "—".into(),
        name: "—".into(),
        kind: String::new(),
        update: String::new(),
        runtime: String::new(),
        pod: String::new(),
        url: String::new(),
        route_port: None,
    });

    f.render_widget(
        Paragraph::new(format!(" Detail · {} / {} ", r.instance_name, r.name)).style(
            Style::default()
                .fg(Color::White)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        chunks[0],
    );

    let info = vec![
        Line::from(vec![Span::styled("instance: ", bold()), Span::raw(r.instance_name.clone())]),
        Line::from(vec![Span::styled("resource: ", bold()), Span::raw(r.name.clone())]),
        Line::from(vec![Span::styled("type:     ", bold()), Span::raw(r.kind.clone())]),
        Line::from(vec![Span::styled("update:   ", bold()), Span::styled(r.update.clone(), status_style(&r.update))]),
        Line::from(vec![Span::styled("runtime:  ", bold()), Span::styled(r.runtime.clone(), status_style(&r.runtime))]),
        Line::from(vec![Span::styled("port:     ", bold()), Span::raw(r.route_port.map(|p| p.to_string()).unwrap_or_default())]),
        Line::from(vec![Span::styled("pod:      ", bold()), Span::raw(r.pod.clone())]),
        Line::from(vec![Span::styled("url:      ", bold()), Span::styled(r.url.clone(), Style::default().fg(Color::Blue))]),
    ];
    f.render_widget(
        Paragraph::new(info).block(Block::default().borders(Borders::ALL).title(" Status ")),
        chunks[1],
    );

    let h = chunks[2].height.saturating_sub(2) as usize;
    f.render_widget(
        Paragraph::new(log_lines(&app.logs, h, app.log_scroll))
            .block(Block::default().borders(Borders::ALL).title(" Logs ")),
        chunks[2],
    );

    f.render_widget(
        Paragraph::new(Span::styled(
            " [Esc/↵] back  [l] full logs  [o] open url  [t] trigger  [R] restart  [p] port  [PgUp/Dn] scroll  [q] quit ",
            Style::default().fg(Color::Gray),
        )),
        chunks[3],
    );
}

fn bold() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

#[cfg(test)]
mod tests {
    use super::{filter_log_lines, hostname_from_url, parse_port, route_port_for_url};
    use crate::daemon::protocol::{DashboardState, RouteInfo};

    #[test]
    fn log_filter_regex_substring_and_empty() {
        let logs = vec![
            "GET /healthz 200".to_string(),
            "ERROR: connection refused".to_string(),
            "GET /users 500".to_string(),
        ];
        // empty => all lines
        assert_eq!(filter_log_lines(&logs, "").len(), 3);
        // case-insensitive substring / regex
        assert_eq!(filter_log_lines(&logs, "error"), vec!["ERROR: connection refused"]);
        // regex: lines with a 5xx status
        assert_eq!(filter_log_lines(&logs, r"\b5\d\d\b"), vec!["GET /users 500"]);
        // invalid regex falls back to substring (no panic)
        assert_eq!(filter_log_lines(&logs, "GET [").len(), 0);
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

    let sel = app
        .selected()
        .map(|r| format!("{} / {}", r.instance_name, r.name))
        .unwrap_or_else(|| "—".into());
    let logs = app.filtered_logs();
    let matched = if app.log_filter.is_empty() {
        format!("{} lines", logs.len())
    } else {
        format!("{} matching lines", logs.len())
    };
    f.render_widget(
        Paragraph::new(format!(" Logs · {sel}  ·  {matched} ")).style(
            Style::default()
                .fg(Color::White)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        chunks[0],
    );

    let h = chunks[1].height.saturating_sub(2) as usize;
    f.render_widget(
        Paragraph::new(log_lines(&logs, h, app.log_scroll))
            .block(Block::default().borders(Borders::ALL)),
        chunks[1],
    );

    let footer = if app.mode == Mode::LogsFilter {
        format!(" filter /{}\u{2588}   (Enter apply · Esc clear) ", app.log_filter)
    } else if !app.log_filter.is_empty() {
        format!(
            " /{}   ·   [/] edit filter  [PgUp/Dn] scroll  [o] open  [l/Esc] back  [q] quit ",
            app.log_filter
        )
    } else {
        " [/] filter (regex)  [PgUp/Dn j/k] scroll  [G] tail  [o] open url  [l/Esc] back  [q] quit "
            .to_string()
    };
    f.render_widget(
        Paragraph::new(Span::styled(footer, Style::default().fg(Color::Gray))),
        chunks[2],
    );
}
