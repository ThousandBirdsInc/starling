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
}

#[derive(PartialEq)]
enum Mode {
    Normal,
    Filter,
    Detail,
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
    /// Lines scrolled up from the bottom; 0 = follow tail.
    log_scroll: usize,
    status_msg: String,
}

impl App {
    fn selected(&self) -> Option<&RowItem> {
        self.table.selected().and_then(|i| self.rows.get(i))
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
        log_scroll: 0,
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
                        _ => {}
                    },
                }
            }
        }
    }
    Ok(())
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
        ["INSTANCE", "RESOURCE", "TYPE", "UPDATE", "RUNTIME", "POD", "URL"]
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
    f.render_widget(
        Paragraph::new(log_lines(&app.logs, h, app.log_scroll)).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Logs · {sel_name}{follow} ")),
        ),
        chunks[2],
    );

    let footer = if app.mode == Mode::Filter {
        format!(" /{}\u{2588}   (Enter apply · Esc clear) ", app.filter)
    } else if app.rows.is_empty() {
        " No resources. Run `starling up` in a project.   [/] filter  [q] quit ".to_string()
    } else if !app.status_msg.is_empty() {
        format!(
            " {}   ·   [↵] detail [t] trigger [R] restart [/] filter [q] quit ",
            app.status_msg
        )
    } else {
        " [j/k] move  [↵] detail  [t] trigger  [R] restart  [/] filter  [PgUp/Dn] logs  [q] quit "
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
            Constraint::Length(8),
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
            " [Esc/↵] back   [t] trigger   [R] restart   [PgUp/Dn] scroll logs   [q] quit ",
            Style::default().fg(Color::Gray),
        )),
        chunks[3],
    );
}

fn bold() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}
