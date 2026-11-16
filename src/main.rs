// 릴리스 빌드에서는 콘솔 창 없이 실행 (대시보드는 브라우저로 연다)
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod app;
mod applog;
mod camfix;
mod capture;
mod config;
mod diagnose;
mod e2e;
mod export;
mod netmon;
mod netscan;
mod onvif;
mod shellopen;
mod stalldiag;
mod store;
mod stream;
mod trigger;
mod web;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use tracing_appender::non_blocking::WorkerGuard;

use crate::{
    app::App,
    applog::LogSwitch,
    config::Config,
    store::{Event, Store},
};

fn main() {
    let home = resolve_home();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4) // FFmpeg 출력 읽기·측정이 늘어서 2 → 4
        .enable_all()
        .build()
        .expect("tokio runtime");
    let result = runtime.block_on(run(home));
    runtime.shutdown_timeout(Duration::from_secs(3));
    if let Err(e) = result {
        report_fatal(&format!("{e:#}"));
        std::process::exit(1);
    }
}

/// 설정/데이터 기준 폴더. `--home <폴더>` > 환경변수 CAPTURE_VMS_HOME > 실행 파일 폴더.
/// 항상 절대 경로로 바꾼다: `--home .`처럼 상대 경로로 주면 기록에 남는 녹화·이미지 경로도 상대 경로가 되어
/// 실행 위치가 달라지면 찾을 수 없게 된다.
fn resolve_home() -> PathBuf {
    let mut args = std::env::args().skip(1);
    let mut home = None;
    while let Some(a) = args.next() {
        if a == "--home" {
            home = args.next().map(PathBuf::from);
            break;
        }
    }
    let home = home
        .or_else(|| std::env::var("CAPTURE_VMS_HOME").ok().map(PathBuf::from))
        .or_else(exe_dir)
        .unwrap_or_else(|| PathBuf::from("."));
    std::path::absolute(&home).unwrap_or(home)
}

fn exe_dir() -> Option<PathBuf> {
    std::env::current_exe().ok().and_then(|p| p.parent().map(Path::to_path_buf))
}

/// FFmpeg 찾기: 설정값 → 프로그램 폴더\ffmpeg\ffmpeg.exe → 프로그램 폴더\ffmpeg.exe → PATH
fn find_ffmpeg(configured: &str, home: &Path) -> Option<PathBuf> {
    let name = if cfg!(windows) { "ffmpeg.exe" } else { "ffmpeg" };
    let mut cands: Vec<PathBuf> = Vec::new();
    if !configured.trim().is_empty() {
        cands.push(config::resolve_path(home, configured));
    }
    for base in [exe_dir(), Some(home.to_path_buf())].into_iter().flatten() {
        cands.push(base.join("ffmpeg").join(name));
        cands.push(base.join(name));
    }
    if let Some(path) = std::env::var_os("PATH") {
        cands.extend(std::env::split_paths(&path).map(|p| p.join(name)));
    }
    cands.into_iter().find(|p| p.is_file())
}

async fn ffmpeg_version(path: &Path) -> Option<String> {
    let mut cmd = tokio::process::Command::new(path);
    cmd.arg("-version").stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null());
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000);
    let out = tokio::time::timeout(Duration::from_secs(5), cmd.output()).await.ok()?.ok()?;
    String::from_utf8_lossy(&out.stdout).lines().next().map(|l| l.trim().to_string())
}

async fn run(home: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&home).with_context(|| format!("폴더를 만들 수 없습니다: {}", home.display()))?;
    let config_path = home.join("config.toml");
    let cfg = Config::load_or_create(&config_path)?;

    let recordings_dir = cfg.storage.recordings_path(&home);
    let logs_dir = cfg.storage.logs_path(&home);
    let store = Store::open(&recordings_dir, &logs_dir).with_context(|| {
        format!("저장 폴더를 열 수 없습니다: {} / {}", recordings_dir.display(), logs_dir.display())
    })?;
    let (_log_guard, app_log) = init_logging(&logs_dir);

    let ffmpeg = find_ffmpeg(&cfg.stream.ffmpeg_path, &home);
    let ffmpeg_ver = match &ffmpeg {
        Some(p) => ffmpeg_version(p).await,
        None => None,
    };
    match &ffmpeg {
        Some(p) => {
            tracing::info!("FFmpeg: {} ({})", p.display(), ffmpeg_ver.as_deref().unwrap_or("버전 확인 실패"));
            let p2 = p.clone();
            let hybrid = tokio::task::spawn_blocking(move || stream::detect_ffmpeg_features(&p2)).await.unwrap_or(false);
            if !hybrid {
                tracing::warn!("이 FFmpeg는 MP4 hybrid_fragmented를 지원하지 않아 녹화 파일을 조각 MP4로 저장합니다 (Windows에서 길이 표시 안 됨). FFmpeg 7.1 이상을 권장합니다");
            }
        }
        None => tracing::warn!("FFmpeg를 찾지 못했습니다. 영상 수신을 할 수 없습니다"),
    }

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(3))
        .user_agent(concat!("capture-vms/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("HTTP 클라이언트 생성 실패")?;

    let addr: SocketAddr = cfg.server.bind.parse()?;
    let url = format!("http://{}", browser_addr(addr));
    // 로그인 자동 실행 시에는 브라우저를 띄우지 않는다 (--no-browser)
    let open_browser = cfg.server.open_browser && !std::env::args().any(|a| a == "--no-browser");

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            // 이미 실행 중이면 새로 띄우지 않고 기존 대시보드만 연다.
            let _ = open::that(&url);
            return Ok(());
        }
        Err(e) => return Err(e).with_context(|| format!("대시보드 주소를 열 수 없습니다: {addr}")),
    };

    let app = Arc::new(App::new(cfg, home, config_path, store, client, app_log, ffmpeg, ffmpeg_ver));
    // 프로그램 안에서 생긴 오류(panic)도 로그에 남긴다 — 창이 없는 프로그램이라 안 남기면 흔적이 없다
    std::panic::set_hook(Box::new(|info| tracing::error!("프로그램 내부 오류(panic): {info}")));
    if let Some(last) = unclean_previous_exit(&app.store.events(None, None, 2000)) {
        let msg = format!("지난 실행이 정상으로 끝나지 않았습니다 (PC가 꺼졌거나 프로그램이 강제 종료됨) — 마지막 기록 {}", last.format("%m/%d %H:%M:%S"));
        tracing::warn!("{msg}");
        app.store.record_event(Event::new("app_crashed", None, msg).with_data(serde_json::json!({ "last_record": last })));
    }
    app.store.record_event(Event::new(
        "app_start",
        None,
        format!("프로그램 시작 v{} (카메라 {}대)", env!("CARGO_PKG_VERSION"), app.cameras().len()),
    ));
    tracing::info!("프로그램 시작 v{} · 기준 폴더 {} · FFmpeg {}", env!("CARGO_PKG_VERSION"), app.home.display(), app.ffmpeg_version.as_deref().unwrap_or("없음"));
    {
        let a = app.clone();
        let days = a.retention_days.load(std::sync::atomic::Ordering::Relaxed);
        tokio::task::spawn_blocking(move || a.store.cleanup(days));
    }
    netmon::spawn(app.clone());
    trigger::apply(&app);
    // 지난번에 갑자기 꺼져 '진행 중'으로 남았던 세션: 중간 저장 시점까지로 마무리하고 녹화 파일 점검
    tokio::spawn(stream::recover_interrupted(app.clone()));
    tracing::info!("대시보드: {url}");
    if open_browser {
        let _ = open::that(&url);
    }

    let shutdown_app = app.clone();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = axum::serve(listener, web::router(app.clone())).with_graceful_shutdown(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => shutdown_app.request_shutdown(),
            _ = shutdown_app.wait_shutdown() => {}
        }
        // 녹화 파일이 정상적으로 닫히도록 수신 세션을 먼저 마무리
        trigger::stop_auto(&shutdown_app, true);
        stream::stop_all(&shutdown_app, Duration::from_secs(10)).await;
        let _ = stopped_tx.send(());
    });
    // 브라우저가 열어 둔 연결(녹화 재생 등)이 안 닫히면 serve가 끝나지 않아 프로세스가 남는다.
    // 수신을 다 마무리한 뒤에는 남은 연결을 5초만 기다린다.
    tokio::select! {
        r = std::future::IntoFuture::into_future(serve) => r.context("웹 서버 오류")?,
        _ = async {
            if stopped_rx.await.is_ok() {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else {
                std::future::pending::<()>().await;
            }
        } => tracing::warn!("열린 대시보드 연결이 남아 있어 기다리지 않고 종료합니다"),
    }

    app.store.record_event(Event::new("app_stop", None, "프로그램 종료"));
    app.store.flush(Duration::from_secs(2));
    Ok(())
}

/// 프로그램 로그(app.*.log)의 시각을 다른 기록(JSONL)과 같은 현지 시각으로 찍는다.
/// 기본은 UTC라 9시간 어긋나서, 끊긴 시각을 다른 기록과 맞춰 볼 때 헷갈린다.
struct LocalTimer;

impl tracing_subscriber::fmt::time::FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// 지난 실행이 정상으로 끝나지 않았으면(PC가 꺼졌거나 강제 종료됨) 그 실행이 남긴 마지막 기록의 시각.
/// 마지막 '프로그램 시작' 뒤에 '프로그램 종료'가 없으면 그런 것이다. `events`: 최근 것부터
fn unclean_previous_exit(events: &[Event]) -> Option<chrono::DateTime<chrono::Local>> {
    let marker = events.iter().find(|e| e.kind == "app_start" || e.kind == "app_stop")?;
    if marker.kind == "app_stop" {
        return None;
    }
    events.first().map(|e| e.ts)
}

fn browser_addr(addr: SocketAddr) -> SocketAddr {
    if addr.ip().is_unspecified() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
    } else {
        addr
    }
}

/// 프로그램 동작 로그. 로그 폴더를 실행 중에 바꿀 수 있도록 LogSwitch를 거쳐 쓴다.
fn init_logging(dir: &Path) -> (Option<WorkerGuard>, Option<Arc<LogSwitch>>) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let Ok(switch) = LogSwitch::new(dir) else {
        return (None, None);
    };
    let (writer, guard) = tracing_appender::non_blocking(switch.writer());
    // 기록 수준은 이 프로그램 전용 환경 변수로만 바꾼다 (예: CAPTURE_VMS_LOG=debug). 다른 프로그램도 쓰는 RUST_LOG를 따르면,
    // 그 PC에 우연히 RUST_LOG=warn 같은 값이 걸려 있을 때 FFmpeg 시작·종료 같은 기록이 통째로 빠진다
    let filter = std::env::var("CAPTURE_VMS_LOG").ok().and_then(|v| EnvFilter::try_new(v).ok()).unwrap_or_else(|| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry().with(filter).with(fmt::layer().with_timer(LocalTimer).with_writer(writer).with_ansi(false));
    #[cfg(debug_assertions)]
    let registry = registry.with(fmt::layer());
    registry.init();
    (Some(guard), Some(switch))
}

fn report_fatal(msg: &str) {
    eprintln!("오류: {msg}");
    tracing::error!("치명적 오류: {msg}");
    #[cfg(windows)]
    unsafe {
        use windows_sys::Win32::UI::WindowsAndMessaging::{MessageBoxW, MB_ICONERROR, MB_OK};
        let text: Vec<u16> = format!("프로그램을 시작할 수 없습니다.\n\n{msg}").encode_utf16().chain(Some(0)).collect();
        let title: Vec<u16> = "Capture VMS".encode_utf16().chain(Some(0)).collect();
        MessageBoxW(std::ptr::null_mut(), text.as_ptr(), title.as_ptr(), MB_OK | MB_ICONERROR);
    }
}

#[cfg(test)]
mod start_tests {
    use super::*;

    fn ev(kind: &str) -> Event {
        Event::new(kind, None, "")
    }

    #[test]
    fn detects_a_run_that_did_not_end_cleanly() {
        // (최근 것부터) 마지막 '프로그램 시작' 뒤에 '프로그램 종료'가 없다 = PC가 꺼졌거나 강제 종료됨
        assert!(unclean_previous_exit(&[ev("stream_stall"), ev("session_start"), ev("app_start"), ev("app_stop")]).is_some());
        // 정상 종료
        assert!(unclean_previous_exit(&[ev("app_stop"), ev("session_end"), ev("app_start")]).is_none());
        // 기록이 없으면(처음 실행) 해당 없음
        assert!(unclean_previous_exit(&[]).is_none());
        assert!(unclean_previous_exit(&[ev("camera_up")]).is_none());
    }
}
