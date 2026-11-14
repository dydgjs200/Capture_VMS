// 릴리스 빌드에서는 콘솔 창 없이 실행
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

//! Capture VMS Viewer — 라이브 화면 (네이티브 창).
//!
//! 카메라에는 직접 붙지 않는다. 수신·녹화·촬영·기록은 엔진(capture-vms.exe)이 하고, 뷰어는
//!   - 엔진이 넘겨주는 원본 영상(`/api/sessions/{id}/live.ts`)을 받아 풀어서 그리고
//!   - 버튼(수신 시작·중지, 촬영, 동시 촬영)을 엔진 API 호출로 바꾼다.
//! 그래서 뷰어를 닫아도 수신·녹화는 계속되고, 다시 열면 이어서 보인다.

mod api;
#[allow(dead_code)]
#[path = "../../e2e.rs"]
mod e2e;
mod ui;
mod video;
mod vlog;

/// `e2e.rs`(엔진과 같은 측정 코드)가 쓰는 요약 형식
mod store {
    use chrono::{DateTime, Local};
    use serde::Serialize;

    #[derive(Debug, Clone, Default, Serialize)]
    pub struct E2eSummary {
        pub count: usize,
        pub avg_ms: f64,
        pub p50_ms: f64,
        pub min_ms: f64,
        pub max_ms: f64,
        pub measured_at: Option<DateTime<Local>>,
    }
}

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

/// 설정/데이터 기준 폴더. 엔진과 같은 규칙: `--home <폴더>` > 환경변수 CAPTURE_VMS_HOME > 실행 파일 폴더
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
        .or_else(|| std::env::current_exe().ok().and_then(|p| p.parent().map(Path::to_path_buf)))
        .unwrap_or_else(|| PathBuf::from("."));
    std::path::absolute(&home).unwrap_or(home)
}

/// 엔진 주소: config.toml의 `[server] bind` (없으면 기본 127.0.0.1:8787)
fn engine_base(home: &Path) -> String {
    let bind = std::fs::read_to_string(home.join("config.toml"))
        .ok()
        .and_then(|t| t.trim_start_matches('\u{feff}').parse::<toml::Table>().ok())
        .and_then(|t| t.get("server")?.get("bind")?.as_str().map(str::to_string))
        .and_then(|b| b.parse::<SocketAddr>().ok())
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8787)));
    let ip = if bind.ip().is_unspecified() { "127.0.0.1".to_string() } else { bind.ip().to_string() };
    format!("http://{ip}:{}", bind.port())
}

fn main() {
    let home = resolve_home();
    let base = engine_base(&home);
    let shared = Arc::new(api::Shared::default());
    vlog::log(format!("뷰어 시작 v{} · 엔진 {base} · 기준 폴더 {}", env!("CARGO_PKG_VERSION"), home.display()));
    let cmd = api::spawn(base.clone(), home.clone(), shared.clone());
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title("Capture VMS Viewer")
            .with_inner_size([1500.0, 900.0])
            .with_min_inner_size([860.0, 560.0]),
        // 화면 가운데에 띄운다 (Windows가 정해 주는 자리는 창 아래쪽이 작업 표시줄 밖으로 나갈 수 있다)
        centered: true,
        ..Default::default()
    };
    let result = eframe::run_native(
        "Capture VMS Viewer",
        options,
        Box::new(move |cc| {
            ui::setup(&cc.egui_ctx);
            shared.set_ctx(cc.egui_ctx.clone());
            Ok(Box::new(ui::ViewerApp::new(base, home, shared, cmd)))
        }),
    );
    vlog::log("뷰어 종료");
    if let Err(e) = result {
        eprintln!("뷰어를 시작할 수 없습니다: {e}");
        std::process::exit(1);
    }
}
