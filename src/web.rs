//! 대시보드 웹 서버 (로컬 전용).

use std::{
    path::{Path as FsPath, PathBuf},
    sync::{atomic::Ordering, Arc},
};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Duration, Local};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    app::{App, CamStatus},
    camfix,
    config::{self, CameraConfig, TriggerConfig},
    export, netmon, netscan, onvif,
    store::{self, CaptureRecord, DirStats, Event, NetPoint, StreamPoint, TriggerRecord, WifiInfo, WifiPoint},
    stream::{self, LiveBrief, SessionView, StartRequest},
    trigger,
};

type AppState = Arc<App>;

const DASHBOARD: &str = include_str!("../assets/dashboard.html");
/// 남은 공간이 이보다 적으면 대시보드에 경고
const LOW_DISK_BYTES: u64 = 5 * 1024 * 1024 * 1024;

pub fn router(app: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/favicon.ico", get(|| async { StatusCode::NO_CONTENT }))
        .route("/api/status", get(status))
        .route("/api/sessions", get(list_sessions).post(start_session))
        .route("/api/sessions/start-all", post(start_all))
        .route("/api/sessions/stop-all", post(stop_all))
        .route("/api/sessions/{id}", get(session_detail))
        .route("/api/sessions/{id}/stop", post(stop_session))
        .route("/api/sessions/{id}/capture", post(take_capture))
        .route("/api/cameras/{id}/capture", post(camera_capture))
        .route("/api/captures", get(list_captures))
        .route("/api/captures/{id}/image", get(capture_image))
        .route("/api/sessions/{id}/e2e/start", post(e2e_start))
        .route("/api/sessions/{id}/e2e/mark", post(e2e_mark))
        .route("/api/sessions/{id}/e2e/stop", post(e2e_stop))
        .route("/api/sessions/{id}/e2e/report", post(e2e_report))
        .route("/api/sessions/{id}/preview.mjpg", get(preview_mjpeg))
        .route("/api/sessions/{id}/snapshot.jpg", get(preview_snapshot))
        .route("/api/sessions/{id}/live.ts", get(live_ts))
        .route("/api/network", get(network))
        .route("/api/wifi", get(wifi))
        .route("/api/events", get(events))
        .route("/api/cameras", get(list_cameras).post(add_camera))
        .route("/api/cameras/discover", post(discover_cameras))
        .route("/api/cameras/test", post(test_camera))
        .route("/api/cameras/{id}", axum::routing::put(update_camera).delete(delete_camera))
        .route("/api/cameras/{id}/move", post(move_camera))
        .route("/api/settings", get(get_settings))
        .route("/api/settings/storage", post(set_storage))
        .route("/api/pick-folder", post(pick_folder))
        .route("/api/open", post(open_path))
        .route("/api/shutdown", post(shutdown))
        .route("/api/location", axum::routing::put(set_location))
        .route("/api/sessions/{id}/files", get(recording_files))
        .route("/api/sessions/{id}/files/{name}", get(recording_file))
        .route("/api/recordings/orphans", get(recording_orphans))
        .route("/api/recordings/{day}/{folder}/files", get(folder_files))
        .route("/api/recordings/{day}/{folder}/files/{name}", get(folder_file))
        .route("/api/sessions/{id}/check-recording", post(recheck_recording))
        .route("/api/trigger", get(trigger_status).post(trigger_fire))
        .route("/api/trigger/config", axum::routing::put(trigger_config))
        .route("/api/trigger/auto", post(trigger_auto_start))
        .route("/api/trigger/auto/stop", post(trigger_auto_stop))
        .route("/api/trigger/selftest", post(trigger_selftest))
        .route("/api/triggers", get(list_triggers))
        .route("/api/diagnose", get(diagnose_all))
        .route("/api/net/adapters", get(net_adapters))
        .route("/api/net/diagnose", post(net_diagnose))
        .route("/api/net/scan", post(net_scan))
        .route("/api/raw", get(raw_rows))
        .route("/api/raw/summary", get(raw_summary))
        .route("/api/export/xlsx", post(export_xlsx))
        .route("/api/export/pdf", post(export_pdf))
        .route("/api/exports", get(list_exports))
        .route("/api/exports/{name}", get(download_export))
        .route("/report", get(report_page))
        .route("/logs/{name}", get(log_file))
        .with_state(app)
}

async fn index() -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8"), (header::CACHE_CONTROL, "no-store")], DASHBOARD).into_response()
}

fn api_error(code: StatusCode, msg: impl Into<String>) -> Response {
    (code, Json(json!({ "error": msg.into() }))).into_response()
}

fn filter(c: &Option<String>) -> Option<&str> {
    c.as_deref().filter(|s| !s.is_empty() && *s != "all")
}

// ---------------- 상태 ----------------

#[derive(Serialize)]
struct CameraView {
    id: String,
    name: String,
    host: String,
    has_main: bool,
    has_sub: bool,
    status: CamStatus,
    /// PC가 이 카메라로 갈 때 쓰는 인터페이스
    route_kind: Option<String>,
    route_name: Option<String>,
    /// same(같은 대역) | routed(다른 대역, 공유기 경유)
    subnet: Option<&'static str>,
    /// 수신 중인 세션 id
    session: Option<String>,
    /// 트리거 때 찍는 카메라인지
    trigger: bool,
    /// 수신 중이면 실시간 요약 (전체 보기 타일)
    live: Option<LiveBrief>,
    last_capture: Option<CaptureRecord>,
}

#[derive(Serialize)]
struct TriggerBrief {
    tcp_enabled: bool,
    tcp_listening: bool,
    modbus_enabled: bool,
    modbus_connected: bool,
    busy: u32,
    auto_running: bool,
    last: Option<TriggerRecord>,
}

#[derive(Serialize)]
struct FfmpegView {
    found: bool,
    path: Option<String>,
    version: Option<String>,
}

#[derive(Serialize)]
struct StatusView {
    version: &'static str,
    max_cameras: usize,
    started_at: DateTime<Local>,
    uptime_secs: i64,
    ffmpeg: FfmpegView,
    recordings_dir: String,
    logs_dir: String,
    free_bytes: Option<u64>,
    probe_interval_secs: u64,
    stall_ms: u64,
    jank_ms: u64,
    delay_warn_ms: u64,
    delay_bad_ms: u64,
    dropped_log_lines: u64,
    wifi: Option<WifiInfo>,
    cameras: Vec<CameraView>,
    /// 수신 중인 카메라 비트레이트 합 (같은 Wi-Fi를 나눠 쓴다)
    total_kbps: f64,
    low_disk: bool,
    /// 이 아래로 내려가면 녹화 중인 수신을 멈춤 (바이트)
    min_free_bytes: u64,
    trigger: TriggerBrief,
    /// 지금 측정 위치 (촬영·트리거·세션 기록에 붙음)
    location: Option<String>,
    /// 최근 쓴 위치들 (고르기용)
    recent_locations: Vec<String>,
    /// 촬영 시간 기준 (ms): 목표 / 허용 한도
    capture_goal_ms: u64,
    capture_limit_ms: u64,
}

async fn status(State(app): State<AppState>) -> Json<StatusView> {
    let cameras: Vec<CameraView> = app
        .cameras()
        .iter()
        .map(|c| {
            let (route_kind, route_name) = netmon::route_to(&c.cfg.ip()).unwrap_or((None, None));
            let session = app.session_for_camera(&c.cfg.id);
            CameraView {
                id: c.cfg.id.clone(),
                name: c.cfg.name.clone(),
                host: c.cfg.host.clone(),
                has_main: !c.cfg.rtsp_main.trim().is_empty(),
                has_sub: !c.cfg.rtsp_sub.trim().is_empty(),
                status: c.status.lock().unwrap().clone(),
                route_kind,
                route_name,
                subnet: netscan::subnet_hint(&c.cfg.ip()),
                session: session.as_ref().map(|s| s.id.clone()),
                trigger: c.cfg.trigger,
                live: session.map(|s| s.brief()),
                last_capture: app.store.last_capture_of(&c.cfg.id),
            }
        })
        .collect();
    let rec = app.store.recordings_dir();
    let free = store::free_space(&rec);
    let rt = &app.trigger;
    let tcfg = rt.config();
    Json(StatusView {
        total_kbps: cameras.iter().filter_map(|c| c.live.as_ref()).map(|l| l.kbps).sum(),
        // 경고는 자동 중지 기준보다 넉넉히 먼저 (기본 5GB, 중지 기준이 크면 그보다 3GB 위)
        low_disk: free.map_or(false, |f| f < LOW_DISK_BYTES.max((app.cfg.storage.min_free_gb.max(0.5) * 1e9) as u64 + 3_000_000_000)),
        min_free_bytes: (app.cfg.storage.min_free_gb.max(0.5) * 1e9) as u64,
        location: app.location(),
        recent_locations: app.store.recent_locations(8),
        capture_goal_ms: app.cfg.stream.capture_goal_ms.max(1),
        capture_limit_ms: app.cfg.stream.capture_limit_ms.max(app.cfg.stream.capture_goal_ms.max(1)),
        trigger: TriggerBrief {
            tcp_enabled: tcfg.tcp_enabled,
            tcp_listening: rt.tcp_listening(),
            modbus_enabled: tcfg.modbus_enabled,
            modbus_connected: rt.modbus_connected(),
            busy: rt.busy(),
            auto_running: trigger::status(&app).auto.running,
            last: rt.last(),
        },
        version: env!("CARGO_PKG_VERSION"),
        max_cameras: MAX_CAMERAS,
        started_at: app.started_at,
        uptime_secs: (Local::now() - app.started_at).num_seconds(),
        ffmpeg: FfmpegView {
            found: app.ffmpeg.is_some(),
            path: app.ffmpeg.as_ref().map(|p| p.display().to_string()),
            version: app.ffmpeg_version.clone(),
        },
        free_bytes: free,
        recordings_dir: rec.display().to_string(),
        logs_dir: app.store.logs_dir().display().to_string(),
        probe_interval_secs: app.cfg.monitor.probe_interval_secs,
        stall_ms: app.cfg.stream.stall_ms,
        jank_ms: app.cfg.stream.jank_ms,
        delay_warn_ms: app.cfg.stream.delay_warn_ms,
        delay_bad_ms: app.cfg.stream.delay_bad_ms,
        dropped_log_lines: app.store.dropped_lines(),
        wifi: app.wifi(),
        cameras,
    })
}

#[derive(Deserialize)]
struct LocationReq {
    location: String,
}

/// 측정 위치 바꾸기 (이후 촬영·트리거·새로 시작하는 세션에 붙는다)
async fn set_location(State(app): State<AppState>, Json(req): Json<LocationReq>) -> Json<serde_json::Value> {
    app.set_location(&req.location);
    let loc = app.location();
    app.store.record_event(Event::new("location", None, match &loc {
        Some(l) => format!("측정 위치: {l}"),
        None => "측정 위치 지움".into(),
    }));
    Json(json!({ "location": loc }))
}

#[derive(Serialize)]
struct RecFile {
    name: String,
    bytes: u64,
    /// 파일이 만들어진 시각 = 이 파일의 영상이 시작된 시각
    started: Option<DateTime<Local>>,
    modified: Option<DateTime<Local>>,
    /// 수신 중인 세션의 마지막 파일 (지금 쓰는 중)
    writing: bool,
}

/// 세션 기록에 적힌 녹화 폴더. 그 경로가 없어졌으면(데이터 폴더를 옮겼거나 다른 PC에서 가져온 기록)
/// 지금의 녹화 폴더 아래 같은 자리(<날짜>/<세션 폴더>)를 찾아보고, 있으면 그쪽을 쓴다.
fn rec_dir_of(app: &App, stored: &str) -> PathBuf {
    let p = PathBuf::from(stored);
    if p.is_dir() {
        return p;
    }
    let moved = p.file_name().zip(p.parent().and_then(|d| d.file_name())).map(|(folder, day)| app.store.recordings_dir().join(day).join(folder));
    moved.filter(|q| q.is_dir()).unwrap_or(p)
}

/// 폴더 안의 녹화 파일 (이름순)
fn read_rec_files(dir: &FsPath) -> Vec<RecFile> {
    let mut files: Vec<RecFile> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    if !name.to_ascii_lowercase().ends_with(".mp4") {
                        return None;
                    }
                    let m = e.metadata().ok()?;
                    let modified = m.modified().ok();
                    // 폴더를 복사해 오면 만든 시각이 복사한 때로 바뀐다 (수정 시각보다 늦음) → 그런 값은 쓰지 않는다
                    let started = m.created().ok().filter(|c| modified.is_some_and(|md| *c <= md));
                    Some(RecFile {
                        name,
                        bytes: m.len(),
                        started: started.map(DateTime::<Local>::from),
                        modified: modified.map(DateTime::<Local>::from),
                        writing: false,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort_by(|a, b| a.name.cmp(&b.name));
    files
}

/// 녹화 파일 목록 응답. `live`: 지금 받고 있는 세션이면 마지막 파일은 쓰는 중
async fn rec_files_response(dir: PathBuf, live: bool) -> Response {
    let d = dir.clone();
    let mut files = tokio::task::spawn_blocking(move || read_rec_files(&d)).await.unwrap_or_default();
    if live {
        if let Some(last) = files.last_mut() {
            last.writing = true;
        }
    }
    Json(json!({ "dir": dir, "exists": dir.is_dir(), "files": files })).into_response()
}

/// 세션의 녹화 파일 목록 (녹화 탭)
async fn recording_files(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(rec) = app.store.session(&id) else { return api_error(StatusCode::NOT_FOUND, "세션을 찾을 수 없습니다") };
    let Some(stored) = rec.recording_dir.as_deref() else { return Json(json!({ "dir": null, "files": [] })).into_response() };
    rec_files_response(rec_dir_of(&app, stored), app.session(&id).is_some()).await
}

/// 주소로 받은 값이 녹화 폴더 밖을 가리키지 못하게: 경로 한 마디로만 쓸 수 있는 이름인지
fn safe_segment(s: &str) -> bool {
    !s.is_empty() && !s.contains(['/', '\\', ':']) && !s.contains("..")
}

/// "<날짜>/<폴더>" → 지금의 녹화 폴더 아래 실제 경로 (없으면 None)
fn rec_folder_path(app: &App, day: &str, folder: &str) -> Option<PathBuf> {
    let ok = day.len() == 8 && day.bytes().all(|b| b.is_ascii_digit()) && safe_segment(folder);
    ok.then(|| app.store.recordings_dir().join(day).join(folder)).filter(|p| p.is_dir())
}

fn norm_path(p: &FsPath) -> String {
    p.to_string_lossy().replace('/', "\\").trim_end_matches('\\').to_lowercase()
}

#[derive(Serialize)]
struct RecFolder {
    /// "<날짜>/<폴더>" — 파일 목록·재생 주소에 쓴다
    key: String,
    name: String,
    files: usize,
    bytes: u64,
    started: Option<DateTime<Local>>,
    modified: Option<DateTime<Local>>,
}

/// 세션 기록이 없는 녹화 폴더: 기록(로그) 폴더를 바꾸거나 지운 뒤 남은 녹화, 다른 곳에서 복사해 온 녹화,
/// 기억해 두는 세션 수(1000개)를 넘어 밀려난 오래된 녹화. 녹화 탭은 세션 기록을 따라 파일을 보여 주므로
/// 이런 폴더는 따로 찾아 줘야 화면에 나온다.
async fn recording_orphans(State(app): State<AppState>) -> Json<Vec<RecFolder>> {
    let root = app.store.recordings_dir();
    let known: std::collections::HashSet<String> =
        app.store.sessions(usize::MAX).iter().filter_map(|r| r.recording_dir.as_deref()).map(|d| norm_path(&rec_dir_of(&app, d))).collect();
    let out = tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        let Ok(days) = std::fs::read_dir(&root) else { return out };
        for day in days.flatten().filter(|e| e.path().is_dir()) {
            let dname = day.file_name().to_string_lossy().to_string();
            if !(dname.len() == 8 && dname.bytes().all(|b| b.is_ascii_digit())) {
                continue;
            }
            let Ok(subs) = std::fs::read_dir(day.path()) else { continue };
            for sub in subs.flatten().filter(|e| e.path().is_dir()) {
                let path = sub.path();
                if known.contains(&norm_path(&path)) {
                    continue;
                }
                let files = read_rec_files(&path);
                if files.is_empty() {
                    continue;
                }
                let name = sub.file_name().to_string_lossy().to_string();
                out.push(RecFolder {
                    key: format!("{dname}/{name}"),
                    name,
                    files: files.len(),
                    bytes: files.iter().map(|f| f.bytes).sum(),
                    started: files.iter().filter_map(|f| f.started).min(),
                    modified: files.iter().filter_map(|f| f.modified).max(),
                })
            }
        }
        out.sort_by(|a, b| b.key.cmp(&a.key));
        out
    })
    .await
    .unwrap_or_default();
    Json(out)
}

/// 세션 기록이 없는 녹화 폴더의 파일 목록
async fn folder_files(State(app): State<AppState>, Path((day, folder)): Path<(String, String)>) -> Response {
    match rec_folder_path(&app, &day, &folder) {
        Some(dir) => rec_files_response(dir, false).await,
        None => api_error(StatusCode::NOT_FOUND, "녹화 폴더가 없습니다"),
    }
}

/// 세션 기록이 없는 녹화 폴더의 파일 재생
async fn folder_file(State(app): State<AppState>, Path((day, folder, name)): Path<(String, String, String)>, headers: axum::http::HeaderMap) -> Response {
    if !safe_segment(&name) || !name.to_ascii_lowercase().ends_with(".mp4") {
        return api_error(StatusCode::BAD_REQUEST, "잘못된 파일 이름입니다");
    }
    match rec_folder_path(&app, &day, &folder) {
        Some(dir) => serve_rec_file(dir.join(&name), headers).await,
        None => api_error(StatusCode::NOT_FOUND, "녹화 폴더가 없습니다"),
    }
}

/// "bytes=START-END" / "bytes=START-" / "bytes=-SUFFIX" → (시작, 끝) (끝 포함)
fn parse_range(v: &str, len: u64) -> Option<Result<(u64, u64), ()>> {
    let spec = v.trim().strip_prefix("bytes=")?.split(',').next()?.trim();
    let (a, b) = spec.split_once('-')?;
    if len == 0 {
        return Some(Err(()));
    }
    let r = if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        (len.saturating_sub(n), len - 1)
    } else {
        let s: u64 = a.parse().ok()?;
        let e = if b.is_empty() { len - 1 } else { b.parse::<u64>().ok()?.min(len - 1) };
        (s, e)
    };
    Some(if r.0 > r.1 || r.0 >= len { Err(()) } else { Ok(r) })
}

/// 녹화 파일 재생 (브라우저 <video>가 구간 이동할 수 있게 Range 요청 지원)
async fn recording_file(State(app): State<AppState>, Path((id, name)): Path<(String, String)>, headers: axum::http::HeaderMap) -> Response {
    if !safe_segment(&name) || !name.to_ascii_lowercase().ends_with(".mp4") {
        return api_error(StatusCode::BAD_REQUEST, "잘못된 파일 이름입니다");
    }
    let Some(dir) = app.store.session(&id).and_then(|r| r.recording_dir) else { return api_error(StatusCode::NOT_FOUND, "녹화가 없는 세션입니다") };
    serve_rec_file(rec_dir_of(&app, &dir).join(&name), headers).await
}

/// 녹화 파일 하나를 내보낸다 (Range 요청 지원)
async fn serve_rec_file(path: PathBuf, headers: axum::http::HeaderMap) -> Response {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut f = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(_) => return api_error(StatusCode::NOT_FOUND, "녹화 파일이 없습니다 (옮겨졌거나 지워졌을 수 있습니다)"),
    };
    let len = f.metadata().await.map(|m| m.len()).unwrap_or(0);
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok()).and_then(|v| parse_range(v, len));
    let (start, end, status) = match range {
        Some(Ok((s, e))) => (s, e, StatusCode::PARTIAL_CONTENT),
        Some(Err(())) => {
            return (StatusCode::RANGE_NOT_SATISFIABLE, [(header::CONTENT_RANGE, format!("bytes */{len}"))]).into_response();
        }
        None => (0, len.saturating_sub(1), StatusCode::OK),
    };
    let count = if len == 0 { 0 } else { end - start + 1 };
    if f.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "파일을 읽을 수 없습니다");
    }
    let reader = f.take(count);
    let stream = futures_util::stream::unfold(reader, |mut r| async move {
        let mut buf = vec![0u8; 256 * 1024];
        match r.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<_, std::io::Error>(Bytes::from(buf)), r))
            }
            Err(e) => Some((Err(e), r)),
        }
    });
    let mut resp = Response::new(Body::from_stream(stream));
    *resp.status_mut() = status;
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("video/mp4"));
    h.insert(header::ACCEPT_RANGES, header::HeaderValue::from_static("bytes"));
    h.insert(header::CACHE_CONTROL, header::HeaderValue::from_static("no-store"));
    if let Ok(v) = header::HeaderValue::from_str(&count.to_string()) {
        h.insert(header::CONTENT_LENGTH, v);
    }
    if status == StatusCode::PARTIAL_CONTENT {
        if let Ok(v) = header::HeaderValue::from_str(&format!("bytes {start}-{end}/{len}")) {
            h.insert(header::CONTENT_RANGE, v);
        }
    }
    resp
}

/// 녹화 파일 다시 점검 (끝난 세션)
async fn recheck_recording(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    if app.session(&id).is_some() {
        return api_error(StatusCode::CONFLICT, "수신 중인 세션입니다. 수신을 멈춘 뒤 점검하세요");
    }
    let Some(rec) = app.store.session(&id) else { return api_error(StatusCode::NOT_FOUND, "세션을 찾을 수 없습니다") };
    if rec.recorded_bytes == 0 && rec.recording_dir.is_none() {
        return api_error(StatusCode::BAD_REQUEST, "녹화하지 않은 세션입니다");
    }
    let interrupted = rec.interrupted;
    crate::stream::attach_rec_check(&app, rec, interrupted).await;
    match app.store.session(&id).and_then(|r| r.rec_check) {
        Some(c) => Json(c).into_response(),
        None => api_error(StatusCode::NOT_FOUND, "녹화 파일이 없습니다 (폴더가 옮겨졌거나 지워졌을 수 있습니다)"),
    }
}

// ---------------- 세션 ----------------

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    camera: Option<String>,
}

fn view_of(app: &App, rec: store::SessionRecord) -> SessionView {
    match app.session(&rec.id) {
        Some(s) => s.view(),
        None => SessionView { record: rec, live: None },
    }
}

async fn list_sessions(State(app): State<AppState>, Query(q): Query<ListQuery>) -> Json<Vec<SessionView>> {
    let cam = filter(&q.camera);
    Json(
        app.store
            .sessions(1000)
            .into_iter()
            .filter(|r| cam.map_or(true, |c| r.camera_id == c))
            .take(q.limit.unwrap_or(200).clamp(1, 1000))
            .map(|r| view_of(&app, r))
            .collect(),
    )
}

async fn start_session(State(app): State<AppState>, Json(req): Json<StartRequest>) -> Response {
    // 연결 테스트 없이 저장해 영상 주소가 비어 있으면 먼저 찾아서 채운다 (이미 있으면 아무것도 하지 않는다)
    let filled = if app.session_for_camera(&req.camera_id).is_none() { camfix::fill_urls(&app, &req.camera_id, req.profile == "sub").await } else { Ok(()) };
    match stream::start(&app, req) {
        Ok(s) => Json(s.view()).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, no_url_reason(e, filled)),
    }
}

/// 영상 주소가 없어 시작하지 못했을 때: 자동으로 찾아보다 실패한 이유를 알려 준다
fn no_url_reason(start_error: String, filled: Result<(), String>) -> String {
    match filled {
        Err(why) if start_error.contains("주소가 없습니다") => format!("영상 주소를 찾지 못해 시작할 수 없습니다: {why}"),
        _ => start_error,
    }
}

#[derive(Deserialize)]
struct StartAll {
    #[serde(default)]
    profile: String,
    #[serde(default)]
    transport: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    preview: Option<String>,
    #[serde(default)]
    record: Option<bool>,
    /// 비우면 등록된 카메라 전부
    #[serde(default)]
    camera_ids: Vec<String>,
}

/// 여러 카메라를 한 번에 수신 시작 (같은 묶음 id로 기록해 나중에 같이 비교)
async fn start_all(State(app): State<AppState>, Json(req): Json<StartAll>) -> Response {
    let group = format!("G{}", Local::now().format("%Y%m%d-%H%M%S"));
    let targets: Vec<String> = app
        .camera_configs()
        .into_iter()
        .map(|c| c.id)
        .filter(|id| req.camera_ids.is_empty() || req.camera_ids.contains(id))
        .collect();
    let mut started = Vec::new();
    let mut errors = Vec::new();
    // 영상 주소가 비어 있는 카메라는 먼저 찾아서 채운다 (여러 대면 같이)
    let need_sub = req.profile == "sub";
    let filled = futures_util::future::join_all(targets.iter().map(|id| {
        let app = &app;
        async move {
            if app.session_for_camera(id).is_some() {
                Ok(())
            } else {
                camfix::fill_urls(app, id, need_sub).await
            }
        }
    }))
    .await;
    for (id, filled) in targets.into_iter().zip(filled) {
        if app.session_for_camera(&id).is_some() {
            continue;
        }
        let r = StartRequest {
            camera_id: id.clone(),
            profile: req.profile.clone(),
            transport: req.transport.clone(),
            label: req.label.clone(),
            preview: req.preview.clone().unwrap_or_else(|| "smooth".into()),
            record: req.record.unwrap_or(true),
            group_id: Some(group.clone()),
        };
        match stream::start(&app, r) {
            Ok(s) => started.push(s.id.clone()),
            Err(e) => errors.push(json!({ "camera_id": id, "error": no_url_reason(e, filled) })),
        }
    }
    Json(json!({ "group_id": group, "started": started, "errors": errors })).into_response()
}

async fn stop_all(State(app): State<AppState>) -> Response {
    let ids: Vec<String> = app.active_sessions().iter().map(|s| s.id.clone()).collect();
    for s in app.active_sessions() {
        s.request_stop();
    }
    Json(json!({ "stopping": ids })).into_response()
}

async fn stop_session(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    match app.session(&id) {
        Some(s) => {
            s.request_stop();
            Json(json!({ "stopping": id })).into_response()
        }
        None => api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다"),
    }
}

// ---------------- 프레임 촬영 ----------------

/// 촬영은 별도 작업으로 돌린다. 브라우저를 새로고침하거나 닫아 요청이 끊겨도 촬영·기록은 끝까지 한다
async fn detached<T: Serialize + Send + 'static>(f: impl std::future::Future<Output = T> + Send + 'static) -> Response {
    match tokio::spawn(f).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, format!("촬영 작업 오류: {e}")),
    }
}

async fn take_capture(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(s) = app.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다. 수신을 시작한 뒤 촬영하세요");
    };
    detached(async move { crate::capture::take(&app, &s, None).await }).await
}

#[derive(Deserialize)]
struct DirectCapture {
    #[serde(default)]
    profile: String,
    #[serde(default)]
    transport: String,
}

/// 카메라 기준 촬영: 수신 중이면 그 영상에서, 아니면 잠깐 접속해서 찍는다
async fn camera_capture(State(app): State<AppState>, Path(id): Path<String>, Json(req): Json<DirectCapture>) -> Response {
    if let Some(s) = app.session_for_camera(&id) {
        return detached(async move { crate::capture::take(&app, &s, None).await }).await;
    }
    if app.camera(&id).is_none() {
        return api_error(StatusCode::NOT_FOUND, "등록되지 않은 카메라입니다");
    }
    detached(async move {
        // 영상 주소가 비어 있으면(연결 테스트 없이 저장) 먼저 찾아서 채운다
        let _ = camfix::fill_urls(&app, &id, req.profile == "sub").await;
        let cfg = app.camera(&id).map(|c| c.cfg.clone()).unwrap_or_default();
        let first = crate::capture::take_direct(&app, &cfg, &req.profile, &req.transport, None).await;
        if first.ok {
            return first;
        }
        // 연결이 안 됐으면 카메라 주소가 바뀌었는지 보고, 바뀌었으면 새 주소로 한 번 더 찍는다
        if camfix::follow(&app, &id).await {
            if let Some(c) = app.camera(&id) {
                return crate::capture::take_direct(&app, &c.cfg, &req.profile, &req.transport, None).await;
            }
        }
        first
    })
    .await
}

#[derive(Deserialize)]
struct CaptureQuery {
    session: Option<String>,
    camera: Option<String>,
    trigger: Option<String>,
    limit: Option<usize>,
}

async fn list_captures(State(app): State<AppState>, Query(q): Query<CaptureQuery>) -> Json<Vec<store::CaptureRecord>> {
    Json(app.store.captures(filter(&q.session), filter(&q.camera), filter(&q.trigger), q.limit.unwrap_or(500).clamp(1, 20_000)))
}

async fn capture_image(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(path) = app.store.capture(&id).and_then(|c| c.image) else {
        return api_error(StatusCode::NOT_FOUND, "촬영 이미지가 없습니다");
    };
    match tokio::fs::read(&path).await {
        Ok(b) => ([(header::CONTENT_TYPE, "image/jpeg"), (header::CACHE_CONTROL, "max-age=86400")], b).into_response(),
        Err(_) => api_error(StatusCode::NOT_FOUND, "이미지 파일이 지워졌거나 옮겨졌습니다"),
    }
}

async fn e2e_start(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(s) = app.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다");
    };
    match s.e2e_start() {
        Ok(()) => Json(json!({ "ok": true, "server_ms": crate::e2e::now_ms() })).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

#[derive(Deserialize)]
struct E2eMark {
    /// 화면 색을 바꾼 시각 (Date.now(), ms)
    t: f64,
    white: bool,
}

async fn e2e_mark(State(app): State<AppState>, Path(id): Path<String>, Json(m): Json<E2eMark>) -> Response {
    match app.session(&id) {
        Some(s) => {
            s.e2e_mark(m.t, m.white);
            StatusCode::NO_CONTENT.into_response()
        }
        None => api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다"),
    }
}

async fn e2e_stop(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(s) = app.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다");
    };
    let sum = s.e2e_stop();
    if let Some(sum) = &sum {
        stream::record_e2e_event(&app, &s, sum);
    }
    Json(json!({ "summary": sum })).into_response()
}

/// 뷰어가 잰 실제 지연(화면 깜빡임 → 뷰어 화면에 그 변화가 나오기까지)을 세션 기록에 남긴다.
/// 수신 쪽에서는 영상을 풀지 않으므로, 풀어서 보고 있는 뷰어가 재서 알려 준다.
async fn e2e_report(State(app): State<AppState>, Path(id): Path<String>, Json(mut sum): Json<store::E2eSummary>) -> Response {
    let Some(s) = app.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다");
    };
    let sane = |v: f64| v.is_finite() && (0.0..=60_000.0).contains(&v);
    if sum.count == 0 || sum.count > 100_000 || ![sum.avg_ms, sum.p50_ms, sum.min_ms, sum.max_ms].into_iter().all(sane) {
        return api_error(StatusCode::BAD_REQUEST, "측정값이 없거나 범위를 벗어났습니다");
    }
    sum.measured_at = Some(Local::now());
    s.e2e_set(sum.clone());
    stream::record_e2e_event(&app, &s, &sum);
    Json(json!({ "summary": sum })).into_response()
}

#[derive(Deserialize)]
struct DetailQuery {
    points: Option<usize>,
    /// 최근 N초만 (라이브 화면용)
    tail: Option<i64>,
}

#[derive(Serialize)]
struct SessionDetail {
    session: SessionView,
    series: Vec<StreamPoint>,
    wifi: Vec<WifiPoint>,
    network: Vec<NetPoint>,
    events: Vec<Event>,
}

async fn session_detail(State(app): State<AppState>, Path(id): Path<String>, Query(q): Query<DetailQuery>) -> Response {
    let Some(rec) = app.store.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "세션을 찾을 수 없습니다");
    };
    let view = view_of(&app, rec);
    let points = q.points.unwrap_or(600).clamp(10, 3600);
    let start = view.record.started_at.unwrap_or_else(Local::now);
    let end = view.record.ended_at.unwrap_or_else(Local::now);
    let from = match q.tail {
        Some(t) => (end - Duration::seconds(t)).max(start),
        None => start,
    };
    let series = app.store.stream_series(&id, points, q.tail);
    let wifi = app.store.wifi_series(from, end, points.min(600));
    let network = app.store.network_series(Some(&view.record.camera_id), from, end, points.min(600));
    let events = app.store.events(None, Some(&id), 200);
    Json(SessionDetail { session: view, series, wifi, network, events }).into_response()
}

/// 미리보기: multipart MJPEG (브라우저 <img>로 바로 재생)
async fn preview_mjpeg(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(s) = app.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다");
    };
    let mut rx = s.preview.subscribe();
    let first = rx.borrow_and_update().clone();
    let stream = futures_util::stream::unfold((rx, first), |(mut rx, first)| async move {
        let jpeg = match first {
            Some(j) => j,
            None => loop {
                if rx.changed().await.is_err() {
                    return None;
                }
                match rx.borrow_and_update().clone() {
                    Some(j) => break j,
                    None => return None, // 세션 종료
                }
            },
        };
        let mut part = BytesMut::with_capacity(jpeg.len() + 100);
        part.extend_from_slice(format!("--frame\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n", jpeg.len()).as_bytes());
        part.extend_from_slice(&jpeg);
        part.extend_from_slice(b"\r\n");
        Some((Ok::<Bytes, std::io::Error>(part.freeze()), (rx, None)))
    });
    (
        [(header::CONTENT_TYPE, "multipart/x-mixed-replace; boundary=frame"), (header::CACHE_CONTROL, "no-store")],
        Body::from_stream(stream),
    )
        .into_response()
}

/// 원본 영상(재인코딩 없음, MPEG-TS)을 받은 그대로 넘겨준다 — 네이티브 뷰어용.
/// 최근 키프레임부터 시작하므로 받는 쪽이 바로 풀 수 있다. 받는 쪽이 밀리면 연결이 끊긴다 (수신·녹화는 기다리지 않음).
async fn live_ts(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let Some(s) = app.session(&id) else {
        return api_error(StatusCode::NOT_FOUND, "진행 중인 세션이 아닙니다");
    };
    let rx = s.ts_buffer().subscribe();
    let stream = futures_util::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|b| (Ok::<Bytes, std::io::Error>(b), rx)) });
    // (rx.recv가 가져간 만큼 '밀린 양'에서 뺀다 → 받는 쪽이 따라오는 한 끊기지 않는다)
    ([(header::CONTENT_TYPE, "video/mp2t"), (header::CACHE_CONTROL, "no-store")], Body::from_stream(stream)).into_response()
}

async fn preview_snapshot(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    match app.session(&id).and_then(|s| s.preview.borrow().clone()) {
        Some(j) => ([(header::CONTENT_TYPE, "image/jpeg"), (header::CACHE_CONTROL, "no-store")], j).into_response(),
        None => api_error(StatusCode::NOT_FOUND, "아직 미리보기가 없습니다"),
    }
}

#[derive(Deserialize)]
struct WindowQuery {
    camera: Option<String>,
    minutes: Option<i64>,
    points: Option<usize>,
}

async fn network(State(app): State<AppState>, Query(q): Query<WindowQuery>) -> impl IntoResponse {
    let now = Local::now();
    let from = now - Duration::minutes(q.minutes.unwrap_or(5).clamp(1, 1440));
    Json(app.store.network_series(filter(&q.camera), from, now, q.points.unwrap_or(300).clamp(10, 2000)))
}

async fn wifi(State(app): State<AppState>, Query(q): Query<WindowQuery>) -> impl IntoResponse {
    let now = Local::now();
    let from = now - Duration::minutes(q.minutes.unwrap_or(5).clamp(1, 1440));
    Json(app.store.wifi_series(from, now, q.points.unwrap_or(300).clamp(10, 2000)))
}

#[derive(Deserialize)]
struct EventQuery {
    camera: Option<String>,
    session: Option<String>,
    limit: Option<usize>,
}

async fn events(State(app): State<AppState>, Query(q): Query<EventQuery>) -> impl IntoResponse {
    Json(app.store.events(filter(&q.camera), filter(&q.session), q.limit.unwrap_or(100).clamp(1, 1000)))
}

// ---------------- 카메라 관리 ----------------

#[derive(Serialize)]
struct CameraEditView {
    id: String,
    name: String,
    host: String,
    rtsp_main: String,
    rtsp_sub: String,
    username: String,
    /// 비밀번호는 내보내지 않는다
    has_password: bool,
    receiving: bool,
    trigger: bool,
    subnet: Option<&'static str>,
    /// 주소가 바뀌어도 같은 카메라인지 알아볼 값(ONVIF 고유 값·MAC)을 알고 있는지 → 알면 주소를 스스로 따라간다
    follows: bool,
}

async fn list_cameras(State(app): State<AppState>) -> Json<Vec<CameraEditView>> {
    Json(
        app.camera_configs()
            .into_iter()
            .map(|c| CameraEditView {
                receiving: app.session_for_camera(&c.id).is_some(),
                has_password: !c.password.is_empty(),
                trigger: c.trigger,
                subnet: netscan::subnet_hint(&c.ip()),
                follows: app.cfg.monitor.follow_camera && (!c.uid.is_empty() || !c.mac.is_empty()),
                id: c.id,
                name: c.name,
                host: c.host,
                rtsp_main: c.rtsp_main,
                rtsp_sub: c.rtsp_sub,
                username: c.username,
            })
            .collect(),
    )
}

#[derive(Deserialize)]
struct CameraInput {
    name: String,
    host: String,
    #[serde(default)]
    rtsp_main: String,
    #[serde(default)]
    rtsp_sub: String,
    #[serde(default)]
    username: String,
    /// 수정할 때 비우면(None 또는 "") 기존 비밀번호 유지
    password: Option<String>,
    /// 트리거 때 찍을지 (없으면 찍음)
    trigger: Option<bool>,
    /// 카메라 찾기에서 얻은 장치 고유 값·MAC (찾은 카메라를 추가할 때)
    uid: Option<String>,
    mac: Option<String>,
}

fn apply_cameras(app: &AppState, list: Vec<CameraConfig>) -> Result<(), Response> {
    camfix::apply_list(app, list).map_err(|e| match e {
        camfix::ApplyError::Invalid(m) => api_error(StatusCode::BAD_REQUEST, m),
        camfix::ApplyError::Save(m) => api_error(StatusCode::INTERNAL_SERVER_ERROR, m),
    })
}

/// 저장하기 전에 카메라에 물어서 채우는 것: 비어 있는 영상 주소(연결 테스트 없이 저장할 때)와 카메라를 알아보는 값.
/// 영상 주소를 못 찾았으면 그 이유를 돌려준다 — 저장은 그대로 하고, 수신을 시작할 때 다시 찾는다.
/// 카메라에 접속하느라 몇 초 걸릴 수 있어 편집 잠금을 잡지 않은 채로 부른다.
async fn prepare_camera(app: &AppState, cfg: &mut CameraConfig) -> Option<String> {
    let mut warning = None;
    if cfg.rtsp_main.trim().is_empty() {
        match camfix::detect_urls(app, cfg).await {
            Ok((main, sub)) => {
                cfg.rtsp_main = main;
                if cfg.rtsp_sub.trim().is_empty() {
                    cfg.rtsp_sub = sub;
                }
            }
            Err(e) => warning = Some(format!("저장은 했지만 영상 주소를 찾지 못했습니다 — {e}")),
        }
    }
    camfix::learn(cfg).await;
    warning
}

fn clean_host(h: &str) -> String {
    let s = h.trim();
    let s = ["http://", "https://", "rtsp://"].iter().find_map(|p| s.strip_prefix(p)).unwrap_or(s);
    s.split('/').next().unwrap_or("").to_string()
}

fn next_camera_id(list: &[CameraConfig]) -> String {
    (1..).map(|n| format!("cam{n:02}")).find(|id| !list.iter().any(|c| &c.id == id)).expect("unbounded")
}

/// 카메라는 2대까지 (라이브 화면도 2칸 고정)
pub const MAX_CAMERAS: usize = 2;

async fn add_camera(State(app): State<AppState>, Json(inp): Json<CameraInput>) -> Response {
    let full = || {
        api_error(
            StatusCode::CONFLICT,
            format!("카메라는 {MAX_CAMERAS}대까지 등록합니다. 카메라 주소가 바뀌었으면 [카메라 찾기]의 [주소 바꾸기]나 [수정]으로 고치고, 다른 카메라로 바꾸려면 쓰지 않는 카메라를 지운 뒤 추가하세요"),
        )
    };
    if app.camera_configs().len() >= MAX_CAMERAS {
        return full();
    }
    let mut cfg = CameraConfig {
        name: inp.name.trim().to_string(),
        host: clean_host(&inp.host),
        rtsp_main: inp.rtsp_main.trim().to_string(),
        rtsp_sub: inp.rtsp_sub.trim().to_string(),
        username: inp.username.trim().to_string(),
        password: inp.password.unwrap_or_default(),
        trigger: inp.trigger.unwrap_or(true),
        uid: inp.uid.unwrap_or_default(),
        mac: inp.mac.unwrap_or_default().to_ascii_lowercase(),
        ..Default::default()
    };
    if cfg.host.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "카메라 IP 주소가 비어 있습니다");
    }
    let warning = prepare_camera(&app, &mut cfg).await;
    let _one = app.camera_edit_lock.lock().await;
    let mut list = app.camera_configs();
    if list.len() >= MAX_CAMERAS {
        return full();
    }
    cfg.id = next_camera_id(&list);
    if cfg.name.is_empty() {
        cfg.name = cfg.id.clone();
    }
    list.push(cfg.clone());
    if let Err(r) = apply_cameras(&app, list) {
        return r;
    }
    app.store.record_event(Event::new("camera_added", Some(&cfg.id), format!("카메라 추가: {} ({})", cfg.name, cfg.host)));
    Json(json!({ "id": cfg.id, "warning": warning, "rtsp_main": cfg.rtsp_main, "rtsp_sub": cfg.rtsp_sub })).into_response()
}

async fn update_camera(State(app): State<AppState>, Path(id): Path<String>, Json(inp): Json<CameraInput>) -> Response {
    let busy = || api_error(StatusCode::CONFLICT, "수신 중인 카메라는 바꿀 수 없습니다. 먼저 수신을 중지하세요");
    if app.session_for_camera(&id).is_some() {
        return busy();
    }
    let Some(old) = app.camera(&id).map(|c| c.cfg.clone()) else {
        return api_error(StatusCode::NOT_FOUND, format!("등록되지 않은 카메라: {id}"));
    };
    let mut cfg = old.clone();
    cfg.name = if inp.name.trim().is_empty() { id.clone() } else { inp.name.trim().to_string() };
    cfg.host = clean_host(&inp.host);
    if cfg.host.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "카메라 IP 주소가 비어 있습니다");
    }
    // 주소를 손으로 다른 카메라 것으로 바꿨을 수 있으므로 알아보는 값은 지우고 새 주소의 장치에서 다시 읽는다
    if cfg.host != old.host {
        cfg.uid.clear();
        cfg.mac.clear();
    }
    cfg.rtsp_main = inp.rtsp_main.trim().to_string();
    cfg.rtsp_sub = inp.rtsp_sub.trim().to_string();
    // IP만 고치고 영상 주소는 그대로 둔 채 저장한 경우: 영상 주소 속의 옛 IP도 새 IP로 바꾼다 (안 그러면 옛 주소로 접속한다)
    if cfg.ip() != old.ip() {
        cfg.rtsp_main = camfix::replace_url_host(&cfg.rtsp_main, &old.ip(), &cfg.ip());
        cfg.rtsp_sub = camfix::replace_url_host(&cfg.rtsp_sub, &old.ip(), &cfg.ip());
    }
    cfg.username = inp.username.trim().to_string();
    if let Some(p) = inp.password.filter(|p| !p.is_empty()) {
        cfg.password = p;
    }
    if let Some(t) = inp.trigger {
        cfg.trigger = t;
    }
    let warning = prepare_camera(&app, &mut cfg).await;
    let _one = app.camera_edit_lock.lock().await;
    if app.session_for_camera(&id).is_some() {
        return busy();
    }
    let mut list = app.camera_configs();
    let Some(slot) = list.iter_mut().find(|c| c.id == id) else {
        return api_error(StatusCode::NOT_FOUND, format!("등록되지 않은 카메라: {id}"));
    };
    *slot = cfg.clone();
    if let Err(r) = apply_cameras(&app, list) {
        return r;
    }
    app.store.record_event(Event::new("camera_updated", Some(&id), format!("카메라 수정: {} ({})", cfg.name, cfg.host)));
    Json(json!({ "id": id, "warning": warning, "rtsp_main": cfg.rtsp_main, "rtsp_sub": cfg.rtsp_sub })).into_response()
}

async fn delete_camera(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    let _one = app.camera_edit_lock.lock().await;
    // 수신 중이면 멈추고 녹화를 마무리한 뒤 지운다 (카메라 대역을 바꿔 옛 주소로 재연결만 반복하는 카메라 등)
    if let Some(s) = app.session_for_camera(&id) {
        s.request_stop();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while app.session_for_camera(&id).is_some() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if app.session_for_camera(&id).is_some() {
            return api_error(StatusCode::CONFLICT, "수신이 10초 안에 멈추지 않아 지우지 못했습니다. 잠시 뒤 다시 눌러 주세요");
        }
    }
    let mut list = app.camera_configs();
    let Some(pos) = list.iter().position(|c| c.id == id) else {
        return api_error(StatusCode::NOT_FOUND, format!("등록되지 않은 카메라: {id}"));
    };
    let removed = list.remove(pos);
    if let Err(r) = apply_cameras(&app, list) {
        return r;
    }
    app.store.record_event(Event::new("camera_removed", Some(&id), format!("카메라 삭제: {} (녹화·기록은 그대로 남음)", removed.name)));
    Json(json!({ "removed": id })).into_response()
}

#[derive(Serialize)]
struct DiscoveredView {
    #[serde(flatten)]
    device: onvif::Discovered,
    /// 이 주소로 이미 등록된 카메라 id
    registered: Option<String>,
    /// 다른 주소로 등록돼 있는 같은 카메라의 id (공유기를 껐다 켜 IP가 바뀐 경우) → [주소 바꾸기]
    same_as: Option<String>,
}

/// 같은 네트워크의 카메라 찾기: ONVIF 탐색에 답한 장치 + 탐색에는 답이 없어도 RTSP 포트가 답한 장치 (`camfix::find`)
async fn discover_cameras(State(app): State<AppState>) -> Response {
    let found = camfix::find(camfix::nets()).await;
    // 등록된 주소에서 찾은 장치의 값(ONVIF 고유 값·MAC)을 적어 둔다 — 나중에 주소가 바뀌어도 알아볼 수 있게.
    // 그 장치가 이 카메라라고 믿을 수 있을 때만 적고(`learn_verified`), 수신 중인 카메라는 건드리지 않는다 (다음에 찾을 때 적는다)
    {
        let _one = app.camera_edit_lock.lock().await;
        let mut list = app.camera_configs();
        let mut learned = false;
        for c in list.iter_mut() {
            if app.session_for_camera(&c.id).is_some() {
                continue;
            }
            if let Some(d) = found.iter().find(|d| d.ip == c.ip()) {
                learned |= camfix::learn_verified(c, d);
            }
        }
        if learned {
            let _ = apply_cameras(&app, list);
        }
    }
    let configs = app.camera_configs();
    Json(
        found
            .iter()
            .map(|d| {
                // 이 주소로 등록된 카메라. 등록된 주소에서 잘 보이는 카메라가 덤으로 알리는 주소(169.254.x.x 등)도 그 카메라로 친다
                // (안 그러면 같은 카메라가 '새로 찾은 카메라'로 한 번 더 나온다)
                let registered = configs.iter().find(|c| c.ip() == d.ip).or_else(|| configs.iter().find(|c| camfix::same_device(c, d) && camfix::at_home(c, &found))).map(|c| c.id.clone());
                // 다른 주소로 등록돼 있는 같은 카메라 = 주소가 바뀐 카메라 (두 카메라의 주소가 서로 바뀐 경우에는 이 주소가
                // 다른 카메라로 등록돼 있다). 등록된 주소에서도 보이는 카메라의 덤 주소(169.254.x.x 등)는 해당하지 않는다
                let same_as = configs.iter().find(|c| camfix::moved_to(c, &found).is_some_and(|m| m.ip == d.ip)).map(|c| c.id.clone());
                DiscoveredView { device: d.clone(), registered, same_as }
            })
            .collect::<Vec<_>>(),
    )
    .into_response()
}

#[derive(Deserialize)]
struct MoveRequest {
    /// 새 주소 (IP)
    host: String,
    /// 찾기에서 얻은 장치 고유 값·MAC (있으면 같이 적어 둔다)
    uid: Option<String>,
    mac: Option<String>,
}

/// 등록된 카메라의 주소만 바꾼다 — 공유기를 껐다 켜서 카메라가 다른 IP를 받았을 때.
/// 지우고 다시 등록하지 않아도 되고, 카메라가 가득(2대) 등록돼 있어도 된다.
/// 새 주소가 다른 카메라에 등록돼 있으면 두 카메라의 주소가 서로 바뀐 것으로 보고 맞바꾼다.
async fn move_camera(State(app): State<AppState>, Path(id): Path<String>, Json(req): Json<MoveRequest>) -> Response {
    let busy = |who: &str| api_error(StatusCode::CONFLICT, format!("수신 중인 카메라({who})는 바꿀 수 없습니다. 먼저 수신을 중지하세요"));
    if app.session_for_camera(&id).is_some() {
        return busy(&id);
    }
    let new_ip = clean_host(&req.host);
    let new_ip = new_ip.rsplit_once(':').filter(|(_, p)| !p.is_empty() && p.chars().all(|ch| ch.is_ascii_digit())).map_or(new_ip.clone(), |(h, _)| h.to_string());
    if new_ip.is_empty() {
        return api_error(StatusCode::BAD_REQUEST, "새 주소가 비어 있습니다");
    }
    let Some(before) = app.camera(&id).map(|c| c.cfg.clone()) else {
        return api_error(StatusCode::NOT_FOUND, format!("등록되지 않은 카메라: {id}"));
    };
    // (잠금 밖에서) 새 주소의 장치에서 알아보는 값을 읽어 둔다
    let seen = match new_ip.parse::<std::net::Ipv4Addr>() {
        Ok(ip) => camfix::identity_at(ip, before.rtsp_port()).await,
        Err(_) => (None, None),
    };
    let _one = app.camera_edit_lock.lock().await;
    let mut list = app.camera_configs();
    let Some(me) = list.iter().position(|c| c.id == id) else {
        return api_error(StatusCode::NOT_FOUND, format!("등록되지 않은 카메라: {id}"));
    };
    if app.session_for_camera(&id).is_some() {
        return busy(&id);
    }
    let (old_host, old_ip) = (list[me].host.clone(), list[me].ip());
    let mut swapped_with = None;
    if let Some(other) = list.iter().position(|c| c.id != id && c.ip() == new_ip) {
        if app.session_for_camera(&list[other].id).is_some() {
            return busy(&list[other].id);
        }
        list[other] = camfix::moved_camera(&list[other], &old_ip);
        swapped_with = Some(list[other].id.clone());
    }
    list[me] = camfix::moved_camera(&list[me], &new_ip);
    if let Some(uid) = req.uid.filter(|u| !u.is_empty()).or(seen.0) {
        list[me].uid = uid;
    }
    if let Some(mac) = req.mac.filter(|m| !m.is_empty()).or(seen.1) {
        list[me].mac = mac.to_ascii_lowercase();
    }
    let (name, host) = (list[me].name.clone(), list[me].host.clone());
    if let Err(r) = apply_cameras(&app, list) {
        return r;
    }
    let label = match &swapped_with {
        Some(o) => format!("카메라 주소 바꿈: {name} ({old_host} → {host}) · {o}와 주소를 맞바꿈"),
        None => format!("카메라 주소 바꿈: {name} ({old_host} → {host})"),
    };
    app.store.record_event(Event::new("camera_moved", Some(&id), label));
    Json(json!({ "id": id, "host": host, "swapped_with": swapped_with })).into_response()
}

#[derive(Deserialize)]
struct TestRequest {
    /// 수정 중인 카메라면 id (비밀번호를 비우면 저장된 비밀번호로 시험)
    camera_id: Option<String>,
    #[serde(default)]
    host: String,
    #[serde(default)]
    username: String,
    password: Option<String>,
    rtsp_main: Option<String>,
    rtsp_sub: Option<String>,
}

async fn test_camera(State(app): State<AppState>, Json(req): Json<TestRequest>) -> Response {
    let stored = req.camera_id.as_deref().and_then(|id| app.camera(id)).map(|c| c.cfg.password.clone());
    let password = req.password.filter(|p| !p.is_empty()).or(stored).unwrap_or_default();
    let result = onvif::test_camera(
        &app.client,
        onvif::TestInput {
            host: req.host,
            username: req.username.trim().to_string(),
            password,
            rtsp_main: req.rtsp_main,
            rtsp_sub: req.rtsp_sub,
            ffmpeg: app.ffmpeg.clone(),
        },
    )
    .await;
    Json(result).into_response()
}

// ---------------- 설정: 저장 위치 ----------------

#[derive(Serialize)]
struct SettingsView {
    config_path: String,
    recordings_dir: String,
    logs_dir: String,
    default_recordings_dir: String,
    default_logs_dir: String,
    retention_days: u32,
    recordings: DirStats,
    logs: DirStats,
    free_bytes: Option<u64>,
    segment_minutes: u32,
    stall_ms: u64,
    jank_ms: u64,
}

async fn get_settings(State(app): State<AppState>) -> Response {
    let a = app.clone();
    let view = tokio::task::spawn_blocking(move || {
        let rec = a.store.recordings_dir();
        let logs = a.store.logs_dir();
        SettingsView {
            config_path: a.config_path.display().to_string(),
            default_recordings_dir: a.cfg.storage.default_recordings_dir(&a.home).display().to_string(),
            default_logs_dir: a.cfg.storage.default_logs_dir(&a.home).display().to_string(),
            retention_days: a.retention_days.load(Ordering::Relaxed),
            recordings: store::dir_stats(&rec, &["mp4"]),
            logs: store::dir_stats(&logs, &["jsonl", "log"]),
            free_bytes: store::free_space(&rec),
            recordings_dir: rec.display().to_string(),
            logs_dir: logs.display().to_string(),
            segment_minutes: a.segment_minutes.load(Ordering::Relaxed),
            stall_ms: a.cfg.stream.stall_ms,
            jank_ms: a.cfg.stream.jank_ms,
        }
    })
    .await;
    match view {
        Ok(v) => Json(v).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct StorageRequest {
    /// 비우면 기본 위치
    recordings_dir: String,
    logs_dir: String,
    retention_days: u32,
    /// 녹화 파일 하나의 길이(분). 없으면 그대로 둔다.
    segment_minutes: Option<u32>,
}

fn check_writable(dir: &FsPath) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let probe = dir.join(".capture-vms-write-test");
    std::fs::write(&probe, b"ok").map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(&probe);
    Ok(())
}

/// 저장 위치 변경: 새 녹화·로그부터 새 폴더에 쌓는다. 기존 파일은 옮기지 않는다
/// (대량 파일 이동은 백신이 랜섬웨어로 오인할 수 있음).
async fn set_storage(State(app): State<AppState>, Json(req): Json<StorageRequest>) -> Response {
    let _one = app.storage_lock.lock().await;
    if req.retention_days > 3650 {
        return api_error(StatusCode::BAD_REQUEST, "보존 기간은 0~3650일 사이로 정해 주세요");
    }
    if req.segment_minutes.is_some_and(|m| !(1..=config::SEGMENT_MINUTES_MAX).contains(&m)) {
        return api_error(StatusCode::BAD_REQUEST, format!("녹화 파일 길이는 1~{}분 사이로 정해 주세요", config::SEGMENT_MINUTES_MAX));
    }
    let s = &app.cfg.storage;
    let new_rec = if req.recordings_dir.trim().is_empty() { s.default_recordings_dir(&app.home) } else { config::resolve_path(&app.home, &req.recordings_dir) };
    let new_logs = if req.logs_dir.trim().is_empty() { s.default_logs_dir(&app.home) } else { config::resolve_path(&app.home, &req.logs_dir) };
    for (label, dir) in [("녹화", &new_rec), ("로그", &new_logs)] {
        let d = dir.clone();
        match tokio::task::spawn_blocking(move || check_writable(&d)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return api_error(StatusCode::BAD_REQUEST, format!("{label} 폴더에 쓸 수 없습니다: {} ({e})", dir.display())),
            Err(e) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }
    let (old_rec, old_logs) = (app.store.recordings_dir(), app.store.logs_dir());
    if !store::same_dir(&old_rec, &new_rec) {
        app.store.set_recordings_dir(&new_rec);
    }
    if !store::same_dir(&old_logs, &new_logs) {
        if let Some(log) = &app.app_log {
            if let Err(e) = log.switch(&new_logs) {
                tracing::warn!("프로그램 로그 폴더 변경 실패: {e}");
            }
        }
        app.store.set_logs_dir(&new_logs);
    }
    app.retention_days.store(req.retention_days, Ordering::Relaxed);
    let old_seg = app.segment_minutes.load(Ordering::Relaxed);
    let seg = req.segment_minutes.unwrap_or(old_seg);
    app.segment_minutes.store(seg, Ordering::Relaxed);
    let mut saved = config::save_storage(&app.config_path, req.recordings_dir.trim(), req.logs_dir.trim(), req.retention_days);
    if saved.is_ok() && seg != old_seg {
        saved = config::save_segment_minutes(&app.config_path, seg);
    }
    let warning = saved
        .err()
        .map(|e| format!("지금 실행에는 적용됐지만 설정 파일 저장에 실패했습니다. 다시 시작하면 예전 설정으로 돌아갑니다: {e:#}"));
    app.store.record_event(
        Event::new(
            "storage_changed",
            None,
            format!("저장 설정 변경 · 녹화 {} · 로그 {} · 보존 {}일 · 녹화 파일 {seg}분", new_rec.display(), new_logs.display(), req.retention_days),
        )
        .with_data(json!({ "recordings_from": old_rec, "recordings_to": new_rec, "logs_from": old_logs, "logs_to": new_logs, "segment_minutes_from": old_seg, "segment_minutes_to": seg })),
    );
    Json(json!({ "recordings_dir": new_rec, "logs_dir": new_logs, "retention_days": req.retention_days, "segment_minutes": seg, "warning": warning })).into_response()
}

#[derive(Deserialize)]
struct PickRequest {
    initial: Option<String>,
}

async fn pick_folder(Json(req): Json<PickRequest>) -> Response {
    match tokio::task::spawn_blocking(move || pick_folder_dialog(req.initial.unwrap_or_default())).await {
        Ok(path) => Json(json!({ "path": path })).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[cfg(windows)]
fn pick_folder_dialog(initial: String) -> Option<String> {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const SCRIPT: &str = "[Console]::OutputEncoding=[Text.Encoding]::UTF8;\
        Add-Type -AssemblyName System.Windows.Forms;\
        $d=New-Object System.Windows.Forms.FolderBrowserDialog;\
        $d.Description='저장할 폴더를 선택하세요';$d.ShowNewFolderButton=$true;\
        if($env:CAPTURE_VMS_PICK_INIT -and (Test-Path -LiteralPath $env:CAPTURE_VMS_PICK_INIT)){$d.SelectedPath=$env:CAPTURE_VMS_PICK_INIT};\
        $o=New-Object System.Windows.Forms.Form -Property @{TopMost=$true;ShowInTaskbar=$false};\
        $r=$d.ShowDialog($o);$o.Dispose();\
        if($r -eq [System.Windows.Forms.DialogResult]::OK){[Console]::Out.Write($d.SelectedPath)}";
    let out = std::process::Command::new("powershell")
        .args(["-NoProfile", "-STA", "-NonInteractive", "-Command", SCRIPT])
        .env("CAPTURE_VMS_PICK_INIT", initial)
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .ok()?;
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!path.is_empty()).then_some(path)
}

#[cfg(not(windows))]
fn pick_folder_dialog(_initial: String) -> Option<String> {
    None
}

#[derive(Deserialize)]
struct OpenRequest {
    /// recordings | logs | config | session | recording | capture | exports | export
    which: String,
    id: Option<String>,
}

async fn open_path(State(app): State<AppState>, Json(req): Json<OpenRequest>) -> Response {
    let target = match req.which.as_str() {
        "recordings" => app.store.recordings_dir(),
        "logs" => app.store.logs_dir(),
        "config" => {
            return match std::process::Command::new("notepad").arg(&app.config_path).spawn() {
                Ok(_) => Json(json!({ "ok": true })).into_response(),
                Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, format!("열 수 없습니다: {e}")),
            }
        }
        "session" => {
            let dir = req.id.as_deref().and_then(|id| app.store.session(id)).and_then(|s| s.recording_dir).map(|d| rec_dir_of(&app, &d));
            match dir {
                Some(d) if d.exists() => d,
                _ => return api_error(StatusCode::NOT_FOUND, "이 세션의 녹화 폴더가 없습니다"),
            }
        }
        // 세션 기록이 없는 녹화 폴더: id = "<날짜>/<폴더>"
        "recording" => match req.id.as_deref().and_then(|k| k.split_once('/')).and_then(|(day, folder)| rec_folder_path(&app, day, folder)) {
            Some(d) => d,
            None => return api_error(StatusCode::NOT_FOUND, "녹화 폴더가 없습니다"),
        },
        "capture" => {
            // 이미지가 있는 폴더를 열고 그 파일을 선택해 둔다
            let img = req.id.as_deref().and_then(|id| app.store.capture(id)).and_then(|c| c.image);
            return match img {
                Some(p) if FsPath::new(&p).exists() => match std::process::Command::new("explorer").arg(format!("/select,{p}")).spawn() {
                    Ok(_) => Json(json!({ "ok": true })).into_response(),
                    Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, format!("열 수 없습니다: {e}")),
                },
                _ => api_error(StatusCode::NOT_FOUND, "이미지 파일이 없습니다"),
            };
        }
        "exports" => {
            let d = export::exports_dir(&app);
            let _ = std::fs::create_dir_all(&d);
            d
        }
        "export" => {
            // 내보낸 파일을 연결된 프로그램(엑셀·PDF 보기)으로 연다
            let name = req.id.unwrap_or_default();
            let p = export::exports_dir(&app).join(&name);
            if !export::safe_export_name(&name) || !p.is_file() {
                return api_error(StatusCode::NOT_FOUND, "파일이 없습니다");
            }
            p
        }
        _ => return api_error(StatusCode::BAD_REQUEST, "which는 recordings | logs | config | session | capture | exports | export 중 하나입니다"),
    };
    match std::process::Command::new("explorer").arg(&target).spawn() {
        Ok(_) => Json(json!({ "ok": true })).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, format!("열 수 없습니다: {e}")),
    }
}

async fn log_file(State(app): State<AppState>, Path(name): Path<String>) -> Response {
    let ok_name = !name.contains(['/', '\\']) && !name.contains("..") && (name.ends_with(".jsonl") || name.ends_with(".log"));
    if !ok_name {
        return api_error(StatusCode::BAD_REQUEST, "잘못된 로그 파일 이름");
    }
    match tokio::fs::read(app.store.logs_dir().join(&name)).await {
        Ok(bytes) => ([(header::CONTENT_TYPE, "text/plain; charset=utf-8"), (header::CACHE_CONTROL, "no-store")], bytes).into_response(),
        Err(_) => api_error(StatusCode::NOT_FOUND, "로그 파일이 아직 없습니다"),
    }
}

/// 문제 진단: 이 PC·카메라별로 지금 상태를 점검해 정상·주의·문제와 해결 방법을 돌려준다 (1~2초 걸림)
async fn diagnose_all(State(app): State<AppState>) -> Response {
    Json(crate::diagnose::run(&app).await).into_response()
}

async fn shutdown(State(app): State<AppState>) -> Response {
    app.request_shutdown();
    Json(json!({ "ok": true })).into_response()
}

// ---------------- 트리거 (동시 촬영) ----------------

async fn trigger_status(State(app): State<AppState>) -> Response {
    Json(trigger::status(&app)).into_response()
}

#[derive(Deserialize, Default)]
struct FireRequest {
    /// button(대시보드) | http(다른 프로그램)
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    detail: Option<String>,
}

/// 신호 한 번 → 대상 카메라 모두 촬영. 다 찍을 때까지 기다렸다가 결과를 돌려준다.
async fn trigger_fire(State(app): State<AppState>, body: Bytes) -> Response {
    let req: FireRequest = if body.iter().all(|b| b.is_ascii_whitespace()) {
        FireRequest::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => return api_error(StatusCode::BAD_REQUEST, format!("요청 형식 오류: {e}")),
        }
    };
    let source = match req.source.as_deref() {
        Some("button") => "button",
        _ => "http",
    };
    detached(async move { trigger::fire(&app, source, req.detail).await }).await
}

async fn trigger_config(State(app): State<AppState>, Json(cfg): Json<TriggerConfig>) -> Response {
    let _one = app.trigger_edit_lock.lock().await;
    let mut cfg = cfg;
    cfg.tcp_bind = cfg.tcp_bind.trim().to_string();
    cfg.modbus_host = cfg.modbus_host.trim().to_string();
    if let Err(e) = cfg.validate() {
        return api_error(StatusCode::BAD_REQUEST, format!("{e:#}"));
    }
    if let Err(e) = config::save_trigger(&app.config_path, &cfg) {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, format!("설정 파일 저장 실패: {e:#}"));
    }
    let mut parts = vec![format!("무시 간격 {}ms", cfg.min_interval_ms)];
    parts.push(if cfg.tcp_enabled { format!("TCP {}", cfg.tcp_bind) } else { "TCP 끔".into() });
    parts.push(if cfg.modbus_enabled { format!("Modbus {} {} {}", cfg.modbus_addr(), cfg.modbus_kind, cfg.modbus_address) } else { "Modbus 끔".into() });
    *app.trigger.cfg.write().unwrap() = cfg;
    trigger::apply(&app);
    app.store.record_event(Event::new("trigger_config", None, format!("트리거 설정 변경: {}", parts.join(" · "))));
    // 바로 뒤 상태(포트 열림 여부)를 보여 주도록 잠깐 기다린다
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    Json(trigger::status(&app)).into_response()
}

#[derive(Deserialize)]
struct AutoRequest {
    interval_ms: u64,
    #[serde(default)]
    count: u64,
}

async fn trigger_auto_start(State(app): State<AppState>, Json(req): Json<AutoRequest>) -> Response {
    match trigger::start_auto(&app, req.interval_ms, req.count) {
        Ok(()) => Json(trigger::status(&app)).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

async fn trigger_auto_stop(State(app): State<AppState>) -> Response {
    trigger::stop_auto(&app, true);
    Json(trigger::status(&app)).into_response()
}

async fn trigger_selftest(State(app): State<AppState>) -> Response {
    match trigger::self_test(&app).await {
        Ok(msg) => Json(json!({ "ok": true, "message": msg })).into_response(),
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

#[derive(Deserialize)]
struct TriggerListQuery {
    camera: Option<String>,
    limit: Option<usize>,
}

async fn list_triggers(State(app): State<AppState>, Query(q): Query<TriggerListQuery>) -> Json<Vec<TriggerRecord>> {
    Json(app.store.triggers(filter(&q.camera), q.limit.unwrap_or(300).clamp(1, 20_000)))
}

// ---------------- 네트워크: IP로 찾기·진단 ----------------

async fn net_adapters() -> Response {
    match tokio::task::spawn_blocking(netscan::adapters).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct DiagnoseRequest {
    ip: String,
}

async fn net_diagnose(Json(req): Json<DiagnoseRequest>) -> Response {
    Json(netscan::diagnose(&req.ip).await).into_response()
}

#[derive(Deserialize)]
struct ScanRequest {
    target: String,
}

async fn net_scan(State(app): State<AppState>, Json(req): Json<ScanRequest>) -> Response {
    match netscan::scan(&app.client, &req.target).await {
        Ok(mut r) => {
            let configs = app.camera_configs();
            let hosts: Vec<serde_json::Value> = r
                .hosts
                .drain(..)
                .map(|h| {
                    let registered = configs.iter().find(|c| c.ip() == h.ip).map(|c| c.id.clone());
                    let mut v = serde_json::to_value(&h).unwrap_or_default();
                    v["registered"] = json!(registered);
                    v
                })
                .collect();
            app.store.record_event(Event::new(
                "net_scan",
                None,
                format!("IP로 찾기: {} ({}개 주소 · 응답 {}대 · {:.1}초)", req.target.trim(), r.scanned, hosts.len(), r.elapsed_ms / 1000.0),
            ));
            Json(json!({ "scanned": r.scanned, "elapsed_ms": r.elapsed_ms, "hosts": hosts, "diagnosis": r.diagnosis, "adapters": netscan::adapters() })).into_response()
        }
        Err(e) => api_error(StatusCode::BAD_REQUEST, e),
    }
}

// ---------------- 원시 데이터 ----------------

/// 쿼리 문자열은 flatten을 쓰지 않는다 (serde_urlencoded + flatten은 숫자 필드를 못 읽음)
#[derive(Deserialize)]
struct RawQuery {
    kind: String,
    from: Option<String>,
    to: Option<String>,
    camera: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
}

async fn raw_rows(State(app): State<AppState>, Query(q): Query<RawQuery>) -> Response {
    let rq = export::RangeQuery { from: q.from.clone(), to: q.to.clone(), camera: q.camera.clone() };
    let range = match rq.resolve() {
        Ok(r) => r,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, e),
    };
    let Some(kind) = store::Kind::parse(&q.kind) else {
        return api_error(StatusCode::BAD_REQUEST, format!("알 수 없는 종류: {}", q.kind));
    };
    let a = app.clone();
    let kind_s = q.kind.clone();
    let rows = tokio::task::spawn_blocking(move || -> Result<Arc<Vec<serde_json::Value>>, String> {
        // 같은 조건·같은 파일 크기면 앞서 읽은 것을 쓴다 (쪽 넘길 때). 진행 중인 세션 값은 늘 바뀌므로 세션은 매번 읽는다
        let key = format!("{kind_s}|{}|{}|{:?}|{}", range.from.timestamp_millis(), range.to.timestamp_millis(), range.camera, a.store.range_signature(kind, range.from, range.to));
        if kind != store::Kind::Sessions {
            if let Some((k, v)) = a.raw_cache.lock().unwrap().as_ref() {
                if *k == key {
                    return Ok(v.clone());
                }
            }
        }
        let v = Arc::new(export::raw_rows(&a, &kind_s, &range)?);
        // 끝 시각이 비면 '지금'이라 다음 쪽에서 키가 달라져 다시 쓰이지 않는다. 그런 결과(수십~수백 MB)는 들고 있지 않는다
        let reusable = kind != store::Kind::Sessions && q.to.as_deref().is_some_and(|t| !t.trim().is_empty());
        *a.raw_cache.lock().unwrap() = reusable.then(|| (key, v.clone()));
        Ok(v)
    })
    .await;
    match rows {
        Ok(Ok(v)) => {
            let off = q.offset.unwrap_or(0).min(v.len());
            let lim = q.limit.unwrap_or(200).clamp(1, 5000);
            let page: Vec<&serde_json::Value> = v[off..].iter().take(lim).collect();
            Json(json!({ "total": v.len(), "offset": off, "rows": page })).into_response()
        }
        Ok(Err(e)) => api_error(StatusCode::BAD_REQUEST, e),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn raw_summary(State(app): State<AppState>, Query(q): Query<export::RangeQuery>) -> Response {
    let range = match q.resolve() {
        Ok(r) => r,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, e),
    };
    let a = app.clone();
    match tokio::task::spawn_blocking(move || {
        let d = export::load(&a, &range, false);
        export::summarize(&a, &range, &d)
    })
    .await
    {
        Ok(s) => Json(s).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct ExportRequest {
    #[serde(flatten)]
    range: export::RangeQuery,
    #[serde(flatten)]
    meta: export::ReportMeta,
    /// 엑셀에 수신 1초 기록을 넣을지 (기본: 넣음, 기간이 길면 자동으로 뺌)
    #[serde(default)]
    stream: Option<bool>,
}

async fn export_xlsx(State(app): State<AppState>, Json(req): Json<ExportRequest>) -> Response {
    let range = match req.range.resolve() {
        Ok(r) => r,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, e),
    };
    let a = app.clone();
    let res = tokio::task::spawn_blocking(move || -> Result<(String, std::path::PathBuf), String> {
        let d = export::load(&a, &range, req.stream.unwrap_or(true));
        let s = export::summarize(&a, &range, &d);
        let name = export::export_name("Raw데이터_목록", &range, "xlsx");
        let path = export::exports_dir(&a).join(&name);
        export::write_xlsx(&path, &s, &d, &req.meta)?;
        Ok((name, path))
    })
    .await;
    export_result(&app, res, "Excel")
}

fn export_result(app: &App, res: Result<Result<(String, std::path::PathBuf), String>, tokio::task::JoinError>, label: &str) -> Response {
    match res {
        Ok(Ok((name, path))) => {
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            app.store.record_event(Event::new("export", None, format!("{label} 내보내기: {name} ({:.1} MB)", bytes as f64 / 1e6)));
            Json(json!({ "name": name, "path": path, "bytes": bytes })).into_response()
        }
        Ok(Err(e)) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// 보고서 페이지 주소 (PDF 인쇄용: 이 PC에서 대시보드가 열린 주소)
fn report_url(app: &App, range: &export::Range, meta: &export::ReportMeta) -> String {
    let mut addr: std::net::SocketAddr = app.cfg.server.bind.parse().unwrap_or_else(|_| "127.0.0.1:8787".parse().unwrap());
    if addr.ip().is_unspecified() {
        addr.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    }
    let mut u = reqwest::Url::parse(&format!("http://{addr}/report")).expect("url");
    {
        let mut q = u.query_pairs_mut();
        q.append_pair("from", &range.from.to_rfc3339()).append_pair("to", &range.to.to_rfc3339());
        if let Some(c) = &range.camera {
            q.append_pair("camera", c);
        }
        q.append_pair("site", &meta.site).append_pair("tester", &meta.tester).append_pair("note", &meta.note).append_pair("images", if meta.images { "1" } else { "0" });
    }
    u.to_string()
}

async fn export_pdf(State(app): State<AppState>, Json(req): Json<ExportRequest>) -> Response {
    let range = match req.range.resolve() {
        Ok(r) => r,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, e),
    };
    let name = export::export_name("보고서", &range, "pdf");
    let path = export::exports_dir(&app).join(&name);
    let url = report_url(&app, &range, &req.meta);
    let res = export::print_pdf(&url, &path).await.map(|_| (name, path));
    export_result(&app, Ok(res), "PDF")
}

async fn list_exports(State(app): State<AppState>) -> Response {
    let dir = export::exports_dir(&app);
    let d2 = dir.clone();
    let files = tokio::task::spawn_blocking(move || export::list_exports(&d2)).await.unwrap_or_default();
    Json(json!({ "dir": dir, "files": files, "pdf_browser": export::find_browser().is_some() })).into_response()
}

async fn download_export(State(app): State<AppState>, Path(name): Path<String>) -> Response {
    if !export::safe_export_name(&name) {
        return api_error(StatusCode::BAD_REQUEST, "잘못된 파일 이름");
    }
    match tokio::fs::read(export::exports_dir(&app).join(&name)).await {
        Ok(bytes) => {
            let ctype = if name.ends_with(".pdf") { "application/pdf" } else { "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" };
            let enc: String = name.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-_.".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") }).collect();
            let disp = format!("attachment; filename=\"export.{}\"; filename*=UTF-8''{enc}", if name.ends_with(".pdf") { "pdf" } else { "xlsx" });
            ([(header::CONTENT_TYPE, ctype.to_string()), (header::CONTENT_DISPOSITION, disp), (header::CACHE_CONTROL, "no-store".to_string())], bytes).into_response()
        }
        Err(_) => api_error(StatusCode::NOT_FOUND, "파일이 없습니다"),
    }
}

#[derive(Deserialize)]
struct ReportQuery {
    from: Option<String>,
    to: Option<String>,
    camera: Option<String>,
    #[serde(default)]
    site: String,
    #[serde(default)]
    tester: String,
    #[serde(default)]
    note: String,
    #[serde(default)]
    images: Option<String>,
}

async fn report_page(State(app): State<AppState>, Query(q): Query<ReportQuery>) -> Response {
    let rq = export::RangeQuery { from: q.from.clone(), to: q.to.clone(), camera: q.camera.clone() };
    let range = match rq.resolve() {
        Ok(r) => r,
        Err(e) => return api_error(StatusCode::BAD_REQUEST, e),
    };
    let meta = export::ReportMeta { site: q.site, tester: q.tester, note: q.note, images: matches!(q.images.as_deref(), Some("1" | "true")) };
    let a = app.clone();
    match tokio::task::spawn_blocking(move || export::report_html(&a, &range, &meta)).await {
        Ok(html) => ([(header::CONTENT_TYPE, "text/html; charset=utf-8"), (header::CACHE_CONTROL, "no-store")], html).into_response(),
        Err(e) => api_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_video_ranges() {
        // 브라우저 <video>가 보내는 구간 요청
        assert_eq!(parse_range("bytes=0-", 1000), Some(Ok((0, 999))));
        assert_eq!(parse_range("bytes=100-199", 1000), Some(Ok((100, 199))));
        assert_eq!(parse_range("bytes=900-5000", 1000), Some(Ok((900, 999))));
        assert_eq!(parse_range("bytes=-100", 1000), Some(Ok((900, 999))));
        assert_eq!(parse_range("bytes=1000-", 1000), Some(Err(())));
        assert_eq!(parse_range("items=0-1", 1000), None);
    }
}

#[cfg(test)]
mod move_tests {
    use super::*;
    use crate::camfix::{moved_camera, replace_url_host};

    #[test]
    fn replaces_only_the_camera_address_in_a_url() {
        assert_eq!(replace_url_host("rtsp://192.168.1.9/media/1/1/Profile1", "192.168.1.9", "192.168.1.3"), "rtsp://192.168.1.3/media/1/1/Profile1");
        assert_eq!(replace_url_host("rtsp://192.168.1.9:8554/a?b=1", "192.168.1.9", "192.168.1.3"), "rtsp://192.168.1.3:8554/a?b=1");
        assert_eq!(replace_url_host("rtsp://user@192.168.1.9:554/x", "192.168.1.9", "10.0.0.2"), "rtsp://user@10.0.0.2:554/x");
        // 다른 장치(녹화기 등)를 가리키는 주소면 그대로 둔다
        assert_eq!(replace_url_host("rtsp://192.168.1.200/x", "192.168.1.9", "192.168.1.3"), "rtsp://192.168.1.200/x");
        assert_eq!(replace_url_host("", "192.168.1.9", "192.168.1.3"), "");
    }

    #[test]
    fn moving_a_camera_keeps_everything_but_the_address() {
        let c = CameraConfig {
            id: "cam01".into(),
            name: "입구".into(),
            host: "192.168.1.9:8080".into(),
            rtsp_main: "rtsp://192.168.1.9/media/1/1/Profile1".into(),
            rtsp_sub: "rtsp://192.168.1.9/media/1/2/Profile2".into(),
            username: "admin".into(),
            password: "secret".into(),
            uid: "urn:uuid:x".into(),
            ..Default::default()
        };
        let m = moved_camera(&c, "192.168.1.3");
        assert_eq!(m.host, "192.168.1.3:8080");
        assert_eq!(m.rtsp_main, "rtsp://192.168.1.3/media/1/1/Profile1");
        assert_eq!(m.rtsp_sub, "rtsp://192.168.1.3/media/1/2/Profile2");
        assert_eq!((m.id, m.name, m.username, m.password, m.uid, m.trigger), (c.id, c.name, c.username, c.password, c.uid, c.trigger));
    }
}
