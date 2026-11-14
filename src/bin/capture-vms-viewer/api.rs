//! 엔진(capture-vms.exe)과의 통신. 화면 스레드를 막지 않도록 따로 도는 스레드에서 상태를 주기적으로 읽고 명령을 보낸다.

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use eframe::egui;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

// ---------------- 엔진이 주는 값 (쓰는 것만) ----------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Status {
    pub version: String,
    /// 엔진의 기록 폴더 (뷰어 로그도 여기에 둔다)
    pub logs_dir: String,
    pub max_cameras: usize,
    pub ffmpeg: Ffmpeg,
    pub free_bytes: Option<u64>,
    pub low_disk: bool,
    pub wifi: Option<Wifi>,
    pub cameras: Vec<Camera>,
    pub location: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Ffmpeg {
    pub found: bool,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Wifi {
    pub state: String,
    pub ssid: Option<String>,
    pub band: Option<String>,
    pub signal_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CamStatus {
    pub online: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Camera {
    pub id: String,
    pub name: String,
    pub host: String,
    pub has_main: bool,
    pub has_sub: bool,
    pub status: CamStatus,
    pub session: Option<String>,
    pub live: Option<Live>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Live {
    pub session_id: String,
    pub profile: String,
    pub transport: String,
    pub record: bool,
    /// connecting | receiving | delayed | stalled | reconnecting | stopping
    pub state: String,
    pub delay_3s_ms: Option<f64>,
    pub fps: f64,
    pub kbps: f64,
    pub camera_fps: Option<f64>,
    pub stalls: u64,
    pub reconnects: u64,
    pub missing_pct: Option<f64>,
    pub elapsed_s: f64,
    pub recorded_bytes: u64,
    pub codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub last_error: Option<String>,
}

/// 그래프용 1초 단위 수신 지표
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Point {
    pub fps: f64,
    pub kbps: f64,
    pub interval_max_ms: Option<f64>,
    pub delay_ms: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub recordings_dir: String,
    pub logs_dir: String,
    pub default_recordings_dir: String,
    pub default_logs_dir: String,
    pub retention_days: u32,
    pub segment_minutes: u32,
}

/// 설정의 카메라 목록 한 줄 (비밀번호는 엔진이 내보내지 않는다)
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CamEdit {
    pub id: String,
    pub name: String,
    pub host: String,
    pub rtsp_main: String,
    pub rtsp_sub: String,
    pub username: String,
    pub has_password: bool,
    pub receiving: bool,
    pub trigger: bool,
    /// same | routed(다른 대역, 공유기 경유)
    pub subnet: Option<String>,
    /// 주소가 바뀌어도 엔진이 같은 카메라를 찾아 따라갈 수 있는지 (알아보는 값을 알고 있음)
    pub follows: bool,
}

/// [카메라 찾기]로 찾은 카메라
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Found {
    pub ip: String,
    pub name: Option<String>,
    pub model: Option<String>,
    pub manufacturer: Option<String>,
    /// 이미 등록돼 있으면 그 카메라 id
    pub registered: Option<String>,
    /// 장치 고유 값 (추가·주소 바꾸기 때 같이 보내 적어 둔다)
    pub uid: Option<String>,
    /// 다른 주소로 등록돼 있는 같은 카메라의 id (IP가 바뀐 것)
    pub same_as: Option<String>,
    /// MAC 주소 (같은 대역일 때)
    pub mac: Option<String>,
    /// 찾은 방법: onvif(탐색에 답함) | rtsp(탐색에는 답이 없고 RTSP 포트만 응답)
    pub via: String,
}

/// 문제 진단 결과 (엔진의 `/api/diagnose`)
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DiagCheck {
    pub title: String,
    /// info | ok | warn | bad
    pub level: String,
    pub summary: String,
    pub advice: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DiagGroup {
    pub title: String,
    pub camera_id: Option<String>,
    pub checks: Vec<DiagCheck>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DiagReport {
    pub at: String,
    pub groups: Vec<DiagGroup>,
}

/// 오래 걸리는 요청의 진행 상태
#[derive(Debug, Clone, Default)]
pub enum Job<T> {
    #[default]
    Idle,
    Running,
    Done(T),
    Failed(String),
}

/// 수신을 시작할 때의 선택
#[derive(Debug, Clone)]
pub struct StartOpts {
    pub profile: String,
    pub transport: String,
    pub record: bool,
}

pub enum Cmd {
    StartAll(StartOpts),
    StopAll,
    Start(String, StartOpts),
    Stop(String),
    /// 카메라 한 대 촬영 (수신 중이면 받고 있는 영상에서, 아니면 잠깐 접속해서)
    Capture(String, StartOpts),
    /// 동시 촬영 (트리거 대상 카메라 모두)
    Trigger,
    SetLocation(String),
    /// 뷰어가 잰 실제 지연 요약을 세션 기록에 남긴다
    E2eReport(String, Value),
    LoadSettings,
    SetSegment(u32),
    LoadCameras,
    /// 같은 네트워크의 ONVIF 카메라 찾기 (사용자가 눌렀을 때만)
    Discover,
    /// 연결 테스트 (요청 본문 그대로)
    TestCamera(Value),
    /// 카메라 추가(id 없음)·수정(id 있음)
    SaveCamera(Option<String>, Value),
    DeleteCamera(String),
    /// 등록된 카메라의 주소만 바꾼다 (id, 새 IP, 장치 고유 값, MAC) — 공유기를 껐다 켜 카메라 IP가 바뀌었을 때
    MoveCamera(String, String, Option<String>, Option<String>),
    /// 문제 진단 한 번 (1~2초)
    Diagnose,
    /// IP·범위·대역으로 카메라 찾기 (포트 응답만 봄, 비밀번호는 넣어 보지 않음)
    Scan(String),
    /// 엔진까지 끈다 (수신·녹화 종료)
    Shutdown,
}

pub struct Note {
    pub text: String,
    pub ok: bool,
    pub at: Instant,
}

#[derive(Default)]
pub struct Inner {
    pub status: Option<Status>,
    /// 엔진에 닿지 않을 때의 안내
    pub error: Option<String>,
    /// 세션 id → 최근 2분 지표
    pub series: HashMap<String, Vec<Point>>,
    pub notes: VecDeque<Note>,
    pub settings: Option<Settings>,
    pub engine_gone: bool,
    /// 설정 → 카메라
    pub cam_list: Option<Vec<CamEdit>>,
    pub found: Job<Vec<Found>>,
    /// 연결 테스트 결과 (엔진이 준 그대로)
    pub test: Job<Value>,
    /// 저장·삭제가 성공할 때마다 1씩 는다 (편집 창을 닫는 신호)
    pub cam_saved: u64,
    /// 카메라 저장 요청이 진행 중 (영상 주소를 찾느라 수 초 걸릴 수 있다)
    pub cam_saving: bool,
    pub diag: Job<DiagReport>,
    /// IP로 찾기 결과 (엔진이 준 그대로)
    pub scan: Job<Value>,
}

#[derive(Default)]
pub struct Shared {
    pub inner: Mutex<Inner>,
    ctx: OnceLock<egui::Context>,
}

impl Shared {
    pub fn set_ctx(&self, ctx: egui::Context) {
        let _ = self.ctx.set(ctx);
    }

    fn repaint(&self) {
        if let Some(c) = self.ctx.get() {
            c.request_repaint();
        }
    }

    pub fn note(&self, text: impl Into<String>, ok: bool) {
        let mut i = self.inner.lock().unwrap();
        i.notes.push_back(Note { text: text.into(), ok, at: Instant::now() });
        while i.notes.len() > 30 {
            i.notes.pop_front();
        }
        drop(i);
        self.repaint();
    }
}

// ---------------- 통신 스레드 ----------------

pub fn spawn(base: String, home: PathBuf, shared: Arc<Shared>) -> mpsc::UnboundedSender<Cmd> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::Builder::new()
        .name("engine-api".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("tokio runtime");
            rt.block_on(run(base, home, shared, rx));
        })
        .expect("통신 스레드");
    tx
}

async fn get<T: serde::de::DeserializeOwned>(c: &reqwest::Client, url: String, timeout: Duration) -> Result<T, String> {
    let r = c.get(url).timeout(timeout).send().await.map_err(|e| e.to_string())?;
    let b = r.bytes().await.map_err(|e| e.to_string())?;
    serde_json::from_slice(&b).map_err(|e| e.to_string())
}

/// POST. 엔진이 돌려준 오류 문구(`error`)가 있으면 그것을 오류로 돌려준다
async fn send(c: &reqwest::Client, method: reqwest::Method, url: String, body: Option<Value>) -> Result<Value, String> {
    // 연결 테스트는 ONVIF 확인과 스트림 접속까지 해서 오래 걸릴 수 있다
    let mut req = c.request(method, url).timeout(Duration::from_secs(90));
    if let Some(b) = body {
        req = req.header("Content-Type", "application/json").body(b.to_string());
    }
    let r = req.send().await.map_err(|e| format!("엔진에 닿지 않습니다 ({e})"))?;
    let ok = r.status().is_success();
    let b = r.bytes().await.map_err(|e| e.to_string())?;
    let v: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
    if ok {
        Ok(v)
    } else {
        Err(v.get("error").and_then(|e| e.as_str()).unwrap_or("요청을 처리하지 못했습니다").to_string())
    }
}

/// 엔진이 계속 응답하지 않을 때 다시 띄워 보는 간격
const ENGINE_RESTART_EVERY: Duration = Duration::from_secs(15);

/// 엔진이 꺼져 있으면 같은 폴더의 capture-vms.exe를 창 없이 띄운다 (브라우저는 열지 않음)
fn start_engine(home: &PathBuf) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let engine = exe.parent().ok_or("실행 파일 폴더를 알 수 없습니다")?.join(if cfg!(windows) { "capture-vms.exe" } else { "capture-vms" });
    if !engine.is_file() {
        return Err(format!("엔진 프로그램이 없습니다: {}", engine.display()));
    }
    let mut cmd = std::process::Command::new(&engine);
    cmd.arg("--no-browser").arg("--home").arg(home).stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // 콘솔 창 띄우지 않기
    }
    // 뷰어를 닫아도 엔진은 계속 돈다 (핸들을 버려도 프로세스는 남는다)
    cmd.spawn().map(|_| ()).map_err(|e| format!("엔진을 시작하지 못했습니다: {e}"))
}

async fn run(base: String, home: PathBuf, shared: Arc<Shared>, mut rx: mpsc::UnboundedReceiver<Cmd>) {
    let client = reqwest::Client::builder().connect_timeout(Duration::from_millis(1500)).tcp_nodelay(true).build().expect("HTTP 클라이언트");
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut n = 0u64;
    let mut fails = 0u32;
    // 엔진을 마지막으로 띄운 때. 엔진이 죽을 때마다 다시 띄우되(수신은 엔진이 스스로 이어받는다), 몰아서 띄우지는 않는다
    let mut engine_started_at: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                n += 1;
                match get::<Status>(&client, format!("{base}/api/status"), Duration::from_secs(3)).await {
                    Ok(st) => {
                        crate::vlog::set_dir(&st.logs_dir);
                        if fails > 0 || n == 1 {
                            crate::vlog::log(format!("엔진에 연결됨 (v{})", st.version));
                        }
                        fails = 0;
                        let sessions: Vec<String> = st.cameras.iter().filter_map(|c| c.session.clone()).collect();
                        {
                            let mut i = shared.inner.lock().unwrap();
                            i.status = Some(st);
                            i.error = None;
                            i.series.retain(|k, _| sessions.contains(k));
                        }
                        // 그래프: 2초마다 최근 2분
                        if n % 4 == 1 {
                            for sid in sessions {
                                #[derive(Deserialize)]
                                struct Detail { #[serde(default)] series: Vec<Point> }
                                if let Ok(d) = get::<Detail>(&client, format!("{base}/api/sessions/{sid}?tail=120&points=120"), Duration::from_secs(3)).await {
                                    shared.inner.lock().unwrap().series.insert(sid, d.series);
                                }
                            }
                        }
                        shared.repaint();
                    }
                    Err(e) => {
                        fails += 1;
                        if fails == 1 {
                            crate::vlog::log(format!("엔진에 닿지 않음: {e}"));
                        }
                        let mut msg = format!("엔진({base})에 연결하는 중입니다…");
                        if shared.inner.lock().unwrap().engine_gone {
                            msg = "엔진을 종료했습니다. 뷰어를 닫으세요.".into();
                        } else if fails >= 2 && engine_started_at.map_or(true, |t| t.elapsed() >= ENGINE_RESTART_EVERY) {
                            engine_started_at = Some(Instant::now());
                            match start_engine(&home) {
                                Ok(()) => crate::vlog::log("엔진이 응답하지 않아 엔진을 실행함"),
                                Err(err) => {
                                    crate::vlog::log(format!("엔진을 실행하지 못함: {err}"));
                                    msg = err;
                                }
                            }
                        } else if fails > 40 {
                            msg = format!("엔진에 닿지 않습니다: {e}");
                        }
                        let mut i = shared.inner.lock().unwrap();
                        if fails >= 3 {
                            i.status = None;
                        }
                        i.error = Some(msg);
                        drop(i);
                        shared.repaint();
                    }
                }
            }
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { break };
                let (c, b, s) = (client.clone(), base.clone(), shared.clone());
                tokio::spawn(async move { handle(c, b, s, cmd).await });
            }
        }
    }
}

fn start_body(camera: Option<&str>, o: &StartOpts) -> Value {
    // 미리보기는 끈다: 수신 쪽에서는 영상을 풀지 않고, 화면은 뷰어가 원본 영상을 받아 직접 그린다
    let mut v = json!({ "profile": o.profile, "transport": o.transport, "label": "", "preview": "off", "record": o.record });
    if let Some(c) = camera {
        v["camera_id"] = json!(c);
    }
    v
}

fn secs(ms: Option<f64>) -> String {
    ms.map_or("—".into(), |m| format!("{:.2}초", m / 1000.0))
}

async fn handle(c: reqwest::Client, base: String, shared: Arc<Shared>, cmd: Cmd) {
    use reqwest::Method;
    let post = |path: String, body: Option<Value>| send(&c, Method::POST, format!("{base}{path}"), body);
    match cmd {
        Cmd::StartAll(o) => match post("/api/sessions/start-all".into(), Some(start_body(None, &o))).await {
            Ok(v) => {
                let errs: Vec<String> = v["errors"].as_array().map(|a| a.iter().map(|e| e.to_string()).collect()).unwrap_or_default();
                if errs.is_empty() {
                    shared.note("모든 카메라 수신을 시작했습니다", true);
                } else {
                    shared.note(format!("일부 카메라를 시작하지 못했습니다: {}", errs.join(", ")), false);
                }
            }
            Err(e) => shared.note(format!("수신 시작 실패: {e}"), false),
        },
        Cmd::StopAll => match post("/api/sessions/stop-all".into(), None).await {
            Ok(_) => shared.note("모든 수신을 멈춥니다 (녹화 파일을 닫는 중)", true),
            Err(e) => shared.note(format!("중지 실패: {e}"), false),
        },
        Cmd::Start(cam, o) => match post("/api/sessions".into(), Some(start_body(Some(&cam), &o))).await {
            Ok(_) => shared.note(format!("{cam} 수신을 시작했습니다"), true),
            Err(e) => shared.note(format!("{cam} 수신 시작 실패: {e}"), false),
        },
        Cmd::Stop(sid) => match post(format!("/api/sessions/{sid}/stop"), None).await {
            Ok(_) => shared.note("수신을 멈춥니다 (녹화 파일을 닫는 중)", true),
            Err(e) => shared.note(format!("중지 실패: {e}"), false),
        },
        Cmd::Capture(cam, o) => match post(format!("/api/cameras/{cam}/capture"), Some(json!({ "profile": o.profile, "transport": o.transport }))).await {
            Ok(v) if v["ok"].as_bool() == Some(true) => shared.note(format!("{cam} 촬영 저장 · 누름→저장 {}", secs(v["total_ms"].as_f64())), true),
            Ok(v) => shared.note(format!("{cam} 촬영 실패: {}", v["error"].as_str().unwrap_or("알 수 없는 오류")), false),
            Err(e) => shared.note(format!("{cam} 촬영 실패: {e}"), false),
        },
        Cmd::Trigger => match post("/api/trigger".into(), Some(json!({ "source": "button" }))).await {
            Ok(v) => {
                let shots: Vec<String> = v["shots"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .map(|s| {
                                let cam = s["camera_id"].as_str().unwrap_or("?");
                                if s["ok"].as_bool() == Some(true) { format!("{cam} {}", secs(s["total_ms"].as_f64())) } else { format!("{cam} 실패") }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let status = v["status"].as_str().unwrap_or("");
                let skew = v["skew_ms"].as_f64().map(|s| format!(" · 카메라 간 편차 {s:.0}ms")).unwrap_or_default();
                match status {
                    "ok" => shared.note(format!("동시 촬영 #{} 저장 · {}{skew}", v["seq"], shots.join(" · ")), true),
                    "ignored" => shared.note("동시 촬영 신호가 너무 빨리 이어져 무시했습니다", false),
                    _ => shared.note(format!("동시 촬영 #{} 일부 실패 · {} {}", v["seq"], shots.join(" · "), v["error"].as_str().unwrap_or("")), false),
                }
            }
            Err(e) => shared.note(format!("동시 촬영 실패: {e}"), false),
        },
        Cmd::SetLocation(loc) => {
            if let Err(e) = send(&c, Method::PUT, format!("{base}/api/location"), Some(json!({ "location": loc }))).await {
                shared.note(format!("측정 위치 저장 실패: {e}"), false);
            }
        }
        Cmd::E2eReport(sid, sum) => match post(format!("/api/sessions/{sid}/e2e/report"), Some(sum)).await {
            Ok(_) => shared.note("실제 지연 측정 결과를 세션 기록에 남겼습니다", true),
            Err(e) => shared.note(format!("실제 지연 측정 결과 저장 실패: {e}"), false),
        },
        Cmd::LoadSettings => {
            if let Ok(s) = get::<Settings>(&c, format!("{base}/api/settings"), Duration::from_secs(10)).await {
                shared.inner.lock().unwrap().settings = Some(s);
                shared.repaint();
            }
        }
        Cmd::SetSegment(m) => {
            let cur = match get::<Settings>(&c, format!("{base}/api/settings"), Duration::from_secs(10)).await {
                Ok(s) => s,
                Err(e) => return shared.note(format!("설정을 읽지 못했습니다: {e}"), false),
            };
            // 기본 위치는 빈 값으로 보낸다 (설정 파일에 절대 경로가 박히지 않게)
            let norm = |p: &str| p.trim().trim_end_matches(['\\', '/']).replace('/', "\\").to_lowercase();
            let as_cfg = |v: &str, def: &str| if norm(v) == norm(def) { String::new() } else { v.to_string() };
            let body = json!({
                "recordings_dir": as_cfg(&cur.recordings_dir, &cur.default_recordings_dir),
                "logs_dir": as_cfg(&cur.logs_dir, &cur.default_logs_dir),
                "retention_days": cur.retention_days,
                "segment_minutes": m,
            });
            match post("/api/settings/storage".into(), Some(body)).await {
                Ok(v) => {
                    match v["warning"].as_str() {
                        Some(w) => shared.note(w.to_string(), false),
                        None => shared.note(format!("녹화 파일 길이를 {m}분으로 저장했습니다 (새로 시작하는 수신부터)"), true),
                    }
                    if let Ok(s) = get::<Settings>(&c, format!("{base}/api/settings"), Duration::from_secs(10)).await {
                        shared.inner.lock().unwrap().settings = Some(s);
                    }
                }
                Err(e) => shared.note(format!("저장 실패: {e}"), false),
            }
        }
        Cmd::LoadCameras => {
            if let Ok(l) = get::<Vec<CamEdit>>(&c, format!("{base}/api/cameras"), Duration::from_secs(10)).await {
                shared.inner.lock().unwrap().cam_list = Some(l);
                shared.repaint();
            }
        }
        Cmd::Discover => {
            shared.inner.lock().unwrap().found = Job::Running;
            let r = post("/api/cameras/discover".into(), None).await.and_then(|v| serde_json::from_value::<Vec<Found>>(v).map_err(|e| e.to_string()));
            shared.inner.lock().unwrap().found = match r {
                Ok(l) => Job::Done(l),
                Err(e) => Job::Failed(e),
            };
            shared.repaint();
        }
        Cmd::TestCamera(body) => {
            shared.inner.lock().unwrap().test = Job::Running;
            let r = post("/api/cameras/test".into(), Some(body)).await;
            shared.inner.lock().unwrap().test = match r {
                Ok(v) => Job::Done(v),
                Err(e) => Job::Failed(e),
            };
            shared.repaint();
        }
        Cmd::SaveCamera(id, body) => {
            shared.inner.lock().unwrap().cam_saving = true;
            shared.repaint();
            let r = match &id {
                Some(id) => send(&c, Method::PUT, format!("{base}/api/cameras/{id}"), Some(body)).await,
                None => post("/api/cameras".into(), Some(body)).await,
            };
            shared.inner.lock().unwrap().cam_saving = false;
            match r {
                Ok(v) => {
                    // 저장은 됐지만 영상 주소를 못 찾았으면 그 이유를 알려 준다 (수신을 시작할 때 다시 찾는다)
                    match v["warning"].as_str() {
                        Some(w) => shared.note(w.to_string(), false),
                        None => shared.note(if id.is_some() { "카메라 설정을 바꿨습니다".to_string() } else { format!("카메라를 추가했습니다 ({})", v["id"].as_str().unwrap_or("")) }, true),
                    }
                    let list = get::<Vec<CamEdit>>(&c, format!("{base}/api/cameras"), Duration::from_secs(10)).await.ok();
                    let mut i = shared.inner.lock().unwrap();
                    i.cam_saved += 1;
                    if list.is_some() {
                        i.cam_list = list;
                    }
                }
                Err(e) => shared.note(format!("카메라 저장 실패: {e}"), false),
            }
            shared.repaint();
        }
        Cmd::MoveCamera(id, host, uid, mac) => {
            match post(format!("/api/cameras/{id}/move"), Some(serde_json::json!({ "host": host, "uid": uid, "mac": mac }))).await {
                Ok(v) => {
                    match v["swapped_with"].as_str() {
                        Some(other) => shared.note(format!("카메라 {id}와 {other}의 주소를 서로 맞바꿨습니다 ({id} → {host})"), true),
                        None => shared.note(format!("카메라 {id}의 주소를 {host}(으)로 바꿨습니다"), true),
                    }
                    let list = get::<Vec<CamEdit>>(&c, format!("{base}/api/cameras"), Duration::from_secs(10)).await.ok();
                    let mut guard = shared.inner.lock().unwrap();
                    let i = &mut *guard;
                    if list.is_some() {
                        i.cam_list = list;
                    }
                    // 찾은 목록을 새 등록 상태에 맞춘다: 방금 옮긴(맞바꾼) 주소는 이제 '등록됨'이다
                    if let (Job::Done(l), Some(cams)) = (&mut i.found, &i.cam_list) {
                        let ip_of = |h: &str| h.rsplit_once(':').filter(|(_, p)| !p.is_empty() && p.chars().all(|ch| ch.is_ascii_digit())).map_or(h.to_string(), |(a, _)| a.to_string());
                        for d in l.iter_mut() {
                            d.registered = cams.iter().find(|cam| ip_of(&cam.host) == d.ip).map(|cam| cam.id.clone());
                            if d.same_as.is_some() && d.same_as == d.registered {
                                d.same_as = None;
                            }
                        }
                    }
                }
                Err(e) => shared.note(format!("주소 바꾸기 실패: {e}"), false),
            }
            shared.repaint();
        }
        Cmd::DeleteCamera(id) => {
            match send(&c, Method::DELETE, format!("{base}/api/cameras/{id}"), None).await {
                Ok(_) => {
                    shared.note(format!("카메라 {id}를 지웠습니다 (녹화·기록은 그대로 남습니다)"), true);
                    let list = get::<Vec<CamEdit>>(&c, format!("{base}/api/cameras"), Duration::from_secs(10)).await.ok();
                    let mut i = shared.inner.lock().unwrap();
                    i.cam_saved += 1;
                    if list.is_some() {
                        i.cam_list = list;
                    }
                }
                Err(e) => shared.note(format!("카메라 삭제 실패: {e}"), false),
            }
            shared.repaint();
        }
        Cmd::Scan(target) => {
            shared.inner.lock().unwrap().scan = Job::Running;
            let r = post("/api/net/scan".into(), Some(json!({ "target": target }))).await;
            shared.inner.lock().unwrap().scan = match r {
                Ok(v) => Job::Done(v),
                Err(e) => Job::Failed(e),
            };
            shared.repaint();
        }
        Cmd::Diagnose => {
            shared.inner.lock().unwrap().diag = Job::Running;
            let r = get::<DiagReport>(&c, format!("{base}/api/diagnose"), Duration::from_secs(30)).await;
            shared.inner.lock().unwrap().diag = match r {
                Ok(d) => Job::Done(d),
                Err(e) => Job::Failed(e),
            };
            shared.repaint();
        }
        Cmd::Shutdown => match post("/api/shutdown".into(), None).await {
            Ok(_) => {
                shared.inner.lock().unwrap().engine_gone = true;
                shared.note("엔진을 종료합니다 (수신·녹화를 마무리하는 중)", true);
            }
            Err(e) => shared.note(format!("종료 실패: {e}"), false),
        },
    }
}
