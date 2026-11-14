//! 기록 저장소.
//!
//! - 모든 기록은 `<로그 폴더>/<종류>-YYYY-MM-DD.jsonl`에 한 줄씩 쌓인다 (날짜별 파일).
//! - 파일 쓰기는 별도 스레드가 맡아서, 디스크가 느려도 수신·측정이 멈추지 않는다.
//! - 대시보드 조회용으로 최근 기록만 메모리에 들고 있다 (상한 고정 → 메모리 사용량이 늘지 않음).

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
        Mutex, RwLock,
    },
    thread,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Local, NaiveDate};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

const MAX_PROBES: usize = 30_000;
const MAX_WIFI: usize = 20_000;
const MAX_EVENTS: usize = 5_000;
/// 1초에 한 줄 → 카메라 2대 기준 약 16시간 (그보다 오래된 것은 파일에서 읽는다)
const MAX_STREAM: usize = 120_000;
const MAX_SESSIONS: usize = 1_000;
const MAX_CAPTURES: usize = 20_000;
const MAX_TRIGGERS: usize = 20_000;
const WRITE_QUEUE: usize = 8_192;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WifiInfo {
    pub ts: DateTime<Local>,
    pub interface: String,
    pub state: String,
    pub ssid: Option<String>,
    pub bssid: Option<String>,
    pub band: Option<String>,
    pub channel: Option<u32>,
    pub signal_pct: Option<u8>,
    pub rssi_dbm: Option<i32>,
    pub rx_mbps: Option<f64>,
    pub tx_mbps: Option<f64>,
    /// Windows에 저장된 Wi-Fi 프로필 이름 (SSID와 다를 수 있다 — 같은 이름이 둘이면 "이름 2"). Wi-Fi 고정에 쓴다
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeSample {
    pub ts: DateTime<Local>,
    pub camera_id: String,
    pub ip: Option<String>,
    pub icmp_ok: bool,
    pub icmp_ms: Option<f64>,
    pub tcp_ok: bool,
    pub tcp_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub ts: DateTime<Local>,
    pub kind: String,
    pub camera_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub data: Value,
}

impl Event {
    pub fn new(kind: &str, camera_id: Option<&str>, message: impl Into<String>) -> Self {
        Self {
            ts: Local::now(),
            kind: kind.to_string(),
            camera_id: camera_id.map(str::to_string),
            session_id: None,
            message: message.into(),
            data: Value::Null,
        }
    }

    pub fn session(mut self, id: &str) -> Self {
        self.session_id = Some(id.to_string());
        self
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }
}

/// 수신 지표 1초 묶음
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamSample {
    pub ts: DateTime<Local>,
    pub session_id: String,
    pub camera_id: String,
    pub frames: u32,
    pub kbps: f64,
    /// 이 1초 안에서 가장 긴 프레임 간격
    pub interval_max_ms: Option<f64>,
    /// 실제 흐른 시간 대비 영상이 뒤처진 정도 (최근 1분 최솟값 기준, 영상 시각이 튄 프레임은 제외)
    pub delay_ms: Option<f64>,
    pub stalled: bool,
    pub connected: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionStats {
    pub frames: u64,
    pub bytes: u64,
    /// 세션 시작 → 첫 영상 프레임까지
    pub connect_ms: Option<f64>,
    pub avg_fps: Option<f64>,
    /// 프레임 간격 중앙값으로 추정한 카메라 fps
    pub nominal_fps: Option<f64>,
    pub avg_kbps: Option<f64>,
    pub interval_p50_ms: Option<f64>,
    pub interval_p95_ms: Option<f64>,
    pub interval_p99_ms: Option<f64>,
    pub interval_max_ms: Option<f64>,
    /// 간격이 jank_ms를 넘은 횟수
    pub janks: u64,
    /// 프레임이 stall_ms 이상 안 온 횟수와 그 시간
    pub stalls: u64,
    pub stall_total_ms: f64,
    pub longest_stall_ms: f64,
    pub reconnects: u64,
    pub downtime_ms: f64,
    pub rtp_loss_events: u64,
    pub rtp_lost_packets: u64,
    pub decode_errors: u64,
    pub delay_max_ms: Option<f64>,
    pub delay_p95_ms: Option<f64>,
    /// 버퍼 지연이 `delay_warn_ms` 이상이었던 초 수 / 그 기준값
    pub delayed_secs: u64,
    pub delay_warn_ms: f64,
    /// 끊김·재연결 시간을 뺀 정상 수신 비율
    pub uptime_pct: Option<f64>,
    pub codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub camera_fps: Option<f64>,
    /// 프레임 누락: 첫 프레임부터의 시간 × 카메라 fps만큼 왔어야 할 수와, 그중 오지 않은 수 (끊김·재연결로 빠진 영상)
    pub expected_frames: Option<u64>,
    pub missing_frames: Option<u64>,
    /// 카메라가 보낸 영상 시각이 튄 프레임 수 (도착보다 앞서 갔거나 같은 시각에 뭉침).
    /// 영상 자체는 받았지만 녹화에서 그 구간이 건너뛰거나 멈춰 보이고, 지연 계산에서는 뺀다
    pub ts_glitch_frames: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetSummary {
    /// 세션 시작 때 PC가 카메라로 가는 인터페이스
    pub path_kind: Option<String>,
    pub path_name: Option<String>,
    pub ssid: Option<String>,
    pub bssids: Vec<String>,
    pub wifi_signal_avg: Option<f64>,
    pub wifi_signal_min: Option<u8>,
    pub rssi_avg: Option<f64>,
    pub roams: u64,
    pub wifi_disconnects: u64,
    pub ping_avg_ms: Option<f64>,
    pub ping_p95_ms: Option<f64>,
    pub ping_loss_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: String,
    pub camera_id: String,
    pub camera_name: String,
    pub label: String,
    /// main | sub
    pub profile: String,
    /// tcp | udp
    pub transport: String,
    /// 계정을 가린 주소
    pub url: String,
    /// running | finished | failed
    pub status: String,
    pub started_at: Option<DateTime<Local>>,
    pub ended_at: Option<DateTime<Local>>,
    pub duration_s: f64,
    pub record: bool,
    pub preview: String,
    pub recording_dir: Option<String>,
    pub recorded_bytes: u64,
    pub stats: SessionStats,
    pub net: NetSummary,
    /// stable | warning | unstable
    pub verdict: Option<String>,
    pub reasons: Vec<String>,
    pub last_error: Option<String>,
    /// [모두 수신 시작]으로 같이 시작한 세션 묶음
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// 화면 깜빡임으로 잰 실제 지연 (측정했을 때만)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2e: Option<E2eSummary>,
    /// 수신 중 마지막으로 중간 저장한 시각 (프로그램이 갑자기 꺼져도 여기까지는 남는다)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saved_at: Option<DateTime<Local>>,
    /// 프로그램이 갑자기 꺼져 중간 저장 시점까지만 남은 세션
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub interrupted: bool,
    /// 녹화 파일 점검 결과 (수신 종료 뒤 자동)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rec_check: Option<RecCheck>,
    /// 측정 위치 (시작할 때 고른 값)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// 녹화 파일 점검: 파일마다 끝까지 읽어 보고, 받은 프레임과 녹화된 프레임을 맞춰 본다
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RecCheck {
    pub checked_at: Option<DateTime<Local>>,
    /// 정상 / 문제 있음
    pub ok: bool,
    pub files: u32,
    /// 읽다가 오류가 난 파일 (이름: 오류)
    pub bad_files: Vec<String>,
    /// 녹화 파일에 든 프레임 수와 그 길이(초)
    pub frames: u64,
    pub seconds: f64,
    /// 수신 중 받은 프레임 수 (비교용)
    pub received_frames: u64,
    /// 수신 시간 - 녹화 길이 (연결·끊김으로 녹화가 안 된 시간)
    pub gap_s: f64,
    /// 한 줄 요약
    pub summary: String,
}

/// 카메라 촬영 → PC 수신까지의 실제 지연 측정 결과
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct E2eSummary {
    pub count: usize,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub measured_at: Option<DateTime<Local>>,
}

/// 프레임 촬영 한 번
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CaptureRecord {
    pub id: String,
    pub session_id: String,
    pub session_label: String,
    pub camera_id: String,
    pub camera_name: String,
    pub profile: String,
    pub transport: String,
    pub path_kind: Option<String>,
    /// session = 수신 중인 영상에서 뽑음 | direct = 수신 없이 카메라에 잠깐 접속해서 찍음
    pub mode: String,
    pub ok: bool,
    pub error: Option<String>,
    /// direct: 누름 → 카메라 RTSP 연결(스트림 정보 받음)까지
    pub connect_ms: Option<f64>,
    /// [프레임 촬영]을 누른 시각 (서버가 요청을 받은 순간)
    pub requested_at: Option<DateTime<Local>>,
    /// 누른 뒤 처음 도착한 프레임을 다 받은 시각
    pub frame_at: Option<DateTime<Local>>,
    /// JPEG 파일을 디스크에 다 쓴 시각
    pub saved_at: Option<DateTime<Local>>,
    /// 누름 → 프레임 도착
    pub wait_ms: Option<f64>,
    /// 프레임 도착 → JPEG 완성 (키프레임부터 풀어야 해서 GOP 길이에 따라 달라짐)
    pub decode_ms: Option<f64>,
    /// JPEG → 디스크 기록 완료
    pub save_ms: Option<f64>,
    /// 누름 → 저장 완료
    pub total_ms: Option<f64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bytes: u64,
    /// 이미지 파일 전체 경로
    pub image: Option<String>,
    /// 이 프레임을 만들려고 푼 프레임 수 (키프레임부터)
    pub frames_decoded: u32,
    /// 수신 중 촬영: 찍은 순간의 버퍼 지연 (영상이 평소보다 얼마나 늦게 도착하고 있었나).
    /// 촬영은 '누른 뒤 처음 도착한 프레임'을 저장하므로, 이만큼 예전 장면이 찍힌다 (total_ms에는 안 들어감)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_delay_ms: Option<f64>,
    /// 트리거(동시 촬영)로 찍었으면 그 신호 id·번호·출처
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_source: Option<String>,
    /// 측정 위치 (라이브 화면에서 고른 값, 예: "정차 지점 A")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// 트리거 한 번에 카메라 한 대의 결과
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TriggerShot {
    pub camera_id: String,
    pub camera_name: String,
    pub capture_id: String,
    /// session | direct
    pub mode: String,
    pub ok: bool,
    pub error: Option<String>,
    /// 신호 → 이 카메라 이미지 저장 완료
    pub total_ms: Option<f64>,
    /// 신호 → 프레임 도착 (direct는 RTSP 연결까지)
    pub wait_ms: Option<f64>,
    pub frame_at: Option<DateTime<Local>>,
    pub saved_at: Option<DateTime<Local>>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// 트리거(동시 촬영 신호) 한 번
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TriggerRecord {
    pub id: String,
    /// 프로그램 기준 일련번호 (무시된 신호는 0)
    pub seq: u64,
    /// 신호를 받은 시각
    pub ts: DateTime<Local>,
    /// button | http | tcp | modbus | auto
    pub source: String,
    /// 보낸 곳·메시지 등
    pub detail: Option<String>,
    /// ok | partial | failed | ignored
    pub status: String,
    pub shots: Vec<TriggerShot>,
    /// 신호 → 모든 카메라 저장 완료 (가장 늦은 카메라)
    pub total_ms: Option<f64>,
    /// 카메라끼리 찍힌 시각 차이 (frame: 프레임 도착 기준, saved: 저장 기준)
    pub skew_ms: Option<f64>,
    pub skew_basis: Option<String>,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Network,
    Wifi,
    Events,
    Stream,
    Sessions,
    Captures,
    Triggers,
}

impl Kind {
    pub fn prefix(self) -> &'static str {
        match self {
            Kind::Network => "network",
            Kind::Wifi => "wifi",
            Kind::Events => "events",
            Kind::Stream => "stream",
            Kind::Sessions => "sessions",
            Kind::Captures => "captures",
            Kind::Triggers => "triggers",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "network" => Kind::Network,
            "wifi" => Kind::Wifi,
            "events" => Kind::Events,
            "stream" => Kind::Stream,
            "sessions" => Kind::Sessions,
            "captures" => Kind::Captures,
            "triggers" => Kind::Triggers,
            _ => return None,
        })
    }
}

enum Msg {
    Line(Kind, NaiveDate, String),
    Flush(mpsc::Sender<()>),
    SetDir(PathBuf),
}

#[derive(Default)]
struct Mem {
    probes: VecDeque<ProbeSample>,
    wifi: VecDeque<WifiInfo>,
    events: VecDeque<Event>,
    stream: VecDeque<StreamSample>,
    /// 최근 세션이 뒤에 (id 기준으로 마지막 상태만 유지)
    sessions: VecDeque<SessionRecord>,
    captures: VecDeque<CaptureRecord>,
    triggers: VecDeque<TriggerRecord>,
}

struct Paths {
    recordings: PathBuf,
    logs: PathBuf,
}

pub struct Store {
    paths: RwLock<Paths>,
    tx: SyncSender<Msg>,
    mem: Mutex<Mem>,
    dropped: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NetPoint {
    pub ts: DateTime<Local>,
    pub n: u32,
    pub icmp_ok: u32,
    pub icmp_avg_ms: Option<f64>,
    pub tcp_ok: u32,
    pub tcp_avg_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WifiPoint {
    pub ts: DateTime<Local>,
    pub n: u32,
    pub connected: u32,
    pub signal_avg: Option<f64>,
    pub rssi_avg: Option<f64>,
    pub bssid: Option<String>,
}

/// 차트용 수신 지표 (구간 집계)
#[derive(Debug, Clone, Serialize)]
pub struct StreamPoint {
    pub ts: DateTime<Local>,
    pub secs: u32,
    pub fps: f64,
    pub kbps: f64,
    pub interval_max_ms: Option<f64>,
    pub delay_ms: Option<f64>,
    pub stalled_secs: u32,
    pub disconnected_secs: u32,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DirStats {
    pub files: u64,
    pub bytes: u64,
}

fn push<T>(q: &mut VecDeque<T>, v: T, cap: usize) {
    if q.len() >= cap {
        q.pop_front();
    }
    q.push_back(v);
}

pub fn avg(v: &[f64]) -> Option<f64> {
    (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
}

pub fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

fn sorted(mut v: Vec<f64>) -> Vec<f64> {
    v.sort_by(|a, b| a.total_cmp(b));
    v
}

impl Store {
    pub fn open(recordings_dir: &Path, logs_dir: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(logs_dir)?;
        fs::create_dir_all(recordings_dir)?;

        let (tx, rx) = mpsc::sync_channel::<Msg>(WRITE_QUEUE);
        let mut dir = logs_dir.to_path_buf();
        thread::Builder::new()
            .name("jsonl-writer".into())
            .spawn(move || {
                for msg in rx {
                    match msg {
                        Msg::Line(kind, date, line) => append_line(&dir, kind, date, &line),
                        Msg::Flush(done) => {
                            let _ = done.send(());
                        }
                        Msg::SetDir(new) => dir = new,
                    }
                }
            })?;

        let store = Self {
            paths: RwLock::new(Paths { recordings: recordings_dir.to_path_buf(), logs: logs_dir.to_path_buf() }),
            tx,
            mem: Mutex::new(Mem::default()),
            dropped: AtomicU64::new(0),
        };
        store.warm_up();
        Ok(store)
    }

    pub fn recordings_dir(&self) -> PathBuf {
        self.paths.read().unwrap().recordings.clone()
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.paths.read().unwrap().logs.clone()
    }

    /// 새 녹화부터 이 폴더에 저장 (기존 파일은 옮기지 않는다)
    pub fn set_recordings_dir(&self, dir: &Path) {
        self.paths.write().unwrap().recordings = dir.to_path_buf();
    }

    /// 이후 로그를 이 폴더에 쓴다. 쓰기 스레드가 순서대로 처리한다.
    pub fn set_logs_dir(&self, dir: &Path) {
        let _ = self.tx.send(Msg::SetDir(dir.to_path_buf()));
        self.paths.write().unwrap().logs = dir.to_path_buf();
    }

    /// 재시작해도 대시보드에 지난 기록이 보이도록 파일에서 다시 읽는다.
    /// 세션 요약은 보존 기간 전체, 나머지는 어제·오늘만.
    fn warm_up(&self) {
        let logs = self.logs_dir();
        let today = Local::now().date_naive();
        let mut mem = self.mem.lock().unwrap();
        for date in [today - Duration::days(1), today] {
            for kind in [Kind::Network, Kind::Wifi, Kind::Events, Kind::Stream] {
                let path = log_path(&logs, kind, date);
                match kind {
                    Kind::Network => read_jsonl::<ProbeSample>(&path).into_iter().for_each(|r| push(&mut mem.probes, r, MAX_PROBES)),
                    Kind::Wifi => read_jsonl::<WifiInfo>(&path).into_iter().for_each(|r| push(&mut mem.wifi, r, MAX_WIFI)),
                    Kind::Events => read_jsonl::<Event>(&path).into_iter().for_each(|r| push(&mut mem.events, r, MAX_EVENTS)),
                    Kind::Stream => read_jsonl::<StreamSample>(&path).into_iter().for_each(|r| push(&mut mem.stream, r, MAX_STREAM)),
                    Kind::Sessions | Kind::Captures | Kind::Triggers => {}
                }
            }
        }
        // 세션: 날짜 순으로 읽고 같은 id는 마지막 상태로 덮어쓴다
        let mut files: Vec<(NaiveDate, PathBuf)> = fs::read_dir(&logs)
            .map(|rd| {
                rd.flatten()
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        (name.starts_with("sessions-") || name.starts_with("captures-") || name.starts_with("triggers-"))
                            .then(|| jsonl_date(&name).map(|d| (d, e.path())))
                            .flatten()
                    })
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        let mut capture_files: Vec<PathBuf> = Vec::new();
        for (_, p) in files {
            let name = p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
            if name.starts_with("captures-") {
                capture_files.push(p);
                continue;
            }
            if name.starts_with("triggers-") {
                for rec in read_jsonl::<TriggerRecord>(&p) {
                    push(&mut mem.triggers, rec, MAX_TRIGGERS);
                }
                continue;
            }
            for mut rec in read_jsonl::<SessionRecord>(&p) {
                // 프로그램이 중간에 꺼져서 '진행 중'으로 남은 세션
                // (마지막 중간 저장까지 받은 영상이 있으면 그 시점까지로 마무리 — stream::finish_interrupted)
                if rec.status == "running" {
                    rec.interrupted = true;
                    rec.status = if rec.stats.frames > 0 { "finished".into() } else { "failed".into() };
                    rec.last_error.get_or_insert_with(|| "프로그램이 종료되어 세션이 중단됨".into());
                }
                upsert_session(&mut mem.sessions, rec);
            }
        }
        // 촬영 기록: 보존 기간 안의 것 전부 (최근 MAX_CAPTURES개)
        for p in capture_files {
            for rec in read_jsonl::<CaptureRecord>(&p) {
                push(&mut mem.captures, rec, MAX_CAPTURES);
            }
        }
    }

    pub fn record_capture(&self, c: &CaptureRecord) {
        self.send(Kind::Captures, c.requested_at.unwrap_or_else(Local::now), c);
        push(&mut self.mem.lock().unwrap().captures, c.clone(), MAX_CAPTURES);
    }

    /// 최근 촬영부터. `session`·`camera`·`trigger`가 있으면 그것만.
    pub fn captures(&self, session: Option<&str>, camera: Option<&str>, trigger: Option<&str>, limit: usize) -> Vec<CaptureRecord> {
        let mem = self.mem.lock().unwrap();
        mem.captures
            .iter()
            .rev()
            .filter(|c| session.map_or(true, |s| c.session_id == s))
            .filter(|c| camera.map_or(true, |s| c.camera_id == s))
            .filter(|c| trigger.map_or(true, |s| c.trigger_id.as_deref() == Some(s)))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn capture(&self, id: &str) -> Option<CaptureRecord> {
        self.mem.lock().unwrap().captures.iter().rev().find(|c| c.id == id).cloned()
    }

    /// 카메라별 마지막 촬영 (라이브 화면 타일용)
    pub fn last_capture_of(&self, camera: &str) -> Option<CaptureRecord> {
        self.mem.lock().unwrap().captures.iter().rev().find(|c| c.camera_id == camera).cloned()
    }

    pub fn record_trigger(&self, t: &TriggerRecord) {
        self.send(Kind::Triggers, t.ts, t);
        push(&mut self.mem.lock().unwrap().triggers, t.clone(), MAX_TRIGGERS);
    }

    /// 최근 트리거부터. `camera`가 있으면 그 카메라를 찍은 것만.
    pub fn triggers(&self, camera: Option<&str>, limit: usize) -> Vec<TriggerRecord> {
        let mem = self.mem.lock().unwrap();
        mem.triggers
            .iter()
            .rev()
            .filter(|t| camera.map_or(true, |c| t.shots.iter().any(|s| s.camera_id == c)))
            .take(limit)
            .cloned()
            .collect()
    }

    /// 다음 트리거 번호 (재시작해도 이어서)
    pub fn last_trigger_seq(&self) -> u64 {
        self.mem.lock().unwrap().triggers.iter().map(|t| t.seq).max().unwrap_or(0)
    }

    /// 기간 안의 기록을 파일에서 읽는다 (메모리 상한과 상관없이 보존 기간 전체).
    /// `ts`가 그 기록의 기준 시각을 돌려준다. 결과는 오래된 것부터.
    pub fn read_range<T: DeserializeOwned>(&self, kind: Kind, from: DateTime<Local>, to: DateTime<Local>, ts: impl Fn(&T) -> Option<DateTime<Local>>) -> Vec<T> {
        // 아직 파일에 안 쓴 줄이 있으면 먼저 쓰게 한다
        self.flush(StdDuration::from_secs(2));
        let logs = self.logs_dir();
        let mut out = Vec::new();
        let (mut d, last) = (from.date_naive() - Duration::days(1), to.date_naive() + Duration::days(1));
        while d <= last {
            for r in read_jsonl::<T>(&log_path(&logs, kind, d)) {
                if ts(&r).map_or(false, |t| t >= from && t <= to) {
                    out.push(r);
                }
            }
            d += Duration::days(1);
        }
        out
    }

    /// 기간과 겹치는 세션 (같은 id는 마지막 상태). 진행 중인 세션은 시작 때 기록만 있다.
    pub fn sessions_in_range(&self, from: DateTime<Local>, to: DateTime<Local>) -> Vec<SessionRecord> {
        self.flush(StdDuration::from_secs(2));
        let logs = self.logs_dir();
        let mut q: VecDeque<SessionRecord> = VecDeque::new();
        let (mut d, last) = (from.date_naive() - Duration::days(2), to.date_naive() + Duration::days(1));
        while d <= last {
            for r in read_jsonl::<SessionRecord>(&log_path(&logs, Kind::Sessions, d)) {
                if let Some(pos) = q.iter().position(|s| s.id == r.id) {
                    q.remove(pos);
                }
                q.push_back(r);
            }
            d += Duration::days(1);
        }
        q.into_iter()
            .filter(|s| {
                let start = s.started_at.unwrap_or(to);
                let end = s.ended_at.unwrap_or_else(Local::now);
                start <= to && end >= from
            })
            .collect()
    }

    /// 기간 안 파일들의 크기 합 (원시 데이터 조회 캐시가 바뀌었는지 볼 때)
    pub fn range_signature(&self, kind: Kind, from: DateTime<Local>, to: DateTime<Local>) -> u64 {
        let logs = self.logs_dir();
        let (mut d, last) = (from.date_naive() - Duration::days(2), to.date_naive() + Duration::days(1));
        let mut sig = 0u64;
        while d <= last {
            sig = sig.wrapping_mul(31).wrapping_add(fs::metadata(log_path(&logs, kind, d)).map(|m| m.len()).unwrap_or(0));
            d += Duration::days(1);
        }
        sig
    }

    fn send(&self, kind: Kind, ts: DateTime<Local>, value: &impl Serialize) {
        let line = match serde_json::to_string(value) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("기록 직렬화 실패: {e}");
                return;
            }
        };
        match self.tx.try_send(Msg::Line(kind, ts.date_naive(), line)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_probe(&self, s: ProbeSample) {
        self.send(Kind::Network, s.ts, &s);
        push(&mut self.mem.lock().unwrap().probes, s, MAX_PROBES);
    }

    pub fn record_wifi(&self, w: WifiInfo) {
        self.send(Kind::Wifi, w.ts, &w);
        push(&mut self.mem.lock().unwrap().wifi, w, MAX_WIFI);
    }

    pub fn record_event(&self, e: Event) {
        tracing::info!(kind = %e.kind, camera = ?e.camera_id, "{}", e.message);
        self.send(Kind::Events, e.ts, &e);
        push(&mut self.mem.lock().unwrap().events, e, MAX_EVENTS);
    }

    pub fn record_stream(&self, s: StreamSample) {
        self.send(Kind::Stream, s.ts, &s);
        push(&mut self.mem.lock().unwrap().stream, s, MAX_STREAM);
    }

    /// 세션 상태 저장 (시작·종료 때). 같은 id는 메모리에서 교체, 파일에는 한 줄 추가.
    pub fn save_session(&self, rec: &SessionRecord) {
        let ts = rec.ended_at.or(rec.started_at).unwrap_or_else(Local::now);
        self.send(Kind::Sessions, ts, rec);
        upsert_session(&mut self.mem.lock().unwrap().sessions, rec.clone());
    }

    pub fn dropped_lines(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn flush(&self, timeout: StdDuration) {
        let (done_tx, done_rx) = mpsc::channel();
        if self.tx.send(Msg::Flush(done_tx)).is_ok() {
            let _ = done_rx.recv_timeout(timeout);
        }
    }

    pub fn sessions(&self, limit: usize) -> Vec<SessionRecord> {
        self.mem.lock().unwrap().sessions.iter().rev().take(limit).cloned().collect()
    }

    /// 최근에 쓴 측정 위치 (중복 없이, 최근 것 먼저)
    pub fn recent_locations(&self, n: usize) -> Vec<String> {
        let mem = self.mem.lock().unwrap();
        let mut out: Vec<String> = Vec::new();
        let all = mem
            .captures
            .iter()
            .rev()
            .filter_map(|c| c.location.clone())
            .chain(mem.sessions.iter().rev().filter_map(|s| s.location.clone()));
        for l in all {
            if out.len() >= n {
                break;
            }
            if !out.contains(&l) {
                out.push(l);
            }
        }
        out
    }

    pub fn session(&self, id: &str) -> Option<SessionRecord> {
        self.mem.lock().unwrap().sessions.iter().rev().find(|s| s.id == id).cloned()
    }

    pub fn events(&self, camera: Option<&str>, session: Option<&str>, limit: usize) -> Vec<Event> {
        let mem = self.mem.lock().unwrap();
        mem.events
            .iter()
            .rev()
            .filter(|e| camera.map_or(true, |f| e.camera_id.as_deref().map_or(true, |id| id == f)))
            .filter(|e| session.map_or(true, |s| e.session_id.as_deref() == Some(s)))
            .take(limit)
            .cloned()
            .collect()
    }

    /// 세션 수신 지표. `max_points`를 넘으면 구간으로 묶는다.
    pub fn stream_series(&self, session_id: &str, max_points: usize, tail_secs: Option<i64>) -> Vec<StreamPoint> {
        let mem = self.mem.lock().unwrap();
        let mut samples: Vec<&StreamSample> = mem.stream.iter().filter(|s| s.session_id == session_id).collect();
        if let Some(t) = tail_secs {
            if let Some(last) = samples.last().map(|s| s.ts) {
                let since = last - Duration::seconds(t);
                samples.retain(|s| s.ts > since);
            }
        }
        if samples.is_empty() {
            return Vec::new();
        }
        let per = samples.len().div_ceil(max_points.max(1)).max(1);
        samples
            .chunks(per)
            .map(|g| {
                let secs = g.len() as u32;
                let iv: Vec<f64> = g.iter().filter_map(|s| s.interval_max_ms).collect();
                let dl: Vec<f64> = g.iter().filter_map(|s| s.delay_ms).collect();
                StreamPoint {
                    ts: g[0].ts,
                    secs,
                    fps: g.iter().map(|s| s.frames as f64).sum::<f64>() / secs as f64,
                    kbps: g.iter().map(|s| s.kbps).sum::<f64>() / secs as f64,
                    interval_max_ms: iv.iter().copied().reduce(f64::max),
                    delay_ms: dl.iter().copied().reduce(f64::max),
                    stalled_secs: g.iter().filter(|s| s.stalled).count() as u32,
                    disconnected_secs: g.iter().filter(|s| !s.connected).count() as u32,
                }
            })
            .collect()
    }

    pub fn network_series(&self, camera: Option<&str>, from: DateTime<Local>, to: DateTime<Local>, max_points: usize) -> Vec<NetPoint> {
        let mem = self.mem.lock().unwrap();
        let samples: Vec<&ProbeSample> = mem
            .probes
            .iter()
            .filter(|s| s.ts >= from && s.ts <= to && camera.map_or(true, |c| c == s.camera_id))
            .collect();
        group_by_time(&samples, |s| s.ts, from, to, max_points)
            .into_iter()
            .map(|(ts, g)| {
                let icmp: Vec<f64> = g.iter().filter_map(|s| s.icmp_ms).collect();
                let tcp: Vec<f64> = g.iter().filter_map(|s| s.tcp_ms).collect();
                NetPoint {
                    ts,
                    n: g.len() as u32,
                    icmp_ok: g.iter().filter(|s| s.icmp_ok).count() as u32,
                    icmp_avg_ms: avg(&icmp),
                    tcp_ok: g.iter().filter(|s| s.tcp_ok).count() as u32,
                    tcp_avg_ms: avg(&tcp),
                }
            })
            .collect()
    }

    pub fn wifi_series(&self, from: DateTime<Local>, to: DateTime<Local>, max_points: usize) -> Vec<WifiPoint> {
        let mem = self.mem.lock().unwrap();
        let samples: Vec<&WifiInfo> = mem.wifi.iter().filter(|w| w.ts >= from && w.ts <= to).collect();
        group_by_time(&samples, |w| w.ts, from, to, max_points)
            .into_iter()
            .map(|(ts, g)| {
                let sig: Vec<f64> = g.iter().filter_map(|w| w.signal_pct.map(f64::from)).collect();
                let rssi: Vec<f64> = g.iter().filter_map(|w| w.rssi_dbm.map(f64::from)).collect();
                WifiPoint {
                    ts,
                    n: g.len() as u32,
                    connected: g.iter().filter(|w| w.state == "connected").count() as u32,
                    signal_avg: avg(&sig),
                    rssi_avg: avg(&rssi),
                    bssid: g.iter().rev().find_map(|w| w.bssid.clone()),
                }
            })
            .collect()
    }

    /// 세션 동안의 Wi-Fi·ping 요약
    pub fn net_summary(&self, camera: &str, from: DateTime<Local>, to: DateTime<Local>) -> NetSummary {
        let mem = self.mem.lock().unwrap();
        let mut n = NetSummary::default();
        let wifi: Vec<&WifiInfo> = mem.wifi.iter().filter(|w| w.ts >= from && w.ts <= to).collect();
        let sig: Vec<f64> = wifi.iter().filter_map(|w| w.signal_pct.map(f64::from)).collect();
        let rssi: Vec<f64> = wifi.iter().filter_map(|w| w.rssi_dbm.map(f64::from)).collect();
        n.wifi_signal_avg = avg(&sig);
        n.wifi_signal_min = wifi.iter().filter_map(|w| w.signal_pct).min();
        n.rssi_avg = avg(&rssi);
        n.ssid = wifi.iter().rev().find_map(|w| w.ssid.clone());
        for b in wifi.iter().filter_map(|w| w.bssid.clone()) {
            if !n.bssids.contains(&b) {
                n.bssids.push(b);
            }
        }
        for e in mem.events.iter().filter(|e| e.ts >= from && e.ts <= to) {
            match e.kind.as_str() {
                "wifi_roam" => n.roams += 1,
                "wifi_disconnected" => n.wifi_disconnects += 1,
                _ => {}
            }
        }
        let probes: Vec<&ProbeSample> = mem.probes.iter().filter(|p| p.ts >= from && p.ts <= to && p.camera_id == camera).collect();
        if !probes.is_empty() {
            let icmp = sorted(probes.iter().filter_map(|p| p.icmp_ms).collect());
            n.ping_avg_ms = avg(&icmp);
            n.ping_p95_ms = percentile(&icmp, 95.0);
            n.ping_loss_pct = Some(probes.iter().filter(|p| !p.icmp_ok).count() as f64 * 100.0 / probes.len() as f64);
        }
        n
    }

    /// 보존 기간이 지난 jsonl 로그와 녹화 날짜 폴더를 지운다.
    pub fn cleanup(&self, retention_days: u32) {
        if retention_days == 0 {
            return;
        }
        let cutoff = Local::now().date_naive() - Duration::days(retention_days as i64);
        if let Ok(rd) = fs::read_dir(self.logs_dir()) {
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(date) = jsonl_date(&name) {
                    if date < cutoff {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        // recordings/YYYYMMDD/... (이 프로그램이 만든 날짜 폴더만)
        if let Ok(days) = fs::read_dir(self.recordings_dir()) {
            for day in days.flatten().filter(|e| e.path().is_dir()) {
                let name = day.file_name().to_string_lossy().to_string();
                if let Ok(date) = NaiveDate::parse_from_str(&name, "%Y%m%d") {
                    if date < cutoff {
                        let _ = fs::remove_dir_all(day.path());
                    }
                }
            }
        }
    }
}

fn upsert_session(q: &mut VecDeque<SessionRecord>, rec: SessionRecord) {
    if let Some(pos) = q.iter().position(|s| s.id == rec.id) {
        q.remove(pos);
    }
    push(q, rec, MAX_SESSIONS);
}

/// 시간 구간으로 묶기 (점이 너무 많을 때)
fn group_by_time<'a, T>(
    items: &[&'a T],
    ts: impl Fn(&T) -> DateTime<Local>,
    from: DateTime<Local>,
    to: DateTime<Local>,
    max_points: usize,
) -> Vec<(DateTime<Local>, Vec<&'a T>)> {
    let span = (to - from).num_milliseconds().max(1);
    let bucket = (span / max_points.max(1) as i64).max(1);
    let mut groups: BTreeMap<i64, Vec<&T>> = BTreeMap::new();
    for it in items {
        let key = (ts(it).timestamp_millis() - from.timestamp_millis()) / bucket;
        groups.entry(key).or_default().push(*it);
    }
    groups
        .into_iter()
        .map(|(k, g)| {
            let t = if bucket <= 1000 { ts(g[0]) } else { from + Duration::milliseconds(k * bucket) };
            (t, g)
        })
        .collect()
}

/// 폴더 안 파일 개수·용량 (설정 화면 표시용)
pub fn dir_stats(dir: &Path, exts: &[&str]) -> DirStats {
    let mut stats = DirStats::default();
    let mut stack = vec![dir.to_path_buf()];
    let mut visited = 0u64;
    while let Some(d) = stack.pop() {
        let Ok(rd) = fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            visited += 1;
            if visited > 500_000 {
                return stats;
            }
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|x| x.to_str()).map_or(false, |x| exts.iter().any(|w| x.eq_ignore_ascii_case(w))) {
                stats.files += 1;
                stats.bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    stats
}

/// 드라이브 남은 용량 (바이트)
#[cfg(windows)]
pub fn free_space(dir: &Path) -> Option<u64> {
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let wide: Vec<u16> = dir.as_os_str().to_string_lossy().encode_utf16().chain(Some(0)).collect();
    let mut free = 0u64;
    let ok = unsafe { GetDiskFreeSpaceExW(wide.as_ptr(), &mut free, std::ptr::null_mut(), std::ptr::null_mut()) };
    (ok != 0).then_some(free)
}

#[cfg(not(windows))]
pub fn free_space(_dir: &Path) -> Option<u64> {
    None
}

fn log_path(dir: &Path, kind: Kind, date: NaiveDate) -> PathBuf {
    dir.join(format!("{}-{}.jsonl", kind.prefix(), date.format("%Y-%m-%d")))
}

fn append_line(dir: &Path, kind: Kind, date: NaiveDate, line: &str) {
    let path = log_path(dir, kind, date);
    let result = fs::create_dir_all(dir)
        .and_then(|_| OpenOptions::new().create(true).append(true).open(&path))
        .and_then(|mut f| writeln!(f, "{line}"));
    if let Err(e) = result {
        tracing::warn!("로그 기록 실패 {}: {e}", path.display());
    }
}

/// 줄마다 따로 읽는다. PC가 갑자기 꺼지면 쓰던 줄이 잘리거나 깨진 글자가 섞일 수 있는데,
/// 그런 줄 하나 때문에 그 뒤의 기록(예: 그날의 나머지 세션)까지 못 읽으면 안 된다.
fn read_jsonl<T: DeserializeOwned>(path: &Path) -> Vec<T> {
    let Ok(bytes) = fs::read(path) else {
        return Vec::new();
    };
    bytes.split(|b| *b == b'\n').filter_map(|line| serde_json::from_slice(line).ok()).collect()
}

fn jsonl_date(name: &str) -> Option<NaiveDate> {
    let stem = name.strip_suffix(".jsonl")?;
    let date = stem.get(stem.len().checked_sub(10)?..)?;
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

pub fn same_dir(a: &Path, b: &Path) -> bool {
    let norm = |p: &Path| {
        let c = fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        c.to_string_lossy().replace('/', "\\").trim_end_matches('\\').to_lowercase()
    };
    norm(a) == norm(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_upsert_keeps_last_state() {
        let mut q = VecDeque::new();
        let a = SessionRecord { id: "s1".into(), status: "running".into(), ..Default::default() };
        upsert_session(&mut q, a.clone());
        upsert_session(&mut q, SessionRecord { status: "finished".into(), ..a });
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].status, "finished");
    }

    #[test]
    fn percentile_nearest_rank() {
        let v = sorted((1..=100).map(f64::from).collect());
        assert_eq!(percentile(&v, 50.0), Some(50.0));
        assert_eq!(percentile(&v, 95.0), Some(95.0));
        assert_eq!(percentile(&v, 99.0), Some(99.0));
    }
}

#[cfg(test)]
mod jsonl_tests {
    use super::*;

    #[test]
    fn a_broken_line_does_not_hide_the_lines_after_it() {
        let dir = std::env::temp_dir().join(format!("capture-vms-jsonl-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let p = dir.join("events-2026-11-14.jsonl");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"{\"a\":1}\r\n");
        // 깨진 글자가 든 채 잘린 줄, 전원이 꺼지며 0으로 채워진 줄
        bytes.extend_from_slice(b"{\"a\":2,\"b\":\"\xff\xfe broken\n\0\0\0\0\n");
        // 그 뒤의 정상 줄들 (마지막 줄은 줄바꿈 없이 끝남)
        bytes.extend_from_slice(b"{\"a\":3}\n{\"a\":4}");
        fs::write(&p, bytes).unwrap();
        #[derive(serde::Deserialize)]
        struct R {
            a: u32,
        }
        let got: Vec<u32> = read_jsonl::<R>(&p).into_iter().map(|r| r.a).collect();
        assert_eq!(got, vec![1, 3, 4]);
        let _ = fs::remove_dir_all(&dir);
    }
}
