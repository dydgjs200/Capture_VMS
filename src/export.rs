//! 원시 데이터 조회·요약과 내보내기 (Excel 파일, PDF 보고서).
//!
//! 모든 기록은 로그 폴더의 날짜별 JSONL 파일에 있다. 여기서 기간·카메라로 골라 읽어
//!   - 대시보드 [원시 데이터] 표 (쪽 단위)
//!   - 요약 (카메라별 수신·촬영·트리거·네트워크와 자동 소견)
//!   - Excel: 요약 + 종류별 시트 (원본 값 그대로)
//!   - PDF: 보고서 HTML을 Edge(화면 없이)로 인쇄
//! 를 만든다. 비밀번호는 기록에 없으므로(주소는 계정을 가린 채 저장) 내보내도 새지 않는다.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    app::App,
    config,
    store::{self, CaptureRecord, Event, Kind, ProbeSample, SessionRecord, StreamSample, TriggerRecord, WifiInfo},
    trigger,
};

/// 수신 1초 기록은 양이 많아(카메라 1대 하루 86,400줄) 이 기간까지만 읽는다
pub const MAX_STREAM_DAYS: i64 = 7;
/// 엑셀 시트 한 장 최대 행
const XLSX_MAX_ROWS: usize = 1_000_000;

// ---------------- 기간 ----------------

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RangeQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub camera: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Range {
    pub from: DateTime<Local>,
    pub to: DateTime<Local>,
    pub camera: Option<String>,
}

/// "2026-11-09T10:15", "2026-11-09 10:15:30", "2026-11-09", RFC3339. 끝 시각은 그 분·그 날의 끝까지 포함.
pub fn parse_time(s: &str, end: bool) -> Option<DateTime<Local>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Local));
    }
    for (f, has_sec) in [("%Y-%m-%dT%H:%M:%S%.f", true), ("%Y-%m-%d %H:%M:%S%.f", true), ("%Y-%m-%dT%H:%M", false), ("%Y-%m-%d %H:%M", false)] {
        if let Ok(n) = NaiveDateTime::parse_from_str(s, f) {
            let n = if end && !has_sec { n + chrono::Duration::milliseconds(59_999) } else { n };
            return Local.from_local_datetime(&n).earliest();
        }
    }
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let n = if end { d.and_hms_milli_opt(23, 59, 59, 999)? } else { d.and_hms_opt(0, 0, 0)? };
    Local.from_local_datetime(&n).earliest()
}

impl RangeQuery {
    pub fn resolve(&self) -> Result<Range, String> {
        let now = Local::now();
        let to = match self.to.as_deref().filter(|s| !s.trim().is_empty()) {
            Some(s) => parse_time(s, true).ok_or_else(|| format!("끝 시각 형식 오류: {s}"))?,
            None => now,
        };
        let from = match self.from.as_deref().filter(|s| !s.trim().is_empty()) {
            Some(s) => parse_time(s, false).ok_or_else(|| format!("시작 시각 형식 오류: {s}"))?,
            None => to - chrono::Duration::hours(24),
        };
        if from > to {
            return Err("시작 시각이 끝 시각보다 늦습니다".into());
        }
        if (to - from).num_days() > 400 {
            return Err("한 번에 400일까지만 볼 수 있습니다".into());
        }
        let camera = self.camera.clone().filter(|c| !c.is_empty() && c != "all");
        Ok(Range { from, to, camera })
    }
}

// ---------------- 읽기 ----------------

#[derive(Default)]
pub struct Data {
    pub sessions: Vec<SessionRecord>,
    pub captures: Vec<CaptureRecord>,
    pub triggers: Vec<TriggerRecord>,
    /// 기간이 길면 읽지 않는다 (None)
    pub stream: Option<Vec<StreamSample>>,
    pub probes: Vec<ProbeSample>,
    pub wifi: Vec<WifiInfo>,
    pub events: Vec<Event>,
}

fn cam_ok(filter: Option<&str>, id: &str) -> bool {
    filter.map_or(true, |c| c == id)
}

pub fn stream_allowed(r: &Range) -> bool {
    (r.to - r.from).num_seconds() <= MAX_STREAM_DAYS * 86_400
}

fn sessions_of(app: &App, r: &Range) -> Vec<SessionRecord> {
    let cam = r.camera.as_deref();
    let mut v = app.store.sessions_in_range(r.from, r.to);
    // 진행 중인 세션은 파일에 시작 때 값만 있으므로 지금 값으로 바꾼다
    for s in v.iter_mut() {
        if let Some(live) = app.session(&s.id) {
            *s = live.view().record;
        }
    }
    v.retain(|s| cam_ok(cam, &s.camera_id));
    v.sort_by_key(|s| s.started_at);
    v
}

fn triggers_of(app: &App, r: &Range) -> Vec<TriggerRecord> {
    let cam = r.camera.as_deref();
    let mut v = app.store.read_range::<TriggerRecord>(Kind::Triggers, r.from, r.to, |t| Some(t.ts));
    // 카메라를 고르면 그 카메라를 찍은 신호만 (무시된 신호는 카메라가 없어서 전체 볼 때만)
    v.retain(|t| cam.map_or(true, |c| t.shots.iter().any(|s| s.camera_id == c)));
    v
}

pub fn load(app: &App, r: &Range, with_stream: bool) -> Data {
    let cam = r.camera.as_deref();
    let st = &app.store;
    let mut captures = st.read_range::<CaptureRecord>(Kind::Captures, r.from, r.to, |c| c.requested_at);
    captures.retain(|c| cam_ok(cam, &c.camera_id));
    let stream = (with_stream && stream_allowed(r)).then(|| {
        let mut v = st.read_range::<StreamSample>(Kind::Stream, r.from, r.to, |s| Some(s.ts));
        v.retain(|s| cam_ok(cam, &s.camera_id));
        v
    });
    let mut probes = st.read_range::<ProbeSample>(Kind::Network, r.from, r.to, |s| Some(s.ts));
    probes.retain(|p| cam_ok(cam, &p.camera_id));
    let mut events = st.read_range::<Event>(Kind::Events, r.from, r.to, |e| Some(e.ts));
    events.retain(|e| e.camera_id.as_deref().map_or(true, |c| cam_ok(cam, c)));
    Data {
        sessions: sessions_of(app, r),
        captures,
        triggers: triggers_of(app, r),
        stream,
        probes,
        wifi: st.read_range::<WifiInfo>(Kind::Wifi, r.from, r.to, |w| Some(w.ts)),
        events,
    }
}

/// [원시 데이터] 표: 종류 하나를 최근 것부터
pub fn raw_rows(app: &App, kind: &str, r: &Range) -> Result<Vec<Value>, String> {
    let cam = r.camera.as_deref();
    let st = &app.store;
    let to_vals = |v: Vec<Value>| -> Vec<Value> { v.into_iter().rev().collect() };
    fn vals<T: Serialize>(v: Vec<T>) -> Vec<Value> {
        v.iter().filter_map(|x| serde_json::to_value(x).ok()).collect()
    }
    Ok(match kind {
        "sessions" => to_vals(vals(sessions_of(app, r))),
        "captures" => {
            let mut v = st.read_range::<CaptureRecord>(Kind::Captures, r.from, r.to, |c| c.requested_at);
            v.retain(|c| cam_ok(cam, &c.camera_id));
            to_vals(vals(v))
        }
        "triggers" => to_vals(vals(triggers_of(app, r))),
        "stream" => {
            if !stream_allowed(r) {
                return Err(format!("수신 1초 기록은 양이 많아 {MAX_STREAM_DAYS}일 이내로만 볼 수 있습니다. 기간을 줄여 주세요"));
            }
            let mut v = st.read_range::<StreamSample>(Kind::Stream, r.from, r.to, |s| Some(s.ts));
            v.retain(|s| cam_ok(cam, &s.camera_id));
            to_vals(vals(v))
        }
        "network" => {
            let mut v = st.read_range::<ProbeSample>(Kind::Network, r.from, r.to, |s| Some(s.ts));
            v.retain(|p| cam_ok(cam, &p.camera_id));
            to_vals(vals(v))
        }
        "wifi" => to_vals(vals(st.read_range::<WifiInfo>(Kind::Wifi, r.from, r.to, |w| Some(w.ts)))),
        "events" => {
            let mut v = st.read_range::<Event>(Kind::Events, r.from, r.to, |e| Some(e.ts));
            v.retain(|e| e.camera_id.as_deref().map_or(true, |c| cam_ok(cam, c)));
            to_vals(vals(v))
        }
        _ => return Err(format!("알 수 없는 종류: {kind}")),
    })
}

// ---------------- 요약 ----------------

#[derive(Debug, Clone, Default, Serialize)]
pub struct Stat {
    pub n: usize,
    pub min: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
}

pub fn stat(mut v: Vec<f64>) -> Stat {
    v.retain(|x| x.is_finite());
    v.sort_by(|a, b| a.total_cmp(b));
    Stat {
        n: v.len(),
        min: v.first().copied(),
        p50: store::percentile(&v, 50.0),
        p95: store::percentile(&v, 95.0),
        max: v.last().copied(),
        mean: store::avg(&v),
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CapStat {
    pub total: usize,
    pub ok: usize,
    /// 성공한 것의 누름(신호) → 저장 시간
    pub ms: Stat,
    /// 기준 시간 (설정 `capture_goal_ms` / `capture_limit_ms`)
    pub goal_ms: u64,
    pub limit_ms: u64,
    /// 전체 시도 중 기준 안에 저장까지 끝난 횟수 (실패는 '기준 밖')
    pub within_goal: usize,
    pub within_limit: usize,
}

impl CapStat {
    pub fn goal_pct(&self) -> Option<f64> {
        (self.total > 0).then(|| self.within_goal as f64 * 100.0 / self.total as f64)
    }
    pub fn limit_pct(&self) -> Option<f64> {
        (self.total > 0).then(|| self.within_limit as f64 * 100.0 / self.total as f64)
    }
    /// "1초 이내 95.0% · 3초 이내 100.0%"
    pub fn goal_text(&self) -> String {
        match (self.goal_pct(), self.limit_pct()) {
            (Some(g), Some(l)) => format!("{} 이내 {g:.1}% · {} 이내 {l:.1}%", ms_label(self.goal_ms), ms_label(self.limit_ms)),
            _ => "—".into(),
        }
    }
}

/// 1000 → "1초", 1500 → "1.5초", 800 → "800ms"
pub fn ms_label(ms: u64) -> String {
    if ms >= 1000 && ms % 100 == 0 {
        let s = ms as f64 / 1000.0;
        if ms % 1000 == 0 { format!("{s:.0}초") } else { format!("{s:.1}초") }
    } else {
        format!("{ms}ms")
    }
}

/// 설정의 촬영 기준 (목표, 허용 한도)
pub fn goals(app: &App) -> (u64, u64) {
    let g = app.cfg.stream.capture_goal_ms.max(1);
    (g, app.cfg.stream.capture_limit_ms.max(g))
}

fn cap_stat(it: impl Iterator<Item = (bool, Option<f64>)>, (goal, limit): (u64, u64)) -> CapStat {
    let mut c = CapStat { goal_ms: goal, limit_ms: limit, ..Default::default() };
    let mut ms = Vec::new();
    for (ok, t) in it {
        c.total += 1;
        if ok {
            c.ok += 1;
            if let Some(t) = t {
                ms.push(t);
                if t <= goal as f64 {
                    c.within_goal += 1;
                }
                if t <= limit as f64 {
                    c.within_limit += 1;
                }
            }
        }
    }
    c.ms = stat(ms);
    c
}

/// 측정 위치별 촬영 결과
#[derive(Debug, Clone, Default, Serialize)]
pub struct LocSummary {
    pub location: String,
    /// 모든 촬영
    pub all: CapStat,
    pub session: CapStat,
    pub direct: CapStat,
    /// 트리거(동시 촬영): 신호 → 모두 저장
    pub trigger: CapStat,
    pub skew_ms: Stat,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CamSummary {
    pub camera_id: String,
    pub camera_name: String,
    /// 보고서에 쓰는 이름. 이름이 같은 카메라가 있으면 `이름 (cam02)`처럼 id를 붙인다
    pub label: String,
    pub host: Option<String>,
    pub sessions: usize,
    pub stable: usize,
    pub warning: usize,
    pub unstable: usize,
    pub failed: usize,
    pub running: usize,
    /// 세션 길이 합 (초)
    pub receive_s: f64,
    pub frames: u64,
    /// 프레임 누락: 와야 할 프레임 수와 그중 안 온 수 (카메라 fps를 아는 세션만 합산), 영상 시각이 튄 프레임 수
    pub expected_frames: u64,
    pub missing_frames: u64,
    pub ts_glitch_frames: u64,
    /// 세션 길이로 가중 평균
    pub avg_fps: Option<f64>,
    pub camera_fps: Option<f64>,
    pub avg_mbps: Option<f64>,
    pub uptime_pct: Option<f64>,
    pub resolution: Option<String>,
    pub codec: Option<String>,
    pub paths: Vec<String>,
    pub stalls: u64,
    pub longest_stall_ms: f64,
    pub reconnects: u64,
    pub downtime_ms: f64,
    pub rtp_loss_events: u64,
    pub rtp_lost_packets: u64,
    pub decode_errors: u64,
    pub delay_max_ms: Option<f64>,
    pub delayed_secs: u64,
    pub interval_p99_max_ms: Option<f64>,
    pub e2e_p50_ms: Option<f64>,
    pub capture_session: CapStat,
    pub capture_direct: CapStat,
    /// 트리거로 찍은 것만
    pub capture_trigger: CapStat,
    pub ping: Stat,
    pub ping_loss_pct: Option<f64>,
    pub probes: usize,
    pub camera_down: usize,
    /// 녹화 점검: 녹화한 세션 수, 점검한 수, 정상 수, 녹화 길이·녹화 안 된 시간 합 (초)
    pub rec_sessions: usize,
    pub rec_checked: usize,
    pub rec_ok: usize,
    pub rec_seconds: f64,
    pub rec_gap_s: f64,
    pub rec_bytes: u64,
    /// 문제 있던 세션의 점검 요약
    pub rec_problems: Vec<String>,
    /// 프로그램이 갑자기 꺼져 중간 저장까지만 남은 세션 수
    pub interrupted: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TrigSummary {
    pub total: usize,
    pub ok: usize,
    pub partial: usize,
    pub failed: usize,
    pub ignored: usize,
    /// 신호 → 모든 카메라 저장 (성공한 것)
    pub total_ms: Stat,
    /// 카메라끼리 찍힌 시각 차이
    pub skew_ms: Stat,
    /// 편차를 프레임 도착 시각으로 잰 횟수 (나머지는 저장 시각 기준)
    pub skew_frame: usize,
    pub by_source: Vec<(String, usize)>,
    /// 신호 → 모두 저장이 기준 안에 끝난 비율 (일부 실패·실패는 기준 밖)
    pub goal: CapStat,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WifiSummary {
    pub samples: usize,
    pub connected_pct: Option<f64>,
    pub signal: Stat,
    pub rssi_avg: Option<f64>,
    pub roams: usize,
    pub disconnects: usize,
    pub ssids: Vec<String>,
    pub bssids: Vec<String>,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventCount {
    pub kind: String,
    pub label: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub from: DateTime<Local>,
    pub to: DateTime<Local>,
    pub camera: Option<String>,
    pub generated_at: DateTime<Local>,
    pub counts: BTreeMap<String, usize>,
    pub cameras: Vec<CamSummary>,
    pub triggers: TrigSummary,
    pub wifi: WifiSummary,
    pub events: Vec<EventCount>,
    /// 사람이 읽을 결론 (보고서 첫머리)
    pub findings: Vec<String>,
    pub stream_included: bool,
    /// 촬영 기준 (ms)
    pub goal_ms: u64,
    pub limit_ms: u64,
    /// 측정 위치별 (위치를 고른 기록이 있을 때만)
    pub locations: Vec<LocSummary>,
    /// 한눈에 보기 (PDF 첫 장·요약 창)
    pub glance: Glance,
}

pub fn event_ko(kind: &str) -> &'static str {
    match kind {
        "app_start" => "프로그램 시작",
        "app_stop" => "프로그램 종료",
        "session_start" => "수신 시작",
        "session_end" => "수신 종료",
        "session_failed" => "연결 실패",
        "stream_stall" => "영상 끊김",
        "stream_lost" => "재연결",
        "stream_recovered" => "수신 재개",
        "camera_up" => "카메라 응답",
        "camera_down" => "카메라 응답 없음",
        "camera_added" => "카메라 추가",
        "camera_updated" => "카메라 수정",
        "camera_moved" => "카메라 주소 바뀜",
        "camera_mismatch" => "카메라 확인 필요",
        "camera_removed" => "카메라 삭제",
        "wifi_roam" => "AP 전환",
        "wifi_connected" => "Wi-Fi 연결",
        "wifi_disconnected" => "Wi-Fi 끊김",
        "wifi_hold" => "Wi-Fi 고정",
        "ts_glitch" => "영상 시각 튐",
        "viewer_lag" => "화면 밀림",
        "stall_diag" => "끊김 진단",
        "pc_stall" => "PC 멈춤",
        "app_crashed" => "비정상 종료",
        "session_resumed" => "수신 이어받기",
        "e2e_measured" => "실제 지연 측정",
        "storage_changed" => "저장 위치 변경",
        "trigger_failed" => "트리거 실패",
        "trigger_config" => "트리거 설정 변경",
        "auto_trigger_start" => "반복 시험 시작",
        "auto_trigger_stop" => "반복 시험 중지",
        "auto_trigger_done" => "반복 시험 끝",
        "plc_tcp_listen" => "PLC TCP 대기",
        "plc_tcp_error" => "PLC TCP 오류",
        "plc_connected" => "PLC 연결",
        "plc_disconnected" => "PLC 연결 끊김",
        "plc_modbus_connected" => "PLC Modbus 연결",
        "plc_modbus_lost" => "PLC Modbus 끊김",
        "plc_modbus_error" => "PLC Modbus 오류",
        "disk_full_stop" => "저장 공간 부족 중지",
        "rec_check_failed" => "녹화 점검 문제",
        "session_recovered" => "중단 세션 정리",
        "location" => "측정 위치",
        "net_scan" => "IP로 찾기",
        "export" => "내보내기",
        _ => "기타",
    }
}

fn fmt_ms(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.0}ms")).unwrap_or_else(|| "—".into())
}

pub fn fmt_dur(sec: f64) -> String {
    let s = sec.round() as i64;
    if s >= 3600 {
        format!("{}시간 {}분", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}분 {}초", s / 60, s % 60)
    } else {
        format!("{s}초")
    }
}

fn weighted(items: impl Iterator<Item = (Option<f64>, f64)>) -> Option<f64> {
    let (mut sum, mut w) = (0.0, 0.0);
    for (v, d) in items {
        if let Some(v) = v {
            let d = d.max(1.0);
            sum += v * d;
            w += d;
        }
    }
    (w > 0.0).then(|| sum / w)
}

/// 같은 모델을 2대 달면 이름이 같아지므로, 겹치는 이름에만 id를 붙인다: (id, 보고서 이름)
fn cam_labels<'a>(cams: impl Iterator<Item = (&'a str, &'a str)> + Clone) -> Vec<(String, String)> {
    cams.clone()
        .map(|(id, name)| {
            let dup = cams.clone().filter(|c| c.1 == name).count() > 1;
            (id.to_string(), if dup { format!("{name} ({id})") } else { name.to_string() })
        })
        .collect()
}

pub fn summarize(app: &App, r: &Range, d: &Data) -> Summary {
    let g = goals(app);
    // 카메라 목록: 등록된 카메라 + 기록에 나온 카메라
    let mut cams: Vec<(String, String, Option<String>)> = app.camera_configs().into_iter().map(|c| (c.id, c.name, Some(c.host))).collect();
    let seen = d
        .sessions
        .iter()
        .map(|s| (s.camera_id.clone(), s.camera_name.clone()))
        .chain(d.captures.iter().map(|c| (c.camera_id.clone(), c.camera_name.clone())))
        .chain(d.triggers.iter().flat_map(|t| t.shots.iter().map(|s| (s.camera_id.clone(), s.camera_name.clone()))));
    for (id, name) in seen {
        if !cams.iter().any(|c| c.0 == id) {
            cams.push((id, name, None));
        }
    }
    // 카메라를 골라 뽑아도 같은 이름이 나오게 거르기 전에 정한다
    let labels = cam_labels(cams.iter().map(|c| (c.0.as_str(), c.1.as_str())));
    if let Some(c) = &r.camera {
        cams.retain(|x| &x.0 == c);
    }

    let mut out_cams = Vec::new();
    for (id, name, host) in cams {
        let ss: Vec<&SessionRecord> = d.sessions.iter().filter(|s| s.camera_id == id).collect();
        let cs: Vec<&CaptureRecord> = d.captures.iter().filter(|c| c.camera_id == id).collect();
        let ps: Vec<&ProbeSample> = d.probes.iter().filter(|p| p.camera_id == id).collect();
        if ss.is_empty() && cs.is_empty() && ps.is_empty() && host.is_none() {
            continue;
        }
        let label = labels.iter().find(|l| l.0 == id).map_or_else(|| name.clone(), |l| l.1.clone());
        let mut c = CamSummary { camera_id: id.clone(), camera_name: name, label, host, ..Default::default() };
        c.sessions = ss.len();
        for s in &ss {
            let v = if s.status == "running" { "running" } else { s.verdict.as_deref().unwrap_or("failed") };
            match v {
                "stable" => c.stable += 1,
                "warning" => c.warning += 1,
                "unstable" => c.unstable += 1,
                "running" => c.running += 1,
                _ => c.failed += 1,
            }
            let st = &s.stats;
            c.receive_s += s.duration_s;
            c.frames += st.frames;
            if let (Some(e), Some(m)) = (st.expected_frames, st.missing_frames) {
                c.expected_frames += e;
                c.missing_frames += m;
            }
            c.ts_glitch_frames += st.ts_glitch_frames;
            c.stalls += st.stalls;
            c.longest_stall_ms = c.longest_stall_ms.max(st.longest_stall_ms);
            c.reconnects += st.reconnects;
            c.downtime_ms += st.downtime_ms;
            c.rtp_loss_events += st.rtp_loss_events;
            c.rtp_lost_packets += st.rtp_lost_packets;
            c.decode_errors += st.decode_errors;
            c.delayed_secs += st.delayed_secs;
            c.delay_max_ms = [c.delay_max_ms, st.delay_max_ms].into_iter().flatten().reduce(f64::max);
            c.interval_p99_max_ms = [c.interval_p99_max_ms, st.interval_p99_ms].into_iter().flatten().reduce(f64::max);
            if st.width.is_some() {
                c.resolution = Some(format!("{}×{}", st.width.unwrap_or(0), st.height.unwrap_or(0)));
            }
            if st.codec.is_some() {
                c.codec = st.codec.clone().map(|x| x.to_uppercase());
            }
            c.camera_fps = st.camera_fps.or(st.nominal_fps).or(c.camera_fps);
            let p = crate::stream::path_label(s.net.path_kind.as_deref()).to_string();
            if !c.paths.contains(&p) {
                c.paths.push(p);
            }
        }
        c.avg_fps = weighted(ss.iter().map(|s| (s.stats.avg_fps, s.duration_s)));
        c.avg_mbps = weighted(ss.iter().map(|s| (s.stats.avg_kbps.map(|k| k / 1000.0), s.duration_s)));
        c.uptime_pct = weighted(ss.iter().map(|s| (s.stats.uptime_pct, s.duration_s)));
        c.e2e_p50_ms = stat(ss.iter().filter_map(|s| s.e2e.as_ref().map(|e| e.p50_ms)).collect()).p50;
        c.capture_session = cap_stat(cs.iter().filter(|x| x.mode != "direct").map(|x| (x.ok, x.total_ms)), g);
        c.capture_direct = cap_stat(cs.iter().filter(|x| x.mode == "direct").map(|x| (x.ok, x.total_ms)), g);
        c.capture_trigger = cap_stat(cs.iter().filter(|x| x.trigger_id.is_some()).map(|x| (x.ok, x.total_ms)), g);
        for s in &ss {
            c.interrupted += s.interrupted as usize;
            if s.record {
                c.rec_sessions += 1;
                c.rec_bytes += s.recorded_bytes;
            }
            if let Some(k) = &s.rec_check {
                c.rec_checked += 1;
                c.rec_ok += k.ok as usize;
                c.rec_seconds += k.seconds;
                c.rec_gap_s += k.gap_s;
                if !k.ok {
                    c.rec_problems.push(format!("{} {}: {}", s.started_at.map(|t| t.format("%m/%d %H:%M").to_string()).unwrap_or_default(), s.label, k.summary));
                }
            }
        }
        c.probes = ps.len();
        c.ping = stat(ps.iter().filter_map(|p| p.icmp_ms).collect());
        c.ping_loss_pct = (!ps.is_empty()).then(|| ps.iter().filter(|p| !p.icmp_ok).count() as f64 * 100.0 / ps.len() as f64);
        c.camera_down = d.events.iter().filter(|e| e.kind == "camera_down" && e.camera_id.as_deref() == Some(id.as_str())).count();
        out_cams.push(c);
    }

    // 트리거
    let mut t = TrigSummary::default();
    let mut by_src: BTreeMap<String, usize> = BTreeMap::new();
    let (mut tot, mut skew) = (Vec::new(), Vec::new());
    for x in &d.triggers {
        if x.status == "ignored" {
            t.ignored += 1;
            continue;
        }
        t.total += 1;
        *by_src.entry(trigger::source_ko(&x.source).to_string()).or_default() += 1;
        match x.status.as_str() {
            "ok" => t.ok += 1,
            "partial" => t.partial += 1,
            _ => t.failed += 1,
        }
        if x.status == "ok" {
            tot.extend(x.total_ms);
        }
        if let Some(s) = x.skew_ms {
            skew.push(s);
            if x.skew_basis.as_deref() == Some("frame") {
                t.skew_frame += 1;
            }
        }
    }
    t.total_ms = stat(tot);
    t.skew_ms = stat(skew);
    t.by_source = by_src.into_iter().collect();
    t.goal = cap_stat(d.triggers.iter().filter(|x| x.status != "ignored").map(|x| (x.status == "ok", x.total_ms)), g);

    // 측정 위치별 (위치가 붙은 기록이 있을 때만)
    let mut locs: Vec<String> = Vec::new();
    for l in d.captures.iter().filter_map(|c| c.location.clone()).chain(d.triggers.iter().filter_map(|t| t.location.clone())) {
        if !locs.contains(&l) {
            locs.push(l);
        }
    }
    let locations: Vec<LocSummary> = locs
        .into_iter()
        .map(|l| {
            let cs: Vec<&CaptureRecord> = d.captures.iter().filter(|c| c.location.as_ref() == Some(&l)).collect();
            let ts: Vec<&TriggerRecord> = d.triggers.iter().filter(|t| t.status != "ignored" && t.location.as_ref() == Some(&l)).collect();
            LocSummary {
                all: cap_stat(cs.iter().map(|x| (x.ok, x.total_ms)), g),
                session: cap_stat(cs.iter().filter(|x| x.mode != "direct").map(|x| (x.ok, x.total_ms)), g),
                direct: cap_stat(cs.iter().filter(|x| x.mode == "direct").map(|x| (x.ok, x.total_ms)), g),
                trigger: cap_stat(ts.iter().map(|x| (x.status == "ok", x.total_ms)), g),
                skew_ms: stat(ts.iter().filter_map(|x| x.skew_ms).collect()),
                location: l,
            }
        })
        .collect();

    // Wi-Fi
    let mut w = WifiSummary { samples: d.wifi.len(), ..Default::default() };
    if !d.wifi.is_empty() {
        w.connected_pct = Some(d.wifi.iter().filter(|x| x.state == "connected").count() as f64 * 100.0 / d.wifi.len() as f64);
        w.signal = stat(d.wifi.iter().filter_map(|x| x.signal_pct.map(f64::from)).collect());
        w.rssi_avg = store::avg(&d.wifi.iter().filter_map(|x| x.rssi_dbm.map(f64::from)).collect::<Vec<_>>());
        for x in &d.wifi {
            for (list, v) in [(&mut w.ssids, x.ssid.clone()), (&mut w.bssids, x.bssid.clone()), (&mut w.channels, x.channel.map(|c| format!("{c} ({})", x.band.clone().unwrap_or_default())))] {
                if let Some(v) = v {
                    if !list.contains(&v) {
                        list.push(v);
                    }
                }
            }
        }
    }
    w.roams = d.events.iter().filter(|e| e.kind == "wifi_roam").count();
    w.disconnects = d.events.iter().filter(|e| e.kind == "wifi_disconnected").count();

    let mut ev: BTreeMap<String, usize> = BTreeMap::new();
    for e in &d.events {
        *ev.entry(e.kind.clone()).or_default() += 1;
    }
    let mut events: Vec<EventCount> = ev.into_iter().map(|(k, n)| EventCount { label: event_ko(&k).into(), kind: k, count: n }).collect();
    events.sort_by(|a, b| b.count.cmp(&a.count));

    let mut counts = BTreeMap::new();
    counts.insert("sessions".to_string(), d.sessions.len());
    counts.insert("captures".to_string(), d.captures.len());
    counts.insert("triggers".to_string(), d.triggers.len());
    counts.insert("network".to_string(), d.probes.len());
    counts.insert("wifi".to_string(), d.wifi.len());
    counts.insert("events".to_string(), d.events.len());
    if let Some(s) = &d.stream {
        counts.insert("stream".to_string(), s.len());
    }

    let findings = findings(&out_cams, &t, &w, &locations);
    let mut s = Summary {
        glance: Glance::default(),
        goal_ms: g.0,
        limit_ms: g.1,
        locations,
        from: r.from,
        to: r.to,
        camera: r.camera.clone(),
        generated_at: Local::now(),
        counts,
        cameras: out_cams,
        triggers: t,
        wifi: w,
        events,
        findings,
        stream_included: d.stream.is_some(),
    };
    s.glance = glance(&s, d);
    s
}

impl Summary {
    /// 기록 한 줄에 쓸 카메라 이름: 요약의 label (겹치는 이름이면 id 포함), 요약에 없는 카메라는 기록에 남은 이름
    pub fn cam_label<'a>(&'a self, id: &str, recorded: &'a str) -> &'a str {
        self.cameras.iter().find(|c| c.camera_id == id).map_or(recorded, |c| c.label.as_str())
    }
}

/// 숫자를 사람이 읽는 결론으로
fn findings(cams: &[CamSummary], t: &TrigSummary, w: &WifiSummary, locs: &[LocSummary]) -> Vec<String> {
    let mut f = Vec::new();
    for c in cams {
        if c.sessions > 0 {
            let mut v = vec![];
            for (n, k) in [(c.stable, "안정"), (c.warning, "주의"), (c.unstable, "불안정"), (c.failed, "실패"), (c.running, "진행 중")] {
                if n > 0 {
                    v.push(format!("{k} {n}"));
                }
            }
            f.push(format!(
                "{}: 수신 {}회 · 총 {} · 판정 {} · 평균 {} · 정상 수신 {} · 끊김 {}회 · 재연결 {}회",
                c.label,
                c.sessions,
                fmt_dur(c.receive_s),
                v.join(" / "),
                c.avg_fps.map(|x| format!("{x:.1}fps")).unwrap_or_else(|| "—".into()),
                c.uptime_pct.map(|x| format!("{x:.2}%")).unwrap_or_else(|| "—".into()),
                c.stalls,
                c.reconnects
            ));
            if c.expected_frames > 0 && c.missing_frames > 0 {
                f.push(format!(
                    "{}: 프레임 누락 {}개 (와야 할 {}개의 {:.2}%) — 끊김·재연결 동안 오지 않은 영상",
                    c.label,
                    c.missing_frames,
                    c.expected_frames,
                    c.missing_frames as f64 / c.expected_frames as f64 * 100.0
                ));
            }
        }
        let caps = [(&c.capture_session, "수신 중 촬영"), (&c.capture_direct, "단독 촬영")];
        for (s, k) in caps {
            if s.total > 0 {
                f.push(format!(
                    "{}: {k} {}회 중 성공 {}회 ({:.1}%) · 누름→저장 중앙값 {} · p95 {} · 최대 {} · {}",
                    c.label,
                    s.total,
                    s.ok,
                    s.ok as f64 * 100.0 / s.total as f64,
                    fmt_ms(s.ms.p50),
                    fmt_ms(s.ms.p95),
                    fmt_ms(s.ms.max),
                    s.goal_text()
                ));
            }
        }
        // 촬영 방식 비교: 상시 수신(빠름, 무선을 계속 씀) vs 요청할 때만 접속(느림, 무선 부담 적음)
        if let (Some(a), Some(b)) = (c.capture_session.ms.p50, c.capture_direct.ms.p50) {
            f.push(format!(
                "{}: 촬영 방식 비교 — 수신 중 촬영 {}가 단독 촬영(요청할 때 접속) {}보다 {} 빠름{}",
                c.label,
                fmt_ms(Some(a)),
                fmt_ms(Some(b)),
                fmt_ms(Some((b - a).max(0.0))),
                c.avg_mbps.map(|m| format!(" (상시 수신은 카메라 1대당 무선 약 {m:.1}Mbps를 계속 씀)")).unwrap_or_default()
            ));
        }
        if c.rec_checked > 0 {
            f.push(format!(
                "{}: 녹화 점검 {}회 중 정상 {}회 · 녹화 {} ({:.1}GB){}",
                c.label,
                c.rec_checked,
                c.rec_ok,
                fmt_dur(c.rec_seconds),
                c.rec_bytes as f64 / 1e9,
                if c.rec_gap_s >= 1.0 { format!(" · 연결·끊김으로 녹화 안 된 시간 {}", fmt_dur(c.rec_gap_s)) } else { String::new() }
            ));
            for p in c.rec_problems.iter().take(3) {
                f.push(format!("{}: 녹화 문제 — {p}", c.label));
            }
        }
        if c.interrupted > 0 {
            f.push(format!("{}: 프로그램이 갑자기 꺼진 세션 {}개 (마지막 중간 저장 시점까지의 기록으로 판정)", c.label, c.interrupted));
        }
        if let Some(loss) = c.ping_loss_pct {
            if loss > 0.0 {
                f.push(format!("{}: ping 손실 {loss:.1}% (응답 없음 {}번 기록)", c.label, c.camera_down));
            }
        }
    }
    if t.total > 0 {
        f.push(format!(
            "트리거(동시 촬영) {}회 중 전부 성공 {}회 ({:.1}%){}{} · 신호→모든 카메라 저장 중앙값 {} · p95 {} · 최대 {}",
            t.total,
            t.ok,
            t.ok as f64 * 100.0 / t.total as f64,
            if t.partial > 0 { format!(" · 일부 실패 {}회", t.partial) } else { String::new() },
            if t.failed > 0 { format!(" · 실패 {}회", t.failed) } else { String::new() },
            fmt_ms(t.total_ms.p50),
            fmt_ms(t.total_ms.p95),
            fmt_ms(t.total_ms.max)
        ));
        f.push(format!("트리거 신호 → 모두 저장: {}", t.goal.goal_text()));
        if t.skew_ms.n > 0 {
            f.push(format!(
                "카메라 간 촬영 시각 차이(편차) 중앙값 {} · p95 {} · 최대 {} ({})",
                fmt_ms(t.skew_ms.p50),
                fmt_ms(t.skew_ms.p95),
                fmt_ms(t.skew_ms.max),
                if t.skew_frame == t.skew_ms.n { "프레임 도착 시각 기준" } else { "일부는 저장 시각 기준" }
            ));
        }
        if t.ignored > 0 {
            f.push(format!("너무 짧은 간격으로 들어와 무시한 신호 {}회 (PLC 접점 채터링·중복 신호 확인)", t.ignored));
        }
    }
    for l in locs {
        let mut parts = Vec::new();
        for (s, k) in [(&l.session, "수신 중 촬영"), (&l.direct, "단독 촬영"), (&l.trigger, "동시 촬영")] {
            if s.total > 0 {
                parts.push(format!("{k} {}/{}회 성공 · 중앙값 {} · 최대 {} · {}", s.ok, s.total, fmt_ms(s.ms.p50), fmt_ms(s.ms.max), s.goal_text()));
            }
        }
        if !parts.is_empty() {
            f.push(format!("위치 '{}': {}", l.location, parts.join(" / ")));
        }
    }
    if w.samples > 0 {
        f.push(format!(
            "이 PC Wi-Fi: 신호 평균 {} · 최저 {} · AP 전환 {}회 · 끊김 {}회{}",
            w.signal.mean.map(|x| format!("{x:.0}%")).unwrap_or_else(|| "—".into()),
            w.signal.min.map(|x| format!("{x:.0}%")).unwrap_or_else(|| "—".into()),
            w.roams,
            w.disconnects,
            if w.ssids.is_empty() { String::new() } else { format!(" · SSID {}", w.ssids.join(", ")) }
        ));
    }
    if f.is_empty() {
        f.push("이 기간에는 기록이 없습니다".into());
    }
    f
}

// ---------------- 공통 표시 ----------------


fn verdict_ko(v: Option<&str>, status: &str) -> &'static str {
    if status == "running" {
        return "수신 중";
    }
    match v {
        Some("stable") => "안정",
        Some("warning") => "주의",
        Some("unstable") => "불안정",
        Some("failed") => "연결 실패",
        _ => "—",
    }
}

fn trig_status_ko(s: &str) -> &'static str {
    match s {
        "ok" => "성공",
        "partial" => "일부 실패",
        "ignored" => "무시",
        _ => "실패",
    }
}

fn r1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

// ---------------- Excel ----------------
// 모양: 맑은 고딕 10pt, 연회색 머리글(아래 실선),
// 행마다 아래 점선, 시각은 실제 날짜 값(hh:mm:ss.000), ms는 #,##0.0, 첫 열 ✓/✗.

enum C {
    S(String),
    /// 소수 (ms 등, #,##0.0)
    N(f64),
    /// 정수 (#,##0)
    I(f64),
    /// 날짜·시각 (yyyy-mm-dd hh:mm:ss.000)
    T(DateTime<Local>),
    E,
}

fn cs(v: impl Into<String>) -> C {
    C::S(v.into())
}
fn cn(v: Option<f64>) -> C {
    v.filter(|x| x.is_finite()).map(C::N).unwrap_or(C::E)
}
fn ci(v: u64) -> C {
    C::I(v as f64)
}
fn ct(v: Option<DateTime<Local>>) -> C {
    v.map(C::T).unwrap_or(C::E)
}
fn co(v: &Option<String>) -> C {
    v.clone().map(C::S).unwrap_or(C::E)
}

/// 엑셀 날짜 값 (1899-12-30부터의 일수, 이 PC 시간대)
fn excel_time(t: DateTime<Local>) -> f64 {
    let base = NaiveDate::from_ymd_opt(1899, 12, 30).and_then(|d| d.and_hms_opt(0, 0, 0)).unwrap_or_default();
    (t.naive_local() - base).num_milliseconds() as f64 / 86_400_000.0
}

struct Fmts {
    head: rust_xlsxwriter::Format,
    text: rust_xlsxwriter::Format,
    num: rust_xlsxwriter::Format,
    int: rust_xlsxwriter::Format,
    seq: rust_xlsxwriter::Format,
    datetime: rust_xlsxwriter::Format,
    time: rust_xlsxwriter::Format,
    ok: rust_xlsxwriter::Format,
    fail: rust_xlsxwriter::Format,
}

impl Fmts {
    fn new() -> Self {
        use rust_xlsxwriter::{Color, Format, FormatAlign, FormatBorder};
        let base = Format::new()
            .set_font_name("Malgun Gothic")
            .set_font_size(10)
            .set_border_bottom(FormatBorder::Dotted)
            .set_border_bottom_color(Color::RGB(0xD5DCDF));
        Fmts {
            head: Format::new()
                .set_font_name("Malgun Gothic")
                .set_font_size(10)
                .set_bold()
                .set_font_color(Color::RGB(0x17343D))
                .set_background_color(Color::RGB(0xEDF1F2))
                .set_text_wrap()
                .set_align(FormatAlign::Center)
                .set_align(FormatAlign::VerticalCenter)
                .set_border_bottom(FormatBorder::Thin)
                .set_border_bottom_color(Color::RGB(0x9AA7AD)),
            num: base.clone().set_num_format("#,##0.0"),
            int: base.clone().set_num_format("#,##0"),
            seq: base.clone().set_num_format("0"),
            datetime: base.clone().set_num_format("yyyy-mm-dd hh:mm:ss.000").set_align(FormatAlign::Center),
            time: base.clone().set_num_format("hh:mm:ss.000").set_align(FormatAlign::Center),
            ok: base.clone().set_font_name("Segoe UI Symbol").set_bold().set_font_color(Color::RGB(0x1F7A45)).set_align(FormatAlign::Center),
            fail: base.clone().set_font_name("Segoe UI Symbol").set_bold().set_font_color(Color::RGB(0xB42318)).set_align(FormatAlign::Center),
            text: base,
        }
    }
}

/// 머리글 + 열 너비 + 틀 고정 + 필터 (기준 엑셀과 같은 머리글 행 높이)
fn sheet_head(ws: &mut rust_xlsxwriter::Worksheet, f: &Fmts, cols: &[(&str, f64)], rows: usize, freeze_col: u16) -> Result<(), rust_xlsxwriter::XlsxError> {
    for (i, (h, w)) in cols.iter().enumerate() {
        ws.write_with_format(0, i as u16, *h, &f.head)?;
        ws.set_column_width(i as u16, *w)?;
    }
    ws.set_row_height(0, 21.75)?;
    ws.set_freeze_panes(1, freeze_col)?;
    if rows > 0 {
        ws.autofilter(0, 0, rows as u32, (cols.len().max(1) - 1) as u16)?;
    }
    Ok(())
}

fn write_cell(ws: &mut rust_xlsxwriter::Worksheet, f: &Fmts, r: u32, c: u16, v: C) -> Result<(), rust_xlsxwriter::XlsxError> {
    match v {
        C::S(s) => ws.write_with_format(r, c, s, &f.text).map(|_| ()),
        C::N(x) => ws.write_with_format(r, c, x, &f.num).map(|_| ()),
        C::I(x) => ws.write_with_format(r, c, x, &f.int).map(|_| ()),
        C::T(t) => ws.write_with_format(r, c, excel_time(t), &f.datetime).map(|_| ()),
        C::E => ws.write_blank(r, c, &f.text).map(|_| ()),
    }
}

fn sheet(wb: &mut rust_xlsxwriter::Workbook, f: &Fmts, name: &str, cols: &[(&str, f64)], rows: Vec<Vec<C>>) -> Result<usize, rust_xlsxwriter::XlsxError> {
    let ws = wb.add_worksheet();
    ws.set_name(name)?;
    let n = rows.len().min(XLSX_MAX_ROWS);
    sheet_head(ws, f, cols, n, 0)?;
    for (r, row) in rows.into_iter().take(XLSX_MAX_ROWS).enumerate() {
        for (c, v) in row.into_iter().enumerate() {
            write_cell(ws, f, (r + 1) as u32, c as u16, v)?;
        }
    }
    Ok(n)
}

/// 원시 데이터 Excel. 첫 시트 '촬영 목록'은 보고용 기본 열·모양이고,
/// 카메라 2대·트리거 때문에 필요한 열은 그 뒤에만 덧붙인다. 나머지 시트는 같은 모양의 원본 기록.
pub fn write_xlsx(path: &Path, sm: &Summary, d: &Data, meta: &ReportMeta) -> Result<(), String> {
    use rust_xlsxwriter::{DocProperties, Workbook};
    let mut wb = Workbook::new();
    let f = Fmts::new();
    let e = |e: rust_xlsxwriter::XlsxError| format!("엑셀 작성 실패: {e}");
    let mut props = DocProperties::new().set_title("Capture VMS 원시 데이터 목록");
    if !meta.tester.trim().is_empty() {
        props = props.set_author(meta.tester.trim());
    }
    let comment = format!("{} {}", meta.site.trim(), meta.note.trim());
    if !comment.trim().is_empty() {
        props = props.set_comment(comment.trim());
    }
    wb.set_properties(&props);

    // ---- 촬영 목록 (기준 엑셀과 같은 13열 + 덧붙인 열) ----
    {
        let ws = wb.add_worksheet();
        ws.set_name("촬영 목록").map_err(e)?;
        let cols = [
            ("촬영 성공", 9.0), ("번호", 7.0), ("세션 ID", 24.0), ("요청 시각", 13.6), ("프레임 시각", 12.3), ("저장 시각", 12.3), ("대기(ms)", 10.0),
            ("JPEG 변환(ms)", 14.0), ("저장(ms)", 10.0), ("전체(ms)", 12.3), ("디코딩 프레임", 13.0), ("bytes", 11.0), ("이미지 파일", 40.0),
            ("카메라", 16.0), ("방식", 13.0), ("트리거 번호", 10.0), ("측정 위치", 14.0), ("오류", 40.0), ("촬영 시 영상 지연(ms)", 18.0),
        ];
        let mut caps: Vec<&CaptureRecord> = d.captures.iter().collect();
        caps.sort_by_key(|c| c.requested_at);
        let n = caps.len().min(XLSX_MAX_ROWS);
        sheet_head(ws, &f, &cols, n, 2).map_err(e)?;
        for (i, c) in caps.into_iter().take(XLSX_MAX_ROWS).enumerate() {
            let r = (i + 1) as u32;
            let mark = if c.ok { ("✓", &f.ok) } else { ("✗", &f.fail) };
            ws.write_with_format(r, 0, mark.0, mark.1).map_err(e)?;
            ws.write_with_format(r, 1, (i + 1) as f64, &f.seq).map_err(e)?;
            ws.write_with_format(r, 2, c.session_id.as_str(), &f.text).map_err(e)?;
            for (col, t) in [(3u16, c.requested_at), (4, c.frame_at), (5, c.saved_at)] {
                match t {
                    Some(t) => ws.write_with_format(r, col, excel_time(t), &f.time).map_err(e)?,
                    None => ws.write_blank(r, col, &f.text).map_err(e)?,
                };
            }
            // 단독 촬영은 '대기' 자리에 RTSP 연결까지 걸린 시간
            let wait = if c.mode == "direct" { c.connect_ms.or(c.wait_ms) } else { c.wait_ms };
            for (col, v) in [(6u16, cn(wait)), (7, cn(c.decode_ms)), (8, cn(c.save_ms)), (9, cn(c.total_ms))] {
                write_cell(ws, &f, r, col, v).map_err(e)?;
            }
            write_cell(ws, &f, r, 10, if c.ok { ci(c.frames_decoded as u64) } else { C::E }).map_err(e)?;
            write_cell(ws, &f, r, 11, if c.bytes > 0 { ci(c.bytes) } else { C::E }).map_err(e)?;
            let file = c.image.as_deref().map(|p| Path::new(p).file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| p.to_string()));
            write_cell(ws, &f, r, 12, co(&file)).map_err(e)?;
            write_cell(ws, &f, r, 13, cs(sm.cam_label(&c.camera_id, &c.camera_name))).map_err(e)?;
            write_cell(ws, &f, r, 14, cs(if c.mode == "direct" { "단독(수신 없이)" } else { "수신 중" })).map_err(e)?;
            write_cell(ws, &f, r, 15, c.trigger_seq.map(ci).unwrap_or(C::E)).map_err(e)?;
            write_cell(ws, &f, r, 16, co(&c.location)).map_err(e)?;
            write_cell(ws, &f, r, 17, co(&c.error)).map_err(e)?;
            write_cell(ws, &f, r, 18, cn(c.stream_delay_ms)).map_err(e)?;
        }
    }

    // ---- 트리거 (카메라별 열) ----
    let mut tcams: Vec<(String, String)> = Vec::new();
    for t in &d.triggers {
        for s in &t.shots {
            if !tcams.iter().any(|c| c.0 == s.camera_id) {
                tcams.push((s.camera_id.clone(), s.camera_name.clone()));
            }
        }
    }
    tcams.sort();
    let mut cols: Vec<(String, f64)> = [
        ("번호", 7.0), ("트리거 id", 28.0), ("신호 받은 시각", 23.0), ("출처", 10.0), ("내용", 30.0), ("결과", 9.0), ("대상(대)", 8.0), ("성공(대)", 8.0),
        ("전체(ms)", 9.0), ("편차(ms)", 9.0), ("편차 기준", 10.0),
    ]
    .iter()
    .map(|(a, b)| (a.to_string(), *b))
    .collect();
    for (id, name) in &tcams {
        cols.push((format!("{name}({id}) 결과"), 14.0));
        cols.push((format!("{name}({id}) ms"), 14.0));
        cols.push((format!("{name}({id}) 방식"), 12.0));
    }
    cols.push(("오류".into(), 50.0));
    cols.push(("측정 위치".into(), 14.0));
    let rows = d
        .triggers
        .iter()
        .map(|t| {
            let mut row = vec![
                if t.seq > 0 { ci(t.seq) } else { C::E },
                cs(&t.id),
                ct(Some(t.ts)),
                cs(trigger::source_ko(&t.source)),
                co(&t.detail),
                cs(trig_status_ko(&t.status)),
                ci(t.shots.len() as u64),
                ci(t.shots.iter().filter(|s| s.ok).count() as u64),
                cn(t.total_ms),
                cn(t.skew_ms),
                t.skew_basis.as_deref().map(|b| cs(if b == "frame" { "프레임 도착" } else { "저장 시각" })).unwrap_or(C::E),
            ];
            for (id, _) in &tcams {
                match t.shots.iter().find(|s| &s.camera_id == id) {
                    Some(s) => {
                        row.push(cs(if s.ok { "성공" } else { "실패" }));
                        row.push(cn(s.total_ms));
                        row.push(cs(if s.mode == "direct" { "단독" } else { "수신 중" }));
                    }
                    None => row.extend([C::E, C::E, C::E]),
                }
            }
            row.push(co(&t.error));
            row.push(co(&t.location));
            row
        })
        .collect();
    let cols_ref: Vec<(&str, f64)> = cols.iter().map(|(a, b)| (a.as_str(), *b)).collect();
    sheet(&mut wb, &f, "트리거", &cols_ref, rows).map_err(e)?;

    // ---- 세션 ----
    let rows = d
        .sessions
        .iter()
        .map(|x| {
            let st = &x.stats;
            vec![
                cs(&x.id),
                co(&x.group_id),
                cs(&x.camera_id),
                cs(sm.cam_label(&x.camera_id, &x.camera_name)),
                cs(&x.label),
                cs(if x.profile == "sub" { "서브" } else { "메인" }),
                cs(x.transport.to_uppercase()),
                cs(crate::stream::path_label(x.net.path_kind.as_deref())),
                co(&x.net.path_name),
                cs(verdict_ko(x.verdict.as_deref(), &x.status)),
                ct(x.started_at),
                ct(x.ended_at),
                C::N(r1(x.duration_s)),
                cs(if x.record { "예" } else { "아니오" }),
                C::N(r1(x.recorded_bytes as f64 / 1e6)),
                co(&st.codec),
                st.width.map(|w| cs(format!("{w}×{}", st.height.unwrap_or(0)))).unwrap_or(C::E),
                cn(st.camera_fps.or(st.nominal_fps)),
                cn(st.avg_fps),
                cn(st.avg_kbps.map(|k| k / 1000.0)),
                cn(st.connect_ms),
                cn(st.interval_p50_ms),
                cn(st.interval_p95_ms),
                cn(st.interval_p99_ms),
                cn(st.interval_max_ms),
                ci(st.janks),
                ci(st.stalls),
                cn(Some(st.stall_total_ms)),
                cn(Some(st.longest_stall_ms)),
                ci(st.reconnects),
                cn(Some(st.downtime_ms)),
                cn(st.uptime_pct),
                ci(st.rtp_loss_events),
                ci(st.rtp_lost_packets),
                ci(st.decode_errors),
                cn(st.delay_max_ms),
                cn(st.delay_p95_ms),
                ci(st.delayed_secs),
                cn(x.e2e.as_ref().map(|e| e.p50_ms)),
                x.e2e.as_ref().map(|e| ci(e.count as u64)).unwrap_or(C::E),
                co(&x.net.ssid),
                cs(x.net.bssids.join(", ")),
                cn(x.net.wifi_signal_avg),
                cn(x.net.wifi_signal_min.map(f64::from)),
                cn(x.net.rssi_avg),
                ci(x.net.roams),
                ci(x.net.wifi_disconnects),
                cn(x.net.ping_avg_ms),
                cn(x.net.ping_p95_ms),
                cn(x.net.ping_loss_pct),
                cs(x.reasons.join(" / ")),
                co(&x.last_error),
                cs(&x.url),
                co(&x.recording_dir),
                co(&x.location),
                cs(if x.interrupted { "예" } else { "" }),
                x.rec_check.as_ref().map(|k| cs(if k.ok { "정상" } else { "문제" })).unwrap_or(C::E),
                x.rec_check.as_ref().map(|k| ci(k.files as u64)).unwrap_or(C::E),
                cn(x.rec_check.as_ref().map(|k| k.seconds)),
                cn(x.rec_check.as_ref().map(|k| k.gap_s)),
                x.rec_check.as_ref().map(|k| ci(k.frames)).unwrap_or(C::E),
                x.rec_check.as_ref().map(|k| cs(&k.summary)).unwrap_or(C::E),
                ci(st.frames),
                st.expected_frames.map(ci).unwrap_or(C::E),
                st.missing_frames.map(ci).unwrap_or(C::E),
                cn(match (st.missing_frames, st.expected_frames) {
                    (Some(m), Some(e)) if e > 0 => Some(m as f64 / e as f64 * 100.0),
                    _ => None,
                }),
                ci(st.ts_glitch_frames),
            ]
        })
        .collect();
    sheet(
        &mut wb,
        &f,
        "세션",
        &[
            ("세션 id", 24.0), ("묶음", 18.0), ("카메라 id", 9.0), ("카메라", 18.0), ("세션 이름", 24.0), ("스트림", 7.0), ("전송", 6.0), ("경로", 8.0), ("경로 이름", 28.0),
            ("판정", 9.0), ("시작", 23.0), ("종료", 23.0), ("길이(초)", 9.0), ("녹화", 6.0), ("녹화 용량(MB)", 12.0), ("코덱", 7.0), ("해상도", 11.0), ("카메라 fps", 10.0),
            ("평균 fps", 9.0), ("평균 Mbps", 10.0), ("첫 영상까지(ms)", 13.0), ("간격 p50(ms)", 11.0), ("간격 p95(ms)", 11.0), ("간격 p99(ms)", 11.0), ("간격 최대(ms)", 12.0),
            ("튐(회)", 7.0), ("끊김(회)", 8.0), ("끊김 합계(ms)", 12.0), ("최장 끊김(ms)", 12.0), ("재연결(회)", 9.0), ("끊긴 시간(ms)", 12.0), ("정상 수신(%)", 11.0),
            ("패킷 손실(건)", 11.0), ("손실 패킷(개)", 11.0), ("디코딩 오류", 10.0), ("버퍼 지연 최대(ms)", 15.0), ("버퍼 지연 p95(ms)", 15.0), ("지연 구간(초)", 11.0),
            ("실제 지연 중앙값(ms)", 16.0), ("실제 지연 횟수", 12.0), ("SSID", 14.0), ("BSSID", 20.0), ("Wi-Fi 신호 평균(%)", 15.0), ("Wi-Fi 신호 최저(%)", 15.0),
            ("RSSI 평균(dBm)", 13.0), ("AP 전환", 8.0), ("Wi-Fi 끊김", 9.0), ("ping 평균(ms)", 12.0), ("ping p95(ms)", 11.0), ("ping 손실(%)", 11.0),
            ("판정 이유", 60.0), ("마지막 오류", 40.0), ("RTSP 주소(계정 가림)", 44.0), ("녹화 폴더", 60.0),
            ("측정 위치", 14.0), ("갑자기 종료", 10.0), ("녹화 점검", 9.0), ("녹화 파일(개)", 11.0), ("녹화 길이(초)", 11.0), ("녹화 안 된 시간(초)", 15.0),
            ("녹화 프레임", 11.0), ("녹화 점검 내용", 60.0),
            ("받은 프레임", 11.0), ("와야 할 프레임", 12.0), ("누락 프레임", 11.0), ("프레임 누락(%)", 12.0), ("영상 시각 튐(프레임)", 16.0),
        ],
        rows,
    )
    .map_err(e)?;

    // ---- 수신(1초) ----
    let rows = d
        .stream
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|x| {
            vec![
                ct(Some(x.ts)),
                cs(&x.camera_id),
                cs(sm.cam_label(&x.camera_id, &x.camera_id)),
                cs(&x.session_id),
                ci(x.frames as u64),
                C::N((x.kbps / 1000.0 * 1000.0).round() / 1000.0),
                cn(x.interval_max_ms),
                cn(x.delay_ms),
                cs(if x.stalled { "끊김" } else { "" }),
                cs(if x.connected { "연결" } else { "끊김" }),
            ]
        })
        .collect();
    sheet(
        &mut wb,
        &f,
        "수신(1초)",
        &[("시각", 23.0), ("카메라 id", 9.0), ("카메라", 18.0), ("세션 id", 24.0), ("프레임(=fps)", 11.0), ("Mbps", 8.0), ("최대 간격(ms)", 12.0), ("버퍼 지연(ms)", 12.0), ("끊김", 6.0), ("연결", 6.0)],
        rows,
    )
    .map_err(e)?;

    // ---- 네트워크(ping) ----
    let rows = d
        .probes
        .iter()
        .map(|p| {
            vec![
                ct(Some(p.ts)),
                cs(&p.camera_id),
                cs(sm.cam_label(&p.camera_id, &p.camera_id)),
                co(&p.ip),
                cs(if p.icmp_ok { "응답" } else { "없음" }),
                cn(p.icmp_ms),
                cs(if p.tcp_ok { "열림" } else { "안 됨" }),
                cn(p.tcp_ms),
            ]
        })
        .collect();
    sheet(&mut wb, &f, "네트워크(ping)", &[("시각", 23.0), ("카메라 id", 9.0), ("카메라", 18.0), ("IP", 15.0), ("ping", 7.0), ("ping(ms)", 9.0), ("RTSP 포트", 9.0), ("TCP 연결(ms)", 12.0)], rows)
        .map_err(e)?;

    // ---- Wi-Fi ----
    let rows = d
        .wifi
        .iter()
        .map(|w| {
            vec![
                ct(Some(w.ts)),
                cs(&w.interface),
                cs(&w.state),
                co(&w.ssid),
                co(&w.bssid),
                co(&w.band),
                w.channel.map(|c| ci(c as u64)).unwrap_or(C::E),
                cn(w.signal_pct.map(f64::from)),
                cn(w.rssi_dbm.map(f64::from)),
                cn(w.rx_mbps),
                cn(w.tx_mbps),
            ]
        })
        .collect();
    sheet(
        &mut wb,
        &f,
        "Wi-Fi",
        &[("시각", 23.0), ("어댑터", 10.0), ("상태", 11.0), ("SSID", 16.0), ("BSSID(AP)", 19.0), ("대역", 9.0), ("채널", 6.0), ("신호(%)", 8.0), ("RSSI(dBm)", 10.0), ("수신 Mbps", 10.0), ("송신 Mbps", 10.0)],
        rows,
    )
    .map_err(e)?;

    // ---- 이벤트 ----
    let rows = d
        .events
        .iter()
        .map(|x| {
            let cam = x.camera_id.as_deref().map(|id| cs(sm.cam_label(id, id))).unwrap_or(C::E);
            vec![ct(Some(x.ts)), cs(event_ko(&x.kind)), cs(&x.kind), co(&x.camera_id), cam, co(&x.session_id), cs(&x.message)]
        })
        .collect();
    sheet(&mut wb, &f, "이벤트", &[("시각", 23.0), ("종류", 14.0), ("종류(코드)", 18.0), ("카메라 id", 9.0), ("카메라", 18.0), ("세션 id", 24.0), ("내용", 90.0)], rows).map_err(e)?;

    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p).map_err(|er| format!("폴더를 만들 수 없습니다: {} ({er})", p.display()))?;
    }
    wb.save(path).map_err(|er| format!("엑셀 저장 실패: {} ({er})", path.display()))
}

type CamRow = (&'static str, fn(&CamSummary) -> String);

fn cam_rows() -> Vec<CamRow> {
    fn o(v: Option<f64>, d: usize, unit: &str) -> String {
        v.map(|x| format!("{x:.d$}{unit}")).unwrap_or_else(|| "—".into())
    }
    fn cap(s: &CapStat) -> String {
        if s.total == 0 {
            "—".into()
        } else {
            format!("{}/{}회 성공 · 중앙값 {} · p95 {} · 최대 {} · {}", s.ok, s.total, fmt_ms(s.ms.p50), fmt_ms(s.ms.p95), fmt_ms(s.ms.max), s.goal_text())
        }
    }
    vec![
        ("IP", |c| c.host.clone().unwrap_or_else(|| "—".into())),
        ("경로", |c| if c.paths.is_empty() { "—".into() } else { c.paths.join(", ") }),
        ("코덱 · 해상도", |c| format!("{} · {}", c.codec.clone().unwrap_or_else(|| "—".into()), c.resolution.clone().unwrap_or_else(|| "—".into()))),
        ("수신 세션", |c| {
            if c.sessions == 0 {
                "0회".into()
            } else {
                format!("{}회 (안정 {} · 주의 {} · 불안정 {} · 실패 {}{})", c.sessions, c.stable, c.warning, c.unstable, c.failed, if c.running > 0 { format!(" · 진행 중 {}", c.running) } else { String::new() })
            }
        }),
        ("총 수신 시간", |c| fmt_dur(c.receive_s)),
        ("평균 fps (카메라 설정)", |c| format!("{} ({})", o(c.avg_fps, 1, ""), o(c.camera_fps, 0, "fps"))),
        ("평균 비트레이트", |c| o(c.avg_mbps, 2, " Mbps")),
        ("정상 수신 비율", |c| o(c.uptime_pct, 2, "%")),
        ("프레임 누락 (와야 할 프레임 대비)", |c| {
            if c.expected_frames == 0 {
                "—".into()
            } else {
                format!("{}개 / {}개 ({:.2}%)", c.missing_frames, c.expected_frames, c.missing_frames as f64 / c.expected_frames as f64 * 100.0)
            }
        }),
        ("끊김 (최장)", |c| format!("{}회 ({})", c.stalls, if c.stalls > 0 { format!("{:.2}초", c.longest_stall_ms / 1000.0) } else { "—".into() })),
        ("재연결 (끊긴 시간)", |c| format!("{}회 ({:.1}초)", c.reconnects, c.downtime_ms / 1000.0)),
        ("프레임 간격 p99 (최댓값)", |c| o(c.interval_p99_max_ms, 0, "ms")),
        ("버퍼 지연 최대 · 지연 구간", |c| format!("{} · {}초", o(c.delay_max_ms, 0, "ms"), c.delayed_secs)),
        ("영상 시각이 튄 프레임", |c| format!("{}개", c.ts_glitch_frames)),
        ("패킷 손실 · 디코딩 오류", |c| format!("{}건({}개) · {}건", c.rtp_loss_events, c.rtp_lost_packets, c.decode_errors)),
        ("실제 지연 (화면 깜빡임)", |c| o(c.e2e_p50_ms, 0, "ms")),
        ("수신 중 촬영", |c| cap(&c.capture_session)),
        ("단독 촬영 (수신 없이)", |c| cap(&c.capture_direct)),
        ("그중 트리거 촬영", |c| cap(&c.capture_trigger)),
        ("녹화 점검", |c| {
            if c.rec_sessions == 0 {
                "녹화 안 함".into()
            } else if c.rec_checked == 0 {
                format!("녹화 {}회 · 점검 기록 없음 ({:.1}GB)", c.rec_sessions, c.rec_bytes as f64 / 1e9)
            } else {
                format!(
                    "{}회 중 정상 {}회 · 녹화 {} ({:.1}GB){}",
                    c.rec_checked,
                    c.rec_ok,
                    fmt_dur(c.rec_seconds),
                    c.rec_bytes as f64 / 1e9,
                    if c.rec_gap_s >= 1.0 { format!(" · 녹화 안 된 시간 {}", fmt_dur(c.rec_gap_s)) } else { String::new() }
                )
            }
        }),
        ("카메라 ping (평균 · p95)", |c| format!("{} · {}", o(c.ping.mean, 1, "ms"), o(c.ping.p95, 1, "ms"))),
        ("ping 손실", |c| format!("{} ({}회 확인 · 응답 없음 {}번)", o(c.ping_loss_pct, 2, "%"), c.probes, c.camera_down)),
    ]
}

/// 위치별 표: (위치, [수신 중, 단독, 동시 촬영, 편차])
fn loc_rows(locs: &[LocSummary]) -> Vec<(String, Vec<String>)> {
    let cap = |s: &CapStat| if s.total == 0 { "—".to_string() } else { format!("{}/{}회 성공 · 중앙값 {} · 최대 {} · {}", s.ok, s.total, fmt_ms(s.ms.p50), fmt_ms(s.ms.max), s.goal_text()) };
    locs.iter()
        .map(|l| {
            (
                l.location.clone(),
                vec![cap(&l.session), cap(&l.direct), cap(&l.trigger), if l.skew_ms.n > 0 { format!("{} · {}", fmt_ms(l.skew_ms.p50), fmt_ms(l.skew_ms.max)) } else { "—".into() }],
            )
        })
        .collect()
}

fn trig_rows(t: &TrigSummary) -> Vec<(&'static str, String)> {
    let pct = |n: usize| if t.total > 0 { format!("{n}회 ({:.1}%)", n as f64 * 100.0 / t.total as f64) } else { "—".into() };
    vec![
        ("신호", format!("{}회", t.total)),
        ("전부 성공", pct(t.ok)),
        ("일부 실패", pct(t.partial)),
        ("실패", pct(t.failed)),
        ("무시한 신호 (간격 짧음)", format!("{}회", t.ignored)),
        ("신호 → 모두 저장 (중앙값 · p95 · 최대)", format!("{} · {} · {}", fmt_ms(t.total_ms.p50), fmt_ms(t.total_ms.p95), fmt_ms(t.total_ms.max))),
        ("기준 이내 (실패는 기준 밖)", t.goal.goal_text()),
        ("카메라 간 편차 (중앙값 · p95 · 최대)", format!("{} · {} · {}", fmt_ms(t.skew_ms.p50), fmt_ms(t.skew_ms.p95), fmt_ms(t.skew_ms.max))),
        ("출처", t.by_source.iter().map(|(k, n)| format!("{k} {n}")).collect::<Vec<_>>().join(" · ")),
    ]
}

// ---------------- 보고서 (HTML → PDF) ----------------

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ReportMeta {
    #[serde(default)]
    pub site: String,
    #[serde(default)]
    pub tester: String,
    #[serde(default)]
    pub note: String,
    /// 촬영 이미지 넣기
    #[serde(default)]
    pub images: bool,
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

const PALETTE: [&str; 4] = ["#2a78d6", "#eb6834", "#1baf7a", "#8e5cd9"];

struct Series {
    name: String,
    color: &'static str,
    pts: Vec<(i64, f64)>,
    dots: bool,
}

fn nice_max(v: f64) -> f64 {
    if !(v > 0.0) {
        return 1.0;
    }
    let p = 10f64.powf(v.log10().floor());
    for m in [1.0, 2.0, 2.5, 5.0, 10.0] {
        if m * p >= v {
            return m * p;
        }
    }
    10.0 * p
}

/// 시간축 선(점) 차트 SVG
fn svg_chart(title: &str, unit: &str, series: &[Series], refline: Option<(f64, String)>) -> String {
    let all: Vec<(i64, f64)> = series.iter().flat_map(|s| s.pts.iter().copied()).collect();
    if all.is_empty() {
        return String::new();
    }
    let (w, h) = (720.0, 190.0);
    let (ml, mr, mt, mb) = (46.0, 12.0, 26.0, 24.0);
    let (pw, ph) = (w - ml - mr, h - mt - mb);
    let t0 = all.iter().map(|p| p.0).min().unwrap_or(0);
    let t1 = all.iter().map(|p| p.0).max().unwrap_or(0).max(t0 + 1000);
    let vmax = all.iter().map(|p| p.1).fold(0.0, f64::max).max(refline.as_ref().map_or(0.0, |r| r.0));
    let ymax = nice_max(vmax * 1.05);
    let x = |t: i64| ml + (t - t0) as f64 / (t1 - t0) as f64 * pw;
    let y = |v: f64| mt + ph - (v.min(ymax) / ymax) * ph;
    let mut s = String::new();
    let _ = write!(s, r#"<figure class="chart"><figcaption>{}</figcaption><svg viewBox="0 0 {w} {h}" width="100%" xmlns="http://www.w3.org/2000/svg">"#, esc(title));
    for i in 0..=4 {
        let v = ymax / 4.0 * i as f64;
        let yy = y(v);
        let lab = if v >= 1000.0 { format!("{:.1}K", v / 1000.0) } else if v.fract() == 0.0 { format!("{v:.0}") } else { format!("{v:.1}") };
        let _ = write!(
            s,
            r##"<line x1="{ml}" x2="{}" y1="{yy:.1}" y2="{yy:.1}" stroke="{}" stroke-width="1"/><text x="{}" y="{:.1}" text-anchor="end" font-size="10" fill="#667085">{lab}</text>"##,
            ml + pw,
            if i == 0 { "#c9ced6" } else { "#eceff3" },
            ml - 6.0,
            yy + 3.5
        );
    }
    let span = t1 - t0;
    let multi_day = span > 20 * 3600 * 1000;
    for i in 0..=5 {
        let t = t0 + span * i / 5;
        let dt = Local.timestamp_millis_opt(t).single().unwrap_or_else(Local::now);
        let lab = if multi_day { dt.format("%m/%d %H:%M").to_string() } else { dt.format("%H:%M:%S").to_string() };
        let _ = write!(s, r##"<text x="{:.1}" y="{}" text-anchor="middle" font-size="10" fill="#667085">{lab}</text>"##, x(t), h - 6.0);
    }
    if let Some((v, lab)) = &refline {
        if *v <= ymax {
            let _ = write!(
                s,
                r##"<line x1="{ml}" x2="{}" y1="{:.1}" y2="{:.1}" stroke="#8a94a6" stroke-dasharray="4 3"/><text x="{}" y="{:.1}" text-anchor="end" font-size="10" fill="#475467">{}</text>"##,
                ml + pw,
                y(*v),
                y(*v),
                ml + pw,
                y(*v) - 4.0,
                esc(lab)
            );
        }
    }
    let mut lx = ml;
    for se in series {
        let _ = write!(s, r##"<rect x="{lx:.1}" y="6" width="12" height="4" rx="1" fill="{}"/><text x="{:.1}" y="12" font-size="10.5" fill="#344054">{}</text>"##, se.color, lx + 16.0, esc(&se.name));
        lx += 26.0 + se.name.chars().count() as f64 * 7.5;
        if se.dots {
            for (t, v) in &se.pts {
                let _ = write!(s, r#"<circle cx="{:.1}" cy="{:.1}" r="2.2" fill="{}"/>"#, x(*t), y(*v), se.color);
            }
        } else {
            // 간격이 크게 벌어지면(수신 안 한 시간) 선을 끊는다
            let gap = (span / 60).max(15_000);
            let mut d = String::new();
            let mut prev: Option<i64> = None;
            for (t, v) in &se.pts {
                let cmd = if prev.map_or(true, |p| t - p > gap) { 'M' } else { 'L' };
                let _ = write!(d, "{cmd}{:.1},{:.1}", x(*t), y(*v));
                prev = Some(*t);
            }
            let _ = write!(s, r#"<path d="{d}" fill="none" stroke="{}" stroke-width="1.6" stroke-linejoin="round"/>"#, se.color);
        }
    }
    let _ = write!(s, r#"</svg><div class="unit">{}</div></figure>"#, esc(unit));
    s
}

/// 1초 기록을 최대 n개 구간 평균으로
fn bucket(pts: Vec<(i64, f64)>, n: usize) -> Vec<(i64, f64)> {
    if pts.len() <= n {
        return pts;
    }
    let per = pts.len().div_ceil(n);
    pts.chunks(per).map(|c| (c[0].0, c.iter().map(|p| p.1).sum::<f64>() / c.len() as f64)).collect()
}

fn td(s: impl AsRef<str>) -> String {
    format!("<td>{}</td>", esc(s.as_ref()))
}
fn tdn(s: impl AsRef<str>) -> String {
    format!(r#"<td class="n">{}</td>"#, esc(s.as_ref()))
}

const REPORT_CSS: &str = r#"
@page { size: A4; margin: 12mm 11mm 14mm; }
* { box-sizing: border-box; }
body { font-family: "Malgun Gothic", "Segoe UI", sans-serif; color: #101828; font-size: 10.5pt; line-height: 1.5; margin: 0; background: #fff; }
.wrap { max-width: 980px; margin: 0 auto; padding: 18px 20px 40px; }
h1 { font-size: 19pt; margin: 0 0 4px; }
h2 { font-size: 13pt; margin: 22px 0 8px; padding-bottom: 4px; border-bottom: 2px solid #2a78d6; break-after: avoid; }
h3 { font-size: 11pt; margin: 14px 0 6px; break-after: avoid; }
.sub { color: #475467; font-size: 10pt; }
.meta { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0; border: 1px solid #d0d5dd; border-radius: 8px; overflow: hidden; margin: 12px 0 4px; }
.meta div { padding: 7px 10px; border-right: 1px solid #e4e7ec; }
.meta div:last-child { border-right: 0; }
.meta b { display: block; font-size: 8.5pt; color: #667085; font-weight: 600; }
.note { margin: 8px 0 0; padding: 8px 10px; background: #f8f9fb; border-left: 3px solid #2a78d6; font-size: 9.5pt; white-space: pre-wrap; }
ul.find { margin: 6px 0 0; padding-left: 18px; font-size: 9.5pt; }
ul.find li { margin: 3px 0; }
.kpis { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 10px 0 12px; }
.kpi { border: 1px solid #e4e7ec; border-left: 5px solid #98a2b3; border-radius: 8px; padding: 8px 11px 9px; break-inside: avoid; }
.kpi.good { border-left-color: #12b76a; } .kpi.warn { border-left-color: #f79009; } .kpi.bad { border-left-color: #f04438; }
.kpi .k { font-size: 8.6pt; color: #667085; font-weight: 600; }
.kpi .v { font-size: 21pt; font-weight: 700; line-height: 1.15; font-variant-numeric: tabular-nums; margin: 1px 0; }
.kpi .v small { font-size: 10pt; font-weight: 600; color: #475467; margin-left: 3px; }
.kpi.good .v { color: #067647; } .kpi.warn .v { color: #b54708; } .kpi.bad .v { color: #b42318; }
.kpi .s { font-size: 8.4pt; color: #475467; }
table.glance td { vertical-align: middle; }
table.glance td b { display: block; }
table.glance .muted { font-size: 8pt; }
ul.issues { margin: 4px 0 0; padding: 0; list-style: none; font-size: 9.5pt; }
ul.issues li { margin: 3px 0; padding-left: 16px; position: relative; }
ul.issues li::before { content: ""; position: absolute; left: 2px; top: .55em; width: 8px; height: 8px; border-radius: 50%; background: #98a2b3; }
ul.issues li.bad::before { background: #f04438; } ul.issues li.warn::before { background: #f79009; }
.ok-line { color: #067647; font-weight: 600; font-size: 9.5pt; margin: 4px 0; }
.legend { font-size: 8pt; color: #667085; margin-top: 2px; }
table { width: 100%; border-collapse: collapse; font-size: 8.6pt; margin: 4px 0 6px; }
th, td { border: 1px solid #e4e7ec; padding: 4px 6px; text-align: left; vertical-align: top; }
th { background: #f2f5f9; font-weight: 600; color: #344054; }
td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
tr { break-inside: avoid; }
thead { display: table-header-group; }
.kvt td:first-child { width: 30%; color: #475467; background: #fafbfc; }
.chart { margin: 6px 0 10px; break-inside: avoid; }
.chart figcaption { font-weight: 600; font-size: 10pt; margin-bottom: 2px; }
.chart .unit { font-size: 8pt; color: #667085; text-align: right; }
.v-stable { color: #087443; font-weight: 600; } .v-warning { color: #b54708; font-weight: 600; } .v-bad { color: #b42318; font-weight: 600; }
.imgs { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
.imgs figure { margin: 0; break-inside: avoid; border: 1px solid #e4e7ec; border-radius: 6px; overflow: hidden; }
.imgs img { width: 100%; display: block; aspect-ratio: 4 / 3; object-fit: contain; background: #0b0d10; }
.imgs figcaption { font-size: 8.5pt; padding: 4px 6px; color: #344054; }
.muted { color: #667085; font-size: 9pt; }
.bar { position: sticky; top: 0; background: #101828; color: #fff; padding: 10px 20px; display: flex; gap: 12px; align-items: center; font-size: 10pt; }
.bar button { font: inherit; padding: 6px 14px; border-radius: 8px; border: 0; background: #2a78d6; color: #fff; cursor: pointer; font-weight: 600; }
@media print { .bar { display: none; } .wrap { padding: 0; max-width: none; } }
"#;

/// 한눈에 보기: 숫자 카드 → 카메라별 한 줄 → 확인할 것 (문제만). PDF 첫 장과 대시보드 요약 창이 같이 쓴다.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Glance {
    pub cards: Vec<GlanceCard>,
    pub rows: Vec<GlanceRow>,
    pub issues: Vec<GlanceIssue>,
    /// 표 머리글용 "1초"
    pub goal_label: String,
    pub legend: String,
}

/// 큰 숫자 카드 하나 (cls: good | warn | bad | "")
#[derive(Debug, Clone, Default, Serialize)]
pub struct GlanceCard {
    pub cls: String,
    pub k: String,
    pub v: String,
    pub unit: String,
    pub sub: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct GlanceRow {
    pub name: String,
    pub sub: String,
    pub verdict: String,
    pub verdict_cls: String,
    pub fps: String,
    pub uptime: String,
    pub stalls: String,
    pub cap_p50: String,
    pub goal_pct: String,
    pub cap_ok: String,
    pub rec: String,
    pub rec_bad: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct GlanceIssue {
    pub cls: String,
    pub text: String,
}

fn card(cls: &str, k: &str, v: &str, unit: &str, sub: &str) -> GlanceCard {
    GlanceCard { cls: cls.into(), k: k.into(), v: v.into(), unit: unit.into(), sub: sub.into() }
}

/// 172 → "172ms", 1340 → "1.34초"
fn fmt_t(v: Option<f64>) -> String {
    match v {
        None => "—".into(),
        Some(x) if x >= 1000.0 => format!("{:.2}초", x / 1000.0),
        Some(x) => format!("{x:.0}ms"),
    }
}

fn pct_s(n: usize, total: usize) -> String {
    if total == 0 {
        return "—".into();
    }
    let p = n as f64 * 100.0 / total as f64;
    if p >= 99.95 { "100".into() } else { format!("{p:.1}") }
}

pub fn glance(s: &Summary, d: &Data) -> Glance {
    let g = (s.goal_ms, s.limit_ms);
    let mut out = Glance { goal_label: ms_label(g.0), ..Default::default() };
    let caps = cap_stat(d.captures.iter().map(|c| (c.ok, c.total_ms)), g);
    let sess = cap_stat(d.captures.iter().filter(|c| c.mode != "direct").map(|c| (c.ok, c.total_ms)), g);
    let dir = cap_stat(d.captures.iter().filter(|c| c.mode == "direct").map(|c| (c.ok, c.total_ms)), g);
    if caps.total > 0 {
        let med = caps.ms.p50;
        let cls = match med {
            Some(m) if m <= g.0 as f64 => "good",
            Some(m) if m <= g.1 as f64 => "warn",
            Some(_) => "bad",
            None => "",
        };
        let (v, u) = match med {
            Some(m) if m >= 1000.0 => (format!("{:.2}", m / 1000.0), "초"),
            Some(m) => (format!("{m:.0}"), "ms"),
            None => ("—".into(), ""),
        };
        let mut sub = Vec::new();
        if sess.total > 0 {
            sub.push(format!("수신 중 {}", fmt_t(sess.ms.p50)));
        }
        if dir.total > 0 {
            sub.push(format!("단독(접속 포함) {}", fmt_t(dir.ms.p50)));
        }
        sub.push(format!("최대 {}", fmt_t(caps.ms.max)));
        out.cards.push(card(cls, "촬영 시간 (누름→저장, 중앙값)", &v, u, &sub.join(" · ")));
        let gp = caps.goal_pct().unwrap_or(0.0);
        let cls = if gp >= 95.0 { "good" } else if gp >= 80.0 { "warn" } else { "bad" };
        out.cards.push(card(cls, &format!("목표 {} 이내", ms_label(g.0)), &pct_s(caps.within_goal, caps.total), "%", &format!("{} 이내 {}% · 실패는 기준 밖으로 셈", ms_label(g.1), pct_s(caps.within_limit, caps.total))));
        let sp = caps.ok as f64 * 100.0 / caps.total as f64;
        let cls = if sp >= 99.0 { "good" } else if sp >= 95.0 { "warn" } else { "bad" };
        out.cards.push(card(cls, "촬영 성공률", &pct_s(caps.ok, caps.total), "%", &format!("{}장 중 {}장 성공{}", caps.total, caps.ok, if caps.total > caps.ok { format!(" · 실패 {}장", caps.total - caps.ok) } else { String::new() })));
    }
    let t = &s.triggers;
    if t.total > 0 {
        let sp = t.ok as f64 * 100.0 / t.total as f64;
        let cls = if sp >= 99.0 { "good" } else if sp >= 95.0 { "warn" } else { "bad" };
        out.cards.push(card(cls, "동시 촬영 (전부 성공)", &pct_s(t.ok, t.total), "%", &format!("{}회 · 신호→모두 저장 {} · 편차 {} (최대 {})", t.total, fmt_t(t.total_ms.p50), fmt_t(t.skew_ms.p50), fmt_t(t.skew_ms.max))));
    }
    let ns: usize = s.cameras.iter().map(|c| c.sessions).sum();
    if ns > 0 {
        let up = weighted(s.cameras.iter().map(|c| (c.uptime_pct, c.receive_s)));
        let cls = match up {
            Some(u) if u >= 99.0 => "good",
            Some(u) if u >= 95.0 => "warn",
            Some(_) => "bad",
            None => "",
        };
        let sum = |f: fn(&CamSummary) -> usize| s.cameras.iter().map(f).sum::<usize>();
        let mut v = vec![];
        for (n, k) in [(sum(|c| c.stable), "안정"), (sum(|c| c.warning), "주의"), (sum(|c| c.unstable), "불안정"), (sum(|c| c.failed), "실패")] {
            if n > 0 {
                v.push(format!("{k} {n}"));
            }
        }
        let rc: u64 = s.cameras.iter().map(|c| c.reconnects).sum();
        out.cards.push(card(
            cls,
            "영상 수신 (정상 수신 비율)",
            &up.map(|u| if u >= 99.995 { "100".into() } else { format!("{u:.2}") }).unwrap_or_else(|| "—".into()),
            if up.is_some() { "%" } else { "" },
            &format!("세션 {ns}회: {} · 재연결 {rc}회", v.join(" · ")),
        ));
    }
    let rs: usize = s.cameras.iter().map(|c| c.rec_sessions).sum();
    if rs > 0 {
        let (chk, ok) = (s.cameras.iter().map(|c| c.rec_checked).sum::<usize>(), s.cameras.iter().map(|c| c.rec_ok).sum::<usize>());
        let secs: f64 = s.cameras.iter().map(|c| c.rec_seconds).sum();
        let gap: f64 = s.cameras.iter().map(|c| c.rec_gap_s).sum();
        let bytes: u64 = s.cameras.iter().map(|c| c.rec_bytes).sum();
        if chk == 0 {
            out.cards.push(card("", "녹화", &format!("{rs}"), "회", &format!("점검 기록 없음 · {:.1}GB", bytes as f64 / 1e9)));
        } else {
            out.cards.push(card(
                if ok == chk { "good" } else { "bad" },
                "녹화 파일 점검",
                &format!("{ok}/{chk}"),
                "정상",
                &format!("녹화 {} · {:.1}GB{}", fmt_dur(secs), bytes as f64 / 1e9, if gap >= 1.0 { format!(" · 녹화 안 된 시간 {}", fmt_dur(gap)) } else { String::new() }),
            ));
        }
    }

    // 카메라별 한 줄
    for c in &s.cameras {
        let cs = cap_stat(d.captures.iter().filter(|x| x.camera_id == c.camera_id).map(|x| (x.ok, x.total_ms)), g);
        let mut v = vec![];
        for (n, k) in [(c.stable, "안정"), (c.warning, "주의"), (c.unstable, "불안정"), (c.failed, "실패")] {
            if n > 0 {
                v.push(format!("{k} {n}"));
            }
        }
        out.rows.push(GlanceRow {
            name: c.label.clone(),
            sub: format!("{} · {}", c.host.as_deref().unwrap_or("—"), if c.paths.is_empty() { "—".into() } else { c.paths.join(", ") }),
            verdict: if c.sessions == 0 { "—".into() } else { v.join(" · ") },
            verdict_cls: if c.sessions == 0 { "" } else if c.unstable + c.failed > 0 { "v-bad" } else if c.warning > 0 { "v-warning" } else { "v-stable" }.into(),
            fps: match (c.avg_fps, c.camera_fps) {
                (Some(a), Some(n)) => format!("{a:.1} / {n:.0}"),
                (Some(a), None) => format!("{a:.1}"),
                _ => "—".into(),
            },
            uptime: c.uptime_pct.map(|u| format!("{u:.2}%")).unwrap_or_else(|| "—".into()),
            stalls: if c.sessions == 0 { "—".into() } else { format!("{} · {}", c.stalls, c.reconnects) },
            cap_p50: fmt_t(cs.ms.p50),
            goal_pct: cs.goal_pct().map(|p| format!("{p:.1}%")).unwrap_or_else(|| "—".into()),
            cap_ok: if cs.total == 0 { "—".into() } else { format!("{}/{}", cs.ok, cs.total) },
            rec: if c.rec_sessions == 0 { "녹화 안 함".into() } else if c.rec_checked == 0 { "점검 없음".into() } else { format!("{}/{} 정상", c.rec_ok, c.rec_checked) },
            rec_bad: c.rec_checked > 0 && c.rec_ok < c.rec_checked,
        });
    }

    // 확인할 것: 문제만 짧게
    let mut iss = |cls: &str, text: String| out.issues.push(GlanceIssue { cls: cls.into(), text });
    for c in &s.cameras {
        let fails: Vec<&CaptureRecord> = d.captures.iter().filter(|x| x.camera_id == c.camera_id && !x.ok).collect();
        if let Some(f) = fails.first() {
            let why: String = f.error.clone().unwrap_or_default().chars().take(45).collect();
            iss("bad", format!("{}: 촬영 실패 {}장 — {why}", c.label, fails.len()));
        }
        if c.unstable + c.failed > 0 {
            let mut p = vec![];
            if c.unstable > 0 {
                p.push(format!("수신 불안정 {}회", c.unstable));
            }
            if c.failed > 0 {
                p.push(format!("연결 실패 {}회", c.failed));
            }
            if c.reconnects > 0 {
                p.push(format!("재연결 {}회 (끊긴 시간 {:.1}초)", c.reconnects, c.downtime_ms / 1000.0));
            }
            iss("warn", format!("{}: {}", c.label, p.join(" · ")));
        }
        if c.rec_checked > c.rec_ok {
            iss("bad", format!("{}: 녹화 파일 문제 {}건 (Excel '세션' 시트의 녹화 점검 내용 참고)", c.label, c.rec_checked - c.rec_ok));
        }
        if c.interrupted > 0 {
            iss("warn", format!("{}: 프로그램이 갑자기 꺼진 세션 {}개 (마지막 중간 저장까지로 판정)", c.label, c.interrupted));
        }
        if c.ping_loss_pct.map_or(false, |l| l > 0.0) {
            iss("warn", format!("{}: 카메라 ping 손실 {:.1}%", c.label, c.ping_loss_pct.unwrap_or(0.0)));
        }
    }
    if t.partial + t.failed > 0 {
        let p: Vec<String> = [(t.partial, "일부 실패"), (t.failed, "실패")].iter().filter(|(n, _)| *n > 0).map(|(n, k)| format!("{k} {n}회")).collect();
        iss("bad", format!("동시 촬영: {}", p.join(" · ")));
    }
    if t.ignored > 0 {
        iss("", format!("너무 짧은 간격으로 들어와 무시한 신호 {}회", t.ignored));
    }
    if s.wifi.disconnects + s.wifi.roams > 0 {
        iss("warn", format!("이 PC Wi-Fi: 끊김 {}회 · AP 전환 {}회", s.wifi.disconnects, s.wifi.roams));
    }
    out.legend = format!(
        "색 기준 — 촬영 시간: 초록 {} 이내 · 주황 {} 이내 · 빨강 초과 / 비율: 초록 95%(성공률·동시 촬영 99%) 이상 · 주황 80%(95%) 이상 / 정상 수신: 초록 99% 이상 · 주황 95% 이상",
        ms_label(g.0),
        ms_label(g.1)
    );
    out
}

/// PDF 첫 장용 HTML
fn glance_html(g: &Glance) -> String {
    let mut h = String::new();
    if g.cards.is_empty() {
        h.push_str("<p class=\"muted\">이 기간에는 기록이 없습니다.</p>");
        return h;
    }
    h.push_str(r#"<div class="kpis">"#);
    for c in &g.cards {
        let _ = write!(
            h,
            r#"<div class="kpi {}"><div class="k">{}</div><div class="v">{}<small>{}</small></div><div class="s">{}</div></div>"#,
            esc(&c.cls),
            esc(&c.k),
            esc(&c.v),
            esc(&c.unit),
            esc(&c.sub)
        );
    }
    h.push_str("</div>");
    if !g.rows.is_empty() {
        let _ = write!(
            h,
            "<table class=\"glance\"><thead><tr><th>카메라</th><th>수신 판정</th><th class=n>평균 fps</th><th class=n>정상 수신</th><th class=n>끊김 · 재연결</th><th class=n>촬영 중앙값</th><th class=n>{} 이내</th><th class=n>촬영 성공</th><th>녹화 점검</th></tr></thead><tbody>",
            esc(&g.goal_label)
        );
        for r in &g.rows {
            let _ = write!(
                h,
                "<tr><td><b>{}</b><span class=\"muted\">{}</span></td><td class=\"{}\">{}</td>{}{}{}{}{}{}<td class=\"{}\">{}</td></tr>",
                esc(&r.name),
                esc(&r.sub),
                esc(&r.verdict_cls),
                esc(&r.verdict),
                tdn(&r.fps),
                tdn(&r.uptime),
                tdn(&r.stalls),
                tdn(&r.cap_p50),
                tdn(&r.goal_pct),
                tdn(&r.cap_ok),
                if r.rec_bad { "v-bad" } else { "" },
                esc(&r.rec)
            );
        }
        h.push_str("</tbody></table>");
    }
    h.push_str("<h3>확인할 것</h3>");
    if g.issues.is_empty() {
        h.push_str("<p class=\"ok-line\">특이 사항 없음</p>");
    } else {
        h.push_str("<ul class=\"issues\">");
        for i in &g.issues {
            let _ = write!(h, "<li class=\"{}\">{}</li>", esc(&i.cls), esc(&i.text));
        }
        h.push_str("</ul>");
    }
    let _ = write!(h, "<p class=\"legend\">{}. 문장으로 된 상세 요약은 부록 A.</p>", esc(&g.legend));
    h
}

pub fn report_html(app: &App, r: &Range, meta: &ReportMeta) -> String {
    let d = load(app, r, true);
    let s = summarize(app, r, &d);
    let mut h = String::with_capacity(256 * 1024);
    let title = "무선 RTSP 수신·촬영 시험 결과 보고서";
    let _ = write!(
        h,
        r#"<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>{title}</title><style>{REPORT_CSS}</style></head><body>
<div class="bar"><b>보고서 미리보기</b><span style="opacity:.75">인쇄 창에서 대상 'PDF로 저장'을 고르면 PDF가 됩니다</span><span style="flex:1"></span><button onclick="window.print()">인쇄 / PDF로 저장</button></div>
<div class="wrap"><h1>{title}</h1><div class="sub">무선(Wi-Fi) 환경 IP 카메라 영상 수신·동시 촬영 검증</div>"#
    );
    let cams_text = if s.cameras.is_empty() { "—".to_string() } else { s.cameras.iter().map(|c| c.host.as_ref().map_or_else(|| c.label.clone(), |h| format!("{} · {h}", c.label))).collect::<Vec<_>>().join(", ") };
    let _ = write!(
        h,
        r#"<div class="meta"><div><b>현장</b>{}</div><div><b>담당자</b>{}</div><div><b>기간</b>{} ~ {}</div><div><b>작성</b>{} · v{}</div></div><div class="sub">카메라: {}</div>"#,
        esc(if meta.site.trim().is_empty() { "—" } else { meta.site.trim() }),
        esc(if meta.tester.trim().is_empty() { "—" } else { meta.tester.trim() }),
        r.from.format("%Y-%m-%d %H:%M"),
        r.to.format("%Y-%m-%d %H:%M"),
        s.generated_at.format("%Y-%m-%d %H:%M"),
        env!("CARGO_PKG_VERSION"),
        esc(&cams_text)
    );
    if !meta.note.trim().is_empty() {
        let _ = write!(h, r#"<div class="note">{}</div>"#, esc(meta.note.trim()));
    }

    // 1. 한눈에 보기 (숫자 카드 · 카메라별 한 줄 · 확인할 것). 문장으로 된 자동 요약은 부록 A
    h.push_str("<h2>1. 한눈에 보기</h2>");
    h.push_str(&glance_html(&s.glance));

    // 2. 카메라별
    if !s.cameras.is_empty() {
        let _ = write!(h, "<h2>2. 카메라별 결과</h2><table class=\"kvt\"><thead><tr><th>항목</th>");
        for c in &s.cameras {
            let _ = write!(h, "<th>{} <span class=\"muted\">{}</span></th>", esc(&c.camera_name), esc(&c.camera_id));
        }
        h.push_str("</tr></thead><tbody>");
        for (label, get) in cam_rows() {
            let _ = write!(h, "<tr><td>{}</td>", esc(label));
            for c in &s.cameras {
                h.push_str(&td(get(c)));
            }
            h.push_str("</tr>");
        }
        h.push_str("</tbody></table>");
    }

    // 3. 트리거
    let t = &s.triggers;
    if t.total + t.ignored > 0 {
        let _ = write!(h, "<h2>3. 트리거(동시 촬영)</h2><table class=\"kvt\"><tbody>");
        for (k, v) in trig_rows(t) {
            let _ = write!(h, "<tr>{}{}</tr>", td(k), td(v));
        }
        h.push_str("</tbody></table><p class=\"muted\">전체 시간 = PLC·버튼 신호를 받은 순간부터 모든 카메라 이미지가 디스크에 저장될 때까지. 편차 = 카메라끼리 같은 신호로 찍힌 프레임의 도착 시각 차이 (수신하지 않던 카메라가 있으면 저장 시각 차이).</p>");
    }
    // 측정 위치별
    if !s.locations.is_empty() {
        h.push_str("<h2>측정 위치별 촬영</h2><table><thead><tr><th>위치</th><th>수신 중 촬영</th><th>단독 촬영 (수신 없이)</th><th>동시 촬영 (트리거)</th><th>편차 (중앙값 · 최대)</th></tr></thead><tbody>");
        for (l, row) in loc_rows(&s.locations) {
            let _ = write!(h, "<tr>{}", td(l));
            for v in row {
                h.push_str(&td(v));
            }
            h.push_str("</tr>");
        }
        let _ = write!(h, "</tbody></table><p class=\"muted\">기준: 목표 {} · 허용 한도 {} (누름·신호 → 이미지 저장). 실패한 촬영은 기준 밖으로 셉니다.</p>", ms_label(s.goal_ms), ms_label(s.limit_ms));
    }

    // 4. 차트
    let mut charts = String::new();
    let cam_color = |id: &str| -> &'static str { PALETTE[s.cameras.iter().position(|c| c.camera_id == id).unwrap_or(0) % PALETTE.len()] };
    let cam_name = |id: &str| s.cam_label(id, id).to_string();
    if let Some(st) = &d.stream {
        let series: Vec<Series> = s
            .cameras
            .iter()
            .map(|c| Series {
                name: c.label.clone(),
                color: cam_color(&c.camera_id),
                pts: bucket(st.iter().filter(|x| x.camera_id == c.camera_id).map(|x| (x.ts.timestamp_millis(), x.frames as f64)).collect(), 500),
                dots: false,
            })
            .filter(|x| !x.pts.is_empty())
            .collect();
        let fps_ref = s.cameras.iter().find_map(|c| c.camera_fps).map(|f| (f, format!("카메라 설정 {f:.0}fps")));
        charts.push_str(&svg_chart("수신 fps (구간 평균)", "fps", &series, fps_ref));
        let series: Vec<Series> = s
            .cameras
            .iter()
            .map(|c| Series {
                name: c.label.clone(),
                color: cam_color(&c.camera_id),
                pts: bucket(st.iter().filter(|x| x.camera_id == c.camera_id).filter_map(|x| x.delay_ms.map(|v| (x.ts.timestamp_millis(), v))).collect(), 500),
                dots: false,
            })
            .filter(|x| !x.pts.is_empty())
            .collect();
        charts.push_str(&svg_chart("버퍼 지연 (구간 평균)", "ms", &series, Some((app.cfg.stream.delay_warn_ms as f64, format!("지연 기준 {}ms", app.cfg.stream.delay_warn_ms)))));
    }
    let cap_series: Vec<Series> = s
        .cameras
        .iter()
        .map(|c| Series {
            name: c.label.clone(),
            color: cam_color(&c.camera_id),
            pts: d.captures.iter().filter(|x| x.camera_id == c.camera_id && x.ok).filter_map(|x| Some((x.requested_at?.timestamp_millis(), x.total_ms?))).collect(),
            dots: true,
        })
        .filter(|x| !x.pts.is_empty())
        .collect();
    charts.push_str(&svg_chart("촬영: 누름(신호) → 저장 시간", "ms", &cap_series, Some((s.goal_ms as f64, format!("목표 {}", ms_label(s.goal_ms))))));
    let skew: Vec<(i64, f64)> = d.triggers.iter().filter_map(|x| Some((x.ts.timestamp_millis(), x.skew_ms?))).collect();
    charts.push_str(&svg_chart("트리거: 카메라 간 편차", "ms", &[Series { name: "편차".into(), color: PALETTE[2], pts: skew, dots: true }], None));
    let ping_series: Vec<Series> = s
        .cameras
        .iter()
        .map(|c| Series {
            name: c.label.clone(),
            color: cam_color(&c.camera_id),
            pts: bucket(d.probes.iter().filter(|p| p.camera_id == c.camera_id).filter_map(|p| Some((p.ts.timestamp_millis(), p.icmp_ms?))).collect(), 500),
            dots: false,
        })
        .filter(|x| !x.pts.is_empty())
        .collect();
    charts.push_str(&svg_chart("카메라 ping", "ms", &ping_series, None));
    let wifi_pts = bucket(d.wifi.iter().filter_map(|w| Some((w.ts.timestamp_millis(), w.signal_pct? as f64))).collect(), 500);
    charts.push_str(&svg_chart("이 PC Wi-Fi 신호", "%", &[Series { name: "신호".into(), color: PALETTE[0], pts: wifi_pts, dots: false }], None));
    if !charts.is_empty() {
        let _ = write!(h, "<h2>4. 추이</h2>{charts}");
        if d.stream.is_none() {
            let _ = write!(h, "<p class=\"muted\">기간이 {MAX_STREAM_DAYS}일보다 길어 수신 fps·버퍼 지연 차트는 넣지 않았습니다.</p>");
        }
    }

    // 5. 세션
    if !d.sessions.is_empty() {
        let _ = write!(
            h,
            "<h2>5. 수신 세션 ({}개)</h2><table><thead><tr><th>판정</th><th>카메라</th><th>세션 이름</th><th>스트림·전송·경로</th><th>시작</th><th class=n>길이</th><th class=n>평균 fps</th><th class=n>Mbps</th><th class=n>간격 p99</th><th class=n>끊김</th><th class=n>재연결</th><th class=n>정상 수신</th></tr></thead><tbody>",
            d.sessions.len()
        );
        for x in &d.sessions {
            let st = &x.stats;
            let v = verdict_ko(x.verdict.as_deref(), &x.status);
            let cls = match x.verdict.as_deref() {
                Some("stable") => "v-stable",
                Some("warning") => "v-warning",
                _ => "v-bad",
            };
            let _ = write!(
                h,
                "<tr><td class=\"{cls}\">{}</td>{}{}{}{}{}{}{}{}{}{}{}</tr>",
                esc(v),
                td(s.cam_label(&x.camera_id, &x.camera_name)),
                td(&x.label),
                td(format!("{} · {} · {}", if x.profile == "sub" { "서브" } else { "메인" }, x.transport.to_uppercase(), crate::stream::path_label(x.net.path_kind.as_deref()))),
                td(x.started_at.map(|t| t.format("%m/%d %H:%M:%S").to_string()).unwrap_or_default()),
                tdn(fmt_dur(x.duration_s)),
                tdn(st.avg_fps.map(|v| format!("{v:.1}")).unwrap_or_default()),
                tdn(st.avg_kbps.map(|v| format!("{:.2}", v / 1000.0)).unwrap_or_default()),
                tdn(st.interval_p99_ms.map(|v| format!("{v:.0}ms")).unwrap_or_default()),
                tdn(format!("{}", st.stalls)),
                tdn(format!("{}", st.reconnects)),
                tdn(st.uptime_pct.map(|v| format!("{v:.2}%")).unwrap_or_default())
            );
            if !x.reasons.is_empty() && x.verdict.as_deref() != Some("stable") {
                let _ = write!(h, "<tr><td></td><td colspan=11 class=\"muted\">{}</td></tr>", esc(&x.reasons.join(" / ")));
            }
            if let Some(k) = &x.rec_check {
                let _ = write!(h, "<tr><td></td><td colspan=11 class=\"{}\">{}</td></tr>", if k.ok { "muted" } else { "v-bad" }, esc(&k.summary));
            }
        }
        h.push_str("</tbody></table>");
    }

    // 6. 트리거 기록 / 촬영 기록
    const MAX_ROWS: usize = 300;
    if !d.triggers.is_empty() {
        let mut tcams: Vec<String> = Vec::new();
        for x in &d.triggers {
            for sh in &x.shots {
                if !tcams.contains(&sh.camera_id) {
                    tcams.push(sh.camera_id.clone());
                }
            }
        }
        tcams.sort();
        let _ = write!(h, "<h2>6. 트리거 기록 ({}회{})</h2><table><thead><tr><th class=n>번호</th><th>시각</th><th>출처</th><th>결과</th><th class=n>전체</th><th class=n>편차</th>", d.triggers.len(), if d.triggers.len() > MAX_ROWS { format!(", 최근 {MAX_ROWS}회만 표시 · 전체는 Excel") } else { String::new() });
        for c in &tcams {
            let _ = write!(h, "<th class=n>{}</th>", esc(&cam_name(c)));
        }
        h.push_str("</tr></thead><tbody>");
        for x in d.triggers.iter().rev().take(MAX_ROWS) {
            let _ = write!(
                h,
                "<tr>{}{}{}<td class=\"{}\">{}</td>{}{}",
                tdn(if x.seq > 0 { x.seq.to_string() } else { "—".into() }),
                td(x.ts.format("%m/%d %H:%M:%S%.3f").to_string()),
                td(trigger::source_ko(&x.source)),
                match x.status.as_str() {
                    "ok" => "v-stable",
                    "ignored" => "muted",
                    _ => "v-bad",
                },
                trig_status_ko(&x.status),
                tdn(fmt_ms(x.total_ms)),
                tdn(fmt_ms(x.skew_ms))
            );
            for c in &tcams {
                let cell = match x.shots.iter().find(|sh| &sh.camera_id == c) {
                    Some(sh) if sh.ok => fmt_ms(sh.total_ms),
                    Some(_) => "실패".into(),
                    None => "—".into(),
                };
                h.push_str(&tdn(cell));
            }
            h.push_str("</tr>");
        }
        h.push_str("</tbody></table>");
    } else if !d.captures.is_empty() {
        let _ = write!(h, "<h2>6. 촬영 기록 ({}장{})</h2><table><thead><tr><th>누른 시각</th><th>카메라</th><th>방식</th><th>결과</th><th class=n>누름→저장</th><th class=n>대기·연결</th><th class=n>변환</th><th class=n>저장</th><th>해상도</th></tr></thead><tbody>", d.captures.len(), if d.captures.len() > MAX_ROWS { format!(", 최근 {MAX_ROWS}장만 표시") } else { String::new() });
        for c in d.captures.iter().rev().take(MAX_ROWS) {
            let _ = write!(
                h,
                "<tr>{}{}{}<td class=\"{}\">{}</td>{}{}{}{}{}</tr>",
                td(c.requested_at.map(|t| t.format("%m/%d %H:%M:%S%.3f").to_string()).unwrap_or_default()),
                td(s.cam_label(&c.camera_id, &c.camera_name)),
                td(if c.mode == "direct" { "단독" } else { "수신 중" }),
                if c.ok { "v-stable" } else { "v-bad" },
                if c.ok { "성공" } else { "실패" },
                tdn(fmt_ms(c.total_ms)),
                tdn(fmt_ms(c.wait_ms)),
                tdn(fmt_ms(c.decode_ms)),
                tdn(fmt_ms(c.save_ms)),
                td(c.width.map(|w| format!("{w}×{}", c.height.unwrap_or(0))).unwrap_or_default())
            );
        }
        h.push_str("</tbody></table>");
    }

    // 7. 이미지
    if meta.images {
        let mut figs = String::new();
        let mut n = 0;
        for x in d.triggers.iter().rev().filter(|x| x.status == "ok") {
            if n >= 8 {
                break;
            }
            for sh in &x.shots {
                let _ = write!(
                    figs,
                    r#"<figure><img src="/api/captures/{}/image" alt=""><figcaption>트리거 {}번 · {} · {} · {}</figcaption></figure>"#,
                    esc(&sh.capture_id),
                    x.seq,
                    esc(s.cam_label(&sh.camera_id, &sh.camera_name)),
                    x.ts.format("%m/%d %H:%M:%S%.3f"),
                    fmt_ms(sh.total_ms)
                );
                n += 1;
            }
        }
        if n == 0 {
            for c in d.captures.iter().rev().filter(|c| c.ok && c.image.is_some()).take(6) {
                let _ = write!(
                    figs,
                    r#"<figure><img src="/api/captures/{}/image" alt=""><figcaption>{} · {} · 누름→저장 {}</figcaption></figure>"#,
                    esc(&c.id),
                    esc(s.cam_label(&c.camera_id, &c.camera_name)),
                    c.requested_at.map(|t| t.format("%m/%d %H:%M:%S%.3f").to_string()).unwrap_or_default(),
                    fmt_ms(c.total_ms)
                );
            }
        }
        if !figs.is_empty() {
            let _ = write!(h, "<h2>7. 촬영 이미지 (최근)</h2><div class=\"imgs\">{figs}</div>");
        }
    }

    // 8. 이벤트
    let important = [
        "stream_stall", "stream_lost", "stream_recovered", "session_failed", "camera_down", "camera_up", "wifi_roam", "wifi_disconnected", "wifi_connected", "trigger_failed", "plc_connected",
        "plc_disconnected", "plc_modbus_connected", "plc_modbus_lost", "plc_modbus_error", "plc_tcp_error", "auto_trigger_start", "auto_trigger_done", "auto_trigger_stop", "session_start", "session_end",
        "e2e_measured",
    ];
    let evs: Vec<&Event> = d.events.iter().filter(|e| important.contains(&e.kind.as_str())).collect();
    if !evs.is_empty() {
        let _ = write!(h, "<h2>8. 주요 이벤트 ({}건{})</h2><table><thead><tr><th style=\"width:16%\">시각</th><th style=\"width:13%\">종류</th><th style=\"width:9%\">카메라</th><th>내용</th></tr></thead><tbody>", evs.len(), if evs.len() > 200 { ", 최근 200건만" } else { "" });
        for e in evs.iter().rev().take(200) {
            let _ = write!(h, "<tr>{}{}{}{}</tr>", td(e.ts.format("%m/%d %H:%M:%S").to_string()), td(event_ko(&e.kind)), td(e.camera_id.clone().unwrap_or_default()), td(&e.message));
        }
        h.push_str("</tbody></table>");
    }

    // 9. 기준
    let cfg = &app.cfg.stream;
    let _ = write!(
        h,
        "<h2>부록 A. 자동 요약 (문장)</h2><ul class=\"find\">{}</ul><h2>부록 B. 측정 방법과 판정 기준</h2><table class=\"kvt\"><tbody>{}{}{}{}{}{}{}</tbody></table>",
        s.findings.iter().map(|f| format!("<li>{}</li>", esc(f))).collect::<String>(),
        format_args!("<tr>{}{}</tr>", td("끊김"), td(format!("프레임이 {}ms 이상 오지 않음", cfg.stall_ms))),
        format_args!("<tr>{}{}</tr>", td("버퍼 지연"), td(format!("영상이 실제 시간보다 뒤처진 정도. {}ms 이상이 3초 넘게 이어지면 '지연 구간'", cfg.delay_warn_ms))),
        format_args!("<tr>{}{}</tr>", td("불안정"), td("재연결 1회 이상 · 정상 수신 99% 미만 · 평균 fps가 카메라 설정의 90% 미만 · 2초 이상 끊김")),
        format_args!("<tr>{}{}</tr>", td("주의"), td("짧은 끊김 · 프레임 간격 p99 100ms 초과 · 버퍼 지연 0.5초 초과 · 지연 구간 1% 이상 · 패킷 손실 · 디코딩 오류")),
        format_args!("<tr>{}{}</tr>", td("수신 중 촬영"), td("받고 있는 영상에서 누른(신호) 뒤 처음 도착한 프레임을 원본 해상도 JPEG로 저장 (카메라에 새로 연결하지 않음)")),
        format_args!("<tr>{}{}</tr>", td("단독 촬영"), td("수신하지 않을 때 카메라에 잠깐 접속해 첫 프레임을 저장 (RTSP 연결 시간 포함)")),
        format_args!("<tr>{}{}</tr>", td("시각 기준"), td("모든 시각은 이 PC 시계 기준이며, 같은 PC에서 재므로 카메라 시계를 맞출 필요가 없습니다")),
    );
    h.push_str("</div></body></html>");
    h
}

// ---------------- 파일 ----------------

pub fn exports_dir(app: &App) -> PathBuf {
    config::resolve_path(&app.home, &app.cfg.storage.data_dir).join("exports")
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportFile {
    pub name: String,
    pub bytes: u64,
    pub modified: Option<DateTime<Local>>,
}

pub fn list_exports(dir: &Path) -> Vec<ExportFile> {
    let mut v: Vec<ExportFile> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_string();
                    if !safe_export_name(&name) {
                        return None;
                    }
                    let m = e.metadata().ok()?;
                    Some(ExportFile { name, bytes: m.len(), modified: m.modified().ok().map(DateTime::<Local>::from) })
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort_by(|a, b| b.modified.cmp(&a.modified));
    v.truncate(50);
    v
}

pub fn safe_export_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['/', '\\', ':']) && !name.contains("..") && (name.ends_with(".xlsx") || name.ends_with(".pdf"))
}

pub fn export_name(kind: &str, r: &Range, ext: &str) -> String {
    format!(
        "CaptureVMS_{kind}_{}-{}{}_{}.{ext}",
        r.from.format("%Y%m%d"),
        r.to.format("%Y%m%d"),
        r.camera.as_deref().map(|c| format!("_{c}")).unwrap_or_default(),
        Local::now().format("%H%M%S")
    )
}

/// PDF 인쇄에 쓸 브라우저 (Windows에 기본으로 있는 Edge, 없으면 Chrome)
pub fn find_browser() -> Option<PathBuf> {
    let mut c: Vec<PathBuf> = Vec::new();
    for var in ["ProgramFiles(x86)", "ProgramFiles", "LOCALAPPDATA"] {
        if let Some(base) = std::env::var_os(var) {
            let b = PathBuf::from(base);
            c.push(b.join(r"Microsoft\Edge\Application\msedge.exe"));
            c.push(b.join(r"Google\Chrome\Application\chrome.exe"));
        }
    }
    c.into_iter().find(|p| p.is_file())
}

/// 보고서 주소를 Edge(화면 없이)로 열어 PDF로 인쇄
pub async fn print_pdf(url: &str, out: &Path) -> Result<(), String> {
    let browser = find_browser().ok_or("Microsoft Edge(또는 Chrome)를 찾지 못해 PDF를 바로 만들 수 없습니다. [보고서 미리보기]에서 인쇄 → 'PDF로 저장'을 쓰세요")?;
    if let Some(p) = out.parent() {
        std::fs::create_dir_all(p).map_err(|e| format!("폴더를 만들 수 없습니다: {} ({e})", p.display()))?;
    }
    // 사용 중인 브라우저 창·로그인과 섞이지 않게 따로 쓰는 프로필
    let profile = std::env::temp_dir().join("capture-vms-pdf-profile");
    let mut cmd = tokio::process::Command::new(&browser);
    cmd.args(["--headless", "--disable-gpu", "--no-first-run", "--no-default-browser-check", "--disable-extensions", "--no-pdf-header-footer", "--print-to-pdf-no-header", "--virtual-time-budget=15000"])
        .arg(format!("--user-data-dir={}", profile.display()))
        .arg(format!("--print-to-pdf={}", out.display()))
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000);
    let _ = std::fs::remove_file(out);
    let mut child = cmd.spawn().map_err(|e| format!("브라우저 실행 실패: {e}"))?;
    match tokio::time::timeout(Duration::from_secs(120), child.wait()).await {
        Ok(_) => {}
        Err(_) => {
            let _ = child.kill().await;
            return Err("PDF 만들기가 2분 안에 끝나지 않았습니다. 기간을 줄이거나 이미지를 빼고 다시 해 보세요".into());
        }
    }
    // 브라우저가 끝난 뒤 파일이 조금 늦게 닫히는 경우가 있다
    for _ in 0..20 {
        if std::fs::metadata(out).map_or(false, |m| m.len() > 0) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    Err("브라우저가 PDF를 만들지 못했습니다 (회사 보안 정책으로 막혔을 수 있음). [보고서 미리보기]에서 인쇄 → 'PDF로 저장'을 쓰세요".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_name_cameras_get_id() {
        let v = cam_labels([("cam01", "ACME CAM-1000"), ("cam02", "ACME CAM-1000"), ("cam03", "입구")].into_iter());
        assert_eq!(v[0].1, "ACME CAM-1000 (cam01)");
        assert_eq!(v[1].1, "ACME CAM-1000 (cam02)");
        assert_eq!(v[2].1, "입구");
    }

    #[test]
    fn parses_times() {
        let a = parse_time("2026-11-09T10:15", false).unwrap();
        let b = parse_time("2026-11-09T10:15", true).unwrap();
        assert_eq!((b - a).num_milliseconds(), 59_999);
        let d = parse_time("2026-11-09", true).unwrap();
        assert_eq!(d.format("%H:%M:%S").to_string(), "23:59:59");
        assert!(parse_time("어제", false).is_none());
        let q = RangeQuery { from: Some("2026-11-10".into()), to: Some("2026-11-09".into()), camera: None };
        assert!(q.resolve().is_err());
        let q = RangeQuery { from: None, to: None, camera: Some("all".into()) };
        let r = q.resolve().unwrap();
        assert!(r.camera.is_none() && (r.to - r.from).num_hours() == 24);
    }

    #[test]
    fn goal_ratio_counts_failures_as_outside() {
        // 성공 3번(200·900·2500ms) + 실패 1번 → 1초 이내 2/4, 3초 이내 3/4
        let c = cap_stat([(true, Some(200.0)), (true, Some(900.0)), (true, Some(2500.0)), (false, None)].into_iter(), (1000, 3000));
        assert_eq!((c.total, c.ok, c.within_goal, c.within_limit), (4, 3, 2, 3));
        assert_eq!(c.goal_text(), "1초 이내 50.0% · 3초 이내 75.0%");
        assert_eq!(ms_label(1500), "1.5초");
        assert_eq!(ms_label(800), "800ms");
        assert_eq!(CapStat::default().goal_text(), "—");
    }

    #[test]
    fn stats_and_names() {
        let s = stat(vec![300.0, 100.0, 200.0, f64::NAN]);
        assert_eq!((s.n, s.min, s.p50, s.max), (3, Some(100.0), Some(200.0), Some(300.0)));
        assert!(safe_export_name("CaptureVMS_원시데이터_20261109-20261109_101500.xlsx"));
        assert!(!safe_export_name("..\\config.toml"));
        assert!(!safe_export_name("a.toml"));
        assert_eq!(bucket((0..1000).map(|i| (i, 1.0)).collect(), 100).len(), 100);
        assert!(svg_chart("t", "ms", &[Series { name: "a".into(), color: PALETTE[0], pts: vec![(0, 1.0), (1000, 2.0)], dots: false }], None).contains("<svg"));
        assert!(svg_chart("t", "ms", &[], None).is_empty());
    }

    #[test]
    fn writes_xlsx() {
        let dir = std::env::temp_dir().join(format!("capture-vms-xlsx-test-{}", std::process::id()));
        let p = dir.join("t.xlsx");
        let now = Local::now();
        let d = Data {
            sessions: vec![SessionRecord { id: "s1".into(), camera_id: "cam01".into(), camera_name: "입구".into(), status: "finished".into(), verdict: Some("stable".into()), started_at: Some(now), duration_s: 60.0, ..Default::default() }],
            triggers: vec![TriggerRecord { id: "t1".into(), seq: 1, ts: now, source: "tcp".into(), status: "ok".into(), total_ms: Some(210.0), ..Default::default() }],
            stream: Some(vec![]),
            ..Default::default()
        };
        let s = Summary {
            from: now,
            to: now,
            camera: None,
            generated_at: now,
            counts: BTreeMap::new(),
            cameras: vec![CamSummary { camera_id: "cam01".into(), camera_name: "입구".into(), label: "입구".into(), sessions: 1, stable: 1, ..Default::default() }],
            triggers: TrigSummary::default(),
            wifi: WifiSummary::default(),
            events: vec![],
            findings: vec!["테스트".into()],
            stream_included: true,
            goal_ms: 1000,
            limit_ms: 3000,
            locations: vec![LocSummary { location: "정차 지점 A".into(), ..Default::default() }],
            glance: Glance::default(),
        };
        write_xlsx(&p, &s, &d, &ReportMeta::default()).unwrap();
        let bytes = std::fs::read(&p).unwrap();
        assert!(bytes.starts_with(b"PK") && bytes.len() > 3000);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
