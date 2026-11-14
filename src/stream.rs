//! RTSP 수신 세션.
//!
//! FFmpeg 하나가 RTSP 연결 **하나**로 세 가지를 동시에 한다 (카메라·Wi-Fi 부하를 늘리지 않기 위해).
//!   1) 원본 녹화: 재인코딩 없이 몇 분 단위로 나눈 MP4 (중간에 끊겨도 앞 파일은 온전)
//!   2) 프레임 도착 기록: framecrc → 로컬 TCP → 도착 간격·끊김·누적 지연 측정
//!   3) 대시보드 미리보기: 작게 줄인 JPEG → stdout
//! 연결이 끊기면 자동으로 다시 연결하고, 재연결 횟수와 끊긴 시간을 센다.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use chrono::Local;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    process::Command,
    sync::{watch, Notify},
};

use crate::{
    app::App,
    config::StreamConfig,
    netmon,
    e2e::{self, E2e, E2eLive},
    store::{self, E2eSummary, Event, NetSummary, SessionRecord, SessionStats, StreamSample},
};

/// 실제 지연 측정용 흑백 축소 영상 크기
const LUMA_W: usize = 64;
const LUMA_H: usize = 48;

/// 첫 프레임 뒤 이 시간 동안은 연결 직후 몰려 들어오는 버퍼 분량이라 간격·지연 통계에서 뺀다.
const WARMUP: Duration = Duration::from_secs(3);
/// 디코딩 오류도 연결 직후(첫 키프레임 전)에는 정상이라 세지 않는다.
const DECODE_GRACE: Duration = Duration::from_secs(5);
/// 연결된 뒤 프레임이 이만큼 안 오면 FFmpeg를 끊고 다시 연결한다 (`stall_reconnect_ms`를 0으로 꺼 둔 경우의 마지막 안전망).
const WATCHDOG: Duration = Duration::from_secs(10);
/// 이만큼 넘게 늦게 온 프레임은 '제때 온 프레임'으로 치지 않는다 (ms).
/// 연결이 끊긴 동안에도 TCP 재전송으로 한두 프레임이 몇 초 늦게 흘러 들어와, 마지막 프레임 이후 시간만으로는 끊김을 못 잡는다.
const STARVE_LATE_MS: f64 = 1000.0;
/// 새로 연결하려고 FFmpeg를 끝낼 때, 읽기 제한 시간에 더해 이만큼 더 기다린다 (넘으면 강제 종료 — 녹화 파일 끝이 잘린다).
const RESTART_MARGIN: Duration = Duration::from_secs(1);
/// 굶는지 보는 감시(0.25초마다)가 이만큼 넘게 늦게 깨어나면 이 프로그램이 멈춰 있던 것으로 본다 (`watch_loop`).
const SELF_STALL: Duration = Duration::from_secs(1);
/// 출력 하나(녹화 파일, 엔진으로 보내는 프레임 기록·화면용 영상)가 막혀 있는 동안 FFmpeg가 수신을 멈추지 않고
/// 쌓아 둘 수 있는 프레임 수 (`build_args` 참고). 60fps면 약 17초 분량.
const OUT_QUEUE_PACKETS: u32 = 1024;
/// 영상 시각이 도착보다 이만큼 넘게 앞서 가면 카메라가 보낸 시각이 튄 것으로 본다 (초).
const TS_FUTURE_S: f64 = 0.5;
/// 앞서 간 상태가 이만큼 이어지면 튄 것이 아니라 새 기준으로 받아들인다.
const TS_FUTURE_ACCEPT: Duration = Duration::from_secs(10);
/// 끊긴 뒤 카메라 RTSP 포트가 다시 열리는지 이 간격으로 확인하고, 열리는 즉시 다시 연결한다.
const REACH_PROBE_EVERY: Duration = Duration::from_millis(200);
const REACH_PROBE_TIMEOUT: Duration = Duration::from_millis(700);
/// 이만큼 확인해도 안 열리면 일단 연결을 시도한다 (오류 내용을 화면에 보여 주려고).
const REACH_WAIT_MAX: Duration = Duration::from_secs(20);
/// 연결 시도 후 이만큼 지나도 첫 프레임이 없으면 다시 시도한다.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// 한 번도 연결되지 않은 채, 카메라가 응답하는데도 이만큼 연달아 실패하면 세션을 실패로 끝낸다.
const MAX_INITIAL_ATTEMPTS: u32 = 5;
/// 받던 영상이 이 시간 넘게 끊겨 있을 때만 카메라 주소가 바뀌었는지 확인한다 (그보다 짧은 끊김은 바로 다시 연결).
/// 공유기를 껐다 켜서 주소가 바뀌는 데는 이보다 훨씬 오래 걸린다
const FOLLOW_AFTER: Duration = Duration::from_secs(15);
/// 수신 중 세션 기록을 이 간격(초)마다 중간 저장한다.
const CHECKPOINT_SECS: u64 = 15;

/// FFmpeg가 MP4 `hybrid_fragmented`를 지원하는지 (시작할 때 한 번 확인)
static MP4_HYBRID: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// 시작할 때 한 번: 녹화 방식을 정하려고 FFmpeg의 MP4 옵션을 확인한다.
pub fn detect_ffmpeg_features(ffmpeg: &Path) -> bool {
    let mut cmd = std::process::Command::new(ffmpeg);
    cmd.args(["-hide_banner", "-h", "muxer=mp4"]).stdin(Stdio::null()).stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000);
    }
    let ok = cmd.output().map(|o| String::from_utf8_lossy(&o.stdout).contains("hybrid_fragmented")).unwrap_or(false);
    let _ = MP4_HYBRID.set(ok);
    ok
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartRequest {
    pub camera_id: String,
    /// main | sub
    pub profile: String,
    /// tcp | udp
    pub transport: String,
    #[serde(default)]
    pub label: String,
    /// smooth | light | off
    #[serde(default = "default_preview")]
    pub preview: String,
    #[serde(default = "default_true")]
    pub record: bool,
    /// [모두 수신 시작]으로 같이 시작한 묶음
    #[serde(default)]
    pub group_id: Option<String>,
}

fn default_preview() -> String {
    "smooth".into()
}

fn default_true() -> bool {
    true
}

// ---------------- 지표 집계 ----------------

/// 고정 크기 히스토그램 (오래 켜 둬도 메모리가 늘지 않는다)
#[derive(Clone)]
struct Hist {
    step: f64,
    counts: Vec<u64>,
    n: u64,
    max: f64,
}

impl Hist {
    fn new(step: f64, buckets: usize) -> Self {
        Self { step, counts: vec![0; buckets], n: 0, max: 0.0 }
    }

    fn add(&mut self, v: f64) {
        let v = v.max(0.0);
        let i = ((v / self.step) as usize).min(self.counts.len() - 1);
        self.counts[i] += 1;
        self.n += 1;
        self.max = self.max.max(v);
    }

    fn pct(&self, p: f64) -> Option<f64> {
        if self.n == 0 {
            return None;
        }
        let target = ((p / 100.0) * self.n as f64).ceil().max(1.0) as u64;
        let mut acc = 0;
        for (i, c) in self.counts.iter().enumerate() {
            acc += c;
            if acc >= target {
                return Some(((i as f64 + 0.5) * self.step).min(self.max));
            }
        }
        Some(self.max)
    }
}

#[derive(Default)]
struct FrameNote {
    /// 방금 끝난 끊김 길이
    stall_ms: Option<f64>,
    /// 재연결 후 첫 프레임: 끊겨 있던 시간
    recovered_after_ms: Option<f64>,
    first_frame: bool,
    /// 영상 시각이 앞으로 튀기 시작함: 얼마나 앞서 갔는지 (초)
    ts_jump_s: Option<f64>,
}

struct Metrics {
    stall_ms: f64,
    jank_ms: f64,
    delay_warn_ms: f64,
    delay_bad_ms: f64,
    /// 지연이 기준 이상으로 3초 넘게 이어진 시간(초). Wi-Fi에서 흔한 1초짜리 튐은 세지 않는다.
    delayed_secs: u64,
    delay_run: u32,
    /// 최근 3초의 1초별 버퍼 지연 최대. 이 중 가장 작은 값이 '지속 지연'
    recent_delay: VecDeque<f64>,
    session_start: Instant,
    first_frame_ever: Option<Instant>,
    // 현재 FFmpeg 실행
    run_start: Option<Instant>,
    run_frames: u64,
    warm_until: Option<Instant>,
    prev_frame: Option<Instant>,
    base: Option<(Instant, f64)>,
    lag_mins: VecDeque<f64>,
    sec_lag_min: Option<f64>,
    /// 이번 연결의 첫 프레임 시각, 최근 프레임의 (도착 시각, 1초 넘게 늦게 왔는지)
    run_first: Option<Instant>,
    recent_arrivals: VecDeque<(Instant, bool)>,
    /// 영상 시각 튐: 직전 프레임의 영상 시각, 앞서 가기 시작한 때와 그 뒤로 센 프레임, 세션 전체 수
    prev_pts: Option<f64>,
    future_since: Option<Instant>,
    future_run: u64,
    ts_glitch_frames: u64,
    // 1초 누적
    sec_frames: u32,
    sec_bytes: u64,
    sec_iv_max: Option<f64>,
    sec_delay_max: Option<f64>,
    recent_iv_max: VecDeque<f64>,
    last_tick: (u32, f64, Option<f64>, Option<f64>),
    // 세션 전체
    frames: u64,
    bytes: u64,
    /// 연결 직후 몰려 들어오는 분량(워밍업)을 뺀 프레임·바이트와 그 시작 시각 → 평균 fps·비트레이트용
    post_frames: u64,
    post_bytes: u64,
    post_start: Option<Instant>,
    iv: Hist,
    delays: Hist,
    janks: u64,
    stalls: u64,
    stall_total_ms: f64,
    longest_stall_ms: f64,
    reconnects: u64,
    downtime_ms: f64,
    down_since: Option<Instant>,
    rtp_loss_events: u64,
    rtp_lost: u64,
    decode_errors: u64,
    last_frame: Option<Instant>,
    connected: bool,
    ever_connected: bool,
    codec: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    camera_fps: Option<f64>,
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

impl Metrics {
    fn new(cfg: &StreamConfig, session_start: Instant) -> Self {
        Self {
            stall_ms: cfg.stall_ms as f64,
            jank_ms: cfg.jank_ms as f64,
            delay_warn_ms: cfg.delay_warn_ms as f64,
            delay_bad_ms: cfg.delay_bad_ms.max(cfg.delay_warn_ms) as f64,
            delayed_secs: 0,
            delay_run: 0,
            recent_delay: VecDeque::new(),
            session_start,
            first_frame_ever: None,
            run_start: None,
            run_frames: 0,
            warm_until: None,
            prev_frame: None,
            base: None,
            lag_mins: VecDeque::new(),
            sec_lag_min: None,
            run_first: None,
            recent_arrivals: VecDeque::new(),
            prev_pts: None,
            future_since: None,
            future_run: 0,
            ts_glitch_frames: 0,
            sec_frames: 0,
            sec_bytes: 0,
            sec_iv_max: None,
            sec_delay_max: None,
            recent_iv_max: VecDeque::new(),
            last_tick: (0, 0.0, None, None),
            frames: 0,
            bytes: 0,
            post_frames: 0,
            post_bytes: 0,
            post_start: None,
            iv: Hist::new(0.5, 8_000),     // 0~4초, 0.5ms 단위
            delays: Hist::new(5.0, 4_000), // 0~20초, 5ms 단위
            janks: 0,
            stalls: 0,
            stall_total_ms: 0.0,
            longest_stall_ms: 0.0,
            reconnects: 0,
            downtime_ms: 0.0,
            down_since: None,
            rtp_loss_events: 0,
            rtp_lost: 0,
            decode_errors: 0,
            last_frame: None,
            connected: false,
            ever_connected: false,
            codec: None,
            width: None,
            height: None,
            camera_fps: None,
        }
    }

    fn begin_run(&mut self, now: Instant) {
        self.run_start = Some(now);
        self.run_frames = 0;
        self.warm_until = None;
        self.prev_frame = None;
        self.base = None;
        self.lag_mins.clear();
        self.sec_lag_min = None;
        self.run_first = None;
        self.recent_arrivals.clear();
        self.prev_pts = None;
        self.future_since = None;
        self.future_run = 0;
    }

    /// `user_stop`이면 사용자가 멈춘 것이라 끊긴 시간으로 세지 않는다.
    fn end_run(&mut self, now: Instant, user_stop: bool) {
        if self.connected {
            self.connected = false;
            if !user_stop {
                self.down_since = Some(self.last_frame.unwrap_or(now));
            }
        }
        self.run_start = None;
    }

    fn on_frame(&mut self, now: Instant, pts_s: Option<f64>, size: u64) -> FrameNote {
        let mut note = FrameNote::default();
        if self.run_frames == 0 {
            note.first_frame = true;
            self.first_frame_ever.get_or_insert(now);
            self.run_first = Some(now);
            self.warm_until = Some(now + WARMUP);
            self.connected = true;
            self.ever_connected = true;
            if let Some(d) = self.down_since.take() {
                let down = ms(now - d);
                self.downtime_ms += down;
                note.recovered_after_ms = Some(down);
            }
        }
        self.run_frames += 1;
        self.frames += 1;
        self.bytes += size;
        self.sec_frames += 1;
        self.sec_bytes += size;
        let warm = self.warm_until.map_or(false, |w| now < w);
        if !warm {
            self.post_start.get_or_insert(now);
            self.post_frames += 1;
            self.post_bytes += size;
        }

        if let Some(prev) = self.prev_frame {
            let gap = ms(now - prev);
            if !warm {
                self.iv.add(gap);
                self.sec_iv_max = Some(self.sec_iv_max.map_or(gap, |m| m.max(gap)));
                if gap > self.jank_ms {
                    self.janks += 1;
                }
            }
            if gap >= self.stall_ms {
                self.stalls += 1;
                self.stall_total_ms += gap;
                self.longest_stall_ms = self.longest_stall_ms.max(gap);
                note.stall_ms = Some(gap);
            }
        }
        self.prev_frame = Some(now);
        self.last_frame = Some(now);

        // 누적 지연: (도착 경과) - (영상 시간 경과). 최근 1분 최솟값 대비 늘어난 만큼이 '버퍼 지연'.
        let mut late = false;
        if !warm {
            if let Some(p) = pts_s {
                // 같은 시각에 뭉친 프레임: 앞으로 튄 시각 뒤에 온 프레임을 FFmpeg가 직전 시각에 붙여 놓은 것
                let frozen = self.prev_pts.map_or(false, |q| (p - q).abs() < 0.0005);
                self.prev_pts = Some(p);
                match self.base {
                    None => self.base = Some((now, p)),
                    Some((t0, p0)) => {
                        let lag = (now - t0).as_secs_f64() - (p - p0);
                        let floor = self.lag_mins.iter().copied().chain(self.sec_lag_min).fold(f64::INFINITY, f64::min);
                        // 굶는지 볼 때 쓰는 '늦게 온 프레임'. 시각이 뭉친 프레임이라도 도착이 1초 넘게 늦었으면 늦은 것이다
                        // (끊긴 동안 흘러 들어오는 프레임은 같은 시각으로 두 번 나오기도 한다)
                        late = floor.is_finite() && (lag - floor) * 1000.0 >= STARVE_LATE_MS;
                        let future = floor.is_finite() && lag < floor - TS_FUTURE_S;
                        if future && self.future_since.map_or(false, |t| now - t >= TS_FUTURE_ACCEPT) {
                            // 계속 앞서 있으면 튄 것이 아니라 밀려 있던 영상이 따라잡은 것 → 여기를 새 기준으로
                            self.base = Some((now, p));
                            self.lag_mins.clear();
                            self.sec_lag_min = None;
                            self.ts_glitch_frames -= self.future_run.min(self.ts_glitch_frames);
                            self.future_since = None;
                            self.future_run = 0;
                        } else if future || frozen {
                            // 카메라가 보낸 시각이 튄 프레임. 영상은 정상으로 온 것이라 지연 계산에 넣으면
                            // 기준(최솟값)이 틀어져 그 뒤 1분 동안 없는 지연이 있는 것처럼 나온다
                            self.ts_glitch_frames += 1;
                            if future && self.future_since.is_none() {
                                self.future_since = Some(now);
                                self.future_run = 0;
                                note.ts_jump_s = Some(floor - lag);
                            }
                            if self.future_since.is_some() {
                                self.future_run += 1;
                            }
                        } else {
                            self.future_since = None;
                            self.future_run = 0;
                            self.sec_lag_min = Some(self.sec_lag_min.map_or(lag, |m| m.min(lag)));
                            let delay = ((lag - floor.min(lag)) * 1000.0).max(0.0);
                            self.sec_delay_max = Some(self.sec_delay_max.map_or(delay, |m| m.max(delay)));
                        }
                    }
                }
            }
        }
        self.recent_arrivals.push_back((now, late));
        while self.recent_arrivals.front().map_or(false, |(t, _)| now - *t > Duration::from_secs(20)) {
            self.recent_arrivals.pop_front();
        }
        note
    }

    /// 수신 중인데 최근 `window` 동안 제때 온 프레임이 하나도 없는지.
    /// 끊긴 채 몇 초 늦은 프레임만 한두 장 흘러 들어오는 상태도 여기에 든다 (그런 프레임은 '제때'가 아니다).
    fn starved(&self, now: Instant, window: Duration) -> bool {
        // 이번 연결에서 워밍업 뒤 `window` 넘게 받은 다음에만 본다 (연결 직후 오판 방지)
        if !self.connected || self.run_first.map_or(true, |f| now.saturating_duration_since(f) < WARMUP + window) {
            return false;
        }
        // 프레임이 드문드문 오는 스트림(초당 1장 이하)은 창을 프레임 간격의 4배까지 늘린다
        let window = self.iv.pct(50.0).map_or(window, |p| window.max(Duration::from_secs_f64(p * 4.0 / 1000.0)));
        !self.recent_arrivals.iter().any(|(t, late)| !late && now.saturating_duration_since(*t) <= window)
    }

    /// 1초마다 호출: 그 1초의 묶음을 돌려주고 누적값을 비운다.
    fn tick(&mut self, now: Instant) -> (u32, f64, Option<f64>, Option<f64>, bool, bool) {
        let stalled = self.connected && self.last_frame.map_or(false, |t| ms(now - t) >= self.stall_ms);
        if let Some(m) = self.sec_lag_min.take() {
            self.lag_mins.push_back(m);
            if self.lag_mins.len() > 60 {
                self.lag_mins.pop_front();
            }
        }
        if let Some(d) = self.sec_delay_max {
            self.delays.add(d);
        }
        match self.sec_delay_max {
            Some(d) if d >= self.delay_warn_ms => {
                self.delay_run += 1;
                self.delayed_secs += match self.delay_run {
                    3 => 3,
                    n if n > 3 => 1,
                    _ => 0,
                };
            }
            _ => self.delay_run = 0,
        }
        // 끊긴 동안은 지연이 아니라 끊김으로 보여 준다
        if self.connected {
            self.recent_delay.push_back(self.sec_delay_max.unwrap_or(0.0));
        } else {
            self.recent_delay.clear();
        }
        if self.recent_delay.len() > 3 {
            self.recent_delay.pop_front();
        }
        self.recent_iv_max.push_back(self.sec_iv_max.unwrap_or(0.0));
        if self.recent_iv_max.len() > 60 {
            self.recent_iv_max.pop_front();
        }
        let out = (
            self.sec_frames,
            self.sec_bytes as f64 * 8.0 / 1000.0,
            self.sec_iv_max,
            self.sec_delay_max,
            stalled,
            self.connected,
        );
        self.last_tick = (out.0, out.1, out.2, out.3);
        self.sec_frames = 0;
        self.sec_bytes = 0;
        self.sec_iv_max = None;
        self.sec_delay_max = None;
        out
    }

    fn since_last_frame(&self, now: Instant) -> Option<Duration> {
        self.last_frame.map(|t| now - t)
    }

    /// 세션 통계. `final_`이면 지금 진행 중인 끊김도 합친다.
    /// fps·비트레이트·정상 수신 비율은 첫 프레임 이후 구간으로 계산한다 (처음 연결하는 시간은 `connect_ms`로 따로).
    fn stats(&self, now: Instant, final_: bool) -> SessionStats {
        // 끝낼 때: 끊긴 채로 끝났으면 지금까지(끊긴 시간 포함), 정상이었으면 마지막 프레임까지
        let end = match (final_, self.down_since) {
            (true, None) => self.last_frame.unwrap_or(now),
            _ => now,
        };
        let duration_s = self.first_frame_ever.map_or(0.0, |f| (end.max(f) - f).as_secs_f64());
        let mut stall_total = self.stall_total_ms;
        let mut longest = self.longest_stall_ms;
        let mut stalls = self.stalls;
        let mut downtime = self.downtime_ms;
        if let Some(d) = self.down_since {
            downtime += ms(now - d);
        } else if final_ && self.connected {
            if let Some(gap) = self.since_last_frame(now).map(ms) {
                if gap >= self.stall_ms {
                    stalls += 1;
                    stall_total += gap;
                    longest = longest.max(gap);
                }
            }
        }
        let p50 = self.iv.pct(50.0);
        let dur_ms = (duration_s * 1000.0).max(1.0);
        let nominal_fps = self.camera_fps.or_else(|| p50.filter(|v| *v > 0.0).map(|v| 1000.0 / v));
        // 프레임 누락: 첫 프레임부터의 시간 × 카메라 fps 대비 실제로 받은 수
        let expected = nominal_fps.filter(|_| duration_s > 0.0).map(|f| (f * duration_s).round() as u64);
        // 평균은 워밍업 이후 구간으로 (짧은 세션에서 연결 직후 몰려온 분량 때문에 60fps가 61fps로 나오는 것 방지)
        let post_s = self.post_start.map_or(0.0, |p| (end.max(p) - p).as_secs_f64());
        let (avg_fps, avg_kbps) = if post_s >= 1.0 {
            (Some(self.post_frames as f64 / post_s), Some(self.post_bytes as f64 * 8.0 / 1000.0 / post_s))
        } else if duration_s > 0.0 {
            (Some(self.frames as f64 / duration_s), Some(self.bytes as f64 * 8.0 / 1000.0 / duration_s))
        } else {
            (None, None)
        };
        SessionStats {
            frames: self.frames,
            bytes: self.bytes,
            connect_ms: self.first_frame_ever.map(|f| ms(f - self.session_start)),
            avg_fps,
            nominal_fps,
            avg_kbps,
            interval_p50_ms: p50,
            interval_p95_ms: self.iv.pct(95.0),
            interval_p99_ms: self.iv.pct(99.0),
            interval_max_ms: (self.iv.n > 0).then_some(self.iv.max),
            janks: self.janks,
            stalls,
            stall_total_ms: stall_total,
            longest_stall_ms: longest,
            reconnects: self.reconnects,
            downtime_ms: downtime,
            rtp_loss_events: self.rtp_loss_events,
            rtp_lost_packets: self.rtp_lost,
            decode_errors: self.decode_errors,
            delay_max_ms: (self.delays.n > 0).then_some(self.delays.max),
            delay_p95_ms: self.delays.pct(95.0),
            delayed_secs: self.delayed_secs,
            delay_warn_ms: self.delay_warn_ms,
            uptime_pct: (self.ever_connected).then(|| ((dur_ms - stall_total - downtime) / dur_ms * 100.0).clamp(0.0, 100.0)),
            codec: self.codec.clone(),
            width: self.width,
            height: self.height,
            camera_fps: self.camera_fps,
            expected_frames: expected,
            missing_frames: expected.map(|e| e.saturating_sub(self.frames)),
            ts_glitch_frames: self.ts_glitch_frames,
        }
    }
}

// ---------------- 세션 ----------------

#[derive(Debug, Clone, Default, Serialize)]
pub struct LiveView {
    /// connecting | receiving | delayed | stalled | reconnecting | stopping
    pub state: String,
    /// 지속 지연(최근 3초 내내 이만큼 이상 늦음)과 그 단계: ok | warn | bad
    pub delay_3s_ms: Option<f64>,
    pub delay_level: Option<&'static str>,
    /// 실제 지연 측정 (측정 중이거나 측정값이 있을 때)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub e2e: Option<E2eLive>,
    /// 실제 지연 측정이 가능한 세션인지 (미리보기 '보통'이어야 함)
    pub e2e_available: bool,
    pub elapsed_s: f64,
    pub since_last_frame_ms: Option<f64>,
    pub fps: f64,
    pub kbps: f64,
    pub interval_max_ms: Option<f64>,
    pub interval_max_1m_ms: Option<f64>,
    pub delay_ms: Option<f64>,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Serialize)]
pub struct SessionView {
    #[serde(flatten)]
    pub record: SessionRecord,
    pub live: Option<LiveView>,
}

/// 라이브 '전체 보기' 타일용 짧은 요약 (상태 조회에 카메라마다 붙인다)
#[derive(Debug, Clone, Serialize)]
pub struct LiveBrief {
    pub session_id: String,
    pub label: String,
    pub profile: String,
    pub transport: String,
    pub preview: String,
    pub record: bool,
    pub state: String,
    pub delay_level: Option<&'static str>,
    pub delay_3s_ms: Option<f64>,
    pub fps: f64,
    pub kbps: f64,
    pub avg_fps: Option<f64>,
    pub camera_fps: Option<f64>,
    pub stalls: u64,
    pub reconnects: u64,
    pub uptime_pct: Option<f64>,
    /// 프레임 누락 비율 (와야 할 프레임 대비, %)
    pub missing_pct: Option<f64>,
    pub elapsed_s: f64,
    pub since_last_frame_ms: Option<f64>,
    pub recorded_bytes: u64,
    pub codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub last_error: Option<String>,
    pub group_id: Option<String>,
}

pub struct Session {
    pub id: String,
    pub camera_id: String,
    rec: Mutex<SessionRecord>,
    metrics: Mutex<Metrics>,
    pub preview: watch::Sender<Option<Bytes>>,
    state: Mutex<(String, u32, Option<String>)>,
    started: Instant,
    rec_dir: Option<PathBuf>,
    /// 지금 붙어 있는(붙으려는) RTSP 주소 (계정 포함). 카메라 주소가 바뀌면 새 주소로 바꾼다
    live_url: Mutex<String>,
    stop: Notify,
    kill: Notify,
    /// kill 알림이 지난 연결에 남아 다음 연결을 끊지 않도록, 실제 요청 여부를 따로 둔다
    kill_requested: AtomicBool,
    /// 영상이 굶을 때 새로 연결하라는 요청 (kill과 달리 녹화 파일을 닫을 시간을 준다)
    restart: Notify,
    restart_requested: AtomicBool,
    stopping: AtomicBool,
    e2e: Mutex<E2e>,
    e2e_capable: bool,
    /// 프레임 촬영용 최근 원본 영상
    ts: Mutex<crate::capture::TsBuffer>,
    pub capture_lock: tokio::sync::Mutex<()>,
}

/// 촬영 기록에 남길 세션 정보
pub struct CaptureInfo {
    pub camera_id: String,
    pub camera_name: String,
    pub label: String,
    pub profile: String,
    pub transport: String,
    pub path_kind: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl Session {
    pub fn ts_buffer(&self) -> std::sync::MutexGuard<'_, crate::capture::TsBuffer> {
        self.ts.lock().unwrap()
    }

    /// 한 번 연결됐다가 끊겨서 다시 연결하는 중인지 (촬영을 기다려도 소용없음)
    pub fn is_reconnecting(&self) -> bool {
        let m = self.metrics.lock().unwrap();
        m.ever_connected && !m.connected
    }

    pub fn capture_info(&self) -> CaptureInfo {
        let r = self.rec.lock().unwrap();
        let m = self.metrics.lock().unwrap();
        CaptureInfo {
            camera_id: r.camera_id.clone(),
            camera_name: r.camera_name.clone(),
            label: r.label.clone(),
            profile: r.profile.clone(),
            transport: r.transport.clone(),
            path_kind: r.net.path_kind.clone(),
            width: m.width,
            height: m.height,
        }
    }

    /// 실제 지연 측정 시작
    pub fn e2e_start(&self) -> Result<(), String> {
        if !self.e2e_capable {
            return Err("미리보기를 '보통'으로 시작한 세션에서만 실제 지연을 잴 수 있습니다".into());
        }
        if self.is_stopping() {
            return Err("세션이 끝나는 중입니다".into());
        }
        self.e2e.lock().unwrap().start();
        Ok(())
    }

    pub fn e2e_mark(&self, t: f64, white: bool) {
        self.e2e.lock().unwrap().mark(t, white);
    }

    /// 측정을 끝내고 요약을 세션 기록에 남긴다
    pub fn e2e_stop(&self) -> Option<E2eSummary> {
        let sum = self.e2e.lock().unwrap().finish()?;
        self.rec.lock().unwrap().e2e = Some(sum.clone());
        Some(sum)
    }

    /// 뷰어가 자기 화면으로 잰 실제 지연 요약을 세션 기록에 남긴다
    pub fn e2e_set(&self, sum: E2eSummary) {
        self.rec.lock().unwrap().e2e = Some(sum);
    }

    fn request_kill(&self) {
        self.kill_requested.store(true, Ordering::SeqCst);
        self.kill.notify_one();
    }

    fn request_restart(&self) {
        self.restart_requested.store(true, Ordering::SeqCst);
        self.restart.notify_one();
    }

    /// 설정에 적힌 이 카메라의 주소가 지금 붙어 있는 주소와 달라졌는지 (주소 따라가기·주소 바꾸기 뒤)
    fn address_changed(&self, app: &App) -> bool {
        self.wanted_url(app).is_some_and(|w| w != *self.live_url.lock().unwrap())
    }

    /// 설정에 적힌 지금의 RTSP 주소 (카메라가 지워졌으면 None)
    fn wanted_url(&self, app: &App) -> Option<String> {
        let profile = self.rec.lock().unwrap().profile.clone();
        app.camera(&self.camera_id).and_then(|c| c.cfg.rtsp_url(&profile))
    }

    fn set_url(&self, url: &str) {
        *self.live_url.lock().unwrap() = url.to_string();
        self.rec.lock().unwrap().url = mask_url(url);
    }

    /// 끊김 진단용: (마지막 프레임 이후 지난 시간, 한 번이라도 받았는지, 지금까지의 연결 시도 횟수)
    pub(crate) fn frame_gap(&self) -> (Option<Duration>, bool, u32) {
        // metrics → state 순서 (둘을 같이 잡지 않는다)
        let (gap, ever) = {
            let m = self.metrics.lock().unwrap();
            (m.since_last_frame(Instant::now()), m.ever_connected)
        };
        (gap, ever, self.state.lock().unwrap().1)
    }

    /// 카메라 RTSP 주소의 (호스트, 포트)
    pub(crate) fn rtsp_host_port(&self) -> Option<(String, u16)> {
        let u = reqwest::Url::parse(&self.rec.lock().unwrap().url).ok()?;
        Some((u.host_str()?.to_string(), u.port().unwrap_or(554)))
    }

    /// 지금 영상이 들어오고 있는지
    pub fn is_receiving(&self) -> bool {
        let m = self.metrics.lock().unwrap();
        m.connected && m.since_last_frame(Instant::now()).map_or(false, |d| d < Duration::from_secs(3))
    }

    /// PC가 Wi-Fi로 카메라에 닿는 세션인지 (Wi-Fi 고정 대상)
    pub fn via_wifi(&self) -> bool {
        self.rec.lock().unwrap().net.path_kind.as_deref() == Some("wifi")
    }

    pub fn request_stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.set_state("stopping");
        self.stop.notify_one();
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn set_state(&self, s: &str) {
        self.state.lock().unwrap().0 = s.to_string();
    }

    fn set_error(&self, e: impl Into<String>) {
        self.state.lock().unwrap().2 = Some(e.into());
    }

    /// 지금까지의 요약 + 실시간 값
    pub fn view(&self) -> SessionView {
        let now = Instant::now();
        let mut rec = self.rec.lock().unwrap().clone();
        let m = self.metrics.lock().unwrap();
        rec.duration_s = (now - self.started).as_secs_f64();
        rec.stats = m.stats(now, false);
        let (state, attempts, err) = self.state.lock().unwrap().clone();
        let since = m.since_last_frame(now).map(ms);
        // 지속 지연: 최근 3초 동안 계속 그 이상 늦었던 값 (1초짜리 튐은 걸러진다)
        let delay_3s = (m.recent_delay.len() >= 3).then(|| m.recent_delay.iter().copied().fold(f64::INFINITY, f64::min));
        let delay_level = delay_3s.map(|d| if d >= m.delay_bad_ms { "bad" } else if d >= m.delay_warn_ms { "warn" } else { "ok" });
        let state = if state == "receiving" && since.map_or(false, |v| v >= m.stall_ms) {
            "stalled".to_string()
        } else if state == "receiving" && matches!(delay_level, Some("warn" | "bad")) {
            "delayed".to_string()
        } else {
            state
        };
        let e2e = {
            let e = self.e2e.lock().unwrap();
            let l = e.live();
            (l.active || l.count > 0).then_some(l)
        };
        let live = LiveView {
            state,
            delay_3s_ms: delay_3s,
            delay_level,
            e2e,
            e2e_available: self.e2e_capable,
            elapsed_s: rec.duration_s,
            since_last_frame_ms: since,
            fps: m.last_tick.0 as f64,
            kbps: m.last_tick.1,
            interval_max_ms: m.last_tick.2,
            interval_max_1m_ms: m.recent_iv_max.iter().copied().reduce(f64::max).filter(|v| *v > 0.0),
            delay_ms: m.last_tick.3,
            attempts,
            last_error: err,
        };
        SessionView { record: rec, live: Some(live) }
    }

    pub fn brief(&self) -> LiveBrief {
        let v = self.view();
        let (r, l) = (v.record, v.live.unwrap_or_default());
        LiveBrief {
            session_id: r.id,
            label: r.label,
            profile: r.profile,
            transport: r.transport,
            preview: r.preview,
            record: r.record,
            state: l.state,
            delay_level: l.delay_level,
            delay_3s_ms: l.delay_3s_ms,
            fps: l.fps,
            kbps: l.kbps,
            avg_fps: r.stats.avg_fps,
            camera_fps: r.stats.camera_fps.or(r.stats.nominal_fps),
            stalls: r.stats.stalls,
            reconnects: r.stats.reconnects,
            uptime_pct: r.stats.uptime_pct,
            missing_pct: match (r.stats.missing_frames, r.stats.expected_frames) {
                (Some(m), Some(e)) if e > 0 => Some(m as f64 / e as f64 * 100.0),
                _ => None,
            },
            elapsed_s: l.elapsed_s,
            since_last_frame_ms: l.since_last_frame_ms,
            recorded_bytes: r.recorded_bytes,
            codec: r.stats.codec,
            width: r.stats.width,
            height: r.stats.height,
            // 화면에 보이는 문구라 알아볼 수 있는 말로 바꾼다 (세션 기록에는 원문이 남는다)
            last_error: l.last_error.map(|e| crate::diagnose::friendly_error(&e)),
            group_id: r.group_id,
        }
    }
}

fn mask_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut u) => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        Err(_) => url.to_string(),
    }
}

/// 폴더 이름에 못 쓰는 문자를 뺀다
fn safe_name(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| if "<>:\"/\\|?*".contains(c) || c.is_control() { '_' } else if c.is_whitespace() || c == '·' { '-' } else { c })
        .collect();
    cleaned.trim().trim_matches('.').chars().take(40).collect()
}

pub fn path_label(kind: Option<&str>) -> &'static str {
    match kind {
        Some("wifi") => "Wi-Fi",
        Some("ethernet") => "유선",
        Some(_) => "기타",
        None => "경로 미상",
    }
}

/// 세션 시작
pub fn start(app: &Arc<App>, req: StartRequest) -> Result<Arc<Session>, String> {
    let cam = app.camera(&req.camera_id).ok_or_else(|| format!("등록되지 않은 카메라: {}", req.camera_id))?;
    let ffmpeg = app.ffmpeg.clone().ok_or("FFmpeg를 찾을 수 없습니다. 설치 폴더의 ffmpeg\\ffmpeg.exe를 확인하세요")?;
    let profile = if req.profile == "sub" { "sub" } else { "main" };
    let transport = if req.transport == "udp" { "udp" } else { "tcp" };
    let url = cam.cfg.rtsp_url(profile).ok_or_else(|| {
        format!("{} 스트림 주소가 없습니다. 설정 → 카메라에서 [연결 테스트]를 해 주세요", if profile == "sub" { "서브" } else { "메인" })
    })?;
    if app.session_for_camera(&cam.cfg.id).is_some() {
        return Err("이 카메라는 이미 수신 중입니다".into());
    }

    let now = Local::now();
    let id = format!("{}-{}", now.format("%Y%m%d-%H%M%S"), cam.cfg.id);
    let (path_kind, path_name) = netmon::route_to(&cam.cfg.ip()).unwrap_or((None, None));
    let label = if req.label.trim().is_empty() {
        format!("{} · {} · {}", path_label(path_kind.as_deref()), if profile == "sub" { "서브" } else { "메인" }, transport.to_uppercase())
    } else {
        req.label.trim().to_string()
    };
    let rec_dir = if req.record {
        // 저장 공간이 이미 기준 아래면 시작하자마자 멈추게 되므로 미리 알린다
        let min = app.cfg.storage.min_free_gb.max(0.5);
        if let Some(f) = store::free_space(&app.store.recordings_dir()).filter(|f| (*f as f64) < min * 1e9) {
            return Err(format!("녹화 폴더의 남은 공간이 {:.1}GB라 녹화할 수 없습니다 (기준 {min}GB). 공간을 비우거나 [녹화 저장]을 끄고 시작하세요", f as f64 / 1e9));
        }
        // 예: 20261103_155008_cam01_메인_TCP_WiFi[_세션이름]
        let mut name = format!(
            "{}_{}_{}_{}_{}",
            now.format("%Y%m%d_%H%M%S"),
            cam.cfg.id,
            if profile == "sub" { "서브" } else { "메인" },
            transport.to_uppercase(),
            match path_kind.as_deref() {
                Some("wifi") => "WiFi",
                Some("ethernet") => "유선",
                _ => "경로미상",
            }
        );
        if !req.label.trim().is_empty() {
            name.push('_');
            name.push_str(&safe_name(req.label.trim()));
        }
        let dir = app.store.recordings_dir().join(now.format("%Y%m%d").to_string()).join(name);
        std::fs::create_dir_all(&dir).map_err(|e| format!("녹화 폴더를 만들 수 없습니다: {} ({e})", dir.display()))?;
        Some(dir)
    } else {
        None
    };

    let rec = SessionRecord {
        id: id.clone(),
        camera_id: cam.cfg.id.clone(),
        camera_name: cam.cfg.name.clone(),
        label: label.clone(),
        profile: profile.into(),
        transport: transport.into(),
        url: mask_url(&url),
        status: "running".into(),
        started_at: Some(now),
        record: req.record,
        preview: req.preview.clone(),
        recording_dir: rec_dir.as_ref().map(|d| d.display().to_string()),
        net: NetSummary { path_kind: path_kind.clone(), path_name: path_name.clone(), ..Default::default() },
        group_id: req.group_id.clone(),
        location: app.location(),
        ..Default::default()
    };
    app.store.save_session(&rec);
    app.store.record_event(
        Event::new(
            "session_start",
            Some(&cam.cfg.id),
            format!(
                "수신 시작: {label} ({}, {} · {})",
                path_label(path_kind.as_deref()),
                if profile == "sub" { "서브" } else { "메인" },
                transport.to_uppercase()
            ),
        )
        .session(&id),
    );

    let (tx, _rx) = watch::channel(None);
    let started = Instant::now();
    let session = Arc::new(Session {
        id: id.clone(),
        camera_id: cam.cfg.id.clone(),
        rec: Mutex::new(rec),
        metrics: Mutex::new(Metrics::new(&app.cfg.stream, started)),
        preview: tx,
        state: Mutex::new(("connecting".into(), 0, None)),
        started,
        rec_dir,
        live_url: Mutex::new(url.clone()),
        stop: Notify::new(),
        kill: Notify::new(),
        kill_requested: AtomicBool::new(false),
        restart: Notify::new(),
        restart_requested: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        e2e: Mutex::new(E2e::default()),
        e2e_capable: req.preview == "smooth",
        ts: Mutex::new(Default::default()),
        capture_lock: tokio::sync::Mutex::new(()),
    });
    app.sessions.lock().unwrap().insert(cam.cfg.id.clone(), session.clone());

    let job = Job { url, transport: transport.into(), preview: req.preview.clone(), ffmpeg };
    tokio::spawn(run(app.clone(), session.clone(), job));
    Ok(session)
}

/// 모든 세션을 멈추고 마무리될 때까지 기다린다 (프로그램 종료 시)
pub async fn stop_all(app: &Arc<App>, timeout: Duration) {
    for s in app.active_sessions() {
        s.request_stop();
    }
    let deadline = Instant::now() + timeout;
    while !app.active_sessions().is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Job {
    url: String,
    transport: String,
    preview: String,
    ffmpeg: PathBuf,
}

async fn run(app: Arc<App>, s: Arc<Session>, mut job: Job) {
    let ticker = tokio::spawn(tick_loop(app.clone(), s.clone()));
    let diag = tokio::spawn(crate::stalldiag::watch(app.clone(), s.clone()));
    let watcher = tokio::spawn(watch_loop(app.clone(), s.clone()));
    let mut attempts_since_ok = 0u32;
    let mut attempt = 0u32;
    // 카메라가 응답하고 있는데도(RTSP 포트가 열려 있는데도) 영상을 못 받은 시도가 이어진 횟수.
    // 카메라가 아예 응답하지 않던 시도(켜지는 중·공유기 재시작 중)는 세지 않는다 — 그건 기다리면 되는 일이다
    let mut refused = 0u32;
    // 녹화 파일 번호: 실제로 녹화 파일이 생긴 연결만 번호를 쓴다 (실패한 시도는 건너뛰지 않게)
    let mut rec_no = 1u32;
    let mut video_only = app.cfg.stream.video_only;
    loop {
        if s.is_stopping() || app.is_stopping() {
            break;
        }
        // 카메라 주소 따라가기: 붙기 전에 등록된 주소에 이 카메라가 있는지 본다 (공유기를 껐다 켜면 IP가 바뀐다).
        // 주소가 멀쩡하면 바로 끝나고, 응답이 없을 때만 같은 카메라를 찾는다.
        // 방금 전까지 받던 영상이 잠깐 끊긴 것(Wi-Fi 끊김)이면 주소가 바뀌었을 리 없으므로 건너뛰고 바로 다시 붙는다
        let brief_gap = matches!(s.frame_gap(), (Some(gap), true, _) if gap < FOLLOW_AFTER);
        if !brief_gap {
            tokio::select! {
                _ = crate::camfix::follow(&app, &s.camera_id) => {}
                _ = s.stop.notified() => {}
            }
        }
        if s.is_stopping() || app.is_stopping() {
            break;
        }
        if let Some(url) = s.wanted_url(&app).filter(|u| *u != job.url) {
            tracing::info!(session = %s.id, "카메라 주소가 바뀌어 새 주소로 연결합니다: {}", mask_url(&url));
            app.store.record_event(Event::new("camera_moved", Some(&s.camera_id), format!("수신을 새 주소로 옮깁니다: {}", mask_url(&url))).session(&s.id));
            s.set_url(&url);
            job.url = url;
        }
        attempt += 1;
        attempts_since_ok += 1;
        {
            // metrics → state 순서로만 잠근다 (view()와 순서가 반대면 서로 기다리며 멈출 수 있음)
            let ever = s.metrics.lock().unwrap().ever_connected;
            let mut st = s.state.lock().unwrap();
            st.0 = if ever { "reconnecting".into() } else { "connecting".into() };
            st.1 = attempt;
        }
        if let Some(dir) = &s.rec_dir {
            // 이미 녹화 파일이 있는 번호는 건너뛴다 — 같은 번호로 다시 쓰면 앞 연결의 녹화를 덮어쓴다
            rec_no = next_rec_no(dir, rec_no);
        }
        // 처음 붙기 전: 이 시도를 시작할 때 카메라가 응답하고 있었는지 (시도가 끝난 뒤에 보면, 그 사이 막 켜진 카메라를
        // '응답하는데 영상이 안 열린다'로 잘못 본다)
        let reachable_before = s.metrics.lock().unwrap().ever_connected || rtsp_port_open(&job.url).await;
        let out = run_ffmpeg_once(&app, &s, &job, rec_no, video_only).await;
        let user_stop = s.is_stopping() || app.is_stopping();
        s.metrics.lock().unwrap().end_run(Instant::now(), user_stop);
        if s.is_stopping() || app.is_stopping() {
            break;
        }
        refused = if !out.got_frames && reachable_before { refused + 1 } else { 0 };
        // 다시 붙을 때까지(카메라가 안 보이면 오래 걸린다) 화면에 '다시 연결하는 중'으로 보이게 한다
        if s.metrics.lock().unwrap().ever_connected {
            s.set_state("reconnecting");
        } else if video_only && !out.got_frames && refused >= 2 {
            // 영상 트랙만 요청하면 받아 주지 않는 카메라일 수 있다 → 남은 시도는 예전처럼 모든 트랙으로
            tracing::warn!(session = %s.id, "영상 트랙만 받기로 두 번 연결되지 않아 모든 트랙으로 다시 시도합니다");
            video_only = false;
        }
        if out.got_frames {
            attempts_since_ok = 1;
            s.metrics.lock().unwrap().reconnects += 1;
            app.store.record_event(
                Event::new("stream_lost", Some(&s.camera_id), format!("영상 수신이 끊겨 다시 연결합니다 ({})", out.reason))
                    .session(&s.id),
            );
        } else if !s.metrics.lock().unwrap().ever_connected && refused >= MAX_INITIAL_ATTEMPTS {
            // 카메라는 응답하는데 영상이 안 열린다(계정·주소가 틀림 등) → 더 해 봐도 같으므로 그만둔다.
            // 카메라가 아예 응답하지 않을 때는(아직 켜지는 중, 공유기 재시작 중) 그만두지 않고 나타날 때까지 계속 시도한다
            let reason = udp_hint(out.reason.clone(), &job.transport);
            s.set_error(reason.clone());
            app.store.record_event(Event::new("session_failed", Some(&s.camera_id), format!("카메라 영상에 연결하지 못했습니다: {reason}")).session(&s.id));
            break;
        }
        s.set_error(out.reason);
        // 다시 연결 (멈추라는 요청이 오면 바로 빠진다).
        let ever = s.metrics.lock().unwrap().ever_connected;
        // 한 번이라도 받았던 카메라는 녹화 공백을 줄이려고, 정해진 시간을 쉬는 대신 카메라 포트가 다시 열리는 순간 붙는다
        // (Wi-Fi가 몇 초 끊겼다 돌아오는 경우). 포트는 열려 있는데 연결이 거듭 실패하면 1·2·3초씩 쉰다.
        let wait = if !ever {
            Duration::from_secs((2 * attempts_since_ok as u64).min(10))
        } else {
            Duration::from_secs((attempts_since_ok.saturating_sub(1) as u64).min(3))
        };
        if !wait.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = s.stop.notified() => {}
            }
        }
        if ever {
            wait_reachable(&app, &s, &job.url).await;
        }
    }
    ticker.abort();
    watcher.abort();
    diag.abort();
    finalize(&app, &s).await;
}

/// 녹화 폴더에서 아직 쓰지 않은 연결 회차 번호: `from`부터, `runNN_…` 파일이 없는 가장 작은 번호.
/// 파일 이름의 앞부분(`runNN_`)만 본다 — 순번 자릿수(`_000` → `_00000`)에 기대면, 자릿수를 바꿨을 때
/// 있는 파일을 못 보고 같은 번호로 덮어쓴다 (v0.5.4~0.6.1: 다시 연결될 때마다 `run01_00000.mp4`를 덮어씀).
fn next_rec_no(dir: &Path, from: u32) -> u32 {
    let used: std::collections::HashSet<u32> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().to_ascii_lowercase();
                    let (no, _) = name.strip_prefix("run")?.split_once('_')?;
                    name.ends_with(".mp4").then(|| no.parse::<u32>().ok()).flatten()
                })
                .collect()
        })
        .unwrap_or_default();
    (from.max(1)..).find(|n| !used.contains(n)).unwrap_or(from)
}

/// 로그·화면에 나가는 문구에서 주소 속 계정(`rtsp://아이디:비밀번호@…`)을 가린다
fn mask_credentials(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("://") {
        let (head, tail) = rest.split_at(i + 3);
        out.push_str(head);
        // 주소의 호스트 부분 끝('/'·공백) 전에 '@'가 있으면 그 앞이 계정
        let end = tail.find(|c: char| c == '/' || c.is_whitespace()).unwrap_or(tail.len());
        match tail[..end].rfind('@') {
            Some(at) => {
                out.push_str("***@");
                rest = &tail[at + 1..];
            }
            None => rest = tail,
        }
    }
    out.push_str(rest);
    out
}

/// UDP로 받으려는데 카메라는 응답하고 영상만 한 번도 안 온 경우: PC 방화벽이 UDP 영상을 막는 일이 흔하다 → TCP를 권한다
/// (계정·주소 문제로 실패한 것이면 그 안내가 먼저이므로 덧붙이지 않는다)
fn udp_hint(reason: String, transport: &str) -> String {
    let low = reason.to_lowercase();
    if transport == "udp" && !["인증", "비밀번호", "401", "unauthorized", "404", "not found", "주소가 없"].iter().any(|k| low.contains(k)) {
        format!("{reason} — UDP로는 영상이 오지 않습니다. PC 방화벽이 막는 경우가 많으니 전송 방식을 TCP로 바꿔 보세요")
    } else {
        reason
    }
}

/// 카메라 RTSP 포트에 지금 연결되는지 (주소를 읽을 수 없으면 true — 판단을 바꾸지 않는다)
async fn rtsp_port_open(url: &str) -> bool {
    let Some((host, port)) = reqwest::Url::parse(url).ok().and_then(|u| Some((u.host_str()?.to_string(), u.port().unwrap_or(554)))) else {
        return true;
    };
    matches!(tokio::time::timeout(Duration::from_millis(1500), tokio::net::TcpStream::connect((host.as_str(), port))).await, Ok(Ok(_)))
}

/// 카메라 RTSP 포트가 열릴 때까지 짧게 반복해서 확인한다. 열리면(또는 멈춤 요청·시간 초과면) 돌아온다.
async fn wait_reachable(app: &App, s: &Session, url: &str) {
    let Some((host, port)) = reqwest::Url::parse(url).ok().and_then(|u| Some((u.host_str()?.to_string(), u.port().unwrap_or(554)))) else {
        return;
    };
    let deadline = Instant::now() + REACH_WAIT_MAX;
    // (카메라 주소가 바뀌었으면 옛 주소를 기다릴 이유가 없다)
    while !s.is_stopping() && !app.is_stopping() && Instant::now() < deadline && !s.address_changed(app) {
        if matches!(tokio::time::timeout(REACH_PROBE_TIMEOUT, tokio::net::TcpStream::connect((host.as_str(), port))).await, Ok(Ok(_))) {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(REACH_PROBE_EVERY) => {}
            _ = s.stop.notified() => return,
        }
    }
}

struct RunOutcome {
    got_frames: bool,
    reason: String,
}

/// FFmpeg의 소켓 읽기 제한 시간. 굶으면 새로 연결하는 기능이 켜져 있으면 짧게 잡는다.
/// 끝내라는 'q'는 읽기가 돌아와야(데이터가 오거나 이 시간이 지나야) 처리되므로, 이 값이 곧
/// '녹화 파일을 정상으로 닫고 끝나는 데 걸리는 시간'의 상한이다. 강제 종료하면 파일 끝 1~2초가 잘린다.
/// (FFmpeg 스스로는 이 시간만큼 아무것도 안 오는 일이 두 번 이어져야 끝낸다)
fn io_timeout(cfg: &StreamConfig) -> Duration {
    if cfg.stall_reconnect_ms == 0 {
        Duration::from_secs(5)
    } else {
        Duration::from_millis((cfg.stall_reconnect_ms * 2 / 3).clamp(1500, 5000))
    }
}

fn build_args(cfg: &StreamConfig, job: &Job, rec_dir: Option<&Path>, run_no: u32, metrics_port: u16, luma_port: Option<u16>, ts_port: Option<u16>, video_only: bool) -> Vec<String> {
    let mut a: Vec<String> = ["-hide_banner", "-nostats", "-loglevel", "level+info"].iter().map(|s| s.to_string()).collect();
    a.extend(["-rtsp_transport", &job.transport, "-timeout", &io_timeout(cfg).as_micros().to_string(), "-fflags", "+genpts"].iter().map(|s| s.to_string()));
    if video_only {
        // 영상 트랙만 받는다. 음성·메타데이터 트랙이 같이 있으면 FFmpeg가 카메라의 RTCP 시각으로 영상 시각을 다시 맞추는데,
        // 끊겼다 이어질 때 그 값이 틀려 영상 시각이 몇 초씩 앞으로 튄다
        a.extend(["-allowed_media_types".into(), "video".into()]);
    }
    if job.preview != "off" && cfg.hwaccel {
        a.extend(["-hwaccel".into(), "d3d11va".into()]);
    }
    if job.preview == "light" {
        // 키프레임만 풀어서 미리보기 (CPU 절약)
        a.extend(["-skip_frame".into(), "nokey".into()]);
    }
    a.extend(["-i".into(), job.url.clone()]);
    // 출력마다 대기열을 넉넉히 둔다. FFmpeg는 출력 하나가 막히면 기본 8장만 쌓고 입력(카메라) 읽기를 멈춘다 —
    // 그러면 카메라가 그동안의 프레임을 버려서, 막힌 출력과 상관없는 나머지 출력까지 모두 끊긴다:
    //  · 녹화 파일을 닫고 새로 여는 일이 느린 PC(백신·문서보안 프로그램, 느린 디스크)에서는 파일이 바뀔 때마다(기본 1분) 수신 전체가 멈추고,
    //  · 엔진이 잠깐 멈칫해(PC 과부하·절전) 프레임 기록·화면용 영상을 못 읽으면 녹화 파일에 그만큼 구멍이 난다
    //    (엔진만 4초 멈추게 해 본 시험에서 녹화 2.7초가 빠졌다).
    // (출력 쪽 thread_queue_size는 FFmpeg 5.1부터. 확실히 있는 7.1 이상에서만 넣는다 — 없는 버전은 이 옵션에서 실행이 실패한다)
    let queue: Vec<String> = if MP4_HYBRID.get().copied().unwrap_or(false) { vec!["-thread_queue_size".into(), OUT_QUEUE_PACKETS.to_string()] } else { Vec::new() };
    if let Some(dir) = rec_dir {
        a.extend(["-map", "0:v:0", "-c", "copy"].iter().map(|s| s.to_string()));
        a.extend(queue.iter().cloned());
        a.extend(
            [
                "-f", "segment",
                "-segment_time", &(cfg.segment_minutes.clamp(1, crate::config::SEGMENT_MINUTES_MAX) * 60).to_string(),
                "-reset_timestamps", "1", "-segment_format", "mp4",
                // 쓰는 동안은 조각(fragment) MP4라 갑자기 꺼져도 마지막 키프레임 전까지 남고,
                // 파일이 정상으로 닫히면 그 자리에서 일반 MP4로 바뀌어 Windows 탐색기·플레이어에 길이가 나온다.
                // (hybrid_fragmented는 FFmpeg 7.1부터. 없으면 예전 방식: 조각 MP4 그대로 — 길이 표시 안 됨)
                "-segment_format_options",
                if MP4_HYBRID.get().copied().unwrap_or(false) {
                    "movflags=+hybrid_fragmented+frag_keyframe"
                } else {
                    "movflags=+frag_keyframe+empty_moov+default_base_moof"
                },
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        // 파일 이름: 연결 회차 + 순번 (run01_00000.mp4 …). 시각 템플릿(strftime)은
        // 경로에 한글이 있으면 Windows에서 이름을 못 만들어 쓰지 않는다. 경로의 '%'는 이스케이프.
        // 순번은 5자리: 1분 단위면 3자리(999)는 17시간 만에 넘쳐 이름순 정렬이 뒤섞인다.
        let dir_s = dir.display().to_string().replace('%', "%%");
        a.push(format!("{dir_s}{}run{run_no:02}_%05d.mp4", std::path::MAIN_SEPARATOR));
    }
    a.extend(["-map", "0:v:0", "-c", "copy"].iter().map(|s| s.to_string()));
    a.extend(queue.iter().cloned());
    a.extend(["-f", "framecrc", "-flush_packets", "1"].iter().map(|s| s.to_string()));
    a.push(format!("tcp://127.0.0.1:{metrics_port}"));
    if job.preview != "off" {
        let vf = if job.preview == "light" {
            format!("scale={}:-2", cfg.preview_width)
        } else {
            format!("fps={},scale={}:-2", cfg.preview_fps.max(1), cfg.preview_width)
        };
        a.extend(["-map", "0:v:0", "-vf", &vf, "-c:v", "mjpeg", "-q:v", "6", "-f", "image2pipe", "pipe:1"].iter().map(|s| s.to_string()));
    }
    // 실제 지연 측정용: 모든 프레임을 아주 작은 흑백으로 (이미 디코딩한 프레임을 쓰므로 부담이 적다)
    if let Some(port) = luma_port {
        a.extend(
            [
                "-map", "0:v:0", "-vf", &format!("scale={LUMA_W}:{LUMA_H}:flags=area,format=gray"),
                "-fps_mode", "passthrough", "-f", "rawvideo", "-flush_packets", "1",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        a.push(format!("tcp://127.0.0.1:{port}"));
    }
    // 프레임 촬영·화면(뷰어)용: 원본 그대로(재인코딩 없음) MPEG-TS.
    // omit_video_pes_length 0: 프레임 길이를 적어 준다(64KB 미만 프레임). 길이가 없으면 받는 쪽 FFmpeg가 다음 프레임이
    // 시작돼야 앞 프레임이 끝난 줄 알아서, 화면이 프레임 한 장만큼 더 늦는다 (60fps에서 측정: 31ms → 16ms)
    if let Some(port) = ts_port {
        a.extend(["-map", "0:v:0", "-c", "copy"].iter().map(|s| s.to_string()));
        a.extend(queue.iter().cloned());
        a.extend(["-f", "mpegts", "-omit_video_pes_length", "0", "-flush_packets", "1"].iter().map(|s| s.to_string()));
        a.push(format!("tcp://127.0.0.1:{port}"));
    }
    a
}

async fn run_ffmpeg_once(app: &Arc<App>, s: &Arc<Session>, job: &Job, run_no: u32, video_only: bool) -> RunOutcome {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(e) => return RunOutcome { got_frames: false, reason: format!("내부 포트를 열 수 없습니다: {e}") },
    };
    let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
    let luma_listener = if s.e2e_capable { TcpListener::bind("127.0.0.1:0").await.ok() } else { None };
    let luma_port = luma_listener.as_ref().and_then(|l| l.local_addr().ok()).map(|a| a.port());
    let ts_listener = TcpListener::bind("127.0.0.1:0").await.ok();
    let ts_port = ts_listener.as_ref().and_then(|l| l.local_addr().ok()).map(|a| a.port());
    s.ts_buffer().reset();
    // 녹화 파일 길이는 실행 중에 설정 화면에서 바꿀 수 있다
    let cfg = StreamConfig { segment_minutes: app.segment_minutes.load(Ordering::Relaxed), ..app.cfg.stream.clone() };
    let args = build_args(&cfg, job, s.rec_dir.as_deref(), run_no, port, luma_port, ts_port, video_only);
    let mut cmd = Command::new(&job.ffmpeg);
    cmd.args(&args)
        .stdin(Stdio::piped())
        .stdout(if job.preview == "off" { Stdio::null() } else { Stdio::piped() })
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000); // 콘솔 창 띄우지 않기
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return RunOutcome { got_frames: false, reason: format!("FFmpeg 실행 실패: {e}") },
    };
    let run_start = Instant::now();
    // 연결마다 시작·끝을 프로그램 로그에 남긴다 (나중에 끊긴 시각과 맞춰 보려고)
    tracing::info!(session = %s.id, "FFmpeg 시작: {} · 녹화 run{run_no:02} · {} · 영상 트랙만={video_only} · pid {}", s.camera_id, job.transport, child.id().unwrap_or(0));
    s.metrics.lock().unwrap().begin_run(run_start);
    s.kill_requested.store(false, Ordering::SeqCst);
    s.restart_requested.store(false, Ordering::SeqCst);

    let err_task = child.stderr.take().map(|e| tokio::spawn(read_stderr(e, app.clone(), s.clone(), run_start)));
    let prev_task = child.stdout.take().map(|o| tokio::spawn(read_preview(o, s.clone())));
    let mut stdin = child.stdin.take();
    let mut frames_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut luma_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut ts_task: Option<tokio::task::JoinHandle<()>> = None;
    // 끝난 accept를 다시 기다리지 않도록 결과와 상관없이 한 번만 받는다
    let (mut frames_accepted, mut luma_accepted, mut ts_accepted) = (false, luma_listener.is_none(), ts_listener.is_none());
    let ts_accept = async {
        match &ts_listener {
            Some(l) => l.accept().await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(ts_accept);
    let accept = listener.accept();
    tokio::pin!(accept);
    let luma_accept = async {
        match &luma_listener {
            Some(l) => l.accept().await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(luma_accept);

    let exit = loop {
        tokio::select! {
            r = &mut accept, if !frames_accepted => {
                frames_accepted = true;
                if let Ok((sock, _)) = r {
                    frames_task = Some(tokio::spawn(read_frames(sock, app.clone(), s.clone())));
                }
            }
            r = &mut luma_accept, if !luma_accepted => {
                luma_accepted = true;
                if let Ok((sock, _)) = r {
                    luma_task = Some(tokio::spawn(read_luma(sock, s.clone())));
                }
            }
            r = &mut ts_accept, if !ts_accepted => {
                ts_accepted = true;
                if let Ok((sock, _)) = r {
                    ts_task = Some(tokio::spawn(crate::capture::read_ts(sock, app.clone(), s.clone())));
                }
            }
            st = child.wait() => break st.ok().and_then(|x| x.code()),
            _ = s.stop.notified() => {
                // 'q'를 보내면 FFmpeg가 녹화 파일을 정상적으로 마무리하고 끝난다
                if let Some(mut i) = stdin.take() {
                    let _ = i.write_all(b"q").await;
                    let _ = i.flush().await;
                }
                match tokio::time::timeout(Duration::from_secs(6), child.wait()).await {
                    Ok(st) => break st.ok().and_then(|x| x.code()),
                    Err(_) => {
                        let _ = child.kill().await;
                        break None;
                    }
                }
            }
            _ = s.kill.notified() => {
                if s.kill_requested.swap(false, Ordering::SeqCst) {
                    let _ = child.kill().await;
                    break None;
                }
            }
            _ = s.restart.notified() => {
                if s.restart_requested.swap(false, Ordering::SeqCst) {
                    // 영상이 안 오는 중이라 잃을 것이 없으므로, 녹화 파일을 정상으로 닫을 시간을 잠깐 주고 끝낸다
                    if let Some(mut i) = stdin.take() {
                        let _ = i.write_all(b"q").await;
                        let _ = i.flush().await;
                    }
                    match tokio::time::timeout(io_timeout(&app.cfg.stream) + RESTART_MARGIN, child.wait()).await {
                        Ok(st) => break st.ok().and_then(|x| x.code()),
                        Err(_) => {
                            let _ = child.kill().await;
                            break None;
                        }
                    }
                }
            }
        }
    };
    // 파이프가 닫히며 읽기 작업들이 끝난다 (안 끝나면 2초 뒤 그냥 둔다)
    for t in [frames_task, luma_task, ts_task, err_task, prev_task].into_iter().flatten() {
        let _ = tokio::time::timeout(Duration::from_secs(2), t).await;
    }
    let run_frames = s.metrics.lock().unwrap().run_frames;
    let got_frames = run_frames > 0;
    let reason = s
        .state
        .lock()
        .unwrap()
        .2
        .clone()
        .unwrap_or_else(|| match exit {
            Some(0) => "카메라가 스트림을 끝냄".into(),
            Some(c) => format!("FFmpeg 종료 코드 {c}"),
            None => "응답 없음으로 연결을 끊음".into(),
        });
    let code = exit.map_or("없음(강제 종료)".to_string(), |c| c.to_string());
    tracing::info!(session = %s.id, "FFmpeg 종료: 종료 코드 {code} · {:.1}초 동안 프레임 {run_frames}개 · {reason}", run_start.elapsed().as_secs_f64());
    RunOutcome { got_frames, reason }
}

/// FFmpeg 경고(패킷 손실·디코딩 오류 등)를 프로그램 로그에 시각과 함께 남긴다.
/// 손실이 이어질 때는 초당 수십 줄씩 나오므로 10초에 20줄까지만 남기고 나머지는 줄 수만 적는다.
struct WarnLog {
    window: Instant,
    lines: u32,
    skipped: u32,
}

impl WarnLog {
    const PER_WINDOW: u32 = 20;
    const WINDOW: Duration = Duration::from_secs(10);

    fn new() -> Self {
        Self { window: Instant::now(), lines: 0, skipped: 0 }
    }

    fn line(&mut self, session: &str, l: &str) {
        if self.window.elapsed() >= Self::WINDOW {
            self.flush(session);
            (self.window, self.lines) = (Instant::now(), 0);
        }
        if self.lines < Self::PER_WINDOW {
            self.lines += 1;
            tracing::info!(session = %session, "ffmpeg: {l}");
        } else {
            self.skipped += 1;
        }
    }

    fn flush(&mut self, session: &str) {
        if self.skipped > 0 {
            tracing::info!(session = %session, "ffmpeg: … 같은 때의 경고 {}줄은 적지 않음", self.skipped);
            self.skipped = 0;
        }
    }
}

/// framecrc 한 줄 = 패킷(프레임) 하나: "0, dts, pts, duration, size, crc"
async fn read_frames(sock: tokio::net::TcpStream, app: Arc<App>, s: Arc<Session>) {
    let mut lines = BufReader::new(sock).lines();
    let mut tb = 1.0 / 90_000.0;
    while let Ok(Some(line)) = lines.next_line().await {
        let now = Instant::now();
        if let Some(rest) = line.strip_prefix("#tb ") {
            // "#tb 0: 1/90000"
            if let Some((num, den)) = rest.split(':').nth(1).and_then(|r| r.trim().split_once('/')) {
                if let (Ok(n), Ok(d)) = (num.trim().parse::<f64>(), den.trim().parse::<f64>()) {
                    if d > 0.0 {
                        tb = n / d;
                    }
                }
            }
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        if f.len() < 5 {
            continue;
        }
        let pts = f[2].parse::<i64>().ok().filter(|p| *p > -1_000_000_000_000).map(|p| p as f64 * tb);
        let size = f[4].parse::<u64>().unwrap_or(0);
        let note = s.metrics.lock().unwrap().on_frame(now, pts, size);
        if note.first_frame {
            s.set_state("receiving");
            s.state.lock().unwrap().2 = None;
        }
        if let Some(down) = note.recovered_after_ms {
            app.store.record_event(
                Event::new("stream_recovered", Some(&s.camera_id), format!("영상 수신 재개 (끊긴 시간 {:.1}초)", down / 1000.0))
                    .session(&s.id)
                    .with_data(serde_json::json!({ "down_ms": down })),
            );
        }
        if let Some(gap) = note.stall_ms {
            app.store.record_event(
                Event::new("stream_stall", Some(&s.camera_id), format!("영상 끊김 {:.2}초", gap / 1000.0))
                    .session(&s.id)
                    .with_data(serde_json::json!({ "gap_ms": gap })),
            );
        }
        if let Some(jump) = note.ts_jump_s {
            app.store.record_event(
                Event::new(
                    "ts_glitch",
                    Some(&s.camera_id),
                    format!("카메라가 보낸 영상 시각이 {jump:.1}초 앞으로 튐 (영상은 정상 수신 · 지연 계산에서 제외 · 녹화의 이 구간 시각이 틀어짐)"),
                )
                .session(&s.id)
                .with_data(serde_json::json!({ "jump_s": jump })),
            );
        }
    }
}

/// 실제 지연 측정용 흑백 프레임: 도착 시각(PC 시계)과 평균 밝기
async fn read_luma(mut sock: tokio::net::TcpStream, s: Arc<Session>) {
    let mut buf = vec![0u8; LUMA_W * LUMA_H];
    while sock.read_exact(&mut buf).await.is_ok() {
        let t = e2e::now_ms();
        let lum = buf.iter().map(|&v| v as u64).sum::<u64>() as f32 / buf.len() as f32;
        s.e2e.lock().unwrap().on_frame(t, lum);
    }
}

/// 미리보기: stdout으로 이어서 나오는 JPEG들을 잘라 최신 한 장만 들고 있는다
async fn read_preview(mut out: impl AsyncRead + Unpin, s: Arc<Session>) {
    let mut buf = BytesMut::with_capacity(512 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        let n = match out.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        buf.extend_from_slice(&chunk[..n]);
        loop {
            let Some(start) = find(&buf, &[0xFF, 0xD8], 0) else {
                buf.clear();
                break;
            };
            let Some(end) = find(&buf, &[0xFF, 0xD9], start + 2) else {
                if start > 0 {
                    let _ = buf.split_to(start);
                }
                break;
            };
            let _ = buf.split_to(start);
            let jpeg = buf.split_to(end - start + 2).freeze();
            // 보는 사람이 없어도 최신 한 장은 남겨 둔다
            s.preview.send_replace(Some(jpeg));
        }
        if buf.len() > 8 * 1024 * 1024 {
            buf.clear(); // 이상한 데이터가 쌓이지 않게
        }
    }
}

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    hay.get(from..)?.windows(needle.len()).position(|w| w == needle).map(|p| p + from)
}

/// FFmpeg 로그 해석: 스트림 정보, RTP 손실, 디코딩 오류, 오류 메시지
async fn read_stderr(err: impl AsyncRead + Unpin, app: Arc<App>, s: Arc<Session>, run_start: Instant) {
    let mut lines = BufReader::new(err).lines();
    let mut stream_info_seen = false;
    let mut warn_log = WarnLog::new();
    while let Ok(Some(line)) = lines.next_line().await {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        if !stream_info_seen && l.contains("Stream #0:0") && l.contains("Video:") {
            stream_info_seen = true;
            let info = parse_stream_info(l);
            let mut m = s.metrics.lock().unwrap();
            m.codec = info.codec.or(m.codec.take());
            m.width = info.width.or(m.width);
            m.height = info.height.or(m.height);
            m.camera_fps = info.fps.or(m.camera_fps);
            continue;
        }
        if l.contains("Timestamps are unset") {
            continue;
        }
        // 녹화 파일이 바뀐 시각
        if l.contains("Opening '") && l.contains("' for writing") {
            if let Some(name) = l.split('\'').nth(1).and_then(|p| p.rsplit(['\\', '/']).next()) {
                tracing::info!(session = %s.id, "녹화 파일 시작: {name}");
            }
            continue;
        }
        if let Some(n) = l.find("RTP: missed ").and_then(|i| l[i + 12..].split_whitespace().next()?.parse::<u64>().ok()) {
            {
                let mut m = s.metrics.lock().unwrap();
                m.rtp_loss_events += 1;
                m.rtp_lost += n;
            }
            warn_log.line(&s.id, l);
            continue;
        }
        if l.contains("max delay reached") {
            s.metrics.lock().unwrap().rtp_loss_events += 1;
            warn_log.line(&s.id, l);
            continue;
        }
        let is_err = l.contains("[error]") || l.contains("[fatal]");
        let is_warn = l.contains("[warning]");
        let decoder = l.contains("[hevc @") || l.contains("[h264 @");
        if decoder && (is_err || is_warn) {
            if run_start.elapsed() > DECODE_GRACE {
                s.metrics.lock().unwrap().decode_errors += 1;
                warn_log.line(&s.id, l);
            }
            continue;
        }
        if is_err {
            // 주소가 든 오류(Error opening input file rtsp://…)에 카메라 계정이 그대로 찍히지 않게 가린다
            let l = &mask_credentials(l);
            let msg = clean_log(l);
            // 녹화 출력만 열리지 않은 채 수신은 계속되는 경우가 있다 (다시 붙은 직후 영상 정보를 못 얻었을 때) →
            // 그대로 두면 받고는 있는데 녹화가 안 남으므로 새로 연결한다
            if s.rec_dir.is_some() && (msg.contains("Could not write header") || msg.contains("dimensions not set")) && !s.is_stopping() {
                tracing::warn!(session = %s.id, "녹화 파일을 열지 못해 새로 연결합니다: {msg}");
                app.store.record_event(Event::new("rec_open_failed", Some(&s.camera_id), "녹화 파일을 열지 못해 새로 연결합니다 (영상 정보를 얻지 못함)").session(&s.id));
                s.request_restart();
            }
            let friendly = if msg.contains("401") {
                "카메라가 인증을 거부했습니다 (401). 아이디·비밀번호를 확인하세요".to_string()
            } else if msg.contains("timed out") || msg.contains("Connection refused") || msg.contains("Network is unreachable") {
                format!("카메라에 연결할 수 없습니다 ({msg})")
            } else {
                msg
            };
            tracing::warn!(session = %s.id, "ffmpeg: {l}");
            s.set_error(friendly);
        } else if is_warn {
            // 그 밖의 경고 (시각이 어긋남, 순서가 뒤바뀜 등)
            warn_log.line(&s.id, &mask_credentials(l));
        }
    }
    warn_log.flush(&s.id);
    let _ = app; // 향후 경고 이벤트 기록용
}

fn clean_log(l: &str) -> String {
    // "[rtsp @ 0000...] [error] 메시지" → "메시지"
    let mut s = l;
    while let Some(rest) = s.strip_prefix('[').and_then(|r| r.split_once(']')).map(|(_, r)| r.trim_start()) {
        s = rest;
    }
    s.to_string()
}

#[derive(Default, Debug, PartialEq)]
pub struct StreamInfo {
    pub codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub fps: Option<f64>,
}

/// "Stream #0:0: Video: hevc (Main), yuvj420p(pc), 2048x1536 [SAR 1:1 DAR 4:3], 60 fps, 60 tbr, 90k tbn"
pub fn parse_stream_info(l: &str) -> StreamInfo {
    let mut info = StreamInfo::default();
    if let Some(v) = l.split("Video:").nth(1) {
        info.codec = v.split([' ', ',']).find(|x| !x.is_empty()).map(str::to_string);
        for part in v.split(',') {
            let p = part.trim();
            if info.width.is_none() {
                if let Some(tok) = p.split_whitespace().find(|t| t.contains('x') && t.split('x').all(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))) {
                    let (w, h) = tok.split_once('x').unwrap();
                    info.width = w.parse().ok();
                    info.height = h.parse().ok();
                }
            }
            if let Some(f) = p.strip_suffix(" fps") {
                info.fps = f.trim().parse().ok();
            }
        }
    }
    info
}

/// 1초마다 지표 기록, 감시(오래 멈추면 재연결), 녹화 용량 갱신
async fn tick_loop(app: Arc<App>, s: Arc<Session>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut n: u64 = 0;
    loop {
        tick.tick().await;
        n += 1;
        let now = Instant::now();
        let (frames, kbps, iv, delay, stalled, connected, since, run_age, ever) = {
            let mut m = s.metrics.lock().unwrap();
            let (f, k, i, d, st, c) = m.tick(now);
            (f, k, i, d, st, c, m.since_last_frame(now), m.run_start.map(|r| now - r), m.ever_connected)
        };
        // 첫 영상이 오기 전(처음 연결 중)은 '끊김'이 아니므로 기록하지 않는다
        if ever {
            app.store.record_stream(StreamSample {
            ts: Local::now(),
            session_id: s.id.clone(),
            camera_id: s.camera_id.clone(),
            frames,
            kbps,
            interval_max_ms: iv,
                delay_ms: delay,
                stalled,
                connected,
            });
        }
        // 감시: 연결된 뒤 오래 멈췄거나, 연결이 너무 오래 안 되면 FFmpeg를 끊고 다시 시도
        if !s.is_stopping() {
            let stuck = connected && since.map_or(false, |d| d > WATCHDOG);
            let never = !connected && run_age.map_or(false, |a| a > CONNECT_TIMEOUT) && s.metrics.lock().unwrap().run_frames == 0;
            if stuck || never {
                s.set_error(if stuck { format!("{}초 동안 영상이 오지 않음", WATCHDOG.as_secs()) } else { "연결 시간 초과".into() });
                s.request_kill();
            }
            // 카메라 주소가 바뀌었으면(주소 따라가기 — 다른 카메라의 수신이 찾아 준 경우 포함) 지금 연결을 닫고 새 주소로 붙는다
            if s.address_changed(&app) {
                s.request_restart();
            }
        }
        if n % 10 == 0 {
            if let Some(dir) = &s.rec_dir {
                let d = dir.clone();
                let bytes = tokio::task::spawn_blocking(move || store::dir_stats(&d, &["mp4"]).bytes).await.unwrap_or(0);
                s.rec.lock().unwrap().recorded_bytes = bytes;
            }
        }
        // 중간 저장: 노트북이 꺼지거나 프로그램이 강제로 끝나도 여기까지의 측정값이 기록에 남는다
        if n % CHECKPOINT_SECS == 0 && !s.is_stopping() {
            let mut rec = s.view().record;
            rec.saved_at = Some(Local::now());
            app.store.save_session(&rec);
        }
        // 저장 공간이 바닥나기 전에 녹화 중인 수신을 멈춘다 (녹화 파일이 깨지지 않게 정상 종료)
        if n % 10 == 5 && s.rec_dir.is_some() && !s.is_stopping() {
            let dir = app.store.recordings_dir();
            let free = tokio::task::spawn_blocking(move || store::free_space(&dir)).await.ok().flatten();
            let min = app.cfg.storage.min_free_gb.max(0.5);
            if let Some(f) = free.filter(|f| (*f as f64) < min * 1e9) {
                let msg = format!("저장 공간이 {:.1}GB 남아(기준 {min}GB) 녹화를 멈췄습니다. 녹화 파일을 옮기거나 지운 뒤 다시 시작하세요", f as f64 / 1e9);
                s.set_error(msg.clone());
                app.store.record_event(Event::new("disk_full_stop", Some(&s.camera_id), msg).session(&s.id));
                s.request_stop();
            }
        }
    }
}

/// 굶는지 감시: 수신 중인데 제때 온 프레임이 한동안 없으면 기다리지 않고 새로 연결한다.
/// Wi-Fi가 몇 초 끊겼다 돌아올 때 기존 연결은 살아 있어도 영상이 더 늦게 돌아오고, 이어지더라도 영상 시각이 틀어진다.
async fn watch_loop(app: Arc<App>, s: Arc<Session>) {
    let window = Duration::from_millis(app.cfg.stream.stall_reconnect_ms);
    if window.is_zero() {
        return;
    }
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut asked: Option<Instant> = None;
    // 이 프로그램 자신이 멈춰 있었으면(PC 절전·과부하·보안 프로그램 등) 그동안은 프레임을 읽지 못한 것이지 영상이 안 온 것이 아니다.
    // 깨어난 직후에는 밀려 있던(늦은) 프레임만 보여서 굶은 것처럼 보이므로, 제때 온 프레임이 다시 쌓일 시간을 준 뒤에 본다.
    // (안 그러면 PC가 3초 넘게 멈칫할 때마다 멀쩡한 연결을 끊고 새로 맺어 끊김이 더 길어지고 녹화 파일이 나뉜다)
    let (mut prev, mut hold_until) = (Instant::now(), None::<Instant>);
    loop {
        tick.tick().await;
        let now = Instant::now();
        if now - prev >= SELF_STALL {
            hold_until = Some(now + window);
        }
        prev = now;
        // 한 번 요청하면 그 연결이 끝날 때까지(최대 몇 초) 다시 묻지 않는다
        if s.is_stopping() || asked.map_or(false, |t| now - t < Duration::from_secs(5)) || hold_until.is_some_and(|t| now < t) {
            continue;
        }
        if s.metrics.lock().unwrap().starved(now, window) {
            asked = Some(now);
            s.set_error(format!("{:.0}초 동안 영상이 오지 않아 새로 연결", window.as_secs_f64()));
            s.request_restart();
        }
    }
}

/// 영상이 한 프레임도 안 들어간 녹화 파일(머리말만 있는 수십 바이트)을 지운다.
/// 다시 연결을 시도하던 중에 끝난 세션에 남아, 녹화 점검에서 '읽을 수 없는 파일'로 잡힌다.
fn remove_empty_recordings(dir: &Path) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for p in rd.flatten().map(|e| e.path()) {
        let mp4 = p.extension().map_or(false, |e| e.eq_ignore_ascii_case("mp4"));
        if mp4 && std::fs::metadata(&p).map_or(false, |m| m.len() < 1024) {
            let _ = std::fs::remove_file(&p);
        }
    }
}

async fn finalize(app: &Arc<App>, s: &Arc<Session>) {
    // 실제 지연 측정 중에 세션이 끝나면 거기까지의 값으로 마무리
    if s.e2e.lock().unwrap().active {
        if let Some(sum) = s.e2e_stop() {
            record_e2e_event(app, s, &sum);
        }
    }
    let now = Instant::now();
    let ended = Local::now();
    let mut rec = s.rec.lock().unwrap().clone();
    rec.ended_at = Some(ended);
    rec.duration_s = (now - s.started).as_secs_f64();
    let ever = {
        let m = s.metrics.lock().unwrap();
        rec.stats = m.stats(now, true);
        m.ever_connected
    };
    if let Some(dir) = &s.rec_dir {
        let d = dir.clone();
        rec.recorded_bytes = tokio::task::spawn_blocking(move || {
            remove_empty_recordings(&d);
            store::dir_stats(&d, &["mp4"]).bytes
        })
        .await
        .unwrap_or(0);
        // 녹화가 하나도 없으면 빈 폴더를 지운다
        if rec.recorded_bytes == 0 {
            let _ = std::fs::remove_dir(dir);
        }
    }
    let path = (rec.net.path_kind.clone(), rec.net.path_name.clone());
    if let Some(start) = rec.started_at {
        rec.net = app.store.net_summary(&rec.camera_id, start, ended);
    }
    rec.net.path_kind = path.0;
    rec.net.path_name = path.1;
    rec.last_error = s.state.lock().unwrap().2.clone();
    rec.status = if ever { "finished".into() } else { "failed".into() };
    let (verdict, reasons) = judge(&rec);
    rec.verdict = Some(verdict.clone());
    rec.reasons = reasons;
    app.store.save_session(&rec);
    let verdict_ko = match verdict.as_str() {
        "stable" => "안정",
        "warning" => "주의",
        "unstable" => "불안정",
        _ => "판정 불가",
    };
    app.store.record_event(
        Event::new(
            "session_end",
            Some(&rec.camera_id),
            format!("수신 종료: {} · {:.0}분 · 판정 {verdict_ko}", rec.label, rec.duration_s / 60.0),
        )
        .session(&rec.id),
    );
    s.preview.send_replace(None);
    app.sessions.lock().unwrap().remove(&s.camera_id);
    // 녹화 파일 점검은 시간이 걸릴 수 있어 따로 (끝나면 세션 기록에 덧붙인다)
    if rec.recorded_bytes > 0 {
        let a = app.clone();
        tokio::spawn(async move { attach_rec_check(&a, rec, false).await });
    }
}

/// 녹화 파일을 점검해 세션 기록에 붙여 저장한다.
pub async fn attach_rec_check(app: &Arc<App>, mut rec: SessionRecord, interrupted: bool) {
    let (Some(ffmpeg), Some(dir)) = (app.ffmpeg.clone(), rec.recording_dir.clone()) else { return };
    let (received, duration) = (rec.stats.frames, rec.duration_s);
    let check = tokio::task::spawn_blocking(move || check_recording(&ffmpeg, Path::new(&dir), received, duration, interrupted)).await.ok().flatten();
    let Some(check) = check else { return };
    if !check.ok {
        app.store.record_event(Event::new("rec_check_failed", Some(&rec.camera_id), format!("녹화 점검: {}", check.summary)).session(&rec.id));
    }
    // 점검하는 동안 다른 내용이 바뀌었을 수 있으니 최신 기록에 붙인다
    if let Some(latest) = app.store.session(&rec.id) {
        rec = latest;
    }
    rec.rec_check = Some(check);
    app.store.save_session(&rec);
}

/// 녹화 폴더의 MP4를 하나씩 끝까지 읽어(재인코딩 없이) 프레임 수·길이·오류를 센다.
/// `received`: 수신 중 받은 프레임 수, `duration_s`: 수신 시간, `interrupted`: 갑자기 꺼진 세션 (마지막 파일 끝 잘림은 정상)
pub fn check_recording(ffmpeg: &Path, dir: &Path, received: u64, duration_s: f64, interrupted: bool) -> Option<store::RecCheck> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |e| e.eq_ignore_ascii_case("mp4")))
        .collect();
    files.sort();
    if files.is_empty() {
        return None;
    }
    let mut c = store::RecCheck { checked_at: Some(Local::now()), files: files.len() as u32, received_frames: received, ..Default::default() };
    let mut notes = Vec::new();
    // 끝만 잘린 파일 수 (강제 종료된 파일: 조각 MP4라 잘린 곳 앞까지는 그대로 재생된다)
    let mut cut = 0usize;
    let last = files.len() - 1;
    for (i, f) in files.iter().enumerate() {
        let name = f.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
        let mut cmd = std::process::Command::new(ffmpeg);
        cmd.args(["-hide_banner", "-nostdin", "-loglevel", "level+info", "-i"])
            .arg(f)
            .args(["-map", "0:v:0", "-c", "copy", "-f", "null", "-"])
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000);
        }
        let out = match cmd.output() {
            Ok(o) => String::from_utf8_lossy(&o.stderr).to_string(),
            Err(e) => {
                c.bad_files.push(format!("{name}: 점검 실행 실패 ({e})"));
                continue;
            }
        };
        let (frames, secs) = parse_progress(&out);
        c.frames += frames;
        c.seconds += secs;
        let err = out.lines().find(|l| l.contains("[error]") || l.contains("[fatal]")).map(|l| {
            let l = l.split("] ").last().unwrap_or(l).trim();
            l.chars().take(120).collect::<String>()
        });
        match err {
            // 갑자기 꺼진 세션의 마지막 파일은 끝부분(마지막 키프레임 뒤)이 잘린 것이 정상
            Some(_) if interrupted && i == last && frames > 0 => notes.push(format!("마지막 파일({name})은 갑자기 종료되어 끝부분이 잘림")),
            // 프레임이 읽혔으면 파일 전체가 못 쓰게 된 것이 아니라 끝(마지막 키프레임 뒤, 보통 1초 안쪽)만 잘린 것
            Some(e) if frames > 0 => {
                cut += 1;
                c.bad_files.push(format!("{name}: 끝이 잘림 — 앞 {}는 재생됨 ({e})", fmt_dur(secs)));
            }
            Some(e) => c.bad_files.push(format!("{name}: 읽을 수 없음 ({e})")),
            None if frames == 0 => c.bad_files.push(format!("{name}: 프레임이 없음")),
            None => {}
        }
    }
    c.gap_s = (duration_s - c.seconds).max(0.0);
    // 녹화 프레임이 받은 프레임의 99% 이상이면 빠짐없이 녹화된 것으로 본다
    let enough = received == 0 || c.frames + 30 >= received || c.frames as f64 >= received as f64 * 0.99;
    c.ok = c.bad_files.is_empty() && enough;
    let mut s = format!(
        "{} · 파일 {}개 · 녹화 {} (수신 {})",
        if c.ok { "녹화 정상" } else { "녹화 문제" },
        c.files,
        fmt_dur(c.seconds),
        fmt_dur(duration_s)
    );
    if c.gap_s >= 1.0 {
        s.push_str(&format!(" · 녹화 안 된 시간 {} (연결·끊김)", fmt_dur(c.gap_s)));
    }
    if !enough {
        s.push_str(&format!(" · 받은 프레임 {received}개 중 {}개만 녹화됨", c.frames));
    }
    if cut > 0 {
        s.push_str(&format!(" · 끝이 잘린 파일 {cut}개 (강제 종료됨 — 잘린 곳 앞까지는 재생됨)"));
    }
    if c.bad_files.len() > cut {
        s.push_str(&format!(" · 읽을 수 없는 파일 {}개", c.bad_files.len() - cut));
    }
    for n in notes {
        s.push_str(&format!(" · {n}"));
    }
    c.summary = s;
    Some(c)
}

/// FFmpeg 진행 표시의 마지막 "frame= N … time=HH:MM:SS.xx"
fn parse_progress(out: &str) -> (u64, f64) {
    let mut frames = 0u64;
    let mut secs = 0.0f64;
    for seg in out.split(['\r', '\n']) {
        if let Some(p) = seg.find("frame=") {
            if let Some(n) = seg[p + 6..].split_whitespace().next().and_then(|n| n.parse().ok()) {
                frames = n;
            }
        }
        if let Some(p) = seg.find("time=") {
            let t = seg[p + 5..].split_whitespace().next().unwrap_or("");
            let parts: Vec<f64> = t.split(':').filter_map(|x| x.parse().ok()).collect();
            if parts.len() == 3 {
                secs = parts[0] * 3600.0 + parts[1] * 60.0 + parts[2];
            }
        }
    }
    (frames, secs)
}

fn fmt_dur(s: f64) -> String {
    let t = s.round() as u64;
    if t >= 3600 {
        format!("{}시간 {:02}분 {:02}초", t / 3600, t / 60 % 60, t % 60)
    } else if t >= 60 {
        format!("{}분 {:02}초", t / 60, t % 60)
    } else {
        format!("{t}초")
    }
}

/// 지난번 실행이 갑자기 끝나 '진행 중'으로 남았던 세션을 마지막 중간 저장 시점까지로 마무리한다:
/// 판정을 다시 내리고, 녹화 용량과 녹화 파일 점검 결과를 붙인다.
/// 시작하자마자 꺼진 세션은 다시 켜졌을 때 잇지 않는다 (그 수신이 프로그램을 죽이는 경우 켤 때마다 되풀이되지 않게).
const RESUME_MIN_RUN_S: f64 = 30.0;

/// 프로그램이 갑자기 꺼졌다 다시 켜졌을 때: 그때 받고 있던 세션을 정리하고(`finish_interrupted`),
/// 꺼진 지 오래되지 않았으면(`resume_within_minutes`) 같은 조건으로 이어서 받는다.
/// 녹화가 사람이 다시 누를 때까지 비지 않게 하려는 것. 정상 종료([프로그램 종료])로 멈춘 세션은 해당 없다.
pub async fn recover_interrupted(app: Arc<App>) {
    let pending: Vec<SessionRecord> = app.store.sessions(500).into_iter().filter(|s| s.interrupted && s.verdict.is_none()).collect();
    finish_interrupted(app.clone()).await;
    let within = app.cfg.stream.resume_within_minutes as i64;
    if within == 0 {
        return;
    }
    let now = Local::now();
    // 오래된 것부터 (`sessions`는 최근 것부터 준다)
    for rec in pending.into_iter().rev() {
        let last = rec.saved_at.or(rec.started_at);
        let recent = last.is_some_and(|t| (now - t).num_seconds() <= within * 60);
        if !recent || rec.duration_s < RESUME_MIN_RUN_S || app.session_for_camera(&rec.camera_id).is_some() {
            continue;
        }
        let req = StartRequest {
            camera_id: rec.camera_id.clone(),
            profile: rec.profile.clone(),
            transport: rec.transport.clone(),
            label: rec.label.clone(),
            preview: rec.preview.clone(),
            record: rec.record,
            group_id: rec.group_id.clone(),
        };
        match start(&app, req) {
            Ok(s) => {
                let gap = last.map_or(0, |t| (now - t).num_seconds());
                tracing::info!(session = %s.id, "갑자기 꺼져 끊겼던 수신을 이어서 시작 (끊긴 세션 {})", rec.id);
                app.store.record_event(
                    Event::new("session_resumed", Some(&rec.camera_id), format!("프로그램이 갑자기 꺼져 끊겼던 수신을 이어서 시작했습니다: {} (마지막 기록 뒤 약 {gap}초)", rec.label))
                        .session(&s.id)
                        .with_data(serde_json::json!({ "from_session": rec.id, "gap_s": gap })),
                );
            }
            Err(e) => {
                tracing::warn!("끊겼던 수신({})을 잇지 못했습니다: {e}", rec.id);
                app.store.record_event(Event::new("session_failed", Some(&rec.camera_id), format!("끊겼던 수신을 이어서 시작하지 못했습니다: {e}")).session(&rec.id));
            }
        }
    }
}

pub async fn finish_interrupted(app: Arc<App>) {
    let pending: Vec<SessionRecord> = app.store.sessions(500).into_iter().filter(|s| s.interrupted && s.verdict.is_none()).collect();
    for mut rec in pending {
        rec.ended_at = rec.saved_at.or_else(|| rec.started_at.map(|t| t + chrono::Duration::milliseconds((rec.duration_s * 1000.0) as i64)));
        if let Some(dir) = rec.recording_dir.clone() {
            let d = PathBuf::from(dir);
            rec.recorded_bytes = tokio::task::spawn_blocking(move || store::dir_stats(&d, &["mp4"]).bytes).await.unwrap_or(0);
        }
        let (verdict, mut reasons) = judge(&rec);
        if rec.stats.frames > 0 {
            let at = rec.saved_at.map(|t| t.format("%m/%d %H:%M:%S").to_string()).unwrap_or_else(|| "알 수 없음".into());
            reasons.insert(0, format!("프로그램이 갑자기 종료됨 — 마지막 중간 저장({at})까지의 기록으로 판정"));
        }
        rec.verdict = Some(verdict);
        rec.reasons = reasons;
        app.store.save_session(&rec);
        app.store.record_event(
            Event::new("session_recovered", Some(&rec.camera_id), format!("갑자기 끝난 세션을 정리했습니다: {} (녹화 {:.0}MB)", rec.label, rec.recorded_bytes as f64 / 1e6))
                .session(&rec.id),
        );
        if rec.recorded_bytes > 0 {
            attach_rec_check(&app, rec, true).await;
        }
    }
}

pub fn record_e2e_event(app: &App, s: &Session, sum: &E2eSummary) {
    app.store.record_event(
        Event::new(
            "e2e_measured",
            Some(&s.camera_id),
            format!(
                "실제 지연 측정 {}회: 중앙값 {:.0}ms (최소 {:.0} · 최대 {:.0}ms)",
                sum.count, sum.p50_ms, sum.min_ms, sum.max_ms
            ),
        )
        .session(&s.id)
        .with_data(serde_json::to_value(sum).unwrap_or_default()),
    );
}

/// 세션 판정과 그 이유
pub fn judge(rec: &SessionRecord) -> (String, Vec<String>) {
    let st = &rec.stats;
    if rec.status == "failed" || st.frames == 0 {
        return ("failed".into(), vec![rec.last_error.clone().unwrap_or_else(|| "영상을 받지 못했습니다".into())]);
    }
    let mut bad = Vec::new();
    let mut warn = Vec::new();
    if st.reconnects > 0 {
        bad.push(format!("연결이 {}번 끊겨 다시 연결함 (끊긴 시간 {:.1}초)", st.reconnects, st.downtime_ms / 1000.0));
    }
    if let Some(u) = st.uptime_pct {
        if u < 99.0 {
            bad.push(format!("정상 수신 비율 {u:.1}% (99% 미만)"));
        }
    }
    if let (Some(avg), Some(nom)) = (st.avg_fps, st.nominal_fps) {
        if nom > 0.0 && avg < nom * 0.9 {
            bad.push(format!("평균 {avg:.1}fps로 카메라 설정 {nom:.0}fps의 90% 미만"));
        }
    }
    if st.longest_stall_ms >= 2000.0 {
        bad.push(format!("최장 끊김 {:.1}초", st.longest_stall_ms / 1000.0));
    }
    // 카메라 fps를 알 때만 (간격으로 추정한 fps는 오차가 있어 누락으로 단정하지 않는다)
    if let (Some(exp), Some(miss), Some(_)) = (st.expected_frames, st.missing_frames, st.camera_fps) {
        if exp > 0 && miss as f64 >= exp as f64 * 0.01 {
            bad.push(format!("프레임 누락 {miss}개 (와야 할 {exp}개의 {:.1}%)", miss as f64 / exp as f64 * 100.0));
        }
    }
    if st.stalls > 0 && st.longest_stall_ms < 2000.0 {
        warn.push(format!("짧은 끊김 {}회 (최장 {:.2}초)", st.stalls, st.longest_stall_ms / 1000.0));
    }
    if let Some(p99) = st.interval_p99_ms {
        if p99 > 100.0 {
            warn.push(format!("프레임 간격 p99 {p99:.0}ms (100ms 초과)"));
        }
    }
    if let Some(d) = st.delay_max_ms {
        if d > 500.0 {
            warn.push(format!("버퍼 지연 최대 {:.1}초", d / 1000.0));
        }
    }
    if rec.duration_s > 0.0 && st.delayed_secs > 0 {
        let share = st.delayed_secs as f64 / rec.duration_s * 100.0;
        if share >= 1.0 {
            warn.push(format!("지연({:.0}ms 이상) 구간 {}초 · 전체의 {share:.1}%", st.delay_warn_ms, st.delayed_secs));
        }
    }
    if st.rtp_lost_packets > 0 || st.rtp_loss_events > 0 {
        warn.push(format!("패킷 손실 {}건 ({}개)", st.rtp_loss_events, st.rtp_lost_packets));
    }
    if st.decode_errors > 0 {
        warn.push(format!("디코딩 오류 {}건 (화면 깨짐 가능)", st.decode_errors));
    }
    if st.ts_glitch_frames >= 30 {
        warn.push(format!("영상 시각이 튄 프레임 {}개 (녹화에서 그 구간이 건너뛰거나 멈춰 보일 수 있음)", st.ts_glitch_frames));
    }
    let mut notes = Vec::new();
    if rec.duration_s < 60.0 {
        notes.push("측정 시간이 1분 미만이라 판정 근거가 약합니다".to_string());
    }
    let verdict = if !bad.is_empty() {
        "unstable"
    } else if !warn.is_empty() {
        "warning"
    } else {
        "stable"
    };
    let mut reasons: Vec<String> = bad.into_iter().chain(warn).collect();
    if reasons.is_empty() {
        reasons.push("끊김·재연결·손실 없이 수신".into());
    }
    reasons.extend(notes);
    (verdict.into(), reasons)
}

/// 연결 테스트용: 한 프레임을 받아 스트림 정보와 미리보기 JPEG를 얻는다
pub struct ProbeResult {
    pub info: StreamInfo,
    pub jpeg: Option<Bytes>,
    pub ms: f64,
}

pub async fn probe(ffmpeg: &Path, url: &str, timeout: Duration) -> Result<ProbeResult, String> {
    let mut cmd = Command::new(ffmpeg);
    cmd.args([
        "-hide_banner", "-nostats", "-loglevel", "level+info", "-rtsp_transport", "tcp", "-timeout", "5000000",
        "-i", url, "-map", "0:v:0", "-frames:v", "1", "-vf", "scale=640:-2", "-c:v", "mjpeg", "-q:v", "5", "-f", "image2pipe", "pipe:1",
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000);
    let t0 = Instant::now();
    let child = cmd.spawn().map_err(|e| format!("FFmpeg 실행 실패: {e}"))?;
    let out = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| format!("{}초 안에 영상이 오지 않았습니다", timeout.as_secs()))?
        .map_err(|e| format!("FFmpeg 오류: {e}"))?;
    let err = String::from_utf8_lossy(&out.stderr);
    let info = err
        .lines()
        .find(|l| l.contains("Stream #0:0") && l.contains("Video:"))
        .map(parse_stream_info)
        .unwrap_or_default();
    let jpeg = out.stdout.starts_with(&[0xFF, 0xD8]).then(|| Bytes::from(out.stdout));
    if jpeg.is_none() {
        let reason = if err.contains("401") {
            "인증 실패 (아이디·비밀번호 확인)".to_string()
        } else if err.contains("404") {
            "스트림 주소가 없습니다 (404)".to_string()
        } else {
            err.lines().filter(|l| l.contains("[error]") || l.contains("[fatal]")).map(clean_log).last().unwrap_or_else(|| "영상을 받지 못했습니다".into())
        };
        return Err(reason);
    }
    Ok(ProbeResult { info, jpeg, ms: t0.elapsed().as_secs_f64() * 1000.0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// UDP로 영상이 한 번도 안 왔으면 TCP를 권한다 — 단, 계정·주소가 틀린 경우에는 그 안내를 흐리지 않는다
    #[test]
    fn suggests_tcp_when_udp_never_delivers() {
        assert!(udp_hint("카메라에서 영상이 오지 않습니다 (응답 시간 초과)".into(), "udp").contains("TCP로 바꿔"));
        assert_eq!(udp_hint("연결 시간 초과".into(), "tcp"), "연결 시간 초과");
        assert_eq!(udp_hint("method DESCRIBE failed: 401 Unauthorized".into(), "udp"), "method DESCRIBE failed: 401 Unauthorized");
        assert!(!udp_hint("카메라에 그 영상 주소가 없습니다 (RTSP 주소 확인)".into(), "udp").contains("TCP"));
    }

    /// 다시 연결될 때마다 새 번호를 써야 앞 연결의 녹화를 덮어쓰지 않는다 (순번 자릿수와 상관없이)
    #[test]
    fn next_recording_number_skips_used_ones() {
        let dir = std::env::temp_dir().join(format!("capture-vms-recno-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(next_rec_no(&dir, 1), 1);
        std::fs::write(dir.join("run01_00000.mp4"), b"x").unwrap();
        std::fs::write(dir.join("run01_00001.mp4"), b"x").unwrap();
        assert_eq!(next_rec_no(&dir, 1), 2, "5자리 순번 파일이 있으면 그 번호는 쓴 것");
        std::fs::write(dir.join("run02_000.mp4"), b"x").unwrap();
        assert_eq!(next_rec_no(&dir, 1), 3, "예전 3자리 순번 파일도 쓴 것으로 본다");
        assert_eq!(next_rec_no(&dir, 2), 3);
        std::fs::write(dir.join("run04_00000.mp4"), b"x").unwrap();
        assert_eq!(next_rec_no(&dir, 3), 3);
        assert_eq!(next_rec_no(&dir, 4), 5);
        // 녹화 파일이 아닌 것은 세지 않는다
        std::fs::write(dir.join("run09_note.txt"), b"x").unwrap();
        assert_eq!(next_rec_no(&dir, 9), 9);
        let _ = std::fs::remove_dir_all(&dir);
        // 폴더가 없어도 멈추지 않는다
        assert_eq!(next_rec_no(&dir.join("없는-폴더"), 7), 7);
    }

    #[test]
    fn masks_credentials_in_urls() {
        assert_eq!(mask_credentials("[error] Error opening input file rtsp://admin:p%40ss@192.168.1.2:554/media/1/1/Profile1."), "[error] Error opening input file rtsp://***@192.168.1.2:554/media/1/1/Profile1.");
        assert_eq!(mask_credentials("Connection to tcp://192.168.1.2:554?timeout=2000000 failed"), "Connection to tcp://192.168.1.2:554?timeout=2000000 failed");
        assert_eq!(mask_credentials("a rtsp://u:p@h/x and rtsp://h2/y user@example"), "a rtsp://***@h/x and rtsp://h2/y user@example");
        assert_eq!(mask_credentials("주소 없음"), "주소 없음");
    }

    #[test]
    fn parses_ffmpeg_progress() {
        // 녹화 점검: FFmpeg가 \r로 덮어쓰는 진행 표시 중 마지막 값
        let out = "[info] frame=  900 fps=0.0 q=-1.0 size=N/A time=00:00:30.00 bitrate=N/A\r[info] frame= 1830 fps=0.0 q=-1.0 Lsize=N/A time=00:01:00.97 bitrate=N/A speed=900x\n";
        let (f, s) = parse_progress(out);
        assert_eq!(f, 1830);
        assert!((s - 60.97).abs() < 1e-6);
        assert_eq!(parse_progress("nothing"), (0, 0.0));
        assert_eq!(fmt_dur(3725.0), "1시간 02분 05초");
        assert_eq!(fmt_dur(97.0), "1분 37초");
    }

    #[test]
    fn parses_ffmpeg_stream_line() {
        let l = "[info]   Stream #0:0: Video: hevc (Main), yuvj420p(pc, bt709, progressive), 2048x1536 [SAR 1:1 DAR 4:3], 60 fps, 60 tbr, 90k tbn";
        let i = parse_stream_info(l);
        assert_eq!(i.codec.as_deref(), Some("hevc"));
        assert_eq!((i.width, i.height), (Some(2048), Some(1536)));
        assert_eq!(i.fps, Some(60.0));
    }

    #[test]
    fn hist_percentiles() {
        let mut h = Hist::new(0.5, 8000);
        for v in 1..=100 {
            h.add(v as f64);
        }
        assert!((h.pct(50.0).unwrap() - 50.0).abs() <= 0.5);
        assert!((h.pct(99.0).unwrap() - 99.0).abs() <= 0.5);
        assert_eq!(h.max, 100.0);
    }

    #[test]
    fn metrics_detect_stall_and_delay() {
        let cfg = StreamConfig::default();
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.begin_run(t0);
        // 3초 워밍업 이후 60fps로 4초, 중간에 800ms 끊김
        let mut t = t0;
        let mut pts = 0.0;
        let mut stall_seen = false;
        for i in 0..600 {
            let step = if i == 400 { 0.8 } else { 1.0 / 60.0 };
            t += Duration::from_secs_f64(step);
            pts += 1.0 / 60.0;
            let note = m.on_frame(t, Some(pts), 1000);
            if note.stall_ms.is_some() {
                stall_seen = true;
            }
            if i % 60 == 0 {
                m.tick(t);
            }
        }
        assert!(stall_seen);
        assert_eq!(m.stalls, 1);
        let s = m.stats(t, false);
        // 첫 프레임 이후 구간 기준이라 연결 직후(첫 프레임 전) 시간은 fps에 안 들어간다
        // (워밍업 뒤 약 420프레임 / 7.8초, 0.8초 끊김 포함 → 약 54fps)
        assert!((52.0..60.0).contains(&s.avg_fps.unwrap()), "avg_fps={:?}", s.avg_fps);
        // 사용자가 멈춘 건 끊긴 시간이 아니다
        m.end_run(t + Duration::from_millis(300), true);
        let f = m.stats(t + Duration::from_millis(400), true);
        assert_eq!(f.downtime_ms, 0.0);
        assert!(s.interval_max_ms.unwrap() >= 799.0);
        // 끊긴 만큼(영상 시간은 1/60초만 흐름) 버퍼 지연이 잡혀야 함
        assert!(s.delay_max_ms.unwrap() > 700.0);
    }

    #[test]
    fn counts_only_sustained_delay() {
        let cfg = StreamConfig::default(); // 기준 150ms
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.connected = true;
        // Wi-Fi에서 흔한 1초짜리 튐(170, 260)은 세지 않고, 4초 연속 지연만 센다
        for d in [40.0, 170.0, 60.0, 260.0, 50.0, 200.0, 220.0, 180.0, 300.0, 40.0] {
            m.sec_delay_max = Some(d);
            m.tick(t0);
        }
        assert_eq!(m.delayed_secs, 4);
        // 지속 지연 = 최근 3초 중 최소 → 마지막 40ms로 정상
        assert_eq!(m.recent_delay.iter().copied().fold(f64::INFINITY, f64::min), 40.0);
    }

    #[test]
    fn judges_reconnect_as_unstable() {
        let mut rec = SessionRecord { status: "finished".into(), duration_s: 600.0, ..Default::default() };
        rec.stats.frames = 36_000;
        rec.stats.reconnects = 1;
        rec.stats.downtime_ms = 3000.0;
        assert_eq!(judge(&rec).0, "unstable");
        rec.stats.reconnects = 0;
        rec.stats.downtime_ms = 0.0;
        rec.stats.uptime_pct = Some(100.0);
        assert_eq!(judge(&rec).0, "stable");
        // 카메라 fps를 아는 세션에서 프레임이 1% 넘게 빠지면 불안정
        rec.stats.camera_fps = Some(60.0);
        rec.stats.expected_frames = Some(36_000);
        rec.stats.missing_frames = Some(100);
        assert_eq!(judge(&rec).0, "stable");
        rec.stats.missing_frames = Some(3_600);
        let (v, why) = judge(&rec);
        assert_eq!(v, "unstable");
        assert!(why.iter().any(|w| w.contains("프레임 누락 3600개")), "{why:?}");
    }

    /// 60fps로 `secs`초 동안 제때 오는 프레임을 넣는다 (1초마다 tick)
    fn feed_live(m: &mut Metrics, t: &mut Instant, pts: &mut f64, secs: u32) {
        for i in 0..secs * 60 {
            *t += Duration::from_secs_f64(1.0 / 60.0);
            *pts += 1.0 / 60.0;
            m.on_frame(*t, Some(*pts), 1000);
            if i % 60 == 59 {
                m.tick(*t);
            }
        }
    }

    #[test]
    fn starved_when_only_late_frames_trickle_in() {
        let cfg = StreamConfig::default();
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.begin_run(t0);
        let win = Duration::from_secs(3);
        let (mut t, mut pts) = (t0, 0.0);
        // 연결 직후(워밍업 + 창)에는 판단하지 않는다
        feed_live(&mut m, &mut t, &mut pts, 2);
        assert!(!m.starved(t + Duration::from_secs(4), win));
        feed_live(&mut m, &mut t, &mut pts, 10);
        assert!(!m.starved(t, win));
        assert!(!m.starved(t + Duration::from_millis(2500), win));
        // 끊긴 지 2.5초 뒤, 끊기기 직전 프레임 한 장이 뒤늦게 도착 (TCP 재전송). 같은 시각으로 한 번 더 나오기도 한다
        m.on_frame(t + Duration::from_millis(2500), Some(pts + 1.0 / 60.0), 1000);
        m.on_frame(t + Duration::from_millis(2510), Some(pts + 1.0 / 60.0 + 1.0 / 90_000.0), 1000);
        // 마지막 프레임은 0.6초 전이지만, 제때 온 프레임은 3초 넘게 없다 → 굶는 중
        assert!(m.starved(t + Duration::from_millis(3100), win));
        // 영상이 다시 제때 오면 풀린다 (끊긴 5초만큼 영상 시각도 건너뜀)
        t += Duration::from_secs(5);
        pts += 5.0;
        feed_live(&mut m, &mut t, &mut pts, 1);
        assert!(!m.starved(t, win));
        // 끊기면(연결 종료) 굶는 것으로 보지 않는다 — 다시 연결하는 중이다
        m.end_run(t, false);
        assert!(!m.starved(t + Duration::from_secs(10), win));
    }

    #[test]
    fn slow_stream_is_not_starved() {
        // 초당 1장으로 오는 스트림: 3초 동안 3장뿐이어도 정상. 창은 프레임 간격의 4배(4초)로 늘어난다
        let cfg = StreamConfig::default();
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.begin_run(t0);
        let win = Duration::from_secs(3);
        let (mut t, mut pts) = (t0, 0.0);
        for _ in 0..30 {
            t += Duration::from_secs(1);
            pts += 1.0;
            m.on_frame(t, Some(pts), 1000);
            m.tick(t);
            assert!(!m.starved(t + Duration::from_millis(900), win));
        }
        assert!(!m.starved(t + Duration::from_millis(3500), win));
        assert!(m.starved(t + Duration::from_millis(4500), win));
    }

    #[test]
    fn future_and_frozen_timestamps_do_not_fake_delay() {
        // 2초 끊긴 뒤: 3초 동안 영상 시각이 2.2초 앞서 찍혀 오고, 이어서 2.2초 분량이 같은 시각에 뭉쳐 온다
        // (카메라 RTCP 시각이 틀어졌다 바로잡히는 모양). 영상은 계속 제때 온 것이므로 지연으로 잡히면 안 된다.
        let cfg = StreamConfig::default();
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.begin_run(t0);
        let dt = 1.0 / 60.0;
        let (mut t, mut pts) = (t0, 0.0);
        feed_live(&mut m, &mut t, &mut pts, 20);
        t += Duration::from_secs(2);
        pts += 2.0;
        let mut jump = None;
        let mut n = 0;
        let mut reported = 0.0;
        for _ in 0..180 {
            t += Duration::from_secs_f64(dt);
            pts += dt;
            reported = pts + 2.2;
            jump = m.on_frame(t, Some(reported), 1000).ts_jump_s.or(jump);
            n += 1;
            if n % 60 == 0 {
                m.tick(t);
            }
        }
        for _ in 0..132 {
            t += Duration::from_secs_f64(dt);
            pts += dt;
            reported += 1.0 / 90_000.0;
            assert!(m.on_frame(t, Some(reported), 1000).ts_jump_s.is_none());
            n += 1;
            if n % 60 == 0 {
                m.tick(t);
            }
        }
        feed_live(&mut m, &mut t, &mut pts, 20);
        let s = m.stats(t, false);
        assert!((jump.unwrap() - 2.2).abs() < 0.1, "jump={jump:?}");
        assert!((300..=330).contains(&s.ts_glitch_frames), "glitch={}", s.ts_glitch_frames);
        assert!(s.delay_max_ms.unwrap() < 100.0, "delay_max={:?}", s.delay_max_ms);
        assert_eq!(s.delayed_secs, 0);
    }

    #[test]
    fn real_catch_up_is_accepted_as_new_baseline() {
        // 영상이 2초 밀린 채 1분 넘게 오다가 따라잡으면, 처음 10초는 '시각 튐'처럼 보이지만 계속되면 새 기준으로 삼는다
        let cfg = StreamConfig::default();
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.begin_run(t0);
        let (mut t, mut pts) = (t0, 0.0);
        feed_live(&mut m, &mut t, &mut pts, 10);
        t += Duration::from_secs(2); // 도착만 2초 늦어짐 (영상 시각은 이어짐) = 실제 지연
        feed_live(&mut m, &mut t, &mut pts, 70);
        assert!(m.stats(t, false).delay_max_ms.unwrap() > 1900.0);
        pts += 2.0; // 따라잡음: 영상 시각이 2초 건너뜀
        feed_live(&mut m, &mut t, &mut pts, 20);
        let s = m.stats(t, false);
        assert_eq!(s.ts_glitch_frames, 0);
        // 새 기준에서 다시 지연을 잰다: 0.5초 늦어지면 잡힌다
        t += Duration::from_millis(500);
        feed_live(&mut m, &mut t, &mut pts, 2);
        assert!(m.last_tick.3.unwrap() > 400.0, "{:?}", m.last_tick);
    }

    #[test]
    fn counts_missing_frames() {
        let cfg = StreamConfig::default();
        let t0 = Instant::now();
        let mut m = Metrics::new(&cfg, t0);
        m.begin_run(t0);
        m.camera_fps = Some(60.0);
        let (mut t, mut pts) = (t0, 0.0);
        feed_live(&mut m, &mut t, &mut pts, 10);
        t += Duration::from_secs(5);
        pts += 5.0;
        feed_live(&mut m, &mut t, &mut pts, 5);
        let s = m.stats(t, false);
        assert_eq!(s.frames, 900);
        assert!((s.expected_frames.unwrap() as i64 - 1200).abs() <= 2, "{:?}", s.expected_frames);
        assert!((s.missing_frames.unwrap() as i64 - 300).abs() <= 2, "{:?}", s.missing_frames);
    }
}
