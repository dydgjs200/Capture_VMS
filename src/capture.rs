//! 프레임 촬영.
//!
//! 수신 중인 FFmpeg가 원본 영상(재인코딩 없음)을 MPEG-TS로도 보내 준다. 최근 키프레임부터의 분량만
//! 메모리에 들고 있다가(메인 6Mbps·GOP 1초면 약 1MB), [프레임 촬영]을 누르면 **누른 뒤 처음 도착한 프레임**까지를
//! 잠깐 띄운 FFmpeg로 풀어 원본 해상도 JPEG로 저장한다. 카메라에 새로 연결하지 않으므로 수신·녹화에 영향이 없다.

use std::{
    collections::VecDeque,
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::{Buf, Bytes, BytesMut};
use chrono::{DateTime, Local};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{oneshot, Notify},
};

use crate::{
    app::App,
    config::CameraConfig,
    netmon,
    store::{CaptureRecord, Event},
    stream::{self, Session},
};

const TS: usize = 188;
/// 키프레임 표시가 없는 이상한 스트림에서 메모리가 늘지 않게
const MAX_GOP_BYTES: usize = 48 * 1024 * 1024;
const FRAME_TIMEOUT: Duration = Duration::from_secs(5);
const DECODE_TIMEOUT: Duration = Duration::from_secs(15);
/// 뷰어가 이만큼 밀려 있으면(가장 오래 기다린 묶음 기준), 다음 키프레임이 올 때 밀린 분량을 버리고 그 키프레임부터 준다.
/// 화면을 푸는 쪽이 수신보다 느린 PC에서도 화면이 몇 초씩 늦어지다 끊기지 않게 한다 (수신·녹화와는 무관).
/// 예전에는 용량(4MB)만 봐서 메인 6Mbps면 약 5초, 서브 1.5Mbps면 약 20초까지 밀린 뒤에야 끊었다.
/// 엔진 밖(HTTP·소켓 버퍼)에 이미 나간 분량은 여기 잡히지 않으므로, 실제로 밀리는 양은 이 값보다 조금 더 크다.
const LIVE_MAX_LAG: Duration = Duration::from_millis(500);
/// 건너뛰어도 소용없는 경우(키프레임 표시가 없는 스트림, 아예 멈춘 뷰어)에 메모리가 늘지 않게: 이만큼 쌓이면 그 뷰어를 끊는다.
/// 수신 쪽은 뷰어를 기다리지 않는다.
const LIVE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// 뷰어가 밀려 건너뛴 일을 기록에 남기는 최소 간격
const LIVE_SKIP_REPORT_EVERY: Duration = Duration::from_secs(30);

/// 뷰어 한 곳에 줄 영상이 기다리는 줄
#[derive(Default)]
struct LiveQueue {
    /// (엔진이 받은 시각, 패킷 묶음)
    chunks: VecDeque<(Instant, Bytes)>,
    bytes: usize,
    /// 세션이 끝났거나, 너무 밀려 끊었거나, 받는 쪽이 사라짐
    closed: bool,
}

/// 보내는 쪽(수신)과 받는 쪽(뷰어 연결)이 같이 들고 있는 것.
/// 보내는 쪽이 밀린 분량을 버릴 수 있어야 해서 채널이 아니라 줄을 직접 둔다.
#[derive(Default)]
struct LiveShared {
    q: Mutex<LiveQueue>,
    wake: Notify,
}

impl LiveShared {
    /// `discard`: 밀려 있던 것도 버린다 (아니면 밀려 있던 것까지는 받고 끝난다)
    fn close(&self, discard: bool) {
        {
            let mut q = self.q.lock().unwrap();
            q.closed = true;
            if discard {
                q.chunks.clear();
                q.bytes = 0;
            }
        }
        self.wake.notify_one();
    }
}

/// 원본 영상을 받아 보는 쪽 하나 (보내는 쪽이 들고 있는 것)
struct LiveSub(Arc<LiveShared>);

impl Drop for LiveSub {
    fn drop(&mut self) {
        self.0.close(false);
    }
}

/// 원본 영상을 받아 보는 쪽 (뷰어 연결이 들고 있는 것)
pub struct LiveRx(Arc<LiveShared>);

impl LiveRx {
    /// 다음 묶음. 세션이 끝났거나 너무 밀려서 끊겼으면 None
    pub async fn recv(&mut self) -> Option<Bytes> {
        loop {
            if let Some(b) = self.try_recv() {
                return b;
            }
            // (넣은 쪽이 먼저 깨워도 표시가 남아 있어 놓치지 않는다)
            self.0.wake.notified().await;
        }
    }

    /// 기다리지 않고: Some(Some) = 묶음, Some(None) = 끝남, None = 아직 없음
    fn try_recv(&mut self) -> Option<Option<Bytes>> {
        let mut q = self.0.q.lock().unwrap();
        if let Some((_, b)) = q.chunks.pop_front() {
            q.bytes -= b.len();
            return Some(Some(b));
        }
        q.closed.then_some(None)
    }
}

impl Drop for LiveRx {
    fn drop(&mut self) {
        self.0.close(true);
    }
}

/// 촬영에 쓸 원본 영상 묶음 (PAT·PMT + 키프레임 ~ 대상 프레임)
pub struct Grab {
    pub ts: Vec<u8>,
    pub frames: u32,
    pub frame_at: Instant,
    pub frame_wall: DateTime<Local>,
}

struct Pending {
    /// 요청 뒤 새 프레임이 시작됐는지 (그 프레임이 끝나면 = 다음 프레임이 시작되면 넘겨준다)
    started: bool,
    tx: oneshot::Sender<Grab>,
}

#[derive(Default)]
pub struct TsBuffer {
    pat: Option<Vec<u8>>,
    pmt_pid: Option<u16>,
    pmt: Option<Vec<u8>>,
    video_pid: Option<u16>,
    gop: Vec<u8>,
    frames: u32,
    has_key: bool,
    pending: Vec<Pending>,
    /// 원본 영상을 그대로 받아 보는 쪽(뷰어)
    live: Vec<LiveSub>,
}

fn payload_offset(p: &[u8]) -> Option<usize> {
    let afc = (p[3] >> 4) & 3;
    let off = if afc & 2 != 0 { 5 + p[4] as usize } else { 4 };
    (afc & 1 != 0 && off < TS).then_some(off)
}

/// PSI 섹션 시작 위치 (pointer_field 건너뜀)
fn section(p: &[u8]) -> Option<&[u8]> {
    let off = payload_offset(p)?;
    let start = off + 1 + *p.get(off)? as usize;
    p.get(start..)
}

impl TsBuffer {
    /// 새 연결: 지난 연결의 영상은 버린다 (기다리는 촬영과 보고 있는 뷰어는 새 영상으로 이어서 처리)
    pub fn reset(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        let live = std::mem::take(&mut self.live);
        *self = Self { pending, live, ..Default::default() };
    }

    /// PAT + PMT (받는 쪽이 어디서 시작하든 바로 풀 수 있게 앞에 붙인다)
    fn psi(&self) -> BytesMut {
        let mut b = BytesMut::with_capacity(2 * TS);
        b.extend_from_slice(self.pat.as_deref().unwrap_or_default());
        b.extend_from_slice(self.pmt.as_deref().unwrap_or_default());
        b
    }

    /// 원본 영상(MPEG-TS)을 그대로 받아 본다. 바로 풀 수 있게 PAT·PMT와 최근 키프레임부터 지금까지를 먼저 주고,
    /// 그 뒤로는 들어오는 대로 준다. 받는 쪽이 밀리면 다음 키프레임에서 밀린 분량을 건너뛴다 (`publish`).
    pub fn subscribe(&mut self) -> LiveRx {
        let mut head = self.psi();
        if self.has_key {
            head.extend_from_slice(&self.gop);
        }
        let shared = Arc::new(LiveShared::default());
        if !head.is_empty() {
            let mut q = shared.q.lock().unwrap();
            q.bytes = head.len();
            q.chunks.push_back((Instant::now(), head.freeze()));
        }
        self.live.retain(|s| !s.0.q.lock().unwrap().closed);
        self.live.push(LiveSub(shared.clone()));
        LiveRx(shared)
    }

    #[cfg(test)]
    fn viewers(&self) -> usize {
        self.live.iter().filter(|s| !s.0.q.lock().unwrap().closed).count()
    }

    /// 방금 받은 패킷 묶음을 보고 있는 쪽에 넘긴다. 기다리지 않는다.
    ///
    /// `key_at`: 이 묶음 안에서 키프레임이 시작하는 자리. `LIVE_MAX_LAG` 넘게 밀려 있는 뷰어는 밀린 분량을 버리고
    /// 이 키프레임부터 받는다 — 연결을 끊지 않고 지금 영상으로 건너뛴다 (키프레임부터라 화면이 깨지지 않는다).
    /// 건너뛴 뷰어가 있으면 (몇 곳, 그중 가장 밀려 있던 시간)을 돌려준다.
    fn publish(&mut self, chunk: &Bytes, key_at: Option<usize>, now: Instant) -> Option<(u32, Duration)> {
        let from_key = key_at.map(|off| (self.psi().freeze(), chunk.slice(off..)));
        let mut skipped: Option<(u32, Duration)> = None;
        self.live.retain(|s| {
            let alive = {
                let mut q = s.0.q.lock().unwrap();
                if q.closed {
                    return false;
                }
                let lag = q.chunks.front().map_or(Duration::ZERO, |(t, _)| now.saturating_duration_since(*t));
                match &from_key {
                    Some((psi, rest)) if lag >= LIVE_MAX_LAG => {
                        q.chunks.clear();
                        q.bytes = psi.len() + rest.len();
                        if !psi.is_empty() {
                            q.chunks.push_back((now, psi.clone()));
                        }
                        q.chunks.push_back((now, rest.clone()));
                        let e = skipped.get_or_insert((0, Duration::ZERO));
                        *e = (e.0 + 1, e.1.max(lag));
                    }
                    _ if q.bytes + chunk.len() > LIVE_MAX_BYTES => {
                        // 끊는다: 밀린 것은 버려서, 그쪽이 바로 다시 붙어 지금 영상부터 받게 한다
                        q.closed = true;
                        q.chunks.clear();
                        q.bytes = 0;
                    }
                    _ => {
                        q.bytes += chunk.len();
                        q.chunks.push_back((now, chunk.clone()));
                    }
                }
                !q.closed
            };
            s.0.wake.notify_one();
            alive
        });
        skipped
    }

    pub fn request(&mut self) -> oneshot::Receiver<Grab> {
        let (tx, rx) = oneshot::channel();
        self.pending.retain(|p| !p.tx.is_closed());
        self.pending.push(Pending { started: false, tx });
        rx
    }

    /// 패킷 하나를 처리한다. 이 패킷에서 키프레임이 시작하면 true
    pub fn on_packet(&mut self, p: &[u8]) -> bool {
        if p.len() != TS || p[0] != 0x47 {
            return false;
        }
        let pid = (((p[1] & 0x1f) as u16) << 8) | p[2] as u16;
        let pusi = p[1] & 0x40 != 0;
        if pid == 0 {
            if pusi {
                if let Some(s) = section(p) {
                    // 프로그램 목록: 첫 번째 프로그램의 PMT PID
                    let len = (((s.get(1).copied().unwrap_or(0) & 0x0f) as usize) << 8) | s.get(2).copied().unwrap_or(0) as usize;
                    let end = (3 + len).saturating_sub(4).min(s.len());
                    let mut i = 8;
                    while i + 4 <= end {
                        let prog = ((s[i] as u16) << 8) | s[i + 1] as u16;
                        if prog != 0 {
                            self.pmt_pid = Some((((s[i + 2] & 0x1f) as u16) << 8) | s[i + 3] as u16);
                            break;
                        }
                        i += 4;
                    }
                }
                self.pat = Some(p.to_vec());
            }
            return false;
        }
        if Some(pid) == self.pmt_pid {
            if pusi {
                if let Some(s) = section(p) {
                    let len = (((s.get(1).copied().unwrap_or(0) & 0x0f) as usize) << 8) | s.get(2).copied().unwrap_or(0) as usize;
                    let end = (3 + len).saturating_sub(4).min(s.len());
                    let pil = (((s.get(10).copied().unwrap_or(0) & 0x0f) as usize) << 8) | s.get(11).copied().unwrap_or(0) as usize;
                    let mut i = 12 + pil;
                    while i + 5 <= end {
                        let st = s[i];
                        let epid = (((s[i + 1] & 0x1f) as u16) << 8) | s[i + 2] as u16;
                        let es_len = (((s[i + 3] & 0x0f) as usize) << 8) | s[i + 4] as usize;
                        // H.264, H.265, MPEG-4, MPEG-2
                        if matches!(st, 0x1b | 0x24 | 0x10 | 0x02) {
                            self.video_pid = Some(epid);
                            break;
                        }
                        i += 5 + es_len;
                    }
                }
                self.pmt = Some(p.to_vec());
            }
            return false;
        }
        if Some(pid) != self.video_pid {
            return false;
        }
        let mut key_start = false;
        if pusi {
            // 앞 프레임이 끝났다 → 그 프레임을 기다리던 촬영에 넘겨준다
            if self.has_key && self.pending.iter().any(|x| x.started) {
                let now = Instant::now();
                let wall = Local::now();
                let mut head = Vec::with_capacity(self.gop.len() + 2 * TS);
                head.extend_from_slice(self.pat.as_deref().unwrap_or_default());
                head.extend_from_slice(self.pmt.as_deref().unwrap_or_default());
                head.extend_from_slice(&self.gop);
                let (ready, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending).into_iter().partition(|x| x.started);
                self.pending = rest;
                for r in ready {
                    let _ = r.tx.send(Grab { ts: head.clone(), frames: self.frames, frame_at: now, frame_wall: wall });
                }
            }
            let afc = (p[3] >> 4) & 3;
            let key = afc & 2 != 0 && p[4] > 0 && p[5] & 0x40 != 0;
            key_start = key;
            if key || self.gop.len() > MAX_GOP_BYTES {
                self.gop.clear();
                self.frames = 0;
                self.has_key = key;
            }
            if self.has_key {
                self.frames += 1;
                // 이 프레임이 '누른 뒤 처음 도착한 프레임'
                for x in self.pending.iter_mut() {
                    x.started = true;
                }
            }
        }
        if self.has_key {
            self.gop.extend_from_slice(p);
        }
        key_start
    }
}

/// 뷰어가 밀려 건너뛴 일을 기록에 남긴다 (수신 품질과는 무관: 화면을 푸는 쪽이 느린 것)
fn report_viewer_lag(app: &App, s: &Session, skips: u32, worst: Duration) {
    app.store.record_event(
        Event::new(
            "viewer_lag",
            Some(&s.camera_id),
            format!(
                "화면(뷰어)이 영상을 따라오지 못해 최신 영상으로 건너뜀 {skips}회 (엔진에 밀려 있던 분량 최대 {:.1}초 · 수신·녹화는 정상)",
                worst.as_secs_f64()
            ),
        )
        .session(&s.id)
        .with_data(serde_json::json!({ "skips": skips, "lag_ms": ms(worst) })),
    );
}

/// FFmpeg가 보내는 MPEG-TS를 들어온 만큼 읽어 188바이트 패킷 단위로 처리하고, 보고 있는 뷰어에 그대로 넘긴다
pub async fn read_ts(mut sock: tokio::net::TcpStream, app: Arc<App>, s: Arc<Session>) {
    let mut buf = BytesMut::with_capacity(256 * 1024);
    // 뷰어가 밀려 건너뛴 일: 느린 PC에서는 몇 초마다 일어나므로 모아서 가끔만 기록한다
    let (mut skips, mut worst, mut reported) = (0u32, Duration::ZERO, None::<Instant>);
    loop {
        buf.reserve(64 * 1024);
        match sock.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        loop {
            // 동기 바이트(0x47)로 시작하는 온전한 패킷이 이어진 만큼
            let mut n = 0;
            while buf.len() - n >= TS && buf[n] == 0x47 {
                n += TS;
            }
            if n > 0 {
                let chunk = buf.split_to(n).freeze();
                let now = Instant::now();
                let skipped = {
                    let mut ts = s.ts_buffer();
                    // 이 묶음 안에서 키프레임이 시작하는 자리 (여러 개면 마지막)
                    let mut key_at = None;
                    for (i, p) in chunk.chunks_exact(TS).enumerate() {
                        if ts.on_packet(p) {
                            key_at = Some(i * TS);
                        }
                    }
                    if ts.live.is_empty() { None } else { ts.publish(&chunk, key_at, now) }
                };
                if let Some((n, lag)) = skipped {
                    skips += n;
                    worst = worst.max(lag);
                    if reported.map_or(true, |t| now.saturating_duration_since(t) >= LIVE_SKIP_REPORT_EVERY) {
                        report_viewer_lag(&app, &s, skips, worst);
                        (skips, worst, reported) = (0, Duration::ZERO, Some(now));
                    }
                }
            }
            if buf.len() < TS || buf[0] == 0x47 {
                break;
            }
            // 어긋났으면 다음 동기 바이트까지 버린다
            let skip = buf.iter().skip(1).position(|&b| b == 0x47).map_or(buf.len(), |i| i + 1);
            buf.advance(skip);
        }
    }
    if skips > 0 {
        report_viewer_lag(&app, &s, skips, worst);
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// 트리거(동시 촬영)로 찍을 때: 모든 카메라가 같은 '신호 받은 시각'부터 시간을 잰다
#[derive(Clone)]
pub struct TriggerCtx {
    pub id: String,
    pub seq: u64,
    pub source: String,
    pub t_req: Instant,
    pub req_wall: DateTime<Local>,
}

fn mark_trigger(c: &mut CaptureRecord, trig: Option<&TriggerCtx>) {
    if let Some(t) = trig {
        c.trigger_id = Some(t.id.clone());
        c.trigger_seq = Some(t.seq);
        c.trigger_source = Some(t.source.clone());
    }
}

/// [프레임 촬영] 한 번. 실패해도 기록은 남긴다.
pub async fn take(app: &Arc<App>, s: &Arc<Session>, trig: Option<&TriggerCtx>) -> CaptureRecord {
    let (t_req, req_wall) = trig.map_or_else(|| (Instant::now(), Local::now()), |t| (t.t_req, t.req_wall));
    let info = s.capture_info();
    let mut c = CaptureRecord {
        id: format!("{}-{}", req_wall.format("%Y%m%d-%H%M%S-%3f"), info.camera_id),
        session_id: s.id.clone(),
        session_label: info.label.clone(),
        camera_id: info.camera_id.clone(),
        camera_name: info.camera_name.clone(),
        profile: info.profile.clone(),
        transport: info.transport.clone(),
        path_kind: info.path_kind.clone(),
        mode: "session".into(),
        requested_at: Some(req_wall),
        width: info.width,
        height: info.height,
        ..Default::default()
    };
    mark_trigger(&mut c, trig);
    // 재연결 중이면 5초를 기다리지 않고 바로 실패 (트리거 응답이 늦어지지 않게)
    if s.is_reconnecting() {
        return finish(app, c, Err("영상이 끊겨 다시 연결하는 중이라 찍지 못했습니다".into()));
    }
    let result = async {
        // 동시에 여러 번 눌러도 차례로 처리 (기다린 시간도 '대기'에 들어간다)
        let _turn = s.capture_lock.lock().await;
        let ffmpeg = app.ffmpeg.clone().ok_or("FFmpeg를 찾을 수 없습니다")?;
        let rx = s.ts_buffer().request();
        let grab = tokio::time::timeout(FRAME_TIMEOUT, rx)
            .await
            .map_err(|_| format!("{}초 동안 새 프레임이 오지 않았습니다 (영상 끊김)", FRAME_TIMEOUT.as_secs()))?
            .map_err(|_| "세션이 끝나 촬영하지 못했습니다".to_string())?;
        c.frame_at = Some(grab.frame_wall);
        c.wait_ms = Some(ms(grab.frame_at - t_req));
        c.frames_decoded = grab.frames;
        c.stream_delay_ms = s.view().live.and_then(|l| l.delay_ms.or(l.delay_3s_ms));

        let jpeg = decode_last(&ffmpeg, grab.ts, grab.frames).await?;
        let t_jpeg = Instant::now();
        c.decode_ms = Some(ms(t_jpeg - grab.frame_at));

        save_jpeg(app, &mut c, jpeg, req_wall, t_req, t_jpeg).await
    }
    .await;
    finish(app, c, result)
}

fn finish(app: &App, mut c: CaptureRecord, result: Result<(), String>) -> CaptureRecord {
    c.location = app.location();
    match result {
        Ok(()) => c.ok = true,
        Err(e) => c.error = Some(e),
    }
    app.store.record_capture(&c);
    c
}

/// JPEG를 디스크에 쓰고(실제 기록까지 기다림) 저장 시각·구간을 채운다
async fn save_jpeg(app: &App, c: &mut CaptureRecord, jpeg: Vec<u8>, req_wall: DateTime<Local>, t_req: Instant, t_jpeg: Instant) -> Result<(), String> {
    let dir = app.store.recordings_dir().join(req_wall.format("%Y%m%d").to_string()).join("captures");
    // 예: 20261103_155008_123_cam01_메인.jpg (누른 시각_카메라_스트림)
    //     20261103_155008_123_T00012_cam01_메인.jpg (트리거 12번: 같은 번호끼리 같은 순간)
    let trig = c.trigger_seq.map(|n| format!("T{n:05}_")).unwrap_or_default();
    let path = dir.join(format!(
        "{}_{trig}{}_{}.jpg",
        req_wall.format("%Y%m%d_%H%M%S_%3f"),
        c.camera_id,
        if c.profile == "sub" { "서브" } else { "메인" }
    ));
    let bytes = jpeg.len() as u64;
    let p2 = path.clone();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        std::fs::create_dir_all(p2.parent().unwrap())?;
        let mut f = std::fs::File::create(&p2)?;
        std::io::Write::write_all(&mut f, &jpeg)?;
        f.sync_all() // 디스크에 실제로 기록될 때까지 (저장 시각을 정확히)
    })
    .await
    .map_err(|e| format!("저장 실패: {e}"))?
    .map_err(|e| format!("저장 실패: {} ({e})", path.display()))?;
    let t_saved = Instant::now();
    c.saved_at = Some(Local::now());
    c.save_ms = Some(ms(t_saved - t_jpeg));
    c.total_ms = Some(ms(t_saved - t_req));
    c.bytes = bytes;
    c.image = Some(path.display().to_string());
    Ok(())
}

/// 수신하지 않을 때의 촬영: 카메라에 잠깐 접속해 첫 프레임(키프레임)을 받아 저장하고 끊는다.
/// 누름 → 저장 시간에 RTSP 연결 시간이 들어간다 (`connect_ms`로 따로 남김).
pub async fn take_direct(app: &Arc<App>, cam: &CameraConfig, profile: &str, transport: &str, trig: Option<&TriggerCtx>) -> CaptureRecord {
    let (t_req, req_wall) = trig.map_or_else(|| (Instant::now(), Local::now()), |t| (t.t_req, t.req_wall));
    let profile = if profile == "sub" { "sub" } else { "main" };
    let transport = if transport == "udp" { "udp" } else { "tcp" };
    let (path_kind, _) = netmon::route_to(&cam.ip()).unwrap_or((None, None));
    let c = CaptureRecord {
        id: format!("{}-{}", req_wall.format("%Y%m%d-%H%M%S-%3f"), cam.id),
        session_id: "direct".into(),
        session_label: "단독 촬영 (수신 없이)".into(),
        camera_id: cam.id.clone(),
        camera_name: cam.name.clone(),
        profile: profile.into(),
        transport: transport.into(),
        path_kind,
        mode: "direct".into(),
        requested_at: Some(req_wall),
        ..Default::default()
    };
    let mut c = c;
    mark_trigger(&mut c, trig);
    let result = async {
        let ffmpeg = app.ffmpeg.clone().ok_or("FFmpeg를 찾을 수 없습니다")?;
        let url = cam
            .rtsp_url(profile)
            .ok_or_else(|| format!("{} 스트림 주소가 없습니다. 설정 → 카메라에서 [연결 테스트]를 해 주세요", if profile == "sub" { "서브" } else { "메인" }))?;
        let mut cmd = Command::new(&ffmpeg);
        cmd.args(["-hide_banner", "-nostats", "-loglevel", "level+info", "-rtsp_transport", transport, "-timeout", "5000000", "-i"])
            .arg(&url)
            .args(["-map", "0:v:0", "-frames:v", "1", "-c:v", "mjpeg", "-q:v", "2", "-f", "image2pipe", "pipe:1"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        cmd.creation_flags(0x0800_0000);
        let mut child = cmd.spawn().map_err(|e| format!("FFmpeg 실행 실패: {e}"))?;
        // 스트림 정보가 나오는 순간 = 카메라 연결 완료
        let err = child.stderr.take().ok_or("FFmpeg 로그를 읽을 수 없습니다")?;
        let log = tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(err).lines();
            let (mut connected, mut info, mut last_err) = (None, stream::StreamInfo::default(), None::<String>);
            while let Ok(Some(l)) = lines.next_line().await {
                if connected.is_none() && l.contains("Stream #0:0") && l.contains("Video:") {
                    connected = Some(Instant::now());
                    info = stream::parse_stream_info(&l);
                } else if l.contains("[error]") || l.contains("[fatal]") {
                    last_err = Some(l);
                }
            }
            (connected, info, last_err)
        });
        let mut out = child.stdout.take().ok_or("FFmpeg 출력을 읽을 수 없습니다")?;
        let mut jpeg = Vec::new();
        let read = tokio::time::timeout(DECODE_TIMEOUT, out.read_to_end(&mut jpeg)).await;
        let t_jpeg = Instant::now();
        if read.is_err() {
            let _ = child.kill().await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
        let (connected, info, last_err) = log.await.unwrap_or((None, Default::default(), None));
        c.width = info.width;
        c.height = info.height;
        if let Some(t) = connected {
            c.connect_ms = Some(ms(t - t_req));
            c.wait_ms = c.connect_ms;
            c.frame_at = None;
            c.decode_ms = Some(ms(t_jpeg - t));
        }
        if read.is_err() {
            return Err(format!("{}초 안에 영상을 받지 못했습니다", DECODE_TIMEOUT.as_secs()));
        }
        if !jpeg.starts_with(&[0xFF, 0xD8]) {
            let e = last_err.unwrap_or_default();
            return Err(if e.contains("401") {
                "카메라가 인증을 거부했습니다 (401). 아이디·비밀번호를 확인하세요".into()
            } else if e.is_empty() {
                "카메라에서 영상을 받지 못했습니다".into()
            } else {
                format!("카메라에서 영상을 받지 못했습니다: {}", e.rsplit("] ").next().unwrap_or(&e))
            });
        }
        save_jpeg(app, &mut c, jpeg, req_wall, t_req, t_jpeg).await
    }
    .await;
    finish(app, c, result)
}

/// 키프레임부터 대상 프레임까지 풀어서 마지막 프레임만 JPEG로.
/// GPU 디코딩은 쓰지 않는다: 한 번 쓰고 끄는 용도라 초기화(약 200ms)가 더 오래 걸린다 (측정: GPU 480ms / CPU 280ms).
async fn decode_last(ffmpeg: &PathBuf, ts: Vec<u8>, frames: u32) -> Result<Vec<u8>, String> {
    let last = frames.saturating_sub(1);
    let mut cmd = Command::new(ffmpeg);
    cmd.args(["-hide_banner", "-loglevel", "error"]);
    cmd.args(["-f", "mpegts", "-i", "pipe:0", "-map", "0:v:0", "-vf"])
        .arg(format!("select=eq(n\\,{last})"))
        .args(["-fps_mode", "passthrough", "-frames:v", "1", "-c:v", "mjpeg", "-q:v", "2", "-f", "image2pipe", "pipe:1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000);
    let mut child = cmd.spawn().map_err(|e| format!("FFmpeg 실행 실패: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("FFmpeg 입력을 열 수 없습니다")?;
    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&ts).await;
        let _ = stdin.shutdown().await;
    });
    let out = tokio::time::timeout(DECODE_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| "JPEG 변환 시간 초과".to_string())?
        .map_err(|e| format!("FFmpeg 오류: {e}"))?;
    let _ = writer.await;
    if out.stdout.starts_with(&[0xFF, 0xD8]) {
        Ok(out.stdout)
    } else {
        let err = String::from_utf8_lossy(&out.stderr);
        Err(format!("프레임을 이미지로 바꾸지 못했습니다{}", err.lines().last().map(|l| format!(": {l}")).unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pkt(pid: u16, pusi: bool, key: bool) -> Vec<u8> {
        let mut p = vec![0xFFu8; TS];
        p[0] = 0x47;
        p[1] = ((pid >> 8) as u8 & 0x1f) | if pusi { 0x40 } else { 0 };
        p[2] = pid as u8;
        if key {
            p[3] = 0x30; // 적응 필드 + 페이로드
            p[4] = 7;
            p[5] = 0x40; // random access
        } else {
            p[3] = 0x10;
        }
        p
    }

    fn psi(pid: u16, body: &[u8]) -> Vec<u8> {
        let mut p = vec![0xFFu8; TS];
        p[0] = 0x47;
        p[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
        p[2] = pid as u8;
        p[3] = 0x10;
        p[4] = 0; // pointer_field
        p[5..5 + body.len()].copy_from_slice(body);
        p
    }

    #[test]
    fn grabs_first_frame_after_request() {
        let mut b = TsBuffer::default();
        // PAT: program 1 → PMT PID 0x1000
        b.on_packet(&psi(0, &[0x00, 0xB0, 0x0D, 0, 1, 0xC1, 0, 0, 0x00, 0x01, 0xF0, 0x00, 0, 0, 0, 0]));
        assert_eq!(b.pmt_pid, Some(0x1000));
        // PMT: HEVC(0x24) on PID 0x100
        b.on_packet(&psi(0x1000, &[0x02, 0xB0, 0x12, 0, 1, 0xC1, 0, 0, 0xE1, 0x00, 0xF0, 0x00, 0x24, 0xE1, 0x00, 0xF0, 0x00, 0, 0, 0, 0]));
        assert_eq!(b.video_pid, Some(0x100));
        // 키프레임 + 프레임 2개 (각 2패킷)
        for (i, key) in [(0, true), (1, false), (2, false)] {
            b.on_packet(&pkt(0x100, true, key));
            b.on_packet(&pkt(0x100, false, false));
            let _ = i;
        }
        let mut rx = b.request();
        // 요청 뒤 첫 프레임 시작 → 아직 안 넘김
        b.on_packet(&pkt(0x100, true, false));
        b.on_packet(&pkt(0x100, false, false));
        assert!(rx.try_recv().is_err());
        // 다음 프레임 시작 = 대상 프레임 끝 → 넘김
        b.on_packet(&pkt(0x100, true, false));
        let g = rx.try_recv().expect("grab");
        assert_eq!(g.frames, 4); // 키프레임부터 대상 프레임까지
        assert_eq!(g.ts.len(), (2 + 8) * TS); // PAT + PMT + 4프레임×2패킷
    }

    /// PAT·PMT까지 받은 버퍼 (영상 PID 0x100)
    fn with_psi() -> TsBuffer {
        let mut b = TsBuffer::default();
        b.on_packet(&psi(0, &[0x00, 0xB0, 0x0D, 0, 1, 0xC1, 0, 0, 0x00, 0x01, 0xF0, 0x00, 0, 0, 0, 0]));
        b.on_packet(&psi(0x1000, &[0x02, 0xB0, 0x12, 0, 1, 0xC1, 0, 0, 0xE1, 0x00, 0xF0, 0x00, 0x24, 0xE1, 0x00, 0xF0, 0x00, 0, 0, 0, 0]));
        b
    }

    /// 기다리지 않고 다음 묶음 (없거나 끝났으면 None)
    fn next(rx: &mut LiveRx) -> Option<Bytes> {
        rx.try_recv().flatten()
    }

    #[test]
    fn live_viewer_starts_at_keyframe_and_stuck_one_is_dropped() {
        let mut b = with_psi();
        // 키프레임 앞의 프레임은 들고 있지 않는다
        assert!(!b.on_packet(&pkt(0x100, true, false)));
        assert!(b.on_packet(&pkt(0x100, true, true)));
        assert!(!b.on_packet(&pkt(0x100, true, false)));
        let mut rx = b.subscribe();
        let t = Instant::now();
        // 처음: PAT + PMT + 키프레임부터 지금까지(2패킷)
        assert_eq!(next(&mut rx).expect("head").len(), 4 * TS);
        let chunk = Bytes::from(pkt(0x100, true, false));
        b.publish(&chunk, None, t);
        assert_eq!(next(&mut rx).expect("live").len(), TS);
        // 다시 연결돼도(reset) 보고 있던 쪽은 이어서 받는다
        b.reset();
        assert_eq!(b.viewers(), 1);
        b.publish(&chunk, None, t);
        assert!(next(&mut rx).is_some());
        // 키프레임이 오지 않는 채로 안 읽고 쌓이기만 하면 끊는다 — 보내는 쪽은 한 번도 기다리지 않는다
        for _ in 0..LIVE_MAX_BYTES / TS + 10 {
            b.publish(&chunk, None, t);
        }
        assert_eq!(b.viewers(), 0);
        // 끊긴 쪽은 밀려 있던 것을 받지 않고 바로 끝난다 (다시 붙어 지금 영상부터 받게)
        assert_eq!(rx.try_recv(), Some(None));
    }

    #[test]
    fn lagging_viewer_skips_to_the_next_keyframe() {
        let mut b = with_psi();
        b.on_packet(&pkt(0x100, true, true));
        let mut rx = b.subscribe();
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let frame = Bytes::from(pkt(0x100, true, false));
        // 뷰어가 안 읽는 동안 쌓인다. 키프레임이 없는 묶음에서는 건너뛰지 않는다 (중간부터 주면 화면이 깨진다)
        for i in 1..=10 {
            assert!(b.publish(&frame, None, at(100 * i)).is_none());
        }
        // 키프레임이 든 묶음: [앞 프레임의 나머지][키프레임 시작] → 밀린 것을 버리고 키프레임부터
        let mut both = pkt(0x100, false, false);
        both.extend(pkt(0x100, true, true));
        let both = Bytes::from(both);
        let (n, lag) = b.publish(&both, Some(TS), at(1100)).expect("skip");
        assert_eq!(n, 1);
        assert!(lag >= Duration::from_millis(1000));
        assert_eq!(next(&mut rx).expect("PAT+PMT").len(), 2 * TS);
        let key = next(&mut rx).expect("keyframe");
        assert_eq!(key.len(), TS);
        assert!(key[1] & 0x40 != 0 && key[5] & 0x40 != 0, "키프레임 시작 패킷부터여야 한다");
        assert!(next(&mut rx).is_none());
        assert_eq!(b.viewers(), 1); // 끊지 않는다
        assert_eq!(rx.0.q.lock().unwrap().bytes, 0);
    }

    #[test]
    fn viewer_that_keeps_up_is_never_skipped() {
        let mut b = with_psi();
        b.on_packet(&pkt(0x100, true, true));
        let mut rx = b.subscribe();
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let frame = Bytes::from(pkt(0x100, true, false));
        let key = Bytes::from(pkt(0x100, true, true));
        // 조금 밀린 정도(LIVE_MAX_LAG 미만)면 키프레임이 와도 그대로 다 준다
        while next(&mut rx).is_some() {}
        b.publish(&frame, None, at(1000));
        b.publish(&frame, None, at(1100));
        assert!(b.publish(&key, Some(0), at(1200)).is_none());
        assert_eq!(std::iter::from_fn(|| next(&mut rx)).count(), 3);
        // 다 읽은 뷰어는 아무리 시간이 지나도 건너뛰지 않는다
        assert!(b.publish(&key, Some(0), at(60_000)).is_none());
        assert_eq!(next(&mut rx).expect("key").len(), TS);
    }

    #[tokio::test]
    async fn live_recv_waits_then_ends_when_session_ends() {
        let mut b = with_psi();
        b.on_packet(&pkt(0x100, true, true));
        let mut rx = b.subscribe();
        assert_eq!(rx.recv().await.expect("head").len(), 3 * TS);
        // 비어 있으면 기다리고, 들어오면 깨어난다
        let waiter = tokio::spawn(async move { (rx.recv().await.map(|c| c.len()), rx.recv().await) });
        tokio::task::yield_now().await;
        b.publish(&Bytes::from(pkt(0x100, true, false)), None, Instant::now());
        // 세션이 끝나면(버퍼가 사라지면) 받는 쪽도 끝난다
        drop(b);
        assert_eq!(waiter.await.unwrap(), (Some(TS), None));
    }
}
