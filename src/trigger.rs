//! 트리거(동시 촬영 신호).
//!
//! 신호가 오면 '트리거 대상' 카메라 전부를 같은 순간에 찍는다. 수신 중인 카메라는 받고 있는 영상에서(약 0.2초),
//! 수신 중이 아닌 카메라는 잠깐 접속해서(약 1초) 찍는다. 모든 카메라가 '신호 받은 시각'부터 시간을 재므로
//! 카메라마다 걸린 시간과 카메라끼리 찍힌 시각 차이(편차)를 그대로 비교할 수 있다.
//!
//! 신호를 받는 방법
//!   - 대시보드 [동시 촬영] 버튼·스페이스바, `POST /api/trigger`
//!   - PLC → 이 PC로 TCP 메시지 (이 PC가 서버): 한 줄(또는 한 덩어리) = 촬영 1회, 끝나면 결과 한 줄을 돌려준다
//!   - Modbus TCP (이 PC가 PLC를 읽음): 정한 주소가 0 → 1이 되면 촬영, 결과를 레지스터에 쓸 수 있다
//!   - 반복 촬영 시험: 정한 간격으로 N번 (PLC 없이 현장에서 성능 확인)

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Local};
use serde::Serialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

use crate::{
    app::App,
    capture::{self, TriggerCtx},
    config::TriggerConfig,
    store::{CaptureRecord, Event, TriggerRecord, TriggerShot},
};

#[derive(Debug, Clone, Default, Serialize)]
pub struct TcpStatus {
    pub enabled: bool,
    pub bind: String,
    pub listening: bool,
    pub error: Option<String>,
    /// 지금 연결된 PLC(클라이언트) 수
    pub clients: u32,
    pub last_peer: Option<String>,
    pub last_message: Option<String>,
    pub last_at: Option<DateTime<Local>>,
    pub messages: u64,
    /// 키워드가 없어 신호로 보지 않은 메시지
    pub skipped: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ModbusStatus {
    pub enabled: bool,
    pub target: String,
    pub connected: bool,
    pub error: Option<String>,
    pub last_value: Option<u16>,
    pub polls: u64,
    pub poll_errors: u64,
    /// 0 → 1로 바뀐 횟수 (= 신호)
    pub rises: u64,
    pub last_rise: Option<DateTime<Local>>,
    pub acks: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AutoStatus {
    pub running: bool,
    pub interval_ms: u64,
    /// 0이면 멈출 때까지
    pub count: u64,
    pub done: u64,
    pub ok: u64,
    pub failed: u64,
    pub started_at: Option<DateTime<Local>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Counts {
    pub total: u64,
    pub ok: u64,
    pub partial: u64,
    pub failed: u64,
    pub ignored: u64,
}

pub struct TriggerRuntime {
    pub cfg: RwLock<TriggerConfig>,
    seq: AtomicU64,
    last_fire: Mutex<Option<Instant>>,
    busy: AtomicU32,
    counts: Mutex<Counts>,
    last: Mutex<Option<TriggerRecord>>,
    tcp: Mutex<TcpStatus>,
    modbus: Mutex<ModbusStatus>,
    auto: Mutex<AutoStatus>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    auto_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Serialize)]
pub struct TargetView {
    pub id: String,
    pub name: String,
    pub receiving: bool,
}

#[derive(Serialize)]
pub struct TriggerStatus {
    pub config: TriggerConfig,
    /// 처리 중인 신호 수
    pub busy: u32,
    /// 이번 실행에서 받은 신호
    pub counts: Counts,
    pub last: Option<TriggerRecord>,
    pub tcp: TcpStatus,
    pub modbus: ModbusStatus,
    pub auto: AutoStatus,
    pub targets: Vec<TargetView>,
}

impl TriggerRuntime {
    /// `last`: 지난 실행의 마지막 신호 (재시작해도 화면에 보이게)
    pub fn new(cfg: TriggerConfig, last_seq: u64, last: Option<TriggerRecord>) -> Self {
        Self {
            cfg: RwLock::new(cfg),
            seq: AtomicU64::new(last_seq),
            last_fire: Mutex::new(None),
            busy: AtomicU32::new(0),
            counts: Mutex::new(Counts::default()),
            last: Mutex::new(last),
            tcp: Mutex::new(TcpStatus::default()),
            modbus: Mutex::new(ModbusStatus::default()),
            auto: Mutex::new(AutoStatus::default()),
            tasks: Mutex::new(Vec::new()),
            auto_task: Mutex::new(None),
        }
    }

    pub fn config(&self) -> TriggerConfig {
        self.cfg.read().unwrap().clone()
    }

    fn track(&self, h: JoinHandle<()>) {
        let mut t = self.tasks.lock().unwrap();
        t.retain(|h| !h.is_finished());
        t.push(h);
    }

    pub fn tcp_listening(&self) -> bool {
        self.tcp.lock().unwrap().listening
    }

    pub fn modbus_connected(&self) -> bool {
        self.modbus.lock().unwrap().connected
    }

    pub fn last(&self) -> Option<TriggerRecord> {
        self.last.lock().unwrap().clone()
    }

    pub fn busy(&self) -> u32 {
        self.busy.load(Ordering::SeqCst)
    }
}

pub fn status(app: &App) -> TriggerStatus {
    let rt = &app.trigger;
    TriggerStatus {
        config: rt.config(),
        busy: rt.busy(),
        counts: rt.counts.lock().unwrap().clone(),
        last: rt.last(),
        tcp: rt.tcp.lock().unwrap().clone(),
        modbus: rt.modbus.lock().unwrap().clone(),
        auto: rt.auto.lock().unwrap().clone(),
        targets: app
            .cameras()
            .iter()
            .filter(|c| c.cfg.trigger)
            .map(|c| TargetView { id: c.cfg.id.clone(), name: c.cfg.name.clone(), receiving: app.session_for_camera(&c.cfg.id).is_some() })
            .collect(),
    }
}

/// 설정대로 PLC 연결(TCP 서버·Modbus 읽기)을 다시 시작한다 (시작할 때와 설정을 바꿨을 때)
pub fn apply(app: &Arc<App>) {
    let rt = &app.trigger;
    for h in rt.tasks.lock().unwrap().drain(..) {
        h.abort();
    }
    let cfg = rt.config();
    *rt.tcp.lock().unwrap() = TcpStatus { enabled: cfg.tcp_enabled, bind: cfg.tcp_bind.trim().to_string(), ..Default::default() };
    *rt.modbus.lock().unwrap() =
        ModbusStatus { enabled: cfg.modbus_enabled, target: if cfg.modbus_enabled { cfg.modbus_addr() } else { String::new() }, ..Default::default() };
    if cfg.tcp_enabled {
        match cfg.tcp_bind.trim().parse::<SocketAddr>() {
            Ok(addr) => rt.track(tokio::spawn(tcp_server(app.clone(), addr))),
            Err(e) => rt.tcp.lock().unwrap().error = Some(format!("주소 형식 오류: {e}")),
        }
    }
    if cfg.modbus_enabled {
        rt.track(tokio::spawn(modbus_loop(app.clone(), cfg)));
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// '촬영 중' 수. 촬영이 중간에 취소돼도(반복 시험 중지 등) Drop에서 줄어들어 표시가 남지 않는다
struct Busy<'a>(&'a AtomicU32);

impl<'a> Busy<'a> {
    fn new(n: &'a AtomicU32) -> Self {
        n.fetch_add(1, Ordering::SeqCst);
        Busy(n)
    }
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 신호 한 번: 대상 카메라를 모두 동시에 찍고 결과를 기록한다.
pub async fn fire(app: &Arc<App>, source: &str, detail: Option<String>) -> TriggerRecord {
    let t_req = Instant::now();
    let wall = Local::now();
    let rt = &app.trigger;
    let cfg = rt.config();
    let detail = detail.map(|d| d.chars().take(120).collect::<String>());
    // 채터링·중복 신호: 앞 신호 바로 뒤에 온 것은 찍지 않고 기록만 남긴다
    let gap = {
        let mut last = rt.last_fire.lock().unwrap();
        let gap = last.map(|p| t_req - p);
        if gap.map_or(true, |g| g >= Duration::from_millis(cfg.min_interval_ms)) {
            *last = Some(t_req);
            None
        } else {
            gap
        }
    };
    if let Some(g) = gap {
        let rec = TriggerRecord {
            id: format!("{}-ignored", wall.format("%Y%m%d-%H%M%S-%3f")),
            ts: wall,
            source: source.into(),
            detail,
            status: "ignored".into(),
            error: Some(format!("앞 신호 뒤 {:.0}ms 만에 들어와 무시함 (기준 {}ms)", ms(g), cfg.min_interval_ms)),
            location: app.location(),
            ..Default::default()
        };
        app.store.record_trigger(&rec);
        rt.counts.lock().unwrap().ignored += 1;
        return rec;
    }

    let seq = rt.seq.fetch_add(1, Ordering::SeqCst) + 1;
    let ctx = TriggerCtx { id: format!("{}-T{seq:05}", wall.format("%Y%m%d-%H%M%S-%3f")), seq, source: source.into(), t_req, req_wall: wall };
    let mut rec = TriggerRecord { id: ctx.id.clone(), seq, ts: wall, source: source.into(), detail, location: app.location(), ..Default::default() };
    let busy = Busy::new(&rt.busy);
    let targets: Vec<_> = app.cameras().into_iter().filter(|c| c.cfg.trigger).collect();
    if targets.is_empty() {
        rec.status = "failed".into();
        rec.error = Some("트리거 대상 카메라가 없습니다 (설정 → 카메라에서 [트리거 대상]을 켜세요)".into());
    } else {
        let caps = futures_util::future::join_all(targets.iter().map(|cam| {
            let (ctx, cfg) = (&ctx, &cfg);
            async move {
                match app.session_for_camera(&cam.cfg.id) {
                    Some(s) => capture::take(app, &s, Some(ctx)).await,
                    None => capture::take_direct(app, &cam.cfg, &cfg.profile, &cfg.transport, Some(ctx)).await,
                }
            }
        }))
        .await;
        // 수신 없이 찍다 실패한 카메라는 영상 주소가 비어 있거나 주소가 바뀌었을 수 있다 → 다음 신호 때는 되도록 뒤에서 바로잡아 둔다
        // (신호를 받고 찍는 시간에는 끼어들지 않는다)
        for c in caps.iter().filter(|c| !c.ok && c.mode == "direct") {
            let (app, id, need_sub) = (app.clone(), c.camera_id.clone(), cfg.profile == "sub");
            tokio::spawn(async move {
                let _ = crate::camfix::fill_urls(&app, &id, need_sub).await;
                crate::camfix::follow(&app, &id).await;
            });
        }
        rec.shots = caps.iter().map(shot_of).collect();
        summarize(&mut rec);
    }
    drop(busy);
    app.store.record_trigger(&rec);
    {
        let mut c = rt.counts.lock().unwrap();
        c.total += 1;
        match rec.status.as_str() {
            "ok" => c.ok += 1,
            "partial" => c.partial += 1,
            _ => c.failed += 1,
        }
    }
    *rt.last.lock().unwrap() = Some(rec.clone());
    if rec.status != "ok" {
        app.store.record_event(
            Event::new("trigger_failed", None, format!("트리거 {}번({}) 촬영 {}: {}", seq, source_ko(source), if rec.status == "partial" { "일부 실패" } else { "실패" }, rec.error.clone().unwrap_or_default()))
                .with_data(serde_json::json!({ "trigger_id": rec.id })),
        );
    }
    rec
}

fn shot_of(c: &CaptureRecord) -> TriggerShot {
    TriggerShot {
        camera_id: c.camera_id.clone(),
        camera_name: c.camera_name.clone(),
        capture_id: c.id.clone(),
        mode: c.mode.clone(),
        ok: c.ok,
        error: c.error.clone(),
        total_ms: c.total_ms,
        wait_ms: c.wait_ms,
        frame_at: c.frame_at,
        saved_at: c.saved_at,
        width: c.width,
        height: c.height,
    }
}

/// 결과·전체 시간·카메라 간 편차를 채운다
pub fn summarize(rec: &mut TriggerRecord) {
    let ok: Vec<&TriggerShot> = rec.shots.iter().filter(|s| s.ok).collect();
    rec.status = if ok.len() == rec.shots.len() && !ok.is_empty() {
        "ok"
    } else if !ok.is_empty() {
        "partial"
    } else {
        "failed"
    }
    .into();
    rec.total_ms = ok.iter().filter_map(|s| s.total_ms).reduce(f64::max);
    // 편차: 모두 수신 중이면 '프레임이 도착한 시각' 차이, 아니면 '저장된 시각' 차이
    if ok.len() >= 2 {
        let span = |v: Vec<DateTime<Local>>| -> Option<f64> {
            let (mn, mx) = (v.iter().min()?, v.iter().max()?);
            Some((*mx - *mn).num_microseconds().unwrap_or(0) as f64 / 1000.0)
        };
        let frames: Vec<_> = ok.iter().filter_map(|s| s.frame_at).collect();
        if frames.len() == ok.len() {
            rec.skew_ms = span(frames);
            rec.skew_basis = Some("frame".into());
        } else {
            rec.skew_ms = span(ok.iter().filter_map(|s| s.saved_at).collect());
            rec.skew_basis = Some("saved".into());
        }
    }
    let errs: Vec<String> = rec.shots.iter().filter(|s| !s.ok).map(|s| format!("{}: {}", s.camera_name, s.error.clone().unwrap_or_default())).collect();
    rec.error = (!errs.is_empty()).then(|| errs.join(" / "));
}

pub fn source_ko(s: &str) -> &'static str {
    match s {
        "button" => "버튼",
        "http" => "HTTP",
        "tcp" => "PLC TCP",
        "modbus" => "PLC Modbus",
        "auto" => "반복 시험",
        _ => "기타",
    }
}

/// PLC에 돌려줄 결과 한 줄: 결과,번호,전체ms,편차ms
pub fn reply_line(rec: &TriggerRecord) -> String {
    let code = match rec.status.as_str() {
        "ok" => "OK",
        "ignored" => "IGNORED",
        _ => "NG",
    };
    let n = |v: Option<f64>| v.map(|x| format!("{x:.0}")).unwrap_or_else(|| "-".into());
    format!("{code},{},{},{}\r\n", rec.seq, n(rec.total_ms), n(rec.skew_ms))
}

// ---------------- PLC → TCP ----------------

async fn tcp_server(app: Arc<App>, bind: SocketAddr) {
    let listener = match TcpListener::bind(bind).await {
        Ok(l) => l,
        Err(e) => {
            let msg = if e.kind() == std::io::ErrorKind::AddrInUse {
                format!("{bind} 포트를 다른 프로그램이 쓰고 있습니다")
            } else {
                format!("{bind}에서 신호를 받을 수 없습니다: {e}")
            };
            app.store.record_event(Event::new("plc_tcp_error", None, format!("PLC TCP 대기 실패: {msg}")));
            app.trigger.tcp.lock().unwrap().error = Some(msg);
            return;
        }
    };
    {
        let mut st = app.trigger.tcp.lock().unwrap();
        st.listening = true;
        st.error = None;
    }
    app.store.record_event(Event::new("plc_tcp_listen", None, format!("PLC TCP 신호 대기 시작: {bind}")));
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let h = tokio::spawn(tcp_client(app.clone(), sock, peer));
                app.trigger.track(h);
            }
            Err(e) => {
                tracing::warn!("PLC TCP 연결 받기 실패: {e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// 받은 바이트를 보기 좋게: 글자면 그대로, 아니면 16진수
fn printable(b: &[u8]) -> String {
    let text = String::from_utf8_lossy(b);
    let t = text.trim();
    if !t.is_empty() && t.chars().all(|c| !c.is_control()) {
        t.chars().take(80).collect()
    } else {
        b.iter().take(32).map(|x| format!("{x:02X}")).collect::<Vec<_>>().join(" ")
    }
}

/// PLC 메시지 자르기: 줄바꿈(CR/LF)이나 NUL이 끝. 줄바꿈 없이 보내는 PLC도 있어서
/// 데이터가 멈추고 50ms가 지나면 거기까지를 한 메시지로 본다.
pub fn split_messages(buf: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(pos) = buf.iter().position(|&b| b == b'\n' || b == b'\r' || b == 0) {
        let msg: Vec<u8> = buf.drain(..=pos).take(pos).collect();
        if !msg.iter().all(|b| b.is_ascii_whitespace()) {
            out.push(msg);
        }
    }
    out
}

async fn tcp_client(app: Arc<App>, sock: TcpStream, peer: SocketAddr) {
    let _ = sock.set_nodelay(true);
    {
        let mut st = app.trigger.tcp.lock().unwrap();
        st.clients += 1;
        st.last_peer = Some(peer.to_string());
    }
    app.store.record_event(Event::new("plc_connected", None, format!("PLC 연결됨: {peer}")));
    let (mut rd, mut wr) = sock.into_split();
    let (tx, mut rx) = mpsc::channel::<String>(64);
    let writer = tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if wr.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let read = if buf.is_empty() {
            rd.read(&mut chunk).await
        } else {
            match tokio::time::timeout(Duration::from_millis(50), rd.read(&mut chunk)).await {
                Ok(r) => r,
                Err(_) => {
                    let msg = std::mem::take(&mut buf);
                    on_tcp_message(&app, peer, &msg, &tx);
                    continue;
                }
            }
        };
        match read {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
        for msg in split_messages(&mut buf) {
            on_tcp_message(&app, peer, &msg, &tx);
        }
        if buf.len() > 4096 {
            let msg = std::mem::take(&mut buf);
            on_tcp_message(&app, peer, &msg, &tx);
        }
    }
    if !buf.is_empty() && !buf.iter().all(|b| b.is_ascii_whitespace()) {
        on_tcp_message(&app, peer, &buf, &tx);
    }
    {
        let mut st = app.trigger.tcp.lock().unwrap();
        st.clients = st.clients.saturating_sub(1);
    }
    app.store.record_event(Event::new("plc_disconnected", None, format!("PLC 연결 끊김: {peer}")));
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(30), writer).await;
}

fn on_tcp_message(app: &Arc<App>, peer: SocketAddr, msg: &[u8], tx: &mpsc::Sender<String>) {
    let text = printable(msg);
    let cfg = app.trigger.config();
    {
        let mut st = app.trigger.tcp.lock().unwrap();
        st.messages += 1;
        st.last_message = Some(text.clone());
        st.last_at = Some(Local::now());
        let kw = cfg.tcp_keyword.trim();
        if !kw.is_empty() && !String::from_utf8_lossy(msg).contains(kw) {
            st.skipped += 1;
            return;
        }
    }
    let (app, tx) = (app.clone(), tx.clone());
    tokio::spawn(async move {
        let rec = fire(&app, "tcp", Some(format!("{peer} · {text}"))).await;
        if cfg.tcp_reply {
            let _ = tx.send(reply_line(&rec)).await;
        }
    });
}

/// TCP 수신이 실제로 되는지 이 PC 안에서 신호를 한 번 보내 본다 (촬영도 실제로 한다)
pub async fn self_test(app: &Arc<App>) -> Result<String, String> {
    let cfg = app.trigger.config();
    if !cfg.tcp_enabled {
        return Err("PLC TCP 신호 받기가 꺼져 있습니다".into());
    }
    if !app.trigger.tcp_listening() {
        return Err(app.trigger.tcp.lock().unwrap().error.clone().unwrap_or_else(|| "아직 신호 대기를 시작하지 못했습니다".into()));
    }
    let mut addr: SocketAddr = cfg.tcp_bind.trim().parse().map_err(|e| format!("주소 형식 오류: {e}"))?;
    if addr.ip().is_unspecified() {
        addr.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
    }
    let mut sock = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
        .await
        .map_err(|_| format!("{addr}에 연결 시간 초과"))?
        .map_err(|e| format!("{addr}에 연결할 수 없습니다: {e}"))?;
    let word = if cfg.tcp_keyword.trim().is_empty() { "TEST".to_string() } else { format!("{} TEST", cfg.tcp_keyword.trim()) };
    sock.write_all(format!("{word}\r\n").as_bytes()).await.map_err(|e| e.to_string())?;
    if !cfg.tcp_reply {
        return Ok(format!("'{word}'를 보냈습니다 (결과 응답은 꺼져 있음)"));
    }
    let mut line = String::new();
    let mut r = BufReader::new(sock);
    tokio::time::timeout(Duration::from_secs(30), r.read_line(&mut line))
        .await
        .map_err(|_| "30초 안에 결과 응답이 없습니다".to_string())?
        .map_err(|e| e.to_string())?;
    Ok(format!("'{word}' 보냄 → 응답 '{}'", line.trim()))
}

// ---------------- Modbus TCP (이 PC가 PLC를 읽음) ----------------

pub struct Modbus {
    sock: TcpStream,
    tid: u16,
    unit: u8,
}

fn modbus_exception(code: u8) -> &'static str {
    match code {
        1 => "지원하지 않는 기능 (영역 종류 확인)",
        2 => "없는 주소 (주소 확인)",
        3 => "잘못된 값",
        4 => "PLC 장치 오류",
        6 => "PLC가 바쁨",
        10 | 11 => "게이트웨이 경로 오류 (Unit ID 확인)",
        _ => "알 수 없는 예외",
    }
}

impl Modbus {
    pub fn new(sock: TcpStream, unit: u8) -> Self {
        Self { sock, tid: 0, unit }
    }

    async fn call(&mut self, pdu: &[u8]) -> Result<Vec<u8>, String> {
        self.tid = self.tid.wrapping_add(1);
        let tid = self.tid;
        let mut req = Vec::with_capacity(7 + pdu.len());
        req.extend_from_slice(&tid.to_be_bytes());
        req.extend_from_slice(&[0, 0]);
        req.extend_from_slice(&((pdu.len() + 1) as u16).to_be_bytes());
        req.push(self.unit);
        req.extend_from_slice(pdu);
        let sock = &mut self.sock;
        let body = tokio::time::timeout(Duration::from_secs(2), async {
            sock.write_all(&req).await.map_err(|e| format!("보내기 실패: {e}"))?;
            loop {
                let mut h = [0u8; 7];
                sock.read_exact(&mut h).await.map_err(|e| format!("응답 읽기 실패: {e}"))?;
                let len = u16::from_be_bytes([h[4], h[5]]) as usize;
                if !(2..=260).contains(&len) {
                    return Err(format!("응답 길이 오류 ({len})"));
                }
                let mut body = vec![0u8; len - 1];
                sock.read_exact(&mut body).await.map_err(|e| format!("응답 읽기 실패: {e}"))?;
                // 늦게 온 지난 응답이면 버리고 다음 것을 읽는다
                if u16::from_be_bytes([h[0], h[1]]) == tid {
                    return Ok(body);
                }
            }
        })
        .await
        .map_err(|_| "PLC 응답 시간 초과 (2초)".to_string())??;
        if body.first().map_or(true, |f| f & 0x80 != 0) {
            let code = body.get(1).copied().unwrap_or(0);
            return Err(format!("PLC 예외 응답 {code}: {}", modbus_exception(code)));
        }
        Ok(body)
    }

    /// 한 칸 읽기: coil·discrete는 0/1, register는 값
    pub async fn read(&mut self, kind: &str, addr: u16) -> Result<u16, String> {
        let fc = match kind {
            "coil" => 1,
            "discrete" => 2,
            "holding" => 3,
            _ => 4,
        };
        let a = addr.to_be_bytes();
        let b = self.call(&[fc, a[0], a[1], 0, 1]).await?;
        let v = match fc {
            1 | 2 => b.get(2).map(|v| (v & 1) as u16),
            _ => (b.len() >= 4).then(|| u16::from_be_bytes([b[2], b[3]])),
        };
        v.ok_or_else(|| "응답 길이 오류".to_string())
    }

    pub async fn write_register(&mut self, addr: u16, value: u16) -> Result<(), String> {
        let (a, v) = (addr.to_be_bytes(), value.to_be_bytes());
        self.call(&[6, a[0], a[1], v[0], v[1]]).await.map(|_| ())
    }
}

async fn modbus_loop(app: Arc<App>, cfg: TriggerConfig) {
    let target = cfg.modbus_addr();
    let poll = Duration::from_millis(cfg.modbus_poll_ms.max(10));
    let mut fail_logged = false;
    loop {
        let conn = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(target.as_str())).await;
        let sock = match conn {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                modbus_fail(&app, &mut fail_logged, format!("PLC({target})에 연결할 수 없습니다: {e}"));
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
            Err(_) => {
                modbus_fail(&app, &mut fail_logged, format!("PLC({target}) 연결 시간 초과"));
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        let _ = sock.set_nodelay(true);
        let mut m = Modbus::new(sock, cfg.modbus_unit);
        // 처음 한 번 읽어 봐야 '연결됨' (주소·영역이 틀리면 여기서 걸린다)
        let first = m.read(&cfg.modbus_kind, cfg.modbus_address).await;
        let mut prev = match first {
            Ok(v) => Some(v != 0),
            Err(e) => {
                modbus_fail(&app, &mut fail_logged, format!("PLC({target}) 읽기 실패: {e}"));
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        {
            let mut st = app.trigger.modbus.lock().unwrap();
            st.connected = true;
            st.error = None;
            st.last_value = prev.map(u16::from);
        }
        app.store.record_event(Event::new(
            "plc_modbus_connected",
            None,
            format!("PLC Modbus 연결됨: {target} · {} {} (현재 값 {})", cfg.modbus_kind, cfg.modbus_address, u8::from(prev.unwrap_or(false))),
        ));
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<u16>();
        let mut tick = tokio::time::interval(poll);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let err = loop {
            tick.tick().await;
            let mut ack_err = None;
            while let Ok(code) = ack_rx.try_recv() {
                match m.write_register(cfg.modbus_ack_address as u16, code).await {
                    Ok(()) => app.trigger.modbus.lock().unwrap().acks += 1,
                    Err(e) => {
                        ack_err = Some(format!("결과 쓰기 실패: {e}"));
                        break;
                    }
                }
            }
            if let Some(e) = ack_err {
                break e;
            }
            match m.read(&cfg.modbus_kind, cfg.modbus_address).await {
                Ok(v) => {
                    let on = v != 0;
                    let rise = prev == Some(false) && on;
                    prev = Some(on);
                    {
                        let mut st = app.trigger.modbus.lock().unwrap();
                        st.polls += 1;
                        st.last_value = Some(v);
                        if rise {
                            st.rises += 1;
                            st.last_rise = Some(Local::now());
                        }
                    }
                    if rise {
                        let (app, tx, ack) = (app.clone(), ack_tx.clone(), cfg.modbus_ack_address);
                        let detail = format!("{target} · {} {} = {v}", cfg.modbus_kind, cfg.modbus_address);
                        tokio::spawn(async move {
                            let rec = fire(&app, "modbus", Some(detail)).await;
                            if ack >= 0 && rec.status != "ignored" {
                                let _ = tx.send(if rec.status == "ok" { 1 } else { 2 });
                            }
                        });
                    }
                }
                Err(e) => break e,
            }
        };
        {
            let mut st = app.trigger.modbus.lock().unwrap();
            st.connected = false;
            st.poll_errors += 1;
            st.error = Some(err.clone());
        }
        app.store.record_event(Event::new("plc_modbus_lost", None, format!("PLC Modbus 연결 끊김: {err}")));
        fail_logged = true;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// 연결 실패는 처음 한 번만 이벤트로 남긴다 (몇 초마다 다시 시도하므로)
fn modbus_fail(app: &App, logged: &mut bool, msg: String) {
    {
        let mut st = app.trigger.modbus.lock().unwrap();
        st.connected = false;
        st.error = Some(msg.clone());
    }
    if !*logged {
        *logged = true;
        app.store.record_event(Event::new("plc_modbus_error", None, msg));
    }
}

// ---------------- 반복 촬영 시험 ----------------

pub fn start_auto(app: &Arc<App>, interval_ms: u64, count: u64) -> Result<(), String> {
    let min = app.trigger.config().min_interval_ms.max(200);
    if interval_ms < min {
        return Err(format!("간격은 {min}ms 이상이어야 합니다 (신호 무시 간격보다 짧으면 무시됨)"));
    }
    if interval_ms > 3_600_000 || count > 1_000_000 {
        return Err("간격은 1시간, 횟수는 100만 번 이하로 정해 주세요".into());
    }
    stop_auto(app, false);
    *app.trigger.auto.lock().unwrap() =
        AutoStatus { running: true, interval_ms, count, started_at: Some(Local::now()), ..Default::default() };
    app.store.record_event(Event::new(
        "auto_trigger_start",
        None,
        format!("반복 촬영 시험 시작: {interval_ms}ms 간격 · {}", if count == 0 { "멈출 때까지".to_string() } else { format!("{count}회") }),
    ));
    let h = tokio::spawn(auto_loop(app.clone(), interval_ms, count));
    *app.trigger.auto_task.lock().unwrap() = Some(h);
    Ok(())
}

pub fn stop_auto(app: &App, log: bool) {
    if let Some(h) = app.trigger.auto_task.lock().unwrap().take() {
        h.abort();
    }
    let st = {
        let mut a = app.trigger.auto.lock().unwrap();
        let was = a.running;
        a.running = false;
        was.then(|| a.clone())
    };
    if let (true, Some(a)) = (log, st) {
        app.store.record_event(Event::new("auto_trigger_stop", None, format!("반복 촬영 시험 중지: {}회 중 성공 {} · 실패 {}", a.done, a.ok, a.failed)));
    }
}

async fn auto_loop(app: Arc<App>, interval_ms: u64, count: u64) {
    let interval = Duration::from_millis(interval_ms);
    let mut i = 0u64;
    while count == 0 || i < count {
        let t0 = Instant::now();
        let label = if count == 0 { format!("{}회째", i + 1) } else { format!("{}/{count}", i + 1) };
        let rec = fire(&app, "auto", Some(label)).await;
        i += 1;
        {
            let mut a = app.trigger.auto.lock().unwrap();
            a.done = i;
            if rec.status == "ok" {
                a.ok += 1;
            } else {
                a.failed += 1;
            }
        }
        let el = t0.elapsed();
        if el < interval {
            tokio::time::sleep(interval - el).await;
        }
    }
    let a = {
        let mut a = app.trigger.auto.lock().unwrap();
        a.running = false;
        a.clone()
    };
    app.store.record_event(Event::new("auto_trigger_done", None, format!("반복 촬영 시험 끝: {}회 · 성공 {} · 실패 {}", a.done, a.ok, a.failed)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shot(cam: &str, ok: bool, total: f64, frame_ms: Option<i64>) -> TriggerShot {
        // 모든 카메라가 같은 기준 시각에서 출발
        static BASE: std::sync::OnceLock<DateTime<Local>> = std::sync::OnceLock::new();
        let base = *BASE.get_or_init(Local::now);
        TriggerShot {
            camera_id: cam.into(),
            camera_name: cam.into(),
            ok,
            total_ms: ok.then_some(total),
            frame_at: frame_ms.map(|m| base + chrono::Duration::milliseconds(m)),
            saved_at: Some(base + chrono::Duration::milliseconds(total as i64)),
            error: (!ok).then(|| "영상 끊김".into()),
            ..Default::default()
        }
    }

    #[test]
    fn summarizes_skew_and_status() {
        let mut r = TriggerRecord { shots: vec![shot("cam01", true, 210.0, Some(20)), shot("cam02", true, 230.0, Some(28))], ..Default::default() };
        summarize(&mut r);
        assert_eq!(r.status, "ok");
        assert_eq!(r.total_ms, Some(230.0));
        assert_eq!(r.skew_basis.as_deref(), Some("frame"));
        assert!((r.skew_ms.unwrap() - 8.0).abs() < 0.01);
        // 한 대가 수신 없이 찍으면 저장 시각으로 비교
        let mut r = TriggerRecord { shots: vec![shot("cam01", true, 200.0, Some(20)), shot("cam02", true, 900.0, None)], ..Default::default() };
        summarize(&mut r);
        assert_eq!(r.skew_basis.as_deref(), Some("saved"));
        let mut r = TriggerRecord { shots: vec![shot("cam01", true, 200.0, Some(20)), shot("cam02", false, 0.0, None)], ..Default::default() };
        summarize(&mut r);
        assert_eq!(r.status, "partial");
        assert!(r.error.unwrap().contains("cam02"));
        assert_eq!(reply_line(&TriggerRecord { status: "ok".into(), seq: 7, total_ms: Some(212.4), ..Default::default() }), "OK,7,212,-\r\n");
    }

    #[test]
    fn splits_plc_messages() {
        let mut b = b"TRIG\r\nTRIG\n\nHB".to_vec();
        let m = split_messages(&mut b);
        assert_eq!(m, vec![b"TRIG".to_vec(), b"TRIG".to_vec()]);
        assert_eq!(b, b"HB".to_vec()); // 줄바꿈이 없는 나머지는 50ms 뒤에 한 메시지로
        assert_eq!(printable(&[0x01, 0x02, 0xff]), "01 02 FF");
    }

    #[tokio::test]
    async fn modbus_reads_and_writes() {
        // 간단한 가짜 PLC: coil 0 = 1, holding 5에 쓴 값을 기억
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut written = 0u16;
            loop {
                let mut h = [0u8; 7];
                if s.read_exact(&mut h).await.is_err() {
                    break;
                }
                let len = u16::from_be_bytes([h[4], h[5]]) as usize;
                let mut pdu = vec![0u8; len - 1];
                s.read_exact(&mut pdu).await.unwrap();
                let resp: Vec<u8> = match pdu[0] {
                    1 => vec![1, 1, 1],
                    3 => {
                        let a = u16::from_be_bytes([pdu[1], pdu[2]]);
                        if a == 5 { vec![3, 2, (written >> 8) as u8, written as u8] } else { vec![0x83, 2] }
                    }
                    6 => {
                        written = u16::from_be_bytes([pdu[3], pdu[4]]);
                        pdu.clone()
                    }
                    f => vec![f | 0x80, 1],
                };
                let mut out = vec![h[0], h[1], 0, 0];
                out.extend_from_slice(&((resp.len() + 1) as u16).to_be_bytes());
                out.push(h[6]);
                out.extend_from_slice(&resp);
                s.write_all(&out).await.unwrap();
            }
        });
        let mut m = Modbus::new(TcpStream::connect(addr).await.unwrap(), 1);
        assert_eq!(m.read("coil", 0).await.unwrap(), 1);
        m.write_register(5, 2).await.unwrap();
        assert_eq!(m.read("holding", 5).await.unwrap(), 2);
        let e = m.read("holding", 9).await.unwrap_err();
        assert!(e.contains("없는 주소"), "{e}");
        assert!(m.read("discrete", 0).await.unwrap_err().contains("지원하지 않는"));
        drop(m);
        let _ = server.await;
    }
}
