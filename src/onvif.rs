//! ONVIF 카메라 찾기(WS-Discovery)와 연결 테스트.
//!
//! 연결 테스트 순서
//!   1. 네트워크 연결 확인 (RTSP 554 / ONVIF 80)
//!   2. ONVIF 장치 정보 (제조사·모델)
//!   3. ONVIF 프로파일 → 해상도·fps·비트레이트와 RTSP 주소 (GetProfiles → GetStreamUri)
//!   4. 안 되면 제조사별로 흔한 RTSP 주소를 차례로 시도
//!   5. FFmpeg로 메인·서브 스트림에 실제로 붙어서 한 프레임 받아 보기 (계정 확인 포함)

use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use base64::Engine;
use reqwest::header::CONTENT_TYPE;
use serde::Serialize;
use sha1::{Digest, Sha1};

use crate::stream;

/// ONVIF로 주소를 못 얻었을 때 시도할 흔한 RTSP 주소: (메인, 서브 — 모르면 빈 문자열, 제조사)
const COMMON_RTSP: &[(&str, &str, &str)] = &[
    ("/Streaming/Channels/101", "/Streaming/Channels/102", "Hikvision"),
    ("/cam/realmonitor?channel=1&subtype=0", "/cam/realmonitor?channel=1&subtype=1", "Dahua"),
    ("/axis-media/media.amp", "", "Axis"),
    ("/profile2/media.smp", "/profile3/media.smp", "Hanwha"),
    ("/media/1/1/Profile1", "/media/1/2/Profile2", "IDIS"),
    ("/stream1", "/stream2", "일반"),
    ("/live", "", "일반"),
];

// ---------------- 카메라 찾기 ----------------

#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Discovered {
    pub ip: String,
    /// ONVIF 주소 (탐색에 답하지 않은 장치는 빈 문자열)
    pub xaddr: String,
    pub name: Option<String>,
    pub model: Option<String>,
    pub manufacturer: Option<String>,
    /// 장치 고유 값 (WS-Discovery의 EndpointReference, 보통 `urn:uuid:…`). 공유기를 껐다 켜 IP가 바뀌어도
    /// 같은 카메라인지 알아보는 데 쓴다
    pub uid: Option<String>,
    /// MAC 주소 (같은 대역일 때만 알 수 있다). ONVIF 탐색에 답하지 않는 카메라를 알아보는 데 쓴다
    pub mac: Option<String>,
    /// RTSP가 응답한 포트 (TCP로 직접 확인했을 때)
    pub rtsp_port: Option<u16>,
    /// 찾은 방법: "onvif"(ONVIF 탐색에 답함) | "rtsp"(탐색에는 답이 없고 RTSP 포트만 응답)
    pub via: String,
}

const PROBE: &str = r#"<?xml version="1.0" encoding="UTF-8"?><e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope" xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery" xmlns:dn="http://www.onvif.org/ver10/network/wsdl"><e:Header><w:MessageID>uuid:{ID}</w:MessageID><w:To e:mustUnderstand="true">urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To><w:Action e:mustUnderstand="true">http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action></e:Header><e:Body><d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe></e:Body></e:Envelope>"#;

/// 탐색 메시지를 받는 포트 (WS-Discovery)
const WSD_PORT: u16 = 3702;

/// 같은 네트워크의 ONVIF 카메라를 찾는다 (블로킹, `wait` 동안 응답을 모은다). `nets`: 이 PC의 (주소, 대역 길이)
///
/// 두 가지 방법을 같이 쓴다:
///  1. 멀티캐스트로 한 번에 묻기 (표준 방식). Wi-Fi·유선이 같이 있으면 어댑터마다 따로 보낸다.
///  2. 같은 대역의 주소 하나하나에 같은 질문을 직접 보내기.
///
/// 1번만으로는 못 찾는 환경이 있다: 멀티캐스트 질문에 대한 답은 '내가 보낸 곳이 아닌 곳에서 온 것'이라 Windows 방화벽이
/// 막을 수 있고(네트워크가 '공용'으로 잡혔거나 보안 프로그램이 있을 때), 공유기가 무선↔유선 사이의 멀티캐스트를 넘겨주지
/// 않기도 한다. 2번은 직접 물은 주소에서 오는 답이라 그런 영향을 받지 않는다. 로그인이나 영상 접속은 하지 않는다.
pub fn discover(wait: Duration, nets: &[(Ipv4Addr, u8)]) -> Vec<Discovered> {
    let mut nets: Vec<(Ipv4Addr, u8)> = nets.to_vec();
    if nets.is_empty() {
        nets.push((default_ipv4().unwrap_or(Ipv4Addr::UNSPECIFIED), 24));
    }
    let msg = PROBE.replace("{ID}", &pseudo_uuid());
    let handles: Vec<_> = nets
        .into_iter()
        .map(|(local, prefix)| {
            let msg = msg.clone();
            std::thread::spawn(move || discover_on(local, prefix, &msg, wait))
        })
        .collect();
    let mut out: Vec<Discovered> = Vec::new();
    for h in handles {
        for d in h.join().unwrap_or_default() {
            if !out.iter().any(|x| x.ip == d.ip) {
                out.push(d);
            }
        }
    }
    out.sort_by_key(|d| d.ip.parse::<Ipv4Addr>().map(u32::from).unwrap_or(u32::MAX));
    out
}

fn discover_on(local: Ipv4Addr, prefix: u8, msg: &str, wait: Duration) -> Vec<Discovered> {
    let Ok(sock) = UdpSocket::bind((local, 0)) else { return Vec::new() };
    let _ = sock.set_read_timeout(Some(Duration::from_millis(100)));
    if !local.is_unspecified() {
        let _ = sock.set_multicast_loop_v4(false);
        // 멀티캐스트가 이 어댑터로 나가게 못 박는다 (주소에 묶는 것만으로는 다른 어댑터로 나가는 PC가 있다)
        set_multicast_if(&sock, local);
    }
    let target: SocketAddr = SocketAddr::from((Ipv4Addr::new(239, 255, 255, 250), WSD_PORT));
    // UDP라 한 번 빠질 수 있어서 두 번 보낸다
    for _ in 0..2 {
        let _ = sock.send_to(msg.as_bytes(), target);
    }
    sweep_and_collect(&sock, msg, &sweep_targets(local, prefix), WSD_PORT, wait)
}

#[cfg(windows)]
fn set_multicast_if(sock: &UdpSocket, local: Ipv4Addr) {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{setsockopt, IPPROTO_IP, IP_MULTICAST_IF};
    let addr = u32::from_ne_bytes(local.octets());
    unsafe {
        setsockopt(sock.as_raw_socket() as usize, IPPROTO_IP as i32, IP_MULTICAST_IF as i32, &addr as *const u32 as *const u8, 4);
    }
}

#[cfg(not(windows))]
fn set_multicast_if(_sock: &UdpSocket, _local: Ipv4Addr) {}

/// 직접 물어볼 주소들: 내 주소가 든 대역의 나머지 주소 (대역이 /24보다 넓으면 내 주소가 든 /24만 — 최대 253개)
pub(crate) fn sweep_targets(local: Ipv4Addr, prefix: u8) -> Vec<Ipv4Addr> {
    // (이 PC 자신(127.x)은 실제로는 대상이 아니다 — `local_ipv4_nets`가 넘겨주지 않는다. 시험할 때만 넘어온다)
    if local.is_unspecified() || local.is_link_local() || prefix >= 31 {
        return Vec::new();
    }
    let mask = u32::MAX << (32 - prefix.max(24));
    let me = u32::from(local);
    let (net, bcast) = (me & mask, (me & mask) | !mask);
    (net + 1..bcast).filter(|a| *a != me).map(Ipv4Addr::from).collect()
}

/// `targets`에 탐색 질문을 직접 보내고, 이 소켓으로 오는 답(멀티캐스트에 대한 답 포함)을 `wait` 동안 모은다.
/// 중간에 아직 답이 없는 주소에 한 번 더 보낸다 (처음 보낸 것은 주소를 찾는 동안(ARP) 버려질 수 있다).
fn sweep_and_collect(sock: &UdpSocket, msg: &str, targets: &[Ipv4Addr], port: u16, wait: Duration) -> Vec<Discovered> {
    let deadline = Instant::now() + wait;
    let send = |found: &[Discovered]| {
        for (i, t) in targets.iter().enumerate() {
            if found.iter().any(|d| d.ip == t.to_string()) {
                continue;
            }
            let _ = sock.send_to(msg.as_bytes(), (*t, port));
            // 한꺼번에 쏟아 내지 않는다
            if i % 32 == 31 {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    };
    let mut out = Vec::new();
    send(&out);
    collect_until(sock, Instant::now() + wait / 3, &mut out);
    send(&out);
    collect_until(sock, deadline, &mut out);
    out
}

fn collect_until(sock: &UdpSocket, deadline: Instant, out: &mut Vec<Discovered>) {
    let mut buf = vec![0u8; 65_535];
    while Instant::now() < deadline {
        let Ok((n, from)) = sock.recv_from(&mut buf) else { continue };
        let ip = from.ip().to_string();
        if out.iter().any(|d| d.ip == ip) {
            continue;
        }
        if let Some(d) = parse_probe_match(&ip, &String::from_utf8_lossy(&buf[..n])) {
            out.push(d);
        }
    }
}

fn collect_matches(sock: &UdpSocket, wait: Duration, only: Option<Ipv4Addr>) -> Vec<Discovered> {
    let mut out: Vec<Discovered> = Vec::new();
    let mut buf = vec![0u8; 65_535];
    let deadline = Instant::now() + wait;
    while Instant::now() < deadline {
        let Ok((n, from)) = sock.recv_from(&mut buf) else { continue };
        let ip = from.ip().to_string();
        if out.iter().any(|d| d.ip == ip) || only.map_or(false, |o| o.to_string() != ip) {
            continue;
        }
        if let Some(d) = parse_probe_match(&ip, &String::from_utf8_lossy(&buf[..n])) {
            out.push(d);
            if only.is_some() {
                break;
            }
        }
    }
    out
}

/// IP를 알 때: 그 주소로 직접 ONVIF 찾기 메시지를 보낸다 (대역이 달라도 길만 있으면 된다, 인증 없음)
pub fn probe_unicast(ip: Ipv4Addr, wait: Duration) -> Option<Discovered> {
    let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
    let _ = sock.set_read_timeout(Some(Duration::from_millis(200)));
    let msg = PROBE.replace("{ID}", &pseudo_uuid());
    for _ in 0..2 {
        let _ = sock.send_to(msg.as_bytes(), (ip, 3702));
    }
    collect_matches(&sock, wait, Some(ip)).into_iter().next()
}

fn parse_probe_match(ip: &str, xml: &str) -> Option<Discovered> {
    let xaddrs = tag_text(xml, "XAddrs").unwrap_or_default();
    // NAS·프린터 같은 ONVIF가 아닌 WS-Discovery 응답은 건너뛴다
    let xaddr = xaddrs
        .split_whitespace()
        .find(|u| u.contains("onvif") && u.contains(ip))
        .or_else(|| xaddrs.split_whitespace().find(|u| u.contains("onvif")))?;
    let scopes = tag_text(xml, "Scopes").unwrap_or_default();
    let scope = |prefix: &str| scopes.split_whitespace().find_map(|s| s.strip_prefix(prefix)).map(percent_decode);
    Some(Discovered {
        ip: ip.to_string(),
        xaddr: xaddr.to_string(),
        name: scope("onvif://www.onvif.org/name/"),
        model: scope("onvif://www.onvif.org/hardware/"),
        manufacturer: scope("onvif://www.onvif.org/mfr/").or_else(|| scope("onvif://www.onvif.org/manufacturer/")),
        uid: xml.find("EndpointReference").and_then(|i| tag_text(&xml[i..], "Address")).map(|a| a.to_ascii_lowercase()),
        mac: None,
        rtsp_port: None,
        via: "onvif".into(),
    })
}

/// 기본 경로로 나가는 로컬 IPv4 (실제로 패킷은 보내지 않는다)
fn default_ipv4() -> Option<Ipv4Addr> {
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("8.8.8.8:80").ok()?;
    match s.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(v4) if !v4.is_unspecified() => Some(v4),
        _ => None,
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn pseudo_uuid() -> String {
    let seed = format!("{:?}-{}-{}", Instant::now(), COUNTER.fetch_add(1, Ordering::Relaxed), chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
    let x: String = Sha1::digest(seed.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &x[0..8], &x[8..12], &x[12..16], &x[16..20], &x[20..32])
}

// ---------------- 연결 테스트 ----------------

#[derive(Debug, Clone, Serialize)]
pub struct Step {
    pub name: String,
    pub ok: bool,
    /// 성공했지만 알아 둘 점이 있음 (예: 카메라 인증이 꺼져 있음)
    pub warn: bool,
    pub detail: String,
}

/// 카메라 설정(ONVIF)에 적힌 프로파일 값
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProfileInfo {
    pub token: String,
    pub name: Option<String>,
    pub encoding: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub fps: Option<f64>,
    pub bitrate_kbps: Option<u32>,
    pub gov: Option<u32>,
    /// 계정이 빠진 RTSP 주소
    pub rtsp: Option<String>,
}

/// FFmpeg로 실제 접속해 본 결과
#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamCheck {
    pub ok: bool,
    pub url: String,
    pub codec: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub fps: Option<f64>,
    pub ms: Option<f64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TestResult {
    pub ok: bool,
    pub error: Option<String>,
    pub steps: Vec<Step>,
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub firmware: Option<String>,
    pub profiles: Vec<ProfileInfo>,
    pub main: Option<StreamCheck>,
    pub sub: Option<StreamCheck>,
    /// 메인 스트림에서 받은 한 프레임 (data URL)
    pub image: Option<String>,
    /// 계정 없이도 RTSP 영상이 열리는지 (true면 카메라 인증이 꺼져 있어 비밀번호를 검사할 수 없음)
    pub auth_open: Option<bool>,
    /// 네트워크 연결이 안 될 때: 대역이 같은지·어떻게 하면 되는지
    pub diagnosis: Option<crate::netscan::Diagnosis>,
}

impl TestResult {
    fn step(&mut self, name: &str, ok: bool, detail: impl Into<String>) {
        self.steps.push(Step { name: name.into(), ok, warn: false, detail: detail.into() });
    }

    fn warn_step(&mut self, name: &str, detail: impl Into<String>) {
        self.steps.push(Step { name: name.into(), ok: true, warn: true, detail: detail.into() });
    }
}

pub struct TestInput {
    pub host: String,
    pub username: String,
    pub password: String,
    /// 직접 입력한 주소가 있으면 ONVIF 대신 그 주소로 시험
    pub rtsp_main: Option<String>,
    pub rtsp_sub: Option<String>,
    pub ffmpeg: Option<PathBuf>,
}

fn with_creds(url: &str, user: &str, pass: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut u) if !user.is_empty() => {
            let _ = u.set_username(user);
            let _ = u.set_password(Some(pass));
            u.to_string()
        }
        _ => url.to_string(),
    }
}

fn strip_creds(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut u) => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        Err(_) => url.to_string(),
    }
}

async fn port_open(host: &str, port: u16) -> Option<f64> {
    let t0 = Instant::now();
    match tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect((host, port))).await {
        Ok(Ok(_)) => Some(t0.elapsed().as_secs_f64() * 1000.0),
        _ => None,
    }
}

fn describe_profile(p: &ProfileInfo) -> String {
    let mut parts = Vec::new();
    if let (Some(w), Some(h)) = (p.width, p.height) {
        parts.push(format!("{w}×{h}"));
    }
    if let Some(f) = p.fps {
        parts.push(format!("{f:.0}fps"));
    }
    if let Some(b) = p.bitrate_kbps {
        parts.push(format!("{:.1}Mbps", b as f64 / 1000.0));
    }
    if let Some(e) = &p.encoding {
        parts.push(e.clone());
    }
    parts.join(" · ")
}

pub async fn test_camera(client: &reqwest::Client, inp: TestInput) -> TestResult {
    let mut r = TestResult::default();
    let host_port = normalize_host(&inp.host);
    if host_port.is_empty() {
        r.error = Some("카메라 IP 주소를 입력하세요".into());
        return r;
    }
    let ip = host_port.split(':').next().unwrap_or(&host_port).to_string();
    let (user, pass) = (inp.username.as_str(), inp.password.as_str());
    let Some(ffmpeg) = inp.ffmpeg.clone() else {
        r.error = Some("FFmpeg를 찾을 수 없어 영상을 시험할 수 없습니다".into());
        return r;
    };

    // 1) 네트워크: RTSP와 ONVIF가 어느 포트에서 답하는지부터 찾는다 (기본 포트가 아닌 카메라가 있다)
    let manual_main = inp.rtsp_main.as_deref().map(str::trim).filter(|u| !u.is_empty()).map(strip_creds);
    let manual_sub = inp.rtsp_sub.as_deref().map(str::trim).filter(|u| !u.is_empty()).map(strip_creds);
    let typed_port = host_port.rsplit_once(':').and_then(|(_, p)| p.parse::<u16>().ok());
    let manual_port = manual_main.as_deref().and_then(|u| reqwest::Url::parse(u).ok()).map(|u| u.port().unwrap_or(554));
    let (rtsp_port, rtsp_ok) = find_rtsp_port(&ip, manual_port, typed_port).await;
    // 입력한 포트가 RTSP 포트였으면 ONVIF 포트로는 쓰지 않는다
    let onvif = find_onvif(client, &ip, typed_port.filter(|p| !(rtsp_ok.is_some() && *p == rtsp_port))).await;
    if rtsp_ok.is_none() && onvif.is_none() {
        r.step("네트워크 연결", false, format!("{ip}의 RTSP({rtsp_port})·ONVIF 포트에 2초 안에 연결되지 않음"));
        let d = crate::netscan::diagnose(&ip).await;
        r.error = Some(format!("카메라에 연결할 수 없습니다. {}", d.summary));
        r.diagnosis = Some(d);
        return r;
    }
    r.step(
        "네트워크 연결",
        rtsp_ok.is_some(),
        format!(
            "RTSP {} · ONVIF {}",
            rtsp_ok.map(|m| format!("{m:.0}ms{}", if rtsp_port == 554 { String::new() } else { format!(" (포트 {rtsp_port})") })).unwrap_or_else(|| "응답 없음".into()),
            onvif.as_ref().map(|o| if o.port == 80 { "응답".to_string() } else { format!("응답 (포트 {})", o.port) }).unwrap_or_else(|| "응답 없음".into())
        ),
    );

    // 2~3) ONVIF
    let mut auth_refused = false;
    if let Some(o) = &onvif {
        let onvif_host = if o.port == 80 { ip.clone() } else { format!("{ip}:{}", o.port) };
        let dev = format!("http://{onvif_host}/onvif/device_service");
        // 카메라 시계에 맞춰서 묻는다 (시계가 틀린 카메라는 올바른 비밀번호도 거절한다 — `clock_offset`)
        let offset = o.clock_offset;
        if !user.is_empty() && offset.abs() >= 60 {
            r.warn_step("카메라 시계", format!("카메라 시계가 이 PC와 {} 다릅니다 → 카메라 시각에 맞춰 접속했습니다 (녹화 시각은 이 PC 시계를 씁니다)", describe_gap(offset)));
        }
        match soap(client, &dev, user, pass, offset, GET_DEVICE_INFO).await {
            Ok(x) => {
                r.manufacturer = tag_text(&x, "Manufacturer").map(str::to_string);
                r.model = tag_text(&x, "Model").map(str::to_string);
                r.firmware = tag_text(&x, "FirmwareVersion").map(str::to_string);
                let label = [r.manufacturer.as_deref(), r.model.as_deref()].into_iter().flatten().collect::<Vec<_>>().join(" ");
                r.step("ONVIF 장치 정보", true, if label.is_empty() { "응답 확인".to_string() } else { label });
            }
            Err(e) => r.step("ONVIF 장치 정보", false, e),
        }
        match onvif_profiles(client, &dev, &onvif_host, user, pass, offset).await {
            Ok(list) if !list.is_empty() => {
                let desc = list.iter().map(|p| format!("{}: {}", p.name.clone().unwrap_or_else(|| p.token.clone()), describe_profile(p))).collect::<Vec<_>>().join(" / ");
                r.step("ONVIF 프로파일", true, format!("{}개 · {desc}", list.len()));
                r.profiles = list;
            }
            Ok(_) => r.step("ONVIF 프로파일", false, "프로파일이 없습니다"),
            Err(e) => r.step("ONVIF 프로파일", false, e),
        }
    }

    // 메인 = 가장 큰 해상도, 서브 = 가장 작은 해상도 (RTSP 주소가 있는 것 중)
    let mut with_url: Vec<&ProfileInfo> = r.profiles.iter().filter(|p| p.rtsp.is_some()).collect();
    with_url.sort_by_key(|p| std::cmp::Reverse(p.width.unwrap_or(0) * p.height.unwrap_or(0)));
    let main_url = manual_main.clone().or_else(|| with_url.first().and_then(|p| p.rtsp.clone()));
    let sub_url = manual_sub.clone().or_else(|| (with_url.len() > 1).then(|| with_url.last().and_then(|p| p.rtsp.clone())).flatten());
    drop(with_url);

    // 5) 실제 접속
    let timeout = Duration::from_secs(12);
    let mut main_check: Option<StreamCheck> = None;
    if let Some(url) = &main_url {
        main_check = Some(check_stream(&ffmpeg, url, user, pass, timeout, &mut r).await);
    } else if rtsp_ok.is_some() {
        // 4) ONVIF로 못 찾았으면 흔한 주소를 차례로 (RTSP 포트가 닫혀 있으면 해 볼 것이 없다)
        r.step("RTSP 주소", false, "ONVIF로 주소를 얻지 못해 제조사별 흔한 주소로 시도합니다");
        for (path, sub_path, vendor) in COMMON_RTSP {
            let url = format!("rtsp://{ip}:{rtsp_port}{path}");
            match stream::probe(&ffmpeg, &with_creds(&url, user, pass), Duration::from_secs(6)).await {
                Ok(p) => {
                    r.step("RTSP 주소", true, format!("{vendor} 기본 주소에서 영상 확인: {url}"));
                    main_check = Some(fill_check(&url, &p, &mut r));
                    // 같은 제조사의 서브 주소도 열리면 같이 쓴다 (안 열리면 서브 없이 등록)
                    if sub_url.is_none() && !sub_path.is_empty() {
                        let s_url = format!("rtsp://{ip}:{rtsp_port}{sub_path}");
                        if let Ok(sp) = stream::probe(&ffmpeg, &with_creds(&s_url, user, pass), Duration::from_secs(6)).await {
                            let mut tmp = TestResult::default();
                            let c = fill_check(&s_url, &sp, &mut tmp);
                            r.steps.extend(tmp.steps.into_iter().map(|mut s| {
                                s.name = s.name.replace("메인", "서브");
                                s
                            }));
                            r.sub = Some(c);
                        }
                    }
                    break;
                }
                Err(e) if e.contains("인증") => {
                    r.step("RTSP 주소", false, format!("{vendor} 주소에서 인증 거부 — 아이디·비밀번호 확인"));
                    auth_refused = true;
                    break;
                }
                Err(_) => {}
            }
        }
    }
    if let Some(url) = &sub_url {
        let mut tmp = TestResult::default();
        let c = check_stream(&ffmpeg, url, user, pass, timeout, &mut tmp).await;
        r.steps.extend(tmp.steps.into_iter().map(|mut s| {
            s.name = s.name.replace("메인", "서브");
            s
        }));
        r.sub = Some(c);
    }
    r.ok = main_check.as_ref().map_or(false, |c| c.ok);
    // 6) 인증 확인: 계정 없이 열어 본다 (열리면 입력한 비밀번호가 맞는지 이 테스트로는 알 수 없다)
    if r.ok {
        let open_url = r.sub.as_ref().filter(|c| c.ok).map(|c| c.url.clone()).or_else(|| main_check.as_ref().map(|c| c.url.clone()));
        if let Some(u) = open_url {
            match stream::probe(&ffmpeg, &strip_creds(&u), Duration::from_secs(8)).await {
                Ok(_) => {
                    r.auth_open = Some(true);
                    r.warn_step(
                        "인증 확인",
                        "이 카메라는 비밀번호 없이도 영상이 열립니다 (카메라 RTSP 인증 꺼짐). 입력한 비밀번호가 맞는지는 확인할 수 없고, 같은 네트워크의 누구나 영상을 볼 수 있습니다",
                    );
                }
                Err(e) if e.contains("인증") => {
                    r.auth_open = Some(false);
                    r.step("인증 확인", true, if user.is_empty() { "카메라가 계정을 요구합니다".to_string() } else { format!("계정 없이는 거부되고 '{user}' 계정으로 열림 → 비밀번호 확인됨") });
                }
                Err(_) => {}
            }
        }
    }
    r.main = main_check;
    if !r.ok {
        r.error = Some(r.main.as_ref().and_then(|c| c.error.clone()).unwrap_or_else(|| {
            if auth_refused {
                if user.is_empty() {
                    "카메라가 아이디·비밀번호를 요구합니다. 카메라에 설정한 계정을 넣고 다시 테스트해 주세요".into()
                } else {
                    format!("카메라가 이 아이디·비밀번호를 받아들이지 않습니다 (아이디: {user}). 카메라에 설정한 계정인지 확인해 주세요 — 대소문자를 구분합니다")
                }
            } else if rtsp_ok.is_none() {
                format!("카메라는 응답하지만 RTSP 포트({rtsp_port})가 닫혀 있습니다. 카메라 설정에서 RTSP가 켜져 있는지 확인하거나, [RTSP 주소를 직접 적기]에 주소를 넣어 보세요")
            } else {
                "카메라는 응답하지만 영상 주소를 찾지 못했습니다. [RTSP 주소를 직접 적기]를 켜고 카메라 설명서의 RTSP 주소를 넣어 보세요".into()
            }
        }));
    }
    r
}

/// RTSP가 답하는 포트. 직접 적은 주소가 있으면 그 포트만 보고, 아니면 입력한 포트 → 554 → 8554 가운데
/// RTSP로 답하는 첫 포트 (답은 없어도 열려 있는 포트가 있으면 그 포트). (포트, 연결에 걸린 ms — 안 열렸으면 None)
async fn find_rtsp_port(ip: &str, manual: Option<u16>, typed: Option<u16>) -> (u16, Option<f64>) {
    let mut cands: Vec<u16> = Vec::new();
    match manual {
        Some(p) => cands.push(p),
        None => {
            for p in [typed.filter(|p| *p != 80), Some(554), Some(8554)].into_iter().flatten() {
                if !cands.contains(&p) {
                    cands.push(p);
                }
            }
        }
    }
    let opens = futures_util::future::join_all(cands.iter().map(|p| port_open(ip, *p))).await;
    for (p, ms) in cands.iter().zip(&opens) {
        if ms.is_some() && speaks_rtsp(ip, *p).await {
            return (*p, *ms);
        }
    }
    // RTSP로 답한 포트는 없다. 열려만 있는 포트라도 있으면 그 포트 — 단, 입력한 포트는 웹·ONVIF 포트일 수 있어 여기서는 뺀다
    match cands.iter().zip(&opens).find(|(p, ms)| ms.is_some() && (manual.is_some() || Some(**p) != typed)) {
        Some((p, ms)) => (*p, *ms),
        None => (manual.unwrap_or(554), None),
    }
}

/// 그 포트가 RTSP로 답하는지 (계정 없이 OPTIONS 한 번)
async fn speaks_rtsp(host: &str, port: u16) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Ok(Ok(mut s)) = tokio::time::timeout(Duration::from_millis(1500), tokio::net::TcpStream::connect((host, port))).await else { return false };
    let req = format!("OPTIONS rtsp://{host}:{port}/ RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: capture-vms\r\n\r\n");
    if s.write_all(req.as_bytes()).await.is_err() {
        return false;
    }
    let mut buf = [0u8; 256];
    matches!(tokio::time::timeout(Duration::from_millis(1500), s.read(&mut buf)).await, Ok(Ok(n)) if buf[..n].starts_with(b"RTSP/"))
}

/// ONVIF가 답하는 곳
struct OnvifAt {
    port: u16,
    /// 카메라 시계가 이 PC보다 앞선 초 (`clock_offset`)
    clock_offset: i64,
}

/// ONVIF가 답하는 포트를 찾는다: 입력한 포트 → 80 → 카메라가 탐색 응답으로 알려 준 포트 → 제조사들이 쓰는 다른 포트.
/// (웹 화면은 80인데 ONVIF는 8000·2020·8899 같은 포트에 두는 카메라가 있다)
async fn find_onvif(client: &reqwest::Client, ip: &str, typed: Option<u16>) -> Option<OnvifAt> {
    const OTHER_PORTS: [u16; 7] = [8080, 8000, 8899, 2020, 5000, 81, 8081];
    let at = |port: u16| async move {
        port_open(ip, port).await?;
        let host = if port == 80 { ip.to_string() } else { format!("{ip}:{port}") };
        let clock_offset = clock_offset(client, &format!("http://{host}/onvif/device_service")).await?;
        Some(OnvifAt { port, clock_offset })
    };
    for p in [typed, Some(80)].into_iter().flatten() {
        if let Some(o) = at(p).await {
            return Some(o);
        }
    }
    // 카메라에 직접 물어본 탐색 응답에 ONVIF 주소(포트)가 들어 있다
    if let Ok(v4) = ip.parse::<Ipv4Addr>() {
        let asked = tokio::task::spawn_blocking(move || probe_unicast(v4, Duration::from_millis(800))).await.ok().flatten();
        let port = asked.and_then(|d| reqwest::Url::parse(&d.xaddr).ok()).and_then(|u| u.port_or_known_default());
        if let Some(p) = port.filter(|p| Some(*p) != typed && *p != 80) {
            if let Some(o) = at(p).await {
                return Some(o);
            }
        }
    }
    let found = futures_util::future::join_all(OTHER_PORTS.iter().filter(|p| Some(**p) != typed).map(|p| at(*p))).await;
    found.into_iter().flatten().next()
}

async fn check_stream(ffmpeg: &std::path::Path, url: &str, user: &str, pass: &str, timeout: Duration, r: &mut TestResult) -> StreamCheck {
    match stream::probe(ffmpeg, &with_creds(url, user, pass), timeout).await {
        Ok(p) => fill_check(url, &p, r),
        Err(e) => {
            r.step("메인 스트림 수신", false, format!("{url}: {e}"));
            StreamCheck { ok: false, url: url.to_string(), error: Some(e), ..Default::default() }
        }
    }
}

fn fill_check(url: &str, p: &stream::ProbeResult, r: &mut TestResult) -> StreamCheck {
    let c = StreamCheck {
        ok: true,
        url: url.to_string(),
        codec: p.info.codec.clone(),
        width: p.info.width,
        height: p.info.height,
        fps: p.info.fps,
        ms: Some(p.ms),
        error: None,
    };
    let dims = match (c.width, c.height) {
        (Some(w), Some(h)) => format!("{w}×{h}"),
        _ => "크기 미상".into(),
    };
    r.step(
        "메인 스트림 수신",
        true,
        format!(
            "{} · {dims} · {} · 첫 프레임까지 {:.1}초",
            c.codec.clone().unwrap_or_else(|| "코덱 미상".into()).to_uppercase(),
            c.fps.map(|f| format!("{f:.0}fps")).unwrap_or_else(|| "fps 미상".into()),
            p.ms / 1000.0
        ),
    );
    if r.image.is_none() {
        if let Some(j) = &p.jpeg {
            r.image = Some(format!("data:image/jpeg;base64,{}", base64::engine::general_purpose::STANDARD.encode(j)));
        }
    }
    c
}

// ---------------- ONVIF SOAP ----------------

const GET_TIME: &str = r#"<tds:GetSystemDateAndTime xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>"#;
const GET_DEVICE_INFO: &str = r#"<tds:GetDeviceInformation xmlns:tds="http://www.onvif.org/ver10/device/wsdl"/>"#;
const GET_CAPS_MEDIA: &str = r#"<tds:GetCapabilities xmlns:tds="http://www.onvif.org/ver10/device/wsdl"><tds:Category>Media</tds:Category></tds:GetCapabilities>"#;
const GET_PROFILES: &str = r#"<trt:GetProfiles xmlns:trt="http://www.onvif.org/ver10/media/wsdl"/>"#;

fn get_stream_uri(token: &str) -> String {
    format!(
        concat!(
            r#"<trt:GetStreamUri xmlns:trt="http://www.onvif.org/ver10/media/wsdl">"#,
            r#"<trt:StreamSetup><tt:Stream xmlns:tt="http://www.onvif.org/ver10/schema">RTP-Unicast</tt:Stream>"#,
            r#"<tt:Transport xmlns:tt="http://www.onvif.org/ver10/schema"><tt:Protocol>RTSP</tt:Protocol></tt:Transport></trt:StreamSetup>"#,
            r#"<trt:ProfileToken>{}</trt:ProfileToken></trt:GetStreamUri>"#
        ),
        xml_escape(token)
    )
}

/// 카메라 시계가 이 PC 시계보다 몇 초 앞서 있는지. 시각은 계정 없이 물을 수 있다.
/// 이 주소가 ONVIF로 답하지 않으면 None (시각을 계정 없이는 안 알려 주는 카메라는 Some(0)).
///
/// ONVIF 계정 확인(UsernameToken)에는 '만든 시각'이 들어가고, 카메라는 그 시각이 자기 시계와 많이 다르면
/// 올바른 비밀번호도 거절한다. 인터넷이 없는 공유기에 물린 카메라는 시계를 맞출 곳이 없어 몇 시간에서 몇 년씩 틀려 있는
/// 일이 흔하다 — 그러면 현장에서 비밀번호가 맞는데도 "인증 실패"가 난다. 그래서 카메라 시계에 맞춰서 보낸다.
async fn clock_offset(client: &reqwest::Client, dev: &str) -> Option<i64> {
    let (status, text) = soap_raw(client, dev, "", "", 0, GET_TIME).await.ok()?;
    // SOAP으로 답했거나(오류 응답이어도) 계정을 요구하면 ONVIF가 있는 것이다. 웹 화면(HTML)이나 404는 아니다
    let speaks_onvif = text.contains("Envelope") || status == 401;
    speaks_onvif.then(|| parse_utc(&text).map_or(0, |t| t - chrono::Utc::now().timestamp()))
}

/// GetSystemDateAndTime 응답의 UTC 시각 (초)
fn parse_utc(xml: &str) -> Option<i64> {
    let b = &xml[xml.find("UTCDateTime")?..];
    let n = |name: &str| tag_text(b, name).and_then(|v| v.parse::<u32>().ok());
    let t = chrono::NaiveDate::from_ymd_opt(n("Year")? as i32, n("Month")?, n("Day")?)?.and_hms_opt(n("Hour")?, n("Minute")?, n("Second")?)?;
    Some(t.and_utc().timestamp())
}

fn describe_gap(secs: i64) -> String {
    let s = secs.abs();
    if s >= 86_400 * 2 {
        format!("{}일", s / 86_400)
    } else if s >= 7200 {
        format!("{}시간", s / 3600)
    } else {
        format!("{}분", (s / 60).max(1))
    }
}

async fn onvif_profiles(client: &reqwest::Client, dev: &str, host: &str, user: &str, pass: &str, offset: i64) -> Result<Vec<ProfileInfo>, String> {
    let caps = soap(client, dev, user, pass, offset, GET_CAPS_MEDIA).await?;
    let media = caps
        .find(":Media>")
        .and_then(|i| tag_text(&caps[i..], "XAddr"))
        .map(|u| rebase(u, host))
        .unwrap_or_else(|| dev.to_string());
    let xml = soap(client, &media, user, pass, offset, GET_PROFILES).await?;
    let mut list = parse_profiles(&xml);
    for p in &mut list {
        if let Ok(x) = soap(client, &media, user, pass, offset, &get_stream_uri(&p.token)).await {
            p.rtsp = tag_text(&x, "Uri").map(|u| strip_creds(&rebase(&u.replace("&amp;", "&"), host)));
        }
    }
    Ok(list)
}

/// GetProfiles 응답에서 프로파일별 인코더 설정을 뽑는다
fn parse_profiles(xml: &str) -> Vec<ProfileInfo> {
    let starts: Vec<usize> = xml.match_indices(":Profiles ").map(|(i, _)| i).collect();
    let mut out = Vec::new();
    for (k, &s) in starts.iter().enumerate() {
        let block = &xml[s..starts.get(k + 1).copied().unwrap_or(xml.len())];
        let tag_end = block.find('>').unwrap_or(block.len());
        let Some(token) = attr(&block[..tag_end], "token") else { continue };
        let enc = block.find(":VideoEncoderConfiguration").map(|i| &block[i..]).unwrap_or("");
        let num = |name: &str| tag_text(enc, name).and_then(|v| v.parse::<f64>().ok());
        out.push(ProfileInfo {
            token,
            name: tag_text(block, "Name").map(str::to_string),
            encoding: tag_text(enc, "Encoding").map(str::to_string),
            width: num("Width").map(|v| v as u32),
            height: num("Height").map(|v| v as u32),
            fps: num("FrameRateLimit"),
            bitrate_kbps: num("BitrateLimit").map(|v| v as u32),
            gov: num("GovLength").map(|v| v as u32),
            rtsp: None,
        });
    }
    out
}

fn attr(tag: &str, name: &str) -> Option<String> {
    let p = format!("{name}=\"");
    let i = tag.find(&p)? + p.len();
    let j = tag[i..].find('"')?;
    Some(tag[i..i + j].to_string())
}

/// 카메라가 알려 준 주소의 호스트가 입력한 주소와 다르면(내부 IP 등) 입력한 주소로 바꾼다.
fn rebase(uri: &str, host: &str) -> String {
    let Ok(mut u) = reqwest::Url::parse(uri) else { return uri.to_string() };
    let (h, port) = match host.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => (h.to_string(), p.parse::<u16>().ok()),
        _ => (host.to_string(), None),
    };
    if u.host_str() != Some(h.as_str()) {
        let _ = u.set_host(Some(&h));
        // HTTP 포트를 RTSP 주소에 붙이면 안 되므로, 포트는 http 주소일 때만 바꾼다
        if port.is_some() && u.scheme().starts_with("http") {
            let _ = u.set_port(port);
        }
    }
    u.to_string()
}

fn describe_err(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "시간 초과".into()
    } else if e.is_connect() {
        "연결할 수 없음".into()
    } else {
        format!("요청 오류: {e}")
    }
}

/// ONVIF 요청 하나. `offset`: 카메라 시계가 이 PC보다 앞선 초 (`clock_offset`).
/// 계정은 두 가지 방식을 모두 쓴다: 본문에 넣는 UsernameToken(대부분의 카메라)과, 카메라가 요구하면 HTTP 다이제스트.
async fn soap(client: &reqwest::Client, url: &str, user: &str, pass: &str, offset: i64, body: &str) -> Result<String, String> {
    let (status, text) = soap_raw(client, url, user, pass, offset, body).await?;
    if !(200..300).contains(&status) {
        let reason = tag_text(&text, "Text").map(|t| format!(" ({t})")).unwrap_or_default();
        return Err(if status == 401 || text.contains("NotAuthorized") { format!("ONVIF 인증 실패{reason}") } else { format!("HTTP {status}{reason}") });
    }
    Ok(text)
}

/// ONVIF 요청을 보내고 (HTTP 상태, 응답 본문)을 그대로 돌려준다. 연결이 안 되면 Err
async fn soap_raw(client: &reqwest::Client, url: &str, user: &str, pass: &str, offset: i64, body: &str) -> Result<(u16, String), String> {
    let envelope = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">{}<s:Body>{body}</s:Body></s:Envelope>"#,
        wss_header(user, pass, offset)
    );
    let send = |authorization: Option<String>| {
        let mut req = client.post(url).header(CONTENT_TYPE, "application/soap+xml; charset=utf-8").body(envelope.clone()).timeout(Duration::from_secs(5));
        if let Some(a) = authorization {
            req = req.header(reqwest::header::AUTHORIZATION, a);
        }
        req.send()
    };
    let mut resp = send(None).await.map_err(|e| describe_err(&e))?;
    // 카메라가 HTTP 다이제스트 인증을 요구하면 그 방식으로 한 번 더 보낸다
    if resp.status().as_u16() == 401 && !user.is_empty() {
        let challenge = resp.headers().get_all(reqwest::header::WWW_AUTHENTICATE).iter().filter_map(|v| v.to_str().ok()).find(|v| v.trim_start().to_ascii_lowercase().starts_with("digest")).map(str::to_string);
        if let Some(ch) = challenge {
            let uri = reqwest::Url::parse(url).map(|u| u.path().to_string()).unwrap_or_else(|_| "/".into());
            resp = send(Some(digest_authorization(&ch, user, pass, "POST", &uri))).await.map_err(|e| describe_err(&e))?;
        }
    }
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(|e| describe_err(&e))?;
    Ok((status, text))
}

/// HTTP 다이제스트 인증 헤더 (RFC 2617, MD5). `challenge`: 카메라가 보낸 `WWW-Authenticate: Digest …` 값
fn digest_authorization(challenge: &str, user: &str, pass: &str, method: &str, uri: &str) -> String {
    let get = |k: &str| {
        let i = challenge.find(&format!("{k}="))?;
        let v = &challenge[i + k.len() + 1..];
        Some(match v.strip_prefix('"') {
            Some(q) => q.split('"').next().unwrap_or("").to_string(),
            None => v.split(',').next().unwrap_or("").trim().to_string(),
        })
    };
    let (realm, nonce) = (get("realm").unwrap_or_default(), get("nonce").unwrap_or_default());
    let hexmd5 = |s: String| md5(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect::<String>();
    let ha1 = hexmd5(format!("{user}:{realm}:{pass}"));
    let ha2 = hexmd5(format!("{method}:{uri}"));
    let mut out = format!(r#"Digest username="{user}", realm="{realm}", nonce="{nonce}", uri="{uri}""#);
    if get("qop").is_some_and(|q| q.split(',').any(|x| x.trim() == "auth")) {
        let cnonce: String = Sha1::digest(format!("{}-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0), COUNTER.fetch_add(1, Ordering::Relaxed)).as_bytes())[..8].iter().map(|b| format!("{b:02x}")).collect();
        let response = hexmd5(format!("{ha1}:{nonce}:00000001:{cnonce}:auth:{ha2}"));
        out.push_str(&format!(r#", response="{response}", qop=auth, nc=00000001, cnonce="{cnonce}""#));
    } else {
        out.push_str(&format!(r#", response="{}""#, hexmd5(format!("{ha1}:{nonce}:{ha2}"))));
    }
    if let Some(o) = get("opaque") {
        out.push_str(&format!(r#", opaque="{o}""#));
    }
    out
}

/// MD5 (HTTP 다이제스트 인증용 — 보안 용도가 아니라 카메라가 쓰는 방식이라 필요하다)
fn md5(data: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
        6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    let k: Vec<u32> = (0..64).map(|i| ((i as f64 + 1.0).sin().abs() * 4294967296.0) as u32).collect();
    let (mut a0, mut b0, mut c0, mut d0) = (0x67452301u32, 0xefcdab89u32, 0x98badcfeu32, 0x10325476u32);
    let mut m = data.to_vec();
    m.push(0x80);
    while m.len() % 64 != 56 {
        m.push(0);
    }
    m.extend_from_slice(&((data.len() as u64) * 8).to_le_bytes());
    for chunk in m.chunks(64) {
        let w: Vec<u32> = chunk.chunks(4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]])).collect();
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f = f.wrapping_add(a).wrapping_add(k[i]).wrapping_add(w[g]);
            (a, d, c) = (d, c, b);
            b = b.wrapping_add(f.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = [0u8; 16];
    for (i, v) in [a0, b0, c0, d0].iter().enumerate() {
        out[4 * i..4 * i + 4].copy_from_slice(&v.to_le_bytes());
    }
    out
}

/// WS-Security UsernameToken (PasswordDigest = Base64(SHA1(nonce + created + password))).
/// `offset`: 카메라 시계가 이 PC보다 앞선 초 — '만든 시각'을 카메라 시계 기준으로 적는다
fn wss_header(user: &str, pass: &str, offset: i64) -> String {
    if user.is_empty() {
        return String::new();
    }
    let created = (chrono::Utc::now() + chrono::Duration::seconds(offset)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let seed = format!("{}-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0), COUNTER.fetch_add(1, Ordering::Relaxed));
    let nonce = &Sha1::digest(seed.as_bytes())[..16];
    let mut h = Sha1::new();
    h.update(nonce);
    h.update(created.as_bytes());
    h.update(pass.as_bytes());
    let b64 = base64::engine::general_purpose::STANDARD;
    format!(
        concat!(
            r#"<s:Header><wsse:Security s:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">"#,
            r#"<wsse:UsernameToken><wsse:Username>{}</wsse:Username>"#,
            r#"<wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{}</wsse:Password>"#,
            r#"<wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{}</wsse:Nonce>"#,
            r#"<wsu:Created>{}</wsu:Created></wsse:UsernameToken></wsse:Security></s:Header>"#
        ),
        xml_escape(user),
        b64.encode(h.finalize()),
        b64.encode(nonce),
        created
    )
}

// ---------------- 작은 XML 도우미 (응답 구조가 단순해서 파서 없이 처리) ----------------

/// 네임스페이스와 상관없이 `<x:Name>값</x:Name>`의 첫 값
fn tag_text<'a>(xml: &'a str, local: &str) -> Option<&'a str> {
    for pat in [format!(":{local}>"), format!("<{local}>")] {
        if let Some(i) = xml.find(&pat) {
            let rest = &xml[i + pat.len()..];
            let end = rest.find('<').unwrap_or(rest.len());
            let v = rest[..end].trim();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        // 바이트 단위로 본다 (문자열을 잘라 쓰면 한글 같은 다바이트 문자에서 패닉)
        if b[i] == b'%' && i + 2 < b.len() {
            let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(v) = hex {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// "http://1.2.3.4:80/abc" → "1.2.3.4:80"
fn normalize_host(input: &str) -> String {
    let s = input.trim();
    let s = ["http://", "https://", "rtsp://"].iter().find_map(|p| s.strip_prefix(p)).unwrap_or(s);
    let s = s.rsplit_once('@').map(|(_, h)| h).unwrap_or(s);
    s.split('/').next().unwrap_or("").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_discovery_and_skips_non_onvif() {
        let cam = r#"<d:XAddrs>http://192.168.3.27/onvif/device_service http://169.254.10.57/onvif/device_service</d:XAddrs><d:Scopes>onvif://www.onvif.org/hardware/CAM-1000 onvif://www.onvif.org/name/ACME%20CAM-1000</d:Scopes>"#;
        let d = parse_probe_match("192.168.3.27", cam).unwrap();
        assert_eq!(d.xaddr, "http://192.168.3.27/onvif/device_service");
        assert_eq!(d.name.as_deref(), Some("ACME CAM-1000"));
        assert!(parse_probe_match("192.168.3.45", r#"<d:XAddrs>http://my-nas:5357/ab</d:XAddrs>"#).is_none());
    }

    #[test]
    fn parses_profiles_with_encoder() {
        let x = r#"<trt:Profiles fixed="true" token="Profile1"><tt:Name>main</tt:Name><tt:VideoSourceConfiguration token="v"><tt:Name>src</tt:Name><tt:Bounds x="0" y="0" width="2048" height="1536"/></tt:VideoSourceConfiguration><tt:VideoEncoderConfiguration token="e1"><tt:Name>enc</tt:Name><tt:Encoding>H264</tt:Encoding><tt:Resolution><tt:Width>2048</tt:Width><tt:Height>1536</tt:Height></tt:Resolution><tt:RateControl><tt:FrameRateLimit>30</tt:FrameRateLimit><tt:BitrateLimit>6144</tt:BitrateLimit></tt:RateControl><tt:H264><tt:GovLength>30</tt:GovLength></tt:H264></tt:VideoEncoderConfiguration></trt:Profiles><trt:Profiles token="Profile2"><tt:Name>sub</tt:Name><tt:VideoEncoderConfiguration><tt:Resolution><tt:Width>640</tt:Width><tt:Height>480</tt:Height></tt:Resolution></tt:VideoEncoderConfiguration></trt:Profiles>"#;
        let p = parse_profiles(x);
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].token, "Profile1");
        assert_eq!(p[0].name.as_deref(), Some("main"));
        assert_eq!((p[0].width, p[0].height, p[0].fps, p[0].bitrate_kbps), (Some(2048), Some(1536), Some(30.0), Some(6144)));
        assert_eq!(p[1].width, Some(640));
    }

    #[test]
    fn md5_matches_the_standard_examples() {
        let hex = |d: &[u8]| md5(d).iter().map(|b| format!("{b:02x}")).collect::<String>();
        // RFC 1321의 시험 값
        assert_eq!(hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(hex(b"message digest"), "f96b697d7cb7938d525a2f31aaf161d0");
        assert_eq!(hex(b"12345678901234567890123456789012345678901234567890123456789012345678901234567890"), "57edf4a22be3c955ac49da2e2107b67a");
        // 다이제스트 인증에 실제로 들어가는 꼴 (md5sum으로 계산한 값)
        assert_eq!(hex(b"Mufasa:testrealm@host.com:Circle Of Life"), "939e7578ed9e3c518a452acee763bce9");
        assert_eq!(hex(b"GET:/dir/index.html"), "39aff3a2bab6126f332b942af96d3366");
    }

    #[test]
    fn answers_an_http_digest_challenge() {
        let hex = |s: String| md5(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect::<String>();
        let field = |h: &str, k: &str| h.split(&format!("{k}=\"")).nth(1).and_then(|s| s.split('"').next()).unwrap_or("").to_string();
        let h = digest_authorization(r#"Digest realm="testrealm@host.com", qop="auth,auth-int", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", opaque="5ccc069c403ebaf9f0171e9517f40e41""#, "Mufasa", "Circle Of Life", "GET", "/dir/index.html");
        let (ha1, ha2) = (hex("Mufasa:testrealm@host.com:Circle Of Life".into()), hex("GET:/dir/index.html".into()));
        assert_eq!(field(&h, "response"), hex(format!("{ha1}:dcd98b7102dd2f0e8b11d0f600bfb0c093:00000001:{}:auth:{ha2}", field(&h, "cnonce"))));
        assert!(h.starts_with("Digest username=\"Mufasa\"") && h.contains("qop=auth, nc=00000001") && h.contains(r#"opaque="5ccc069c403ebaf9f0171e9517f40e41""#));
        // qop가 없는 예전 방식
        let old = digest_authorization(r#"Digest realm="IPCAM", nonce="abc""#, "admin", "pw", "POST", "/onvif/device_service");
        assert!(!old.contains("qop") && !old.contains("cnonce"));
        assert_eq!(field(&old, "response"), hex(format!("{}:abc:{}", hex("admin:IPCAM:pw".into()), hex("POST:/onvif/device_service".into()))));
    }

    #[test]
    fn reads_the_camera_clock_and_signs_in_camera_time() {
        let x = r#"<tds:SystemDateAndTime><tt:DateTimeType>NTP</tt:DateTimeType><tt:DaylightSavings>false</tt:DaylightSavings><tt:TimeZone><tt:TZ>KST-9</tt:TZ></tt:TimeZone><tt:UTCDateTime><tt:Time><tt:Hour>0</tt:Hour><tt:Minute>0</tt:Minute><tt:Second>7</tt:Second></tt:Time><tt:Date><tt:Year>2000</tt:Year><tt:Month>1</tt:Month><tt:Day>1</tt:Day></tt:Date></tt:UTCDateTime><tt:LocalDateTime><tt:Time><tt:Hour>9</tt:Hour><tt:Minute>0</tt:Minute><tt:Second>7</tt:Second></tt:Time><tt:Date><tt:Year>2000</tt:Year><tt:Month>1</tt:Month><tt:Day>1</tt:Day></tt:Date></tt:LocalDateTime></tds:SystemDateAndTime>"#;
        assert_eq!(parse_utc(x), Some(946_684_807));
        assert_eq!(parse_utc("<a/>"), None);
        // 카메라 시계가 1시간 앞서 있으면 '만든 시각'도 1시간 앞으로 적는다
        let created = |h: &str| chrono::DateTime::parse_from_rfc3339(tag_text(h, "Created").unwrap()).unwrap().timestamp();
        let gap = created(&wss_header("admin", "pw", 3600)) - created(&wss_header("admin", "pw", 0));
        assert!((3599..=3601).contains(&gap), "{gap}");
        assert_eq!(wss_header("", "pw", 0), "");
        assert_eq!((describe_gap(3600).as_str(), describe_gap(-90_000).as_str(), describe_gap(86_400 * 400).as_str()), ("60분", "25시간", "400일"));
    }

    #[test]
    fn common_addresses_come_with_their_sub_stream() {
        // 서브 주소는 같은 제조사의 메인 주소와 짝이어야 한다
        let idis = COMMON_RTSP.iter().find(|(_, _, v)| *v == "IDIS").unwrap();
        assert_eq!((idis.0, idis.1), ("/media/1/1/Profile1", "/media/1/2/Profile2"));
        assert!(COMMON_RTSP.iter().all(|(m, s, _)| m.starts_with('/') && (s.is_empty() || (s.starts_with('/') && s != m))));
    }

    #[test]
    fn rebases_and_normalizes() {
        assert_eq!(rebase("rtsp://169.254.1.2:554/media/1/1/Profile1", "192.168.3.27"), "rtsp://192.168.3.27:554/media/1/1/Profile1");
        assert_eq!(rebase("http://10.0.0.5/onvif/media", "1.2.3.4:8080"), "http://1.2.3.4:8080/onvif/media");
        assert_eq!(normalize_host(" rtsp://admin:x@192.168.3.27:554/media "), "192.168.3.27:554");
        assert_eq!(strip_creds("rtsp://a:b@1.2.3.4/x"), "rtsp://1.2.3.4/x");
    }
}

#[cfg(test)]
mod find_tests {
    use super::*;

    const MATCH: &str = r#"<?xml version="1.0"?><e:Envelope><e:Header><w:MessageID>uuid:1</w:MessageID><w:To>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</w:To></e:Header><e:Body><d:ProbeMatches><d:ProbeMatch><w:EndpointReference><w:Address>urn:uuid:0A1B2C3D-0000-4000-8000-00005E005318</w:Address></w:EndpointReference><d:Types>dn:NetworkVideoTransmitter</d:Types><d:Scopes>onvif://www.onvif.org/name/CAM-1000 onvif://www.onvif.org/hardware/CAM-1000</d:Scopes><d:XAddrs>http://192.168.1.3/onvif/device_service</d:XAddrs></d:ProbeMatch></d:ProbeMatches></e:Body></e:Envelope>"#;

    #[test]
    fn asks_the_rest_of_my_subnet_directly() {
        let t = sweep_targets(Ipv4Addr::new(192, 168, 1, 5), 24);
        assert_eq!(t.len(), 253); // .1 ~ .254 가운데 나(.5)만 빼고
        assert!(t.contains(&Ipv4Addr::new(192, 168, 1, 3)) && t.contains(&Ipv4Addr::new(192, 168, 1, 254)));
        assert!(!t.contains(&Ipv4Addr::new(192, 168, 1, 5)) && !t.contains(&Ipv4Addr::new(192, 168, 1, 0)) && !t.contains(&Ipv4Addr::new(192, 168, 1, 255)));
        // 넓은 대역(/16)이면 내 주소가 든 /24만
        let t = sweep_targets(Ipv4Addr::new(10, 1, 7, 20), 16);
        assert_eq!(t.len(), 253);
        assert!(t.iter().all(|a| a.octets()[..3] == [10, 1, 7]));
        // 좁은 대역(/28)이면 그 안에서만: .17 ~ .30 가운데 나(.18)만 빼고
        assert_eq!(sweep_targets(Ipv4Addr::new(192, 168, 0, 18), 28).len(), 13);
        // 묻지 않는 주소
        assert!(sweep_targets(Ipv4Addr::new(169, 254, 3, 4), 16).is_empty());
        assert!(sweep_targets(Ipv4Addr::UNSPECIFIED, 24).is_empty());
    }

    #[test]
    fn reads_the_device_id_that_survives_an_address_change() {
        let d = parse_probe_match("192.168.1.3", MATCH).expect("match");
        assert_eq!(d.uid.as_deref(), Some("urn:uuid:0a1b2c3d-0000-4000-8000-00005e005318"));
        assert_eq!(d.name.as_deref(), Some("CAM-1000"));
        // 주소가 바뀌어도 같은 값
        let moved = parse_probe_match("192.168.1.9", &MATCH.replace("192.168.1.3", "192.168.1.9")).expect("match");
        assert_eq!(moved.uid, d.uid);
    }

    #[test]
    fn finds_a_camera_by_asking_its_address_directly() {
        // 멀티캐스트에는 답이 오지 않는 환경: 주소에 직접 물으면 답하는 가짜 카메라
        let cam = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = cam.local_addr().unwrap().port();
        cam.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let t = std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            if let Ok((n, from)) = cam.recv_from(&mut buf) {
                assert!(String::from_utf8_lossy(&buf[..n]).contains("NetworkVideoTransmitter"));
                let _ = cam.send_to(MATCH.replace("192.168.1.3", "127.0.0.1").as_bytes(), from);
            }
        });
        let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        let msg = PROBE.replace("{ID}", &pseudo_uuid());
        let found = sweep_and_collect(&sock, &msg, &[Ipv4Addr::LOCALHOST], port, Duration::from_millis(900));
        t.join().unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].ip, "127.0.0.1");
        assert!(found[0].uid.as_deref().is_some_and(|u| u.starts_with("urn:uuid:")));
    }
}
