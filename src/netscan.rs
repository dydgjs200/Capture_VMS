//! IP로 카메라 찾기와 네트워크 진단.
//!
//! 공유기(예: 10.x 대역)에 붙은 PC에서 대역이 다른 카메라(예: 192.168.2.27)를 찾을 때를 위한 것.
//! ONVIF 자동 찾기(멀티캐스트)는 같은 대역에서만 되므로, IP·범위를 직접 넣어 포트를 두드려 본다.
//! 비밀번호는 넣어 보지 않는다 (인증 없이 돌아오는 정보만 읽음). 이 PC의 네트워크 설정은 바꾸지 않는다.

use std::{
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Semaphore,
};

use crate::{netmon, onvif};

/// 찾을 때 두드려 볼 포트 (이름)
const PORTS: &[(u16, &str)] = &[(554, "RTSP"), (80, "웹/ONVIF"), (8554, "RTSP(대체)"), (443, "HTTPS"), (8080, "웹(대체)"), (8000, "제조사 SDK")];
/// 한 번에 찾을 수 있는 최대 주소 수 (/22)
pub const MAX_HOSTS: usize = 1024;

#[derive(Debug, Clone, Serialize)]
pub struct Ipv4Net {
    pub ip: String,
    pub prefix: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct Adapter {
    pub name: String,
    pub description: String,
    /// wifi | ethernet | other
    pub kind: String,
    pub up: bool,
    pub ipv4: Vec<Ipv4Net>,
    pub gateways: Vec<String>,
    pub speed_mbps: Option<f64>,
}

fn mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix.min(32))
    }
}

pub fn in_subnet(ip: Ipv4Addr, net: Ipv4Addr, prefix: u8) -> bool {
    (u32::from(ip) & mask(prefix)) == (u32::from(net) & mask(prefix))
}

/// "10.0.0.0/24" 같은 대역 표기
pub fn network_of(ip: Ipv4Addr, prefix: u8) -> String {
    format!("{}/{prefix}", Ipv4Addr::from(u32::from(ip) & mask(prefix)))
}

static CACHE: Mutex<Option<(Instant, Vec<Adapter>)>> = Mutex::new(None);

/// 이 PC의 네트워크 어댑터 (5초 캐시)
pub fn adapters() -> Vec<Adapter> {
    let mut c = CACHE.lock().unwrap();
    if let Some((t, v)) = c.as_ref() {
        if t.elapsed() < Duration::from_secs(5) {
            return v.clone();
        }
    }
    let v = read_adapters();
    *c = Some((Instant::now(), v.clone()));
    v
}

/// 켜져 있는 어댑터 중 이 IP와 같은 대역인 것: (어댑터, 내 주소)
pub fn local_match(ip: Ipv4Addr) -> Option<(Adapter, Ipv4Net)> {
    adapters().into_iter().filter(|a| a.up).find_map(|a| {
        let n = a.ipv4.iter().find(|n| n.ip.parse::<Ipv4Addr>().map_or(false, |me| in_subnet(ip, me, n.prefix)))?.clone();
        Some((a, n))
    })
}

/// 이 IP로 갈 때 쓰는 어댑터의 공유기(게이트웨이) 주소. 그 어댑터에 게이트웨이가 없으면 None
/// (다른 어댑터의 공유기는 이 카메라로 가는 길과 상관없으므로 쓰지 않는다)
pub fn gateway_for(ip: Ipv4Addr) -> Option<Ipv4Addr> {
    if ip.is_loopback() {
        return None;
    }
    let (_, name) = netmon::route_to(&ip.to_string())?;
    let name = name?;
    adapters().into_iter().find(|a| a.up && name.starts_with(&a.name))?.gateways.first()?.parse().ok()
}

/// 상태 표시용: same(같은 대역) | routed(다른 대역, 게이트웨이 경유) | None(IP가 아님)
pub fn subnet_hint(ip: &str) -> Option<&'static str> {
    let ip: Ipv4Addr = ip.parse().ok()?;
    Some(if ip.is_loopback() || local_match(ip).is_some() { "same" } else { "routed" })
}

/// 이 PC의 (IPv4 주소, 대역 길이)들 — 카메라 찾기가 어댑터마다 묻는 데 쓴다
pub fn local_ipv4_nets() -> Vec<(Ipv4Addr, u8)> {
    adapters()
        .into_iter()
        .filter(|a| a.up)
        .flat_map(|a| a.ipv4.into_iter())
        .filter_map(|n| Some((n.ip.parse::<Ipv4Addr>().ok()?, n.prefix)))
        .filter(|(ip, _)| !ip.is_loopback() && !ip.is_unspecified())
        .collect()
}

/// 이 PC에서 IPv4 멀티캐스트를 보낼 주소들 (ONVIF 찾기용)
pub fn local_ipv4s() -> Vec<Ipv4Addr> {
    adapters()
        .into_iter()
        .filter(|a| a.up)
        .flat_map(|a| a.ipv4.into_iter())
        .filter_map(|n| n.ip.parse::<Ipv4Addr>().ok())
        .filter(|ip| !ip.is_loopback() && !ip.is_unspecified())
        .collect()
}

#[cfg(windows)]
fn read_adapters() -> Vec<Adapter> {
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetAdaptersAddresses, GAA_FLAG_INCLUDE_GATEWAYS, GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_DNS_SERVER, GAA_FLAG_SKIP_MULTICAST, IP_ADAPTER_ADDRESSES_LH,
    };
    const AF_INET: u32 = 2;
    const ERROR_BUFFER_OVERFLOW: u32 = 111;

    unsafe fn wide(p: *const u16) -> String {
        if p.is_null() {
            return String::new();
        }
        let mut n = 0;
        while *p.add(n) != 0 && n < 512 {
            n += 1;
        }
        String::from_utf16_lossy(std::slice::from_raw_parts(p, n))
    }
    /// SOCKADDR → IPv4 (sin_family=2이면 4바이트 주소가 오프셋 4에)
    unsafe fn v4(sa: *const u8) -> Option<Ipv4Addr> {
        if sa.is_null() || std::ptr::read_unaligned(sa as *const u16) != 2 {
            return None;
        }
        let b = std::slice::from_raw_parts(sa.add(4), 4);
        Some(Ipv4Addr::new(b[0], b[1], b[2], b[3]))
    }

    let flags = GAA_FLAG_INCLUDE_GATEWAYS | GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER;
    let mut size: u32 = 32 * 1024;
    let mut buf: Vec<u64> = vec![0; size as usize / 8 + 1];
    let mut ret = 0;
    for _ in 0..3 {
        ret = unsafe { GetAdaptersAddresses(AF_INET, flags, std::ptr::null(), buf.as_mut_ptr() as *mut IP_ADAPTER_ADDRESSES_LH, &mut size) };
        if ret == ERROR_BUFFER_OVERFLOW {
            buf = vec![0; size as usize / 8 + 1];
            continue;
        }
        break;
    }
    if ret != 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    unsafe {
        let mut p = buf.as_ptr() as *const IP_ADAPTER_ADDRESSES_LH;
        while !p.is_null() {
            let a = &*p;
            p = a.Next;
            if a.IfType == 24 {
                continue; // loopback
            }
            let mut ipv4 = Vec::new();
            let mut u = a.FirstUnicastAddress;
            while !u.is_null() {
                if let Some(ip) = v4((*u).Address.lpSockaddr as *const u8) {
                    ipv4.push(Ipv4Net { ip: ip.to_string(), prefix: (*u).OnLinkPrefixLength });
                }
                u = (*u).Next;
            }
            let mut gateways = Vec::new();
            let mut g = a.FirstGatewayAddress;
            while !g.is_null() {
                if let Some(ip) = v4((*g).Address.lpSockaddr as *const u8) {
                    gateways.push(ip.to_string());
                }
                g = (*g).Next;
            }
            let bps = a.ReceiveLinkSpeed;
            out.push(Adapter {
                name: wide(a.FriendlyName),
                description: wide(a.Description),
                kind: match a.IfType {
                    71 => "wifi",
                    6 => "ethernet",
                    _ => "other",
                }
                .into(),
                up: a.OperStatus == 1,
                ipv4,
                gateways,
                speed_mbps: (bps > 0 && bps != u64::MAX).then(|| bps as f64 / 1e6),
            });
        }
    }
    out
}

#[cfg(not(windows))]
fn read_adapters() -> Vec<Adapter> {
    Vec::new()
}

// ---------------- 진단 ----------------

#[derive(Debug, Clone, Serialize)]
pub struct PortCheck {
    pub port: u16,
    pub name: String,
    pub ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Diagnosis {
    pub target: String,
    /// ok | routed | no_rtsp | unreachable_same | unreachable_routed | invalid
    pub verdict: String,
    pub summary: String,
    pub advice: Vec<String>,
    pub same_subnet: bool,
    /// 같은 대역인 내 어댑터·주소
    pub local_adapter: Option<String>,
    pub local_address: Option<String>,
    /// 이 PC가 카메라로 보낼 때 쓰는 어댑터 (Windows 경로)
    pub route_kind: Option<String>,
    pub route_name: Option<String>,
    pub gateway: Option<String>,
    pub ping_ms: Option<f64>,
    pub ports: Vec<PortCheck>,
}

async fn tcp_ms(ip: Ipv4Addr, port: u16, timeout: Duration) -> Option<f64> {
    let t0 = Instant::now();
    match tokio::time::timeout(timeout, TcpStream::connect((ip, port))).await {
        Ok(Ok(_)) => Some(t0.elapsed().as_secs_f64() * 1000.0),
        _ => None,
    }
}

async fn ping_ms(ip: Ipv4Addr, timeout_ms: u32) -> Option<f64> {
    tokio::task::spawn_blocking(move || netmon::icmp::ping(ip, timeout_ms)).await.ok().flatten()
}

/// 카메라 IP 하나에 대해: 같은 대역인지, 어디로 나가는지, 응답하는지, 안 되면 어떻게 하면 되는지
pub async fn diagnose(target: &str) -> Diagnosis {
    let host = target.trim().split(['/', ' ']).next().unwrap_or("").trim();
    let host = host.rsplit_once(':').filter(|(_, p)| p.chars().all(|c| c.is_ascii_digit())).map_or(host, |(h, _)| h);
    let mut d = Diagnosis {
        target: host.to_string(),
        verdict: "invalid".into(),
        summary: String::new(),
        advice: Vec::new(),
        same_subnet: false,
        local_adapter: None,
        local_address: None,
        route_kind: None,
        route_name: None,
        gateway: None,
        ping_ms: None,
        ports: Vec::new(),
    };
    let Ok(ip) = host.parse::<Ipv4Addr>() else {
        d.summary = format!("'{host}'는 IPv4 주소가 아닙니다 (예: 192.168.2.27)");
        return d;
    };
    let ads = adapters();
    let up: Vec<&Adapter> = ads.iter().filter(|a| a.up && !a.ipv4.is_empty()).collect();
    let local = local_match(ip);
    d.same_subnet = local.is_some() || ip.is_loopback();
    if let Some((a, n)) = &local {
        d.local_adapter = Some(a.name.clone());
        d.local_address = Some(format!("{}/{}", n.ip, n.prefix));
    }
    if let Some((kind, name)) = netmon::route_to(&ip.to_string()) {
        d.route_kind = kind;
        d.route_name = name;
    }
    // 경로 어댑터의 게이트웨이 (없으면 게이트웨이가 있는 첫 어댑터)
    let route_adapter = d.route_name.as_deref().and_then(|rn| up.iter().find(|a| rn.starts_with(&a.name)).copied());
    d.gateway = route_adapter.or_else(|| up.iter().find(|a| !a.gateways.is_empty()).copied()).and_then(|a| a.gateways.first().cloned());

    let (ping, ports) = tokio::join!(
        ping_ms(ip, 1000),
        futures_util::future::join_all(PORTS[..3].iter().map(|(p, n)| async move { PortCheck { port: *p, name: n.to_string(), ms: tcp_ms(ip, *p, Duration::from_millis(1500)).await } }))
    );
    d.ping_ms = ping;
    d.ports = ports;
    let rtsp_ok = d.ports.iter().any(|p| (p.port == 554 || p.port == 8554) && p.ms.is_some());
    let any_ok = ping.is_some() || d.ports.iter().any(|p| p.ms.is_some());
    let my_nets: Vec<String> = up
        .iter()
        .flat_map(|a| a.ipv4.iter().filter_map(move |n| n.ip.parse::<Ipv4Addr>().ok().map(|me| format!("{} {} ({})", a.name, network_of(me, n.prefix), n.ip))))
        .collect();
    let my_nets_text = if my_nets.is_empty() { "연결된 네트워크 없음".to_string() } else { my_nets.join(", ") };

    if any_ok {
        if d.same_subnet {
            d.verdict = if rtsp_ok { "ok" } else { "no_rtsp" }.into();
            d.summary = format!("같은 대역({})에 있고 응답합니다", d.local_address.clone().unwrap_or_default());
        } else {
            d.verdict = if rtsp_ok { "routed" } else { "no_rtsp" }.into();
            d.summary = format!(
                "대역은 다르지만 {}를 거쳐 응답합니다. 영상도 받을 수 있습니다",
                d.gateway.as_deref().map(|g| format!("게이트웨이 {g}")).unwrap_or_else(|| "공유기·라우터".into())
            );
            d.advice.push("공유기가 두 대역 사이를 중계하므로 같은 대역일 때보다 지연·끊김이 조금 늘 수 있습니다. 최종 측정은 같은 대역에서 하는 것을 권합니다".into());
        }
        if !rtsp_ok {
            d.summary.push_str(" — 단, RTSP(554·8554) 포트가 닫혀 있습니다");
            d.advice.push("카메라 웹 설정에서 RTSP 사용·포트 번호를 확인하세요. 554·8554가 아닌 포트를 쓰면 카메라 추가 화면에서 [RTSP 주소를 직접 적기]를 켜고 주소를 넣으세요".into());
        }
        return d;
    }

    if d.same_subnet {
        d.verdict = "unreachable_same".into();
        d.summary = format!("같은 대역({})인데 응답이 없습니다", d.local_address.clone().unwrap_or_default());
        d.advice.extend([
            "카메라 전원·PoE 전원과 랜선 연결(링크 램프)을 확인하세요".to_string(),
            "공유기를 껐다 켠 뒤라면 카메라가 다른 주소를 받았을 수 있습니다. 설정 → 카메라 → [카메라 찾기]를 누르면 바뀐 주소가 나오고 [주소 바꾸기]로 고칠 수 있습니다".to_string(),
            "IP 주소에 오타가 없는지, 카메라가 실제로 이 주소를 쓰는지 확인하세요 (카메라 라벨·제조사 IP 설정 도구)".to_string(),
            "카메라가 공유기 LAN 포트(또는 그 아래 스위치)에 꽂혀 있는지 확인하세요. WAN(인터넷) 포트에 꽂으면 보이지 않습니다".to_string(),
        ]);
        return d;
    }

    d.verdict = "unreachable_routed".into();
    let pc = local_ipv4s().into_iter().next();
    d.summary = format!("카메라({ip})가 이 PC와 다른 대역이라 바로 연결되지 않습니다. 이 PC: {my_nets_text}");
    let cam_net = network_of(ip, 24);
    // 이 PC 대역 안에서 카메라 끝자리를 그대로 쓴 주소를 예로 든다
    let suggest = up
        .iter()
        .flat_map(|a| a.ipv4.iter())
        .find_map(|n| {
            let me: Ipv4Addr = n.ip.parse().ok()?;
            if me.is_link_local() || n.prefix < 16 || n.prefix > 30 {
                return None;
            }
            let host_bits = !mask(n.prefix);
            let cand = (u32::from(me) & mask(n.prefix)) | (u32::from(ip) & host_bits);
            let cand = Ipv4Addr::from(cand);
            (cand != me && (u32::from(cand) & host_bits) != 0 && (u32::from(cand) & host_bits) != host_bits).then_some(cand)
        })
        .map(|c| format!(" (예: {c} — 다른 장치와 겹치지 않는지 확인)"))
        .unwrap_or_default();
    d.advice.extend([
        format!("① 카메라 IP를 이 PC와 같은 대역으로 바꾸기 (권장){suggest}. 카메라 제조사의 IP 설정 도구를 쓰면 대역이 달라도 같은 공유기에 연결돼 있으면 찾아서 바꿀 수 있습니다"),
        format!(
            "② 공유기 내부(LAN) 대역을 카메라 대역({cam_net})으로 바꾸기: 공유기 관리 페이지{}에서 LAN IP를 {} 등으로 바꾸면 PC가 다시 연결될 때 같은 대역 주소를 받습니다",
            d.gateway.as_deref().map(|g| format!("(http://{g})")).unwrap_or_default(),
            Ipv4Addr::from((u32::from(ip) & mask(24)) | 1)
        ),
        format!("③ 이 PC에 {cam_net} 대역 보조 IP 추가 (카메라와 같은 스위치에 유선으로 연결할 때). Wi-Fi 연결이 끊길 수 있어 이 프로그램은 바꾸지 않습니다. 네트워크 담당자와 상의하세요"),
        "카메라가 공유기 LAN 포트(또는 그 아래 스위치)에 꽂혀 있는지도 확인하세요. WAN(인터넷) 포트에 꽂으면 보이지 않습니다".to_string(),
    ]);
    if pc.is_none() {
        d.advice.insert(0, "이 PC가 어느 네트워크에도 연결돼 있지 않습니다. Wi-Fi·랜선 연결부터 확인하세요".into());
    }
    d
}

// ---------------- IP로 찾기 ----------------

/// "192.168.2.27", "192.168.2.1-254", "192.168.2.1-192.168.2.50", "192.168.2.0/24", 쉼표로 여러 개
pub fn parse_targets(spec: &str) -> Result<Vec<Ipv4Addr>, String> {
    let mut out: Vec<Ipv4Addr> = Vec::new();
    for part in spec.split([',', ';', ' ', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
        let bad = || format!("'{part}'를 알아볼 수 없습니다 (예: 192.168.2.27 · 192.168.2.1-254 · 192.168.2.0/24)");
        if let Some((base, pfx)) = part.split_once('/') {
            let base: Ipv4Addr = base.trim().parse().map_err(|_| bad())?;
            let pfx: u8 = pfx.trim().parse().map_err(|_| bad())?;
            if !(22..=32).contains(&pfx) {
                return Err(format!("'{part}': 한 번에 /22(1024개)까지만 찾을 수 있습니다"));
            }
            let net = u32::from(base) & mask(pfx);
            let size = 1u32 << (32 - pfx);
            let (first, last) = if size > 2 { (net + 1, net + size - 2) } else { (net, net + size - 1) };
            out.extend((first..=last).map(Ipv4Addr::from));
        } else if let Some((a, b)) = part.split_once('-') {
            let a: Ipv4Addr = a.trim().parse().map_err(|_| bad())?;
            let b = b.trim();
            let b: Ipv4Addr = if b.contains('.') {
                b.parse().map_err(|_| bad())?
            } else {
                let last: u8 = b.parse().map_err(|_| bad())?;
                let o = a.octets();
                Ipv4Addr::new(o[0], o[1], o[2], last)
            };
            let (x, y) = (u32::from(a), u32::from(b));
            if y < x {
                return Err(format!("'{part}': 범위의 끝이 시작보다 작습니다"));
            }
            if (y - x) as usize >= MAX_HOSTS {
                return Err(format!("'{part}': 한 번에 {MAX_HOSTS}개까지만 찾을 수 있습니다"));
            }
            out.extend((x..=y).map(Ipv4Addr::from));
        } else {
            out.push(part.parse().map_err(|_| bad())?);
        }
        if out.len() > MAX_HOSTS {
            return Err(format!("한 번에 {MAX_HOSTS}개 주소까지만 찾을 수 있습니다"));
        }
    }
    out.sort();
    out.dedup();
    if out.is_empty() {
        return Err("찾을 IP를 입력하세요 (예: 192.168.2.27 · 192.168.2.1-254 · 192.168.2.0/24)".into());
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
pub struct FoundHost {
    pub ip: String,
    pub ping_ms: Option<f64>,
    pub open_ports: Vec<u16>,
    /// RTSP OPTIONS 응답 (계정 없이)
    pub rtsp_status: Option<String>,
    pub rtsp_server: Option<String>,
    pub http_server: Option<String>,
    pub http_title: Option<String>,
    /// 웹이 인증을 요구할 때 알려 주는 이름 (제조사가 보이는 경우가 많음)
    pub http_realm: Option<String>,
    pub onvif: Option<onvif::Discovered>,
    /// RTSP가 열려 있거나 ONVIF 응답이 있으면 카메라일 가능성이 높음
    pub likely_camera: bool,
    /// 같은 대역인지
    pub same_subnet: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScanResult {
    pub scanned: usize,
    pub elapsed_ms: f64,
    pub hosts: Vec<FoundHost>,
    /// 주소 하나만 찾았을 때 진단
    pub diagnosis: Option<Diagnosis>,
}

/// RTSP OPTIONS (계정 없이): 상태 줄과 Server 헤더
async fn rtsp_options(ip: Ipv4Addr, port: u16) -> Option<(String, Option<String>)> {
    let mut s = tokio::time::timeout(Duration::from_millis(1200), TcpStream::connect((ip, port))).await.ok()?.ok()?;
    let req = format!("OPTIONS rtsp://{ip}:{port}/ RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: capture-vms\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = vec![0u8; 2048];
    let n = tokio::time::timeout(Duration::from_millis(1500), s.read(&mut buf)).await.ok()?.ok()?;
    let text = String::from_utf8_lossy(&buf[..n]).to_string();
    let status = text.lines().next()?.trim().to_string();
    if !status.starts_with("RTSP/") {
        return None;
    }
    let server = header(&text, "server");
    Some((status, server))
}

fn header(text: &str, name: &str) -> Option<String> {
    text.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim().chars().take(80).collect())
    })
}

fn html_title(body: &str) -> Option<String> {
    let lower = body.to_lowercase();
    let i = lower.find("<title")?;
    let start = i + lower[i..].find('>')? + 1;
    let end = start + lower[start..].find("</title")?;
    let t = body.get(start..end)?.split_whitespace().collect::<Vec<_>>().join(" ");
    (!t.is_empty()).then(|| t.chars().take(80).collect())
}

async fn http_info(client: &reqwest::Client, ip: Ipv4Addr, port: u16) -> (Option<String>, Option<String>, Option<String>) {
    let url = format!("http://{ip}:{port}/");
    let Ok(Ok(resp)) = tokio::time::timeout(Duration::from_secs(3), client.get(&url).timeout(Duration::from_secs(3)).send()).await else {
        return (None, None, None);
    };
    let server = resp.headers().get("server").and_then(|v| v.to_str().ok()).map(|s| s.chars().take(80).collect());
    let realm = resp
        .headers()
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split("realm=\"").nth(1))
        .and_then(|r| r.split('"').next())
        .map(|s| s.chars().take(60).collect());
    let body = tokio::time::timeout(Duration::from_secs(2), resp.text()).await.ok().and_then(|r| r.ok()).unwrap_or_default();
    // 한글 등 여러 바이트 글자 중간에서 자르면 패닉하므로 글자 경계까지 당긴다
    let mut cut = body.len().min(65_536);
    while !body.is_char_boundary(cut) {
        cut -= 1;
    }
    (server, html_title(&body[..cut]), realm)
}

async fn check_host(client: reqwest::Client, ip: Ipv4Addr, sem: Arc<Semaphore>) -> Option<FoundHost> {
    let _permit = sem.acquire_owned().await.ok()?;
    let timeout = Duration::from_millis(800);
    let (ping, open) = tokio::join!(
        ping_ms(ip, 800),
        futures_util::future::join_all(PORTS.iter().map(|(p, _)| async move { tcp_ms(ip, *p, timeout).await.map(|_| *p) }))
    );
    let open: Vec<u16> = open.into_iter().flatten().collect();
    if ping.is_none() && open.is_empty() {
        return None;
    }
    let rtsp_port = [554u16, 8554].into_iter().find(|p| open.contains(p));
    let web_port = [80u16, 8080].into_iter().find(|p| open.contains(p));
    let (rtsp, web, onv) = tokio::join!(
        async {
            match rtsp_port {
                Some(p) => rtsp_options(ip, p).await,
                None => None,
            }
        },
        async {
            match web_port {
                Some(p) => http_info(&client, ip, p).await,
                None => (None, None, None),
            }
        },
        async {
            if web_port.is_some() || rtsp_port.is_some() {
                tokio::task::spawn_blocking(move || onvif::probe_unicast(ip, Duration::from_millis(1200))).await.ok().flatten()
            } else {
                None
            }
        }
    );
    Some(FoundHost {
        ip: ip.to_string(),
        ping_ms: ping,
        likely_camera: rtsp.is_some() || onv.is_some() || rtsp_port.is_some(),
        open_ports: open,
        rtsp_status: rtsp.as_ref().map(|r| r.0.clone()),
        rtsp_server: rtsp.and_then(|r| r.1),
        http_server: web.0,
        http_title: web.1,
        http_realm: web.2,
        onvif: onv,
        same_subnet: local_match(ip).is_some(),
    })
}

pub async fn scan(client: &reqwest::Client, spec: &str) -> Result<ScanResult, String> {
    let targets = parse_targets(spec)?;
    let t0 = Instant::now();
    let sem = Arc::new(Semaphore::new(96));
    let single = targets.len() == 1;
    let diag = async {
        if single {
            Some(diagnose(&targets[0].to_string()).await)
        } else {
            None
        }
    };
    let found = futures_util::future::join_all(targets.iter().map(|ip| check_host(client.clone(), *ip, sem.clone())));
    let (diag, found) = tokio::join!(diag, found);
    let mut hosts: Vec<FoundHost> = found.into_iter().flatten().collect();
    hosts.sort_by_key(|h| (!h.likely_camera, h.ip.parse::<Ipv4Addr>().map(u32::from).unwrap_or(0)));
    Ok(ScanResult { scanned: targets.len(), elapsed_ms: t0.elapsed().as_secs_f64() * 1000.0, hosts, diagnosis: diag })
}

// ---------------- MAC 주소 · RTSP 포트로 찾기 (카메라 찾기·주소 따라가기용) ----------------

/// 그 IP를 쓰는 장치의 MAC 주소 (예: "00:00:5e:00:53:18"). 같은 대역(공유기를 거치지 않는 주소)일 때만 알 수 있다.
/// 공유기를 껐다 켜서 IP가 바뀌어도 MAC은 그대로라서 같은 카메라인지 알아보는 데 쓴다. ARP 질문 하나만 보낸다.
/// 응답이 없는 주소에 부르면 몇 초 걸리므로, 방금 응답한 주소에만 부른다.
pub fn mac_of(ip: Ipv4Addr) -> Option<String> {
    if let Some(m) = test_mac(ip) {
        return Some(m);
    }
    if ip.is_loopback() || local_match(ip).is_none() {
        return None;
    }
    arp(ip)
}

#[cfg(windows)]
fn arp(ip: Ipv4Addr) -> Option<String> {
    use windows_sys::Win32::NetworkManagement::IpHelper::SendARP;
    let mut mac = [0u8; 8];
    let mut len = 6u32;
    // 주소는 네트워크 바이트 순서 그대로
    let r = unsafe { SendARP(u32::from_ne_bytes(ip.octets()), 0, mac.as_mut_ptr().cast(), &mut len) };
    (r == 0 && len == 6 && mac[..6].iter().any(|b| *b != 0)).then(|| format_mac(&mac[..6]))
}

#[cfg(not(windows))]
fn arp(_ip: Ipv4Addr) -> Option<String> {
    None
}

fn format_mac(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect::<Vec<_>>().join(":")
}

/// (디버그 빌드만) `CAPTURE_VMS_TEST_MACS=<파일>`의 `IP=MAC` 줄로 MAC을 대신 준다 — 이 PC의 127.0.0.x에 띄운 가짜 카메라에는
/// ARP가 없어서, MAC으로 같은 카메라를 알아보는 길을 시험하려는 것. 릴리스 빌드에서는 항상 None.
fn test_mac(ip: Ipv4Addr) -> Option<String> {
    if !cfg!(debug_assertions) {
        return None;
    }
    let text = std::fs::read_to_string(std::env::var("CAPTURE_VMS_TEST_MACS").ok()?).ok()?;
    let ip = ip.to_string();
    text.lines().filter_map(|l| l.trim().split_once('=')).find(|(k, _)| k.trim() == ip).map(|(_, v)| v.trim().to_ascii_lowercase())
}

/// RTSP로 답한 장치
pub struct RtspHost {
    pub ip: Ipv4Addr,
    pub port: u16,
    /// 장치가 알려 준 서버 이름 (제조사가 보이는 경우가 많다)
    pub server: Option<String>,
}

/// 이 PC와 같은 대역의 주소마다 RTSP 포트(554·8554)를 두드려, RTSP로 답하는 장치를 찾는다 (TCP, 계정 없이 OPTIONS 한 번).
/// ONVIF 탐색(UDP)이 방화벽·공유기에 막힌 환경이나 ONVIF가 꺼진 카메라도 이 방법으로는 보인다. 약 1~2초.
pub async fn sweep_rtsp(nets: &[(Ipv4Addr, u8)]) -> Vec<RtspHost> {
    let mut targets: Vec<Ipv4Addr> = Vec::new();
    for (local, prefix) in nets {
        for t in onvif::sweep_targets(*local, *prefix) {
            if !targets.contains(&t) {
                targets.push(t);
            }
        }
    }
    targets.truncate(MAX_HOSTS);
    let sem = Arc::new(Semaphore::new(128));
    let found = futures_util::future::join_all(targets.into_iter().map(|ip| {
        let sem = sem.clone();
        async move {
            let _permit = sem.acquire_owned().await.ok()?;
            let timeout = Duration::from_millis(700);
            let (a, b) = tokio::join!(tcp_ms(ip, 554, timeout), tcp_ms(ip, 8554, timeout));
            for port in [a.map(|_| 554u16), b.map(|_| 8554u16)].into_iter().flatten() {
                if let Some((_, server)) = rtsp_options(ip, port).await {
                    return Some(RtspHost { ip, port, server });
                }
            }
            None
        }
    }))
    .await;
    found.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_mac_like_a_label() {
        assert_eq!(format_mac(&[0x00, 0x00, 0x5E, 0x00, 0x53, 0x18]), "00:00:5e:00:53:18");
        // 이 PC 자신(127.x)에는 MAC이 없다
        assert!(arp_free_lookup_is_none());
    }

    /// 실제 ARP로 MAC을 읽는 길 (수동 실행: `cargo test real_arp -- --ignored --nocapture`).
    /// 이 PC의 공유기(게이트웨이) 주소 하나에만 묻는다. `arp -a`의 값과 같아야 한다
    #[test]
    #[ignore]
    fn real_arp_reads_the_gateway_mac() {
        let gw = adapters().into_iter().filter(|a| a.up).flat_map(|a| a.gateways).find_map(|g| g.parse::<Ipv4Addr>().ok()).expect("이 PC에 게이트웨이가 없음");
        let mac = mac_of(gw).expect("게이트웨이의 MAC을 읽지 못함");
        println!("{gw} -> {mac}");
        assert_eq!(mac.len(), 17);
        assert!(mac.split(':').count() == 6 && mac.chars().all(|c| c == ':' || c.is_ascii_hexdigit()));
    }

    fn arp_free_lookup_is_none() -> bool {
        std::env::var("CAPTURE_VMS_TEST_MACS").is_ok() || mac_of(Ipv4Addr::new(127, 0, 0, 1)).is_none()
    }

    #[test]
    fn parses_ranges() {
        assert_eq!(parse_targets("192.168.2.27").unwrap(), vec![Ipv4Addr::new(192, 168, 2, 27)]);
        let r = parse_targets("192.168.2.1-10").unwrap();
        assert_eq!(r.len(), 10);
        let c = parse_targets("192.168.2.0/24").unwrap();
        assert_eq!((c.len(), c[0], c[253]), (254, Ipv4Addr::new(192, 168, 2, 1), Ipv4Addr::new(192, 168, 2, 254)));
        assert_eq!(parse_targets("10.0.0.5, 10.0.0.5 10.0.0.6").unwrap().len(), 2);
        assert!(parse_targets("10.0.0.0/16").is_err());
        assert!(parse_targets("10.0.0.9-3").is_err());
        assert!(parse_targets("카메라").is_err());
        assert!(parse_targets("").is_err());
    }

    #[test]
    fn subnet_math() {
        let ip = Ipv4Addr::new(192, 168, 2, 27);
        assert!(in_subnet(ip, Ipv4Addr::new(192, 168, 2, 200), 24));
        assert!(!in_subnet(ip, Ipv4Addr::new(10, 0, 0, 5), 24));
        assert!(in_subnet(ip, Ipv4Addr::new(192, 168, 0, 1), 16));
        assert_eq!(network_of(ip, 24), "192.168.2.0/24");
    }

    #[test]
    fn reads_headers_and_title() {
        let t = "RTSP/1.0 200 OK\r\nCSeq: 1\r\nServer: ACME RTSP Server\r\n\r\n";
        assert_eq!(header(t, "server").as_deref(), Some("ACME RTSP Server"));
        assert_eq!(html_title("<html><head><TITLE>\n CAM-1000  Web </TITLE>").as_deref(), Some("CAM-1000 Web"));
    }

    #[test]
    fn reads_local_adapters() {
        // 어댑터가 하나도 없는 PC는 없으니 최소한 호출이 안전한지만 본다
        let _ = adapters();
        assert!(subnet_hint("127.0.0.1") == Some("same"));
        assert!(subnet_hint("카메라").is_none());
    }
}
