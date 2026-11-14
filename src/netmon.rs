//! 네트워크 모니터.
//!
//! - 카메라마다 주기적으로 ICMP ping + TCP 연결 시간을 잰다.
//! - 노트북 Wi-Fi 상태(신호, BSSID, 속도)를 주기적으로 읽는다.
//!   BSSID가 바뀌면 중계기 ↔ 공유기 사이를 옮겨 탄 것(로밍)으로 기록한다.

use std::{net::Ipv4Addr, sync::Arc, time::Duration, time::Instant};

use chrono::Local;
use tokio::{net::TcpStream, time::MissedTickBehavior};

use crate::{
    app::{App, CamRuntime},
    store::{Event, ProbeSample, WifiInfo},
};

pub fn spawn(app: Arc<App>) {
    for cam in app.cameras() {
        start_probe(&app, &cam);
    }
    tokio::spawn(wifi_loop(app.clone()));
    tokio::spawn(load_loop(app.clone()));
    tokio::spawn(crate::stalldiag::heartbeat(app.clone()));
    tokio::spawn(retention_loop(app));
}

/// PC 부하 기록: 수신 중일 때 30초마다 CPU 사용률과 메모리를 프로그램 로그(app.log)에 남긴다.
/// 다른 PC에서 시험한 뒤, 화면이 버벅이거나 촬영이 느린 것이 PC 성능 때문인지 가릴 때 본다.
async fn load_loop(app: Arc<App>) {
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut prev = host::cpu_times();
    let (mut sum, mut peak, mut n) = (0.0f64, 0.0f64, 0u32);
    loop {
        tick.tick().await;
        if app.is_stopping() {
            break;
        }
        let now = host::cpu_times();
        let pct = prev.zip(now).and_then(|(a, b)| host::cpu_pct(a, b));
        prev = now;
        let sessions = app.active_sessions().len();
        let Some(pct) = pct.filter(|_| sessions > 0) else {
            (sum, peak, n) = (0.0, 0.0, 0);
            continue;
        };
        sum += pct;
        peak = peak.max(pct);
        n += 1;
        if n >= 3 {
            let mem = host::memory_gb().map_or(String::new(), |(used, total)| format!(" · 메모리 {used:.1}/{total:.1}GB"));
            tracing::info!("PC 부하 (최근 30초): CPU 평균 {:.0}% · 최고 {:.0}%{mem} · 수신 {sessions}대", sum / n as f64, peak);
            (sum, peak, n) = (0.0, 0.0, 0);
        }
    }
}

pub(crate) mod host {
    /// (쉬는 시간, 전체 시간) 누적값. 단위는 100나노초
    #[cfg(windows)]
    pub fn cpu_times() -> Option<(u64, u64)> {
        use windows_sys::Win32::{Foundation::FILETIME, System::Threading::GetSystemTimes};
        let to_u64 = |f: FILETIME| ((f.dwHighDateTime as u64) << 32) | f.dwLowDateTime as u64;
        let zero = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
        let (mut idle, mut kernel, mut user) = (zero, zero, zero);
        // kernel 시간에는 쉬는 시간이 들어 있다
        let ok = unsafe { GetSystemTimes(&mut idle, &mut kernel, &mut user) } != 0;
        ok.then(|| (to_u64(idle), to_u64(kernel) + to_u64(user)))
    }

    #[cfg(not(windows))]
    pub fn cpu_times() -> Option<(u64, u64)> {
        None
    }

    /// 두 시점 사이의 PC 전체 CPU 사용률(%)
    pub fn cpu_pct(prev: (u64, u64), now: (u64, u64)) -> Option<f64> {
        let total = now.1.checked_sub(prev.1).filter(|t| *t > 0)?;
        let idle = now.0.saturating_sub(prev.0).min(total);
        Some((total - idle) as f64 / total as f64 * 100.0)
    }

    /// (쓰는 중, 전체) 메모리 GB
    #[cfg(windows)]
    pub fn memory_gb() -> Option<(f64, f64)> {
        use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
        let mut m: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        m.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
        let ok = unsafe { GlobalMemoryStatusEx(&mut m) } != 0;
        ok.then(|| ((m.ullTotalPhys - m.ullAvailPhys) as f64 / 1e9, m.ullTotalPhys as f64 / 1e9))
    }

    #[cfg(not(windows))]
    pub fn memory_gb() -> Option<(f64, f64)> {
        None
    }

    #[cfg(test)]
    mod tests {
        use super::cpu_pct;

        #[test]
        fn cpu_percent_from_idle_and_total() {
            // 10초(1억 단위) 동안 쉰 시간이 7.5초면 사용률 25%
            assert_eq!(cpu_pct((1_000, 5_000), (75_001_000, 100_005_000)), Some(25.0));
            assert_eq!(cpu_pct((0, 100), (0, 100)), None);
            // 쉰 시간이 전체보다 크게 읽혀도 0% 아래로 가지 않는다
            assert_eq!(cpu_pct((0, 0), (200, 100)), Some(0.0));
        }
    }
}

/// 카메라 한 대의 ping/TCP 모니터링 시작 (카메라를 추가·수정했을 때도 부른다)
pub fn start_probe(app: &Arc<App>, cam: &Arc<CamRuntime>) {
    cam.set_probe(tokio::spawn(probe_loop(app.clone(), cam.clone())));
}

pub(crate) async fn resolve_ipv4(host: &str) -> Option<Ipv4Addr> {
    if let Ok(ip) = host.parse::<Ipv4Addr>() {
        return Some(ip);
    }
    tokio::net::lookup_host((host, 0)).await.ok()?.find_map(|a| match a.ip() {
        std::net::IpAddr::V4(v4) => Some(v4),
        _ => None,
    })
}

async fn tcp_probe(host: &str, port: u16, timeout: Duration) -> Option<f64> {
    let t0 = Instant::now();
    match tokio::time::timeout(timeout, TcpStream::connect((host, port))).await {
        Ok(Ok(_stream)) => Some((t0.elapsed().as_secs_f64() * 100_000.0).round() / 100.0),
        _ => None,
    }
}

async fn probe_loop(app: Arc<App>, cam: Arc<CamRuntime>) {
    let m = &app.cfg.monitor;
    let mut tick = tokio::time::interval(Duration::from_secs(m.probe_interval_secs.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let tcp_timeout = Duration::from_millis(m.tcp_timeout_ms);
    let icmp_timeout = m.icmp_timeout_ms;
    let down_after = m.down_after_failures.max(1);

    loop {
        tick.tick().await;
        if app.is_stopping() || cam.is_removed() {
            break;
        }
        let cam = &cam;
        let host = cam.cfg.ip();
        let ip = resolve_ipv4(&host).await;
        let icmp_ms = match ip {
            Some(ip) => tokio::task::spawn_blocking(move || icmp::ping(ip, icmp_timeout))
                .await
                .ok()
                .flatten(),
            None => None,
        };
        // RTSP 포트(보통 554)가 열려 있는지
        let tcp_ms = tcp_probe(&host, cam.cfg.rtsp_port(), tcp_timeout).await;

        let sample = ProbeSample {
            ts: Local::now(),
            camera_id: cam.cfg.id.clone(),
            ip: ip.map(|i| i.to_string()),
            icmp_ok: icmp_ms.is_some(),
            icmp_ms,
            tcp_ok: tcp_ms.is_some(),
            tcp_ms,
        };

        // ping이 막힌 카메라도 있으므로 둘 중 하나라도 되면 '연결됨'으로 본다.
        let reachable = sample.icmp_ok || sample.tcp_ok;
        let mut events = Vec::new();
        {
            let mut st = cam.status.lock().unwrap();
            st.last_probe = Some(sample.ts);
            st.ip = sample.ip.clone();
            st.last_icmp_ms = icmp_ms;
            st.last_tcp_ms = tcp_ms;
            if reachable {
                if st.online != Some(true) {
                    let msg = if st.online.is_none() { "카메라 연결 확인" } else { "카메라 연결 복구" };
                    events.push(
                        Event::new("camera_up", Some(&cam.cfg.id), msg)
                            .with_data(serde_json::json!({ "after_failures": st.consecutive_fail })),
                    );
                }
                st.online = Some(true);
                st.consecutive_fail = 0;
            } else {
                st.consecutive_fail += 1;
                if st.consecutive_fail == down_after {
                    st.online = Some(false);
                    events.push(Event::new(
                        "camera_down",
                        Some(&cam.cfg.id),
                        format!("카메라 응답 없음 (연속 {}회)", st.consecutive_fail),
                    ));
                }
            }
        }
        for e in events {
            app.store.record_event(e);
        }
        app.store.record_probe(sample);
    }
}

async fn wifi_loop(app: Arc<App>) {
    let mut tick = tokio::time::interval(Duration::from_secs(app.cfg.monitor.wifi_interval_secs.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut warned = false;
    let mut hold = WifiHold::default();

    loop {
        tick.tick().await;
        if app.is_stopping() {
            break;
        }
        let Some(now) = tokio::task::spawn_blocking(wifi::query).await.ok().flatten() else {
            if !warned {
                tracing::warn!("Wi-Fi 상태를 읽지 못했습니다 (무선 어댑터 없음 또는 WLAN 서비스 꺼짐)");
                warned = true;
            }
            continue;
        };

        let prev = app.wifi();
        if let Some(prev) = &prev {
            if prev.state != now.state {
                let (kind, msg) = if now.state == "connected" {
                    ("wifi_connected", format!("Wi-Fi 연결됨 ({})", now.ssid.clone().unwrap_or_default()))
                } else {
                    ("wifi_disconnected", "Wi-Fi 연결 끊김".to_string())
                };
                app.store.record_event(Event::new(kind, None, msg));
            } else if now.state == "connected" && prev.bssid.is_some() && now.bssid.is_some() && prev.bssid != now.bssid {
                app.store.record_event(
                    Event::new(
                        "wifi_roam",
                        None,
                        format!(
                            "AP 전환: {} → {}",
                            prev.bssid.clone().unwrap_or_default(),
                            now.bssid.clone().unwrap_or_default()
                        ),
                    )
                    .with_data(serde_json::json!({
                        "from": prev.bssid, "to": now.bssid,
                        "signal_before": prev.signal_pct, "signal_after": now.signal_pct,
                    })),
                );
            }
        }
        if app.cfg.monitor.wifi_hold {
            hold.step(&app, &now).await;
        }
        *app.wifi_latest.write().unwrap() = Some(now.clone());
        app.store.record_wifi(now);
    }
}

/// Wi-Fi 고정: 수신 중에 PC가 다른 Wi-Fi로 넘어가거나 Wi-Fi가 끊긴 채 남아 영상이 안 오면,
/// 영상을 받고 있던 Wi-Fi로 다시 연결한다.
///
/// 카메라 쪽 Wi-Fi에 인터넷이 없으면 Windows가 인터넷 되는 다른 저장된 Wi-Fi로 스스로 옮겨 가고,
/// 수동 연결로 저장된 Wi-Fi는 끊긴 뒤 다시 붙지 않는다. 그러면 사람이 다시 붙일 때까지 수신이 멈춘다.
#[derive(Default)]
struct WifiHold {
    /// 영상이 들어오던 때의 (SSID, 프로필 이름)
    home: Option<(String, String)>,
    /// 수신이 멈춘 채 다른 곳에 있는(또는 끊긴) 것으로 본 연속 횟수
    away: u32,
    last_try: Option<Instant>,
}

impl WifiHold {
    /// 다시 연결을 시도하는 최소 간격
    const RETRY: Duration = Duration::from_secs(15);

    /// 이번 확인에서 다시 연결해야 하면 (돌아갈 SSID, 프로필 이름, 지금 붙어 있는 곳)을 돌려준다.
    /// `wifi_sessions`: Wi-Fi로 받는 수신이 있는지, `receiving`: 그중 지금 영상이 들어오는 것이 있는지
    fn decide(&mut self, wifi_sessions: bool, receiving: bool, now: &WifiInfo) -> Option<(String, String, String)> {
        if !wifi_sessions {
            *self = Self::default();
            return None;
        }
        let connected = now.state == "connected";
        if receiving {
            // 영상이 들어오는 동안 붙어 있는 Wi-Fi가 '카메라 쪽 Wi-Fi'
            if let (true, Some(ssid), Some(profile)) = (connected, &now.ssid, &now.profile) {
                self.home = Some((ssid.clone(), profile.clone()));
            }
            self.away = 0;
            return None;
        }
        let (ssid, profile) = self.home.clone()?;
        if connected && now.ssid.as_deref() == Some(ssid.as_str()) {
            // 같은 Wi-Fi에 붙어 있는데 영상만 안 오는 것은 여기서 할 일이 아니다 (수신 쪽이 다시 연결한다)
            self.away = 0;
            return None;
        }
        self.away += 1;
        // 바로 하지 않는다: 잠깐 끊겼다가 Windows가 스스로 다시 붙는 중일 수 있다.
        // 다른 Wi-Fi에 붙어 있으면 두 번째 확인에서, 끊긴 채면 세 번째 확인에서 한다
        let need = if connected { 2 } else { 3 };
        if self.away < need || self.last_try.map_or(false, |t| t.elapsed() < Self::RETRY) {
            return None;
        }
        self.last_try = Some(Instant::now());
        let at = if connected { now.ssid.clone().unwrap_or_default() } else { "끊김".to_string() };
        Some((ssid, profile, at))
    }

    async fn step(&mut self, app: &Arc<App>, now: &WifiInfo) {
        let sessions: Vec<_> = app.active_sessions().into_iter().filter(|s| s.via_wifi()).collect();
        let receiving = sessions.iter().any(|s| s.is_receiving());
        let Some((ssid, profile, at)) = self.decide(!sessions.is_empty(), receiving, now) else { return };
        let (p, s) = (profile.clone(), ssid.clone());
        let res = tokio::task::spawn_blocking(move || wifi::connect(&p, &s)).await.unwrap_or_else(|e| Err(e.to_string()));
        let msg = match &res {
            Ok(()) => format!("Wi-Fi 고정: 수신 중 Wi-Fi가 바뀌어({at}) 영상이 멈춤 → {ssid}에 다시 연결합니다"),
            Err(e) => format!("Wi-Fi 고정: {ssid}에 다시 연결하지 못했습니다 ({e})"),
        };
        app.store.record_event(Event::new("wifi_hold", None, msg).with_data(serde_json::json!({ "from": at, "to": ssid, "profile": profile, "ok": res.is_ok() })));
    }
}

async fn retention_loop(app: Arc<App>) {
    let mut tick = tokio::time::interval(Duration::from_secs(6 * 3600));
    loop {
        tick.tick().await;
        if app.is_stopping() {
            break;
        }
        // 설정 화면에서 바뀔 수 있으므로 매번 읽는다
        let days = app.retention_days.load(std::sync::atomic::Ordering::Relaxed);
        let a = app.clone();
        let _ = tokio::task::spawn_blocking(move || a.store.cleanup(days)).await;
    }
}

#[cfg(test)]
mod hold_tests {
    use super::*;

    fn wifi(state: &str, ssid: Option<&str>, profile: Option<&str>) -> WifiInfo {
        let mut w = wifi::parse("    Name                   : Wi-Fi\n    State                  : disconnected\n").unwrap();
        w.state = state.into();
        w.ssid = ssid.map(str::to_string);
        w.profile = profile.map(str::to_string);
        w
    }

    #[test]
    fn returns_to_the_wifi_where_video_was_flowing() {
        let mut h = WifiHold::default();
        let cam = wifi("connected", Some("CamNet5G"), Some("CamNet5G 2"));
        let office = wifi("connected", Some("OfficeNet"), Some("OfficeNet"));
        let off = wifi("disconnected", None, None);
        // 영상이 들어오는 동안: 아무것도 하지 않고 그 Wi-Fi를 기억한다
        assert_eq!(h.decide(true, true, &cam), None);
        // 같은 Wi-Fi에서 영상만 멈춘 것(주기적 끊김)은 건드리지 않는다
        assert_eq!(h.decide(true, false, &cam), None);
        assert_eq!(h.decide(true, false, &cam), None);
        // Wi-Fi가 끊겼다가 다른 곳에 붙음: 한 번은 넘기고, 두 번째에 원래 Wi-Fi로
        assert_eq!(h.decide(true, false, &off), None);
        assert_eq!(h.decide(true, false, &office), Some(("CamNet5G".into(), "CamNet5G 2".into(), "OfficeNet".into())));
        // 바로 또 시도하지 않는다
        assert_eq!(h.decide(true, false, &office), None);
        // 돌아와 영상이 들어오면 처음 상태로
        assert_eq!(h.decide(true, true, &cam), None);
        assert_eq!(h.away, 0);
        // 수신이 없으면 다른 Wi-Fi에 붙어 있어도 아무것도 하지 않는다 (사용자가 일부러 옮긴 것)
        assert_eq!(h.decide(false, false, &office), None);
        assert_eq!(h.decide(false, false, &office), None);
        assert!(h.home.is_none());
        // 수신을 다른 Wi-Fi에서 시작했다가 영상이 안 온 경우: 돌아갈 곳을 모르므로 하지 않는다
        assert_eq!(h.decide(true, false, &office), None);
        assert_eq!(h.decide(true, false, &office), None);
    }

    #[test]
    fn reconnects_when_wifi_stays_disconnected() {
        // 수동 연결로 저장된 Wi-Fi는 끊긴 뒤 Windows가 다시 붙이지 않는다 → 세 번째 확인(약 10초)에서 다시 연결
        let mut h = WifiHold::default();
        let off = wifi("disconnected", None, None);
        assert_eq!(h.decide(true, true, &wifi("connected", Some("cam-net"), Some("cam-net"))), None);
        assert_eq!(h.decide(true, false, &off), None);
        assert_eq!(h.decide(true, false, &off), None);
        assert_eq!(h.decide(true, false, &off), Some(("cam-net".into(), "cam-net".into(), "끊김".into())));
    }
}

/// PC가 이 IP로 보낼 때 쓰는 인터페이스: (wifi | ethernet | other, "이름 (설명 · 속도)")
#[cfg(windows)]
pub fn route_to(ip: &str) -> Option<(Option<String>, Option<String>)> {
    use windows_sys::Win32::NetworkManagement::IpHelper::{GetBestInterface, GetIfEntry2, MIB_IF_ROW2};
    let ip: Ipv4Addr = ip.parse().ok()?;
    let wide = |s: &[u16]| {
        let end = s.iter().position(|&c| c == 0).unwrap_or(s.len());
        String::from_utf16_lossy(&s[..end])
    };
    unsafe {
        let mut idx = 0u32;
        if GetBestInterface(u32::from_ne_bytes(ip.octets()), &mut idx) != 0 {
            return None;
        }
        let mut row: MIB_IF_ROW2 = std::mem::zeroed();
        row.InterfaceIndex = idx;
        if GetIfEntry2(&mut row) != 0 {
            return Some((None, None));
        }
        // IF_TYPE_IEEE80211 = 71, IF_TYPE_ETHERNET_CSMACD = 6
        let kind = match row.Type {
            71 => "wifi",
            6 => "ethernet",
            _ => "other",
        };
        let bps = row.ReceiveLinkSpeed;
        let speed = if bps >= 1_000_000_000 {
            format!(" · {:.1} Gbps", bps as f64 / 1e9)
        } else if bps > 0 {
            format!(" · {:.0} Mbps", bps as f64 / 1e6)
        } else {
            String::new()
        };
        let name = format!("{} ({}{speed})", wide(&row.Alias), wide(&row.Description));
        Some((Some(kind.to_string()), Some(name)))
    }
}

#[cfg(not(windows))]
pub fn route_to(_ip: &str) -> Option<(Option<String>, Option<String>)> {
    None
}

#[cfg(windows)]
pub(crate) mod icmp {
    use std::net::Ipv4Addr;

    use windows_sys::Win32::{
        Foundation::INVALID_HANDLE_VALUE,
        NetworkManagement::IpHelper::{IcmpCloseHandle, IcmpCreateFile, IcmpSendEcho, ICMP_ECHO_REPLY},
    };

    /// Windows ICMP API. 관리자 권한 없이 동작한다. 실패/시간 초과면 None.
    pub fn ping(ip: Ipv4Addr, timeout_ms: u32) -> Option<f64> {
        let payload = [0x61u8; 32];
        // 응답 구조체 + 페이로드 + 여유. u64로 잡아 정렬을 맞춘다.
        let mut reply = vec![0u64; 64];
        unsafe {
            let handle = IcmpCreateFile();
            if handle == INVALID_HANDLE_VALUE {
                return None;
            }
            let n = IcmpSendEcho(
                handle,
                u32::from_ne_bytes(ip.octets()),
                payload.as_ptr().cast(),
                payload.len() as u16,
                std::ptr::null(),
                reply.as_mut_ptr().cast(),
                (reply.len() * 8) as u32,
                timeout_ms,
            );
            let result = if n > 0 {
                let r = &*(reply.as_ptr() as *const ICMP_ECHO_REPLY);
                (r.Status == 0).then_some(r.RoundTripTime as f64)
            } else {
                None
            };
            IcmpCloseHandle(handle);
            result
        }
    }
}

#[cfg(not(windows))]
pub(crate) mod icmp {
    pub fn ping(_ip: std::net::Ipv4Addr, _timeout_ms: u32) -> Option<f64> {
        None
    }
}

/// 지금의 Wi-Fi 상태 (netsh를 띄우므로 0.1~0.3초 걸린다 — 끊김 진단이 끊긴 동안에만 부른다)
pub(crate) fn wifi_now() -> Option<WifiInfo> {
    wifi::query()
}

mod wifi {
    use super::WifiInfo;
    use chrono::Local;

    #[cfg(windows)]
    pub fn query() -> Option<WifiInfo> {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        // UTF-8 코드 페이지로 바꿔서 실행 (한글 Windows에서 SSID·항목명 깨짐 방지)
        let out = std::process::Command::new("cmd")
            .args(["/C", "chcp 65001 >nul & netsh wlan show interfaces"])
            .creation_flags(CREATE_NO_WINDOW)
            .output()
            .ok()?;
        parse(&String::from_utf8_lossy(&out.stdout))
    }

    #[cfg(not(windows))]
    pub fn query() -> Option<WifiInfo> {
        None
    }

    /// `netsh wlan show interfaces` 출력 해석. 영문/한글 항목명 모두 처리.
    pub fn parse(text: &str) -> Option<WifiInfo> {
        let mut blocks: Vec<Vec<(String, String)>> = Vec::new();
        for line in text.lines() {
            let Some((k, v)) = line.split_once(": ") else { continue };
            let key = k.trim().to_lowercase();
            let val = v.trim().to_string();
            if key == "name" || key == "이름" {
                blocks.push(Vec::new());
            }
            if let Some(b) = blocks.last_mut() {
                b.push((key, val));
            }
        }
        let get = |b: &Vec<(String, String)>, keys: &[&str]| {
            b.iter().find(|(k, _)| keys.contains(&k.as_str())).map(|(_, v)| v.clone())
        };
        let is_connected = |b: &Vec<(String, String)>| {
            get(b, &["state", "상태"]).map(|s| normalize_state(&s) == "connected").unwrap_or(false)
        };
        let block = blocks.iter().find(|b| is_connected(b)).or(blocks.first())?;

        let num = |s: Option<String>| s.and_then(|v| v.trim_end_matches('%').trim().parse::<f64>().ok());
        Some(WifiInfo {
            ts: Local::now(),
            interface: get(block, &["name", "이름"]).unwrap_or_default(),
            state: get(block, &["state", "상태"]).map(|s| normalize_state(&s)).unwrap_or_default(),
            ssid: get(block, &["ssid"]),
            bssid: get(block, &["ap bssid", "bssid"]).map(|s| s.to_lowercase()),
            band: get(block, &["band", "대역"]),
            channel: num(get(block, &["channel", "채널"])).map(|v| v as u32),
            signal_pct: num(get(block, &["signal", "신호"])).map(|v| v.clamp(0.0, 100.0) as u8),
            rssi_dbm: num(get(block, &["rssi"])).map(|v| v as i32),
            rx_mbps: num(get(block, &["receive rate (mbps)", "수신 속도(mbps)", "수신 속도 (mbps)"])),
            tx_mbps: num(get(block, &["transmit rate (mbps)", "전송 속도(mbps)", "전송 속도 (mbps)"])),
            profile: get(block, &["profile", "프로필"]).filter(|p| !p.is_empty()),
        })
    }

    /// 저장된 프로필로 Wi-Fi에 연결한다 (Windows의 Wi-Fi 목록에서 그 이름을 누르는 것과 같다)
    #[cfg(windows)]
    pub fn connect(profile: &str, ssid: &str) -> Result<(), String> {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let out = std::process::Command::new("netsh")
            .args(["wlan", "connect", &format!("name={profile}"), &format!("ssid={ssid}")])
            .creation_flags(CREATE_NO_WINDOW)
            .output()
            .map_err(|e| e.to_string())?;
        if out.status.success() {
            Ok(())
        } else {
            // netsh의 설명은 한글 Windows에서 코드 페이지가 달라 깨지므로 싣지 않는다 (프로필이 지워졌거나 무선이 꺼진 경우)
            Err(format!("netsh 오류 {}", out.status.code().unwrap_or(-1)))
        }
    }

    #[cfg(not(windows))]
    pub fn connect(_profile: &str, _ssid: &str) -> Result<(), String> {
        Err("Windows에서만 됩니다".into())
    }

    fn normalize_state(s: &str) -> String {
        let s = s.to_lowercase();
        // "disconnected"에 "connected"가 들어 있으므로 끊김을 먼저 확인
        if s.contains("disconnected") || s.contains("끊") {
            "disconnected".into()
        } else if s.contains("connected") || s.contains("연결됨") {
            "connected".into()
        } else {
            s
        }
    }

    #[cfg(test)]
    mod tests {
        use super::parse;

        #[test]
        fn parses_english_netsh() {
            let text = "There is 1 interface on the system:\n\n    Name                   : Wi-Fi\n    State                  : connected\n    SSID                   : OfficeNet\n    AP BSSID               : 00:00:5E:00:53:A1\n    Band                   : 2.4 GHz\n    Channel                : 6\n    Receive rate (Mbps)    : 144.4\n    Transmit rate (Mbps)   : 144.4\n    Signal                 : 95%\n    Rssi                   : -42\n";
            let w = parse(text).unwrap();
            assert_eq!(w.state, "connected");
            assert_eq!(w.ssid.as_deref(), Some("OfficeNet"));
            assert_eq!(w.bssid.as_deref(), Some("00:00:5e:00:53:a1"));
            assert_eq!(w.channel, Some(6));
            assert_eq!(w.signal_pct, Some(95));
            assert_eq!(w.rssi_dbm, Some(-42));
            assert_eq!(w.rx_mbps, Some(144.4));
            assert_eq!(w.profile, None);
        }

        #[test]
        fn parses_profile_name() {
            // 같은 SSID가 다른 보안 설정으로 두 번 저장되면 프로필 이름에 번호가 붙는다
            let text = "    Name                   : Wi-Fi\n    State                  : connected\n    SSID                   : CamNet5G\n    AP BSSID               : 00:00:5e:00:53:b2\n    Connection mode        : Profile\n    Signal                 : 99% \n    Profile                : CamNet5G 2 \n";
            let w = parse(text).unwrap();
            assert_eq!(w.ssid.as_deref(), Some("CamNet5G"));
            assert_eq!(w.profile.as_deref(), Some("CamNet5G 2"));
        }

        #[test]
        fn disconnected_is_not_connected() {
            let text = "    Name                   : Wi-Fi\n    State                  : disconnected\n";
            assert_eq!(parse(text).unwrap().state, "disconnected");
        }
    }
}
