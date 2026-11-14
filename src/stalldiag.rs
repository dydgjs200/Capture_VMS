//! 끊김 진단.
//!
//! 영상이 1초 넘게 안 오면, 다시 올 때까지 0.5초마다 카메라와 공유기에 ping을 보내고 카메라의 RTSP 포트와
//! Wi-Fi 상태를 같이 본다. 끊김이 끝나면 그 결과로 **어디가 끊겼는지**를 이벤트 `stall_diag` 한 줄로 남긴다:
//! PC↔공유기 구간인지, 공유기↔카메라 구간인지, 통신은 되는데 영상만 멈춘 것인지, 이 PC가 멈칫한 것인지.
//!
//! 평소의 ping·Wi-Fi 확인은 5초에 한 번이라 5~10초짜리 끊김은 한두 번밖에 걸리지 않고, 공유기까지의 ping이 없어
//! 끊긴 구간을 가를 수 없다. 그래서 끊긴 동안에만 촘촘히 본다 (평소에는 아무것도 보내지 않는다).

use std::{
    net::Ipv4Addr,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::time::MissedTickBehavior;

use crate::{app::App, netmon, netscan, store::Event, stream::Session};

/// 영상이 이만큼 안 오면 진단을 시작한다
const START_AFTER: Duration = Duration::from_millis(1000);
/// 끊긴 동안 점검하는 간격과, 한 번의 응답을 기다리는 시간
const ROUND_EVERY: Duration = Duration::from_millis(500);
const PROBE_TIMEOUT_MS: u32 = 400;
/// 한 번의 끊김을 이만큼까지만 따라간다 (그 뒤에도 끊겨 있으면 거기까지의 결과를 남긴다)
const MAX_FOLLOW: Duration = Duration::from_secs(60);
/// 프로그램이 이만큼 넘게 돌지 못하면 'PC 멈춤'으로 본다 (ms)
const FREEZE_MS: f64 = 1000.0;
/// 이 PC가 멈췄던 일을 끊김과 연결 짓는 범위: 끊김이 시작되기 이만큼 전부터
const FREEZE_LOOKBACK: Duration = Duration::from_secs(2);

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// 한 번의 끊김 동안 본 것
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Probes {
    /// 마지막 프레임부터 다시 들어온 프레임까지 (아직 끊겨 있으면 지금까지)
    pub gap_ms: f64,
    /// 점검 횟수
    pub rounds: u32,
    /// 카메라 ping 성공, RTSP 포트 연결 성공, 둘 중 하나라도 된 횟수 (ping을 막아 둔 카메라도 있다)
    pub cam_ping_ok: u32,
    pub cam_tcp_ok: u32,
    pub cam_reach_ok: u32,
    /// 공유기(게이트웨이) ping (성공, 시도). 주소를 모르거나 평소에도 ping에 답하지 않는 공유기면 None
    pub gw_ping: Option<(u32, u32)>,
    /// Wi-Fi로 받는 세션이면 끊긴 동안 본 상태 (바뀔 때만 적는다)
    pub wifi_states: Vec<String>,
    /// 끊긴 동안 붙어 있는 AP(BSSID)가 바뀌었는지
    pub bssid_changed: bool,
    /// 그 무렵 프로그램이 돌지 못한 시간
    pub pc_freeze_ms: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// 이 PC(프로그램)가 멈춰 있었다
    PcFreeze,
    /// PC ↔ 공유기 구간 (Wi-Fi로 받는 중이면 Wi-Fi 연결)
    PcToRouter,
    /// 공유기까지는 되는데 카메라에 닿지 않는다
    BeyondRouter,
    /// 카메라에 닿지 않는다 (공유기까지 되는지는 모름)
    NetworkPath,
    /// 카메라는 ping에 답하는데 영상 포트가 닫혀 있다
    CameraService,
    /// 통신은 되는데 영상만 오지 않는다
    VideoOnly,
    /// 반반이라 가르기 어렵다
    Unclear,
}

impl Cause {
    pub fn code(self) -> &'static str {
        match self {
            Cause::PcFreeze => "pc_freeze",
            Cause::PcToRouter => "pc_to_router",
            Cause::BeyondRouter => "beyond_router",
            Cause::NetworkPath => "network_path",
            Cause::CameraService => "camera_service",
            Cause::VideoOnly => "video_only",
            Cause::Unclear => "unclear",
        }
    }
}

/// 끊긴 동안 본 것으로 어디가 끊겼는지 가른다
pub fn classify(p: &Probes) -> Cause {
    if p.pc_freeze_ms.is_some_and(|f| f >= FREEZE_MS && f >= p.gap_ms * 0.5) {
        return Cause::PcFreeze;
    }
    if p.rounds == 0 {
        return Cause::Unclear;
    }
    let most = |ok: u32, n: u32| ok * 2 > n;
    let few = |ok: u32, n: u32| ok * 2 < n;
    // Wi-Fi가 끊겼다 붙었거나 다른 AP로 넘어갔으면 그것이 원인
    if p.wifi_states.iter().any(|s| s != "connected") || p.bssid_changed {
        return Cause::PcToRouter;
    }
    if few(p.cam_reach_ok, p.rounds) {
        // 카메라에 닿지 않았다 = 영상과 상관없이 통신이 끊김. 공유기까지 되는지로 구간을 가른다
        return match p.gw_ping {
            Some((ok, n)) if n > 0 && few(ok, n) => Cause::PcToRouter,
            Some((ok, n)) if n > 0 && most(ok, n) => Cause::BeyondRouter,
            _ => Cause::NetworkPath,
        };
    }
    if most(p.cam_reach_ok, p.rounds) {
        if most(p.cam_ping_ok, p.rounds) && few(p.cam_tcp_ok, p.rounds) {
            return Cause::CameraService;
        }
        return Cause::VideoOnly;
    }
    Cause::Unclear
}

/// 사람이 읽는 한 줄. `reconnected`: 프로그램이 새로 연결해서 돌아왔는지, `ongoing`: 아직 끊겨 있는지
pub fn describe(cause: Cause, p: &Probes, via_wifi: bool, reconnected: bool, ongoing: bool) -> String {
    let link = if via_wifi { "PC↔공유기 Wi-Fi 구간" } else { "PC↔공유기 구간" };
    let what = match cause {
        Cause::PcFreeze => format!("이 PC가 {:.1}초 멈춤 (프로그램이 그동안 돌지 못함 — 절전·과부하·보안 프로그램 등)", p.pc_freeze_ms.unwrap_or(0.0) / 1000.0),
        Cause::PcToRouter => format!("네트워크 문제 · {link} (카메라도 공유기도 닿지 않거나 Wi-Fi 연결이 바뀜)"),
        Cause::BeyondRouter => "네트워크 문제 · 공유기↔카메라 구간 또는 카메라 (공유기까지는 정상)".to_string(),
        Cause::NetworkPath => "네트워크 문제 · 카메라에 닿지 않음 (공유기 주소를 모르거나 공유기가 ping에 답하지 않아, 공유기 앞인지 뒤인지는 가르지 못함)".to_string(),
        Cause::CameraService => "카메라는 응답하는데 영상 포트(RTSP)가 닫힘 (카메라 재시작·카메라 쪽 문제)".to_string(),
        Cause::VideoOnly => "통신은 정상인데 영상만 멈춤 (카메라의 영상 전송 또는 이 PC의 수신 프로그램 쪽 — 네트워크 끊김 아님)".to_string(),
        Cause::Unclear => "가르기 어려움 (점검 결과가 반반이거나 너무 짧음)".to_string(),
    };
    let mut seen = Vec::new();
    if p.rounds > 0 {
        seen.push(format!("카메라 ping {}/{}", p.cam_ping_ok, p.rounds));
        seen.push(format!("영상 포트 {}/{}", p.cam_tcp_ok, p.rounds));
    }
    if let Some((ok, n)) = p.gw_ping {
        seen.push(format!("공유기 ping {ok}/{n}"));
    }
    if !p.wifi_states.is_empty() {
        seen.push(format!("Wi-Fi {}{}", p.wifi_states.join("→"), if p.bssid_changed { " (AP 바뀜)" } else { "" }));
    }
    let end = if ongoing {
        "아직 끊겨 있음"
    } else if reconnected {
        "새로 연결해서 복구"
    } else {
        "같은 연결에서 저절로 복구"
    };
    let seen = if seen.is_empty() { String::new() } else { format!(" · 끊긴 동안 {}", seen.join(", ")) };
    format!("끊김 진단: {:.1}초 끊김 — {what}{seen} · {end}", p.gap_ms / 1000.0)
}

/// 프로그램이 제때 돌고 있는지 본다. 0.2초마다 깨어나는데 1초 넘게 늦으면 그동안 이 PC(프로그램)가 멈춰 있던 것이다 —
/// 그동안은 FFmpeg가 보내는 프레임도 읽지 못해 영상이 끊긴 것처럼 기록된다.
pub async fn heartbeat(app: Arc<App>) {
    let step = Duration::from_millis(200);
    let mut prev = Instant::now();
    loop {
        tokio::time::sleep(step).await;
        if app.is_stopping() {
            break;
        }
        let now = Instant::now();
        let over = ms(now - prev) - ms(step);
        prev = now;
        if over < FREEZE_MS {
            continue;
        }
        *app.last_freeze.lock().unwrap() = Some((now, over));
        if !app.active_sessions().is_empty() {
            app.store.record_event(
                Event::new(
                    "pc_stall",
                    None,
                    format!("이 PC(프로그램)가 {:.1}초 동안 멈췄습니다 (절전·과부하·보안 프로그램 등) — 이 무렵 기록된 끊김은 네트워크가 아니라 이 PC가 멈춘 탓입니다", over / 1000.0),
                )
                    .with_data(serde_json::json!({ "ms": over })),
            );
        }
    }
}

/// 세션 하나를 지켜보다가 영상이 끊기면 진단한다 (세션이 끝나면 호출한 쪽이 멈춘다)
pub async fn watch(app: Arc<App>, s: Arc<Session>) {
    let Some((host, port)) = s.rtsp_host_port() else { return };
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if s.is_stopping() || app.is_stopping() {
            continue;
        }
        let (gap, ever, attempt) = s.frame_gap();
        // 처음 연결하는 중(아직 한 장도 못 받음)은 끊김이 아니다
        if let Some(gap) = gap.filter(|g| ever && *g >= START_AFTER) {
            incident(&app, &s, &host, port, gap, attempt).await;
        }
    }
}

/// ping 응답 시간(ms). 답이 없으면 None
async fn ping(ip: Option<Ipv4Addr>) -> Option<f64> {
    let ip = ip?;
    tokio::task::spawn_blocking(move || netmon::icmp::ping(ip, PROBE_TIMEOUT_MS)).await.ok().flatten()
}

/// 기록에 남길 시간순 점검 결과는 이만큼까지 (0.5초 간격이면 1분)
const TIMELINE_MAX: usize = 120;

async fn incident(app: &Arc<App>, s: &Arc<Session>, host: &str, port: u16, gap: Duration, attempt0: u32) {
    let began = Instant::now() - gap;
    let ip = netmon::resolve_ipv4(host).await;
    let gw = match ip {
        Some(ip) => tokio::task::spawn_blocking(move || netscan::gateway_for(ip)).await.ok().flatten(),
        None => None,
    };
    let via_wifi = s.via_wifi();
    let mut p = Probes { gw_ping: gw.map(|_| (0, 0)), ..Default::default() };
    // 끊기기 직전(최근 5초 안)에 붙어 있던 AP
    let mut bssid = app.wifi().and_then(|w| w.bssid);
    let (mut resumed, mut reconnected): (bool, bool);
    // 나중에 기록만 보고 원인을 따질 수 있게: 점검 한 번마다의 결과와 Wi-Fi 상태를 시간순으로 남긴다
    // (t_ms = 마지막 프레임이 온 뒤 지난 시간)
    let (mut timeline, mut wifi_seen): (Vec<serde_json::Value>, Vec<serde_json::Value>) = (Vec::new(), Vec::new());
    loop {
        let round = Instant::now();
        let t_ms = ms(began.elapsed()).round();
        let tcp = async { matches!(tokio::time::timeout(Duration::from_millis(PROBE_TIMEOUT_MS as u64), tokio::net::TcpStream::connect((host, port))).await, Ok(Ok(_))) };
        // Wi-Fi 상태는 1초에 한 번 (netsh를 띄우는 일이라 조금 무겁다)
        let wifi = async {
            if via_wifi && p.rounds % 2 == 0 {
                tokio::task::spawn_blocking(netmon::wifi_now).await.ok().flatten()
            } else {
                None
            }
        };
        let (cam_ms, gw_ms, tcp_ok, w) = tokio::join!(ping(ip), ping(gw), tcp, wifi);
        let (cam_ok, gw_ok) = (cam_ms.is_some(), gw_ms.is_some());
        if s.is_stopping() || app.is_stopping() {
            return; // 사용자가 멈춘 것은 끊김이 아니다
        }
        let (gap_now, _, attempt) = s.frame_gap();
        reconnected = attempt != attempt0;
        // 다시 들어오기 시작했으면 이번 점검은 '끊긴 동안'이 아니므로 세지 않는다 (공유기가 ping에 답하는지만 본다)
        resumed = gap_now.is_some_and(|g| g < Duration::from_millis(300));
        if resumed {
            p.gap_ms = ms(began.elapsed()) - gap_now.map_or(0.0, ms);
            if let Some((ok, _)) = p.gw_ping {
                // 통신이 돌아온 뒤에도(카메라는 답하는데) 공유기만 답이 없다 = ping에 답하지 않는 공유기 → 공유기 ping은 판단에서 뺀다.
                // 카메라도 아직 답이 없으면 통신이 돌아온 것이 아니다: FFmpeg가 붙들고 있던 마지막 프레임이 읽기 제한 시간에
                // 뒤늦게 나온 것뿐이므로 공유기 ping 0번을 그대로 두어 'PC↔공유기 구간'으로 가른다
                if ok == 0 && !gw_ok && (cam_ok || tcp_ok) && ping(gw).await.is_none() {
                    p.gw_ping = None;
                }
            }
            break;
        }
        p.rounds += 1;
        if timeline.len() < TIMELINE_MAX {
            timeline.push(serde_json::json!({ "t_ms": t_ms, "cam_ping_ms": cam_ms, "gw_ping_ms": gw_ms, "rtsp_port": tcp_ok }));
        }
        p.cam_ping_ok += cam_ok as u32;
        p.cam_tcp_ok += tcp_ok as u32;
        p.cam_reach_ok += (cam_ok || tcp_ok) as u32;
        if let Some((ok, n)) = p.gw_ping.as_mut() {
            *ok += gw_ok as u32;
            *n += 1;
        }
        if let Some(w) = w {
            if wifi_seen.len() < TIMELINE_MAX {
                wifi_seen.push(serde_json::json!({
                    "t_ms": t_ms, "state": w.state, "ssid": w.ssid, "bssid": w.bssid, "channel": w.channel,
                    "signal_pct": w.signal_pct, "rssi_dbm": w.rssi_dbm, "rx_mbps": w.rx_mbps, "tx_mbps": w.tx_mbps,
                }));
            }
            if p.wifi_states.last() != Some(&w.state) {
                p.wifi_states.push(w.state.clone());
            }
            if let (Some(a), Some(b)) = (&bssid, &w.bssid) {
                p.bssid_changed |= a != b;
            }
            bssid = w.bssid.or(bssid);
        }
        if began.elapsed() >= MAX_FOLLOW {
            p.gap_ms = ms(began.elapsed());
            break;
        }
        tokio::time::sleep(ROUND_EVERY.saturating_sub(round.elapsed())).await;
    }
    p.pc_freeze_ms = app.last_freeze.lock().unwrap().filter(|(at, _)| *at + FREEZE_LOOKBACK >= began).map(|(_, f)| f);
    let cause = classify(&p);
    app.store.record_event(
        Event::new("stall_diag", Some(&s.camera_id), describe(cause, &p, via_wifi, reconnected, !resumed)).session(&s.id).with_data(serde_json::json!({
            "cause": cause.code(),
            "gap_ms": p.gap_ms,
            "rounds": p.rounds,
            "cam_ping_ok": p.cam_ping_ok,
            "cam_tcp_ok": p.cam_tcp_ok,
            "gateway": gw.map(|g| g.to_string()),
            "gw_ping": p.gw_ping,
            "wifi_states": p.wifi_states,
            "bssid_changed": p.bssid_changed,
            "pc_freeze_ms": p.pc_freeze_ms,
            "reconnected": reconnected,
            "ongoing": !resumed,
            "path": if via_wifi { "wifi" } else { "other" },
            "camera_ip": ip.map(|i| i.to_string()),
            "rtsp_port": port,
            "timeline": timeline,
            "wifi": wifi_seen,
        })),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probes(rounds: u32, ping: u32, tcp: u32, reach: u32, gw: Option<(u32, u32)>) -> Probes {
        Probes { gap_ms: rounds as f64 * 500.0 + 1000.0, rounds, cam_ping_ok: ping, cam_tcp_ok: tcp, cam_reach_ok: reach, gw_ping: gw, ..Default::default() }
    }

    #[test]
    fn network_outage_is_split_by_the_router_ping() {
        // 카메라도 공유기도 안 닿음 → PC와 공유기 사이
        assert_eq!(classify(&probes(10, 0, 0, 0, Some((0, 10)))), Cause::PcToRouter);
        // 공유기는 닿는데 카메라만 안 닿음 → 공유기 너머
        assert_eq!(classify(&probes(10, 0, 0, 0, Some((10, 10)))), Cause::BeyondRouter);
        // 공유기 주소를 모르면 구간은 못 가른다
        assert_eq!(classify(&probes(10, 1, 0, 1, None)), Cause::NetworkPath);
    }

    #[test]
    fn reachable_camera_means_it_is_not_the_network() {
        // ping도 영상 포트도 되는데 영상만 안 옴
        assert_eq!(classify(&probes(10, 10, 10, 10, Some((10, 10)))), Cause::VideoOnly);
        // ping을 막아 둔 카메라: 영상 포트만 돼도 '닿는다'
        assert_eq!(classify(&probes(10, 0, 10, 10, None)), Cause::VideoOnly);
        // ping은 되는데 영상 포트가 닫힘 → 카메라 쪽
        assert_eq!(classify(&probes(10, 10, 0, 10, Some((10, 10)))), Cause::CameraService);
    }

    #[test]
    fn wifi_change_and_pc_freeze_take_priority() {
        // Wi-Fi가 끊겼다 붙음: ping 결과와 상관없이 PC↔공유기
        let mut p = probes(10, 6, 6, 6, Some((6, 10)));
        p.wifi_states = vec!["disconnected".into(), "connected".into()];
        assert_eq!(classify(&p), Cause::PcToRouter);
        let mut p = probes(4, 4, 4, 4, None);
        p.bssid_changed = true;
        assert_eq!(classify(&p), Cause::PcToRouter);
        // 프로그램이 끊긴 시간의 절반 넘게 멈춰 있었으면 PC 멈춤 (ping은 그동안 보내지도 못했다)
        let mut p = probes(2, 2, 2, 2, None);
        p.gap_ms = 5000.0;
        p.pc_freeze_ms = Some(4200.0);
        assert_eq!(classify(&p), Cause::PcFreeze);
        // 잠깐 멈칫한 정도는 원인으로 보지 않는다
        p.pc_freeze_ms = Some(1100.0);
        assert_eq!(classify(&p), Cause::VideoOnly);
    }

    #[test]
    fn half_and_half_is_unclear() {
        assert_eq!(classify(&probes(10, 5, 5, 5, None)), Cause::Unclear);
        assert_eq!(classify(&probes(0, 0, 0, 0, None)), Cause::Unclear);
    }

    #[test]
    fn message_says_where_and_how_it_ended() {
        let p = Probes { gap_ms: 6200.0, rounds: 10, gw_ping: Some((0, 10)), wifi_states: vec!["connected".into()], ..Default::default() };
        let m = describe(classify(&p), &p, true, true, false);
        assert!(m.contains("6.2초 끊김") && m.contains("PC↔공유기 Wi-Fi 구간") && m.contains("공유기 ping 0/10") && m.contains("새로 연결해서 복구"), "{m}");
        let p = probes(8, 8, 8, 8, Some((8, 8)));
        let m = describe(classify(&p), &p, false, false, false);
        assert!(m.contains("영상만 멈춤") && m.contains("저절로 복구"), "{m}");
    }
}
