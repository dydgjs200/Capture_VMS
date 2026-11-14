//! 문제 진단: 지금 상태를 한 번에 점검해 신호등(정상·주의·문제)과 원인·해결 방법으로 정리한다.
//!
//! 새로 재는 것은 카메라까지의 연결(ping·포트)과 PC 부하(1초)뿐이고, 나머지는 이미 측정하고 있는 값을 모아 해석한다.
//! 수신·녹화에는 손대지 않는다 (카메라에 영상 연결을 새로 맺지 않음).

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Local};
use serde::Serialize;

use crate::{app::App, netmon, netscan, store};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// 참고 (판정 없음)
    Info,
    Ok,
    Warn,
    Bad,
}

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub title: String,
    pub level: Level,
    pub summary: String,
    /// 이렇게 해 보세요
    pub advice: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Group {
    pub title: String,
    pub camera_id: Option<String>,
    pub level: Level,
    pub checks: Vec<Check>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub at: DateTime<Local>,
    pub level: Level,
    pub headline: String,
    pub groups: Vec<Group>,
}

fn check(title: &str, level: Level, summary: impl Into<String>, advice: &[&str]) -> Check {
    Check { title: title.into(), level, summary: summary.into(), advice: advice.iter().map(|s| s.to_string()).collect() }
}

fn group(title: String, camera_id: Option<String>, checks: Vec<Check>) -> Group {
    Group { level: checks.iter().map(|c| c.level).max().unwrap_or(Level::Ok), title, camera_id, checks }
}

/// FFmpeg가 남긴 오류 문구를 알아볼 수 있는 말로 (모르는 것은 그대로)
pub fn friendly_error(e: &str) -> String {
    let l = e.to_ascii_lowercase();
    let has = |keys: &[&str]| keys.iter().any(|k| l.contains(k));
    if has(&["401", "unauthorized"]) {
        "카메라가 아이디·비밀번호를 거부했습니다".into()
    } else if has(&["-138", "timed out", "-10060"]) {
        "카메라에서 영상이 오지 않습니다 (응답 시간 초과)".into()
    } else if has(&["-10054", "connection reset", "-10053"]) {
        "카메라가 연결을 끊었습니다".into()
    } else if has(&["-10061", "connection refused"]) {
        "카메라가 연결을 받지 않습니다 (RTSP 포트가 닫혀 있음)".into()
    } else if has(&["-10051", "-10065", "unreachable"]) {
        "카메라까지 가는 길이 없습니다 (네트워크 끊김)".into()
    } else if has(&["404", "not found"]) {
        "카메라에 그 영상 주소가 없습니다 (RTSP 주소 확인)".into()
    } else if has(&["invalid argument", "return code -22"]) {
        "카메라에서 영상이 오지 않습니다".into()
    } else {
        e.to_string()
    }
}

/// 수신 중인 세션의 수치 해석 (재연결·끊김·누락·fps·지연)
pub fn receive_check(l: &crate::stream::LiveBrief, delay_warn_ms: f64, delay_bad_ms: f64) -> Check {
    let title = "영상 수신";
    match l.state.as_str() {
        "connecting" | "reconnecting" => {
            return check(
                title,
                Level::Bad,
                format!("카메라에 {}입니다{}", if l.state == "connecting" { "연결하는 중" } else { "다시 연결하는 중" }, l.last_error.as_deref().map(|e| format!(" — {}", friendly_error(e))).unwrap_or_default()),
                &["위의 '카메라까지 연결' 항목이 정상인지 먼저 보세요", "연결은 되는데 영상이 안 오면 카메라 웹 설정에서 스트림(RTSP)이 켜져 있는지, 아이디·비밀번호가 맞는지 확인하세요"],
            )
        }
        "stalled" => return check(title, Level::Bad, "영상이 끊겼습니다 (프레임이 오지 않음)", &["Wi-Fi 신호와 카메라 전원·랜선을 확인하세요. 3초 넘게 안 오면 자동으로 다시 연결합니다"]),
        _ => {}
    }
    let mut level = Level::Ok;
    let mut parts = vec![format!("초당 {:.0}장 수신{}", l.fps, l.camera_fps.map(|c| format!(" (카메라 설정 {c:.0}장)")).unwrap_or_default())];
    let mut advice: Vec<&str> = Vec::new();
    if let Some(c) = l.camera_fps.filter(|c| *c > 0.0) {
        let ratio = l.fps / c;
        if ratio < 0.5 {
            level = level.max(Level::Bad);
        } else if ratio < 0.9 {
            level = level.max(Level::Warn);
        }
        if ratio < 0.9 {
            advice.push("카메라가 보내는 장수보다 적게 받고 있습니다. Wi-Fi 신호·혼잡(다른 기기 사용량)을 확인하고, 카메라와 공유기 사이 거리를 줄여 보세요");
        }
    }
    if let Some(m) = l.missing_pct.filter(|m| *m >= 0.1) {
        parts.push(format!("누락 {m:.1}%"));
        level = level.max(if m >= 1.0 { Level::Bad } else { Level::Warn });
    }
    if l.reconnects > 0 {
        parts.push(format!("재연결 {}회", l.reconnects));
        level = level.max(Level::Bad);
        advice.push("수신 중에 연결이 끊겨 다시 붙었습니다. 웹의 기록 → 세션 상세에서 언제 끊겼는지와 그때의 Wi-Fi 상태를 볼 수 있습니다");
    }
    if l.stalls > 0 {
        parts.push(format!("짧은 끊김 {}회", l.stalls));
        level = level.max(Level::Warn);
    }
    if let Some(d) = l.delay_3s_ms {
        if d >= delay_bad_ms {
            parts.push(format!("지연 {d:.0}ms (심함)"));
            level = level.max(Level::Bad);
        } else if d >= delay_warn_ms {
            parts.push(format!("지연 {d:.0}ms"));
            level = level.max(Level::Warn);
        }
        if d >= delay_warn_ms {
            advice.push("영상이 평소보다 늦게 들어오고 있습니다. 대개 Wi-Fi 구간이 밀리는 것이므로 신호·채널 혼잡을 확인하세요");
        }
    }
    if level == Level::Ok {
        parts.push("끊김·재연결 없음".into());
    }
    check(title, level, parts.join(" · "), &advice)
}

fn wifi_check(w: &store::WifiInfo, used: bool) -> Check {
    let title = "Wi-Fi";
    if w.state != "connected" {
        return if used {
            check(title, Level::Bad, "Wi-Fi가 끊겨 있습니다", &["Windows 작업 표시줄에서 카메라 쪽 공유기의 Wi-Fi에 다시 연결하세요"])
        } else {
            check(title, Level::Info, "Wi-Fi에 연결돼 있지 않습니다 (카메라는 유선 등 다른 경로로 연결)", &[])
        };
    }
    let name = format!("{} {}", w.ssid.as_deref().unwrap_or("?"), w.band.as_deref().unwrap_or(""));
    let sig = w.signal_pct.unwrap_or(0);
    let mut level = if sig >= 60 { Level::Ok } else if sig >= 35 { Level::Warn } else { Level::Bad };
    let mut advice: Vec<&str> = Vec::new();
    if sig < 60 {
        advice.push("신호가 약합니다. 노트북을 공유기에 가깝게 두거나 사이의 장애물(철제 랙·벽)을 피하세요");
    }
    if w.band.as_deref().is_some_and(|b| b.starts_with("2.4")) {
        level = level.max(Level::Warn);
        advice.push("2.4GHz는 간섭이 많고 느립니다. 공유기의 5GHz 이름(SSID)으로 연결하세요");
    }
    let speed = w.rx_mbps.map(|r| format!(" · 연결 속도 {r:.0}Mbps")).unwrap_or_default();
    let level = if used { level } else { level.min(Level::Info) };
    check(title, level, format!("{name} · 신호 {sig}%{speed}"), &advice)
}

/// 한 번 점검
pub async fn run(app: &Arc<App>) -> Report {
    let cams = app.cameras();
    // 카메라까지 연결과 PC 부하는 같이 잰다 (각 1~2초)
    let nets = futures_util::future::join_all(cams.iter().map(|c| {
        let (ip, port) = (c.cfg.ip(), c.cfg.rtsp_port());
        async move {
            // 이 카메라가 실제로 쓰는 RTSP 포트가 열려 있는지도 본다 (기본 554가 아닐 수 있다)
            let rtsp = tokio::time::timeout(Duration::from_millis(1500), tokio::net::TcpStream::connect((ip.as_str(), port)));
            let (d, rtsp) = tokio::join!(netscan::diagnose(&ip), rtsp);
            (d, port, matches!(rtsp, Ok(Ok(_))))
        }
    }));
    let load = async {
        let a = netmon::host::cpu_times();
        tokio::time::sleep(Duration::from_millis(1000)).await;
        a.zip(netmon::host::cpu_times()).and_then(|(a, b)| netmon::host::cpu_pct(a, b))
    };
    let (nets, cpu) = tokio::join!(nets, load);

    // ---------- 이 PC ----------
    let mut pc = Vec::new();
    let ads: Vec<netscan::Adapter> = netscan::adapters().into_iter().filter(|a| a.up && !a.ipv4.is_empty()).collect();
    if ads.is_empty() {
        pc.push(check("네트워크", Level::Bad, "연결된 네트워크가 없습니다", &["Wi-Fi나 랜선을 연결하세요"]));
    } else {
        let list: Vec<String> = ads.iter().map(|a| format!("{} {}", a.name, a.ipv4.iter().map(|n| format!("{}/{}", n.ip, n.prefix)).collect::<Vec<_>>().join(", "))).collect();
        pc.push(check("네트워크", Level::Ok, list.join(" · "), &[]));
    }
    let wifi_used = nets.iter().any(|(d, ..)| d.route_kind.as_deref() == Some("wifi"));
    if let Some(w) = app.wifi() {
        pc.push(wifi_check(&w, wifi_used));
    }
    let receiving = app.active_sessions().len();
    match cpu {
        Some(p) => {
            let mem = netmon::host::memory_gb();
            let mem_text = mem.map(|(u, t)| format!(" · 메모리 {u:.1}/{t:.1}GB")).unwrap_or_default();
            let mem_high = mem.is_some_and(|(u, t)| t > 0.0 && u / t >= 0.92);
            let level = if p >= 90.0 { Level::Bad } else if p >= 75.0 || mem_high { Level::Warn } else { Level::Ok };
            let mut advice: Vec<&str> = Vec::new();
            if p >= 75.0 {
                advice.push("PC가 바쁩니다. 다른 프로그램을 닫고, 뷰어 설정 → 화면에서 '화면용 크기'를 줄이거나 한 대만 크게 보세요 (녹화·촬영에는 영향 없음)");
            }
            if mem_high {
                advice.push("메모리가 거의 찼습니다. 브라우저 탭 등 다른 프로그램을 닫으세요");
            }
            pc.push(check("PC 부하", level, format!("CPU {p:.0}%{mem_text} · 수신 {receiving}대"), &advice));
        }
        None => pc.push(check("PC 부하", Level::Info, "측정하지 못했습니다", &[])),
    }
    let rec_dir = app.store.recordings_dir();
    let min_free = (app.cfg.storage.min_free_gb.max(0.5) * 1e9) as u64;
    let recording_kbps: f64 = app.active_sessions().iter().map(|s| s.brief()).filter(|b| b.record).map(|b| b.kbps).sum();
    match store::free_space(&rec_dir) {
        Some(free) => {
            let gb = free as f64 / 1e9;
            let hours = (recording_kbps > 100.0).then(|| free.saturating_sub(min_free) as f64 * 8.0 / (recording_kbps * 1000.0) / 3600.0);
            let left = hours.map(|h| format!(" · 지금 속도로 약 {h:.0}시간 녹화 가능")).unwrap_or_default();
            if free < min_free {
                pc.push(check("저장 공간", Level::Bad, format!("남은 공간 {gb:.1}GB — 녹화를 멈추는 기준({:.1}GB) 아래입니다", min_free as f64 / 1e9), &["오래된 녹화를 지우거나 설정에서 녹화 저장 위치를 여유 있는 드라이브로 바꾸세요"]));
            } else if gb < 5.0 || hours.is_some_and(|h| h < 2.0) {
                pc.push(check("저장 공간", Level::Warn, format!("남은 공간 {gb:.1}GB{left}"), &["곧 가득 찹니다. 오래된 녹화를 지우거나 저장 위치를 바꾸세요"]));
            } else {
                pc.push(check("저장 공간", Level::Ok, format!("남은 공간 {gb:.0}GB{left}"), &[]));
            }
        }
        None => pc.push(check("저장 공간", Level::Warn, format!("녹화 폴더를 확인하지 못했습니다: {}", rec_dir.display()), &["설정에서 녹화 저장 위치를 확인하세요"])),
    }
    if app.ffmpeg.is_none() {
        pc.push(check("영상 처리 프로그램(FFmpeg)", Level::Bad, "찾지 못했습니다 — 영상을 받을 수 없습니다", &["프로그램 폴더의 ffmpeg\\ffmpeg.exe가 있는지 확인하고, 없으면 다시 설치하세요"]));
    }
    let mut groups = vec![group("이 PC".into(), None, pc)];

    // ---------- 카메라마다 ----------
    let (dw, db) = (app.cfg.stream.delay_warn_ms as f64, app.cfg.stream.delay_bad_ms as f64);
    if cams.is_empty() {
        groups.push(group("카메라".into(), None, vec![check("카메라 등록", Level::Bad, "등록된 카메라가 없습니다", &["설정 → 카메라에서 [카메라 찾기]나 [직접 추가]로 등록하세요"])]));
    }
    for (c, (d, rtsp_port, rtsp_open)) in cams.iter().zip(nets) {
        let mut v = Vec::new();
        let ping = d.ping_ms.map(|p| format!(" · ping {p:.0}ms")).unwrap_or_default();
        let advice: Vec<&str> = d.advice.iter().map(String::as_str).collect();
        let reachable = matches!(d.verdict.as_str(), "ok" | "routed" | "no_rtsp");
        let level = match d.verdict.as_str() {
            "ok" => Level::Ok,
            "routed" => Level::Warn,
            // 기본 포트는 닫혀 있어도 이 카메라가 쓰는 포트가 열려 있으면 된다
            "no_rtsp" if rtsp_open => Level::Ok,
            _ => Level::Bad,
        };
        let shown_advice: &[&str] = if level == Level::Ok { &[] } else { &advice };
        v.push(check("카메라까지 연결", level, format!("{}{ping}", d.summary), shown_advice));
        if reachable && !rtsp_open && !c.cfg.rtsp_main.trim().is_empty() {
            v.push(check(
                "영상 포트",
                Level::Bad,
                format!("카메라는 응답하는데 영상 포트(RTSP {rtsp_port})가 닫혀 있습니다"),
                &["카메라가 켜지는 중이면 잠시 뒤 다시 점검하세요", "카메라 웹 설정에서 RTSP 사용과 포트 번호를 확인하세요"],
            ));
        }
        if c.cfg.rtsp_main.trim().is_empty() && c.cfg.rtsp_sub.trim().is_empty() {
            v.push(check("영상 주소", Level::Bad, "RTSP 주소가 없어 수신을 시작할 수 없습니다", &["설정 → 카메라 → [수정] → [연결 테스트]를 하면 주소를 찾아 채워 줍니다"]));
        }
        match app.session_for_camera(&c.cfg.id) {
            Some(s) => {
                let b = s.brief();
                v.push(receive_check(&b, dw, db));
                if b.record {
                    if b.recorded_bytes > 0 {
                        v.push(check("녹화", Level::Ok, format!("녹화 중 · {:.0}MB 저장됨", b.recorded_bytes as f64 / 1e6), &[]));
                    } else if b.elapsed_s > 15.0 && matches!(b.state.as_str(), "receiving" | "delayed") {
                        v.push(check("녹화", Level::Bad, "영상은 받고 있는데 녹화 파일이 쌓이지 않습니다", &["녹화 저장 위치에 쓸 수 있는지(권한·남은 공간) 확인하세요. 백신이 폴더를 막고 있을 수도 있습니다"]));
                    } else {
                        v.push(check("녹화", Level::Info, "녹화를 시작하는 중입니다", &[]));
                    }
                } else {
                    v.push(check("녹화", Level::Info, "녹화하지 않는 수신입니다", &[]));
                }
            }
            None => v.push(check("영상 수신", Level::Info, "수신하고 있지 않습니다", &[])),
        }
        let name = if c.cfg.name.is_empty() || c.cfg.name == c.cfg.id { c.cfg.id.clone() } else { format!("{} · {}", c.cfg.id, c.cfg.name) };
        groups.push(group(format!("카메라 {name} ({})", c.cfg.host), Some(c.cfg.id.clone()), v));
    }

    let all = || groups.iter().flat_map(|g| g.checks.iter());
    let (bad, warn) = (all().filter(|c| c.level == Level::Bad).count(), all().filter(|c| c.level == Level::Warn).count());
    let level = groups.iter().map(|g| g.level).max().unwrap_or(Level::Ok);
    let headline = match (bad, warn) {
        (0, 0) => "문제가 없습니다".to_string(),
        (0, w) => format!("확인할 것 {w}건 (동작에는 지장 없음)"),
        (b, 0) => format!("문제 {b}건"),
        (b, w) => format!("문제 {b}건 · 확인할 것 {w}건"),
    };
    Report { at: Local::now(), level, headline, groups }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::LiveBrief;

    fn brief(state: &str, fps: f64) -> LiveBrief {
        LiveBrief {
            session_id: "s".into(),
            label: String::new(),
            profile: "main".into(),
            transport: "tcp".into(),
            preview: "off".into(),
            record: true,
            state: state.into(),
            delay_level: None,
            delay_3s_ms: Some(20.0),
            fps,
            kbps: 6000.0,
            avg_fps: Some(fps),
            camera_fps: Some(60.0),
            stalls: 0,
            reconnects: 0,
            uptime_pct: Some(100.0),
            missing_pct: Some(0.0),
            elapsed_s: 60.0,
            since_last_frame_ms: Some(10.0),
            recorded_bytes: 1,
            codec: None,
            width: None,
            height: None,
            last_error: None,
            group_id: None,
        }
    }

    #[test]
    fn grades_reception() {
        assert_eq!(receive_check(&brief("receiving", 60.0), 150.0, 500.0).level, Level::Ok);
        // 카메라 60장 중 50장만 → 주의, 20장 → 문제
        assert_eq!(receive_check(&brief("receiving", 50.0), 150.0, 500.0).level, Level::Warn);
        assert_eq!(receive_check(&brief("receiving", 20.0), 150.0, 500.0).level, Level::Bad);
        // 재연결이 있었으면 문제, 짧은 끊김만 있으면 주의
        let mut b = brief("receiving", 60.0);
        b.stalls = 2;
        assert_eq!(receive_check(&b, 150.0, 500.0).level, Level::Warn);
        b.reconnects = 1;
        let c = receive_check(&b, 150.0, 500.0);
        assert_eq!(c.level, Level::Bad);
        assert!(c.summary.contains("재연결 1회") && !c.advice.is_empty());
        // 지연
        let mut b = brief("delayed", 60.0);
        b.delay_3s_ms = Some(200.0);
        assert_eq!(receive_check(&b, 150.0, 500.0).level, Level::Warn);
        b.delay_3s_ms = Some(900.0);
        assert_eq!(receive_check(&b, 150.0, 500.0).level, Level::Bad);
        // 연결 중·끊김은 문제로 바로 표시
        assert_eq!(receive_check(&brief("reconnecting", 0.0), 150.0, 500.0).level, Level::Bad);
        assert_eq!(receive_check(&brief("stalled", 0.0), 150.0, 500.0).level, Level::Bad);
    }

    #[test]
    fn explains_ffmpeg_errors() {
        assert!(friendly_error("Error during demuxing: Error number -10054 occurred").contains("연결을 끊었습니다"));
        assert!(friendly_error("Error during demuxing: Error number -138 occurred").contains("시간 초과"));
        assert!(friendly_error("Terminating thread with return code -22 (Invalid argument)").contains("영상이 오지 않습니다"));
        assert!(friendly_error("Server returned 401 Unauthorized").contains("거부"));
        assert_eq!(friendly_error("처음 보는 오류"), "처음 보는 오류");
    }

    #[test]
    fn levels_order_by_severity() {
        assert!(Level::Bad > Level::Warn && Level::Warn > Level::Ok && Level::Ok > Level::Info);
    }
}
