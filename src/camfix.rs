//! 카메라 연결을 스스로 바로잡기.
//!
//! 현장에서 카메라 설정이 틀어지는 흔한 경우를 사람이 손대지 않아도 넘어가게 한다.
//!  - 공유기를 껐다 켜서 카메라 IP가 바뀜 → 같은 카메라를 찾아 주소를 따라간다 (`follow`)
//!  - 연결 테스트 없이 저장해서 영상 주소가 비어 있음 → 저장·수신 시작·촬영 때 찾아서 채운다 (`fill_urls`)
//!  - 같은 카메라인지 알아보는 값(ONVIF 고유 값·MAC)을 등록할 때 적어 둔다 (`learn`)
//!
//! 카메라 찾기(`find`)는 두 가지 방법을 같이 쓴다. 하나가 막힌 환경에서도 다른 하나로 찾으려는 것이다.
//!  1. ONVIF 탐색 (UDP: 멀티캐스트 + 같은 대역 주소마다 직접)
//!  2. RTSP 포트 확인 (TCP: 같은 대역 주소마다 554·8554)
//! 어느 쪽도 로그인하거나 비밀번호를 넣어 보지 않는다.

use std::{
    collections::{HashMap, HashSet},
    net::Ipv4Addr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    app::App,
    config::{self, CameraConfig},
    netmon, netscan,
    onvif::{self, Discovered},
    store::Event,
};

/// 같은 카메라를 다시 찾아보는 최소 간격 (연결이 안 되는 동안 네트워크를 계속 훑지 않게)
const FOLLOW_EVERY: Duration = Duration::from_secs(20);
/// ONVIF 탐색 응답을 기다리는 시간
const FIND_WAIT: Duration = Duration::from_secs(3);

// ---------------- 찾기 ----------------

/// 카메라 찾기가 볼 대역: 이 PC 어댑터들의 (주소, 대역 길이).
/// (디버그 빌드만) `CAPTURE_VMS_TEST_NETS=127.0.0.1/24`처럼 주면 그 대역만 본다 — 이 PC의 127.0.0.x에 띄운 가짜 카메라를
/// 실제 탐색 코드로 찾아보는 시험용. 릴리스 빌드에서는 항상 PC의 어댑터 대역이다.
pub fn nets() -> Vec<(Ipv4Addr, u8)> {
    if cfg!(debug_assertions) {
        if let Ok(v) = std::env::var("CAPTURE_VMS_TEST_NETS") {
            return v.split(',').filter_map(|s| s.trim().split_once('/')).filter_map(|(ip, p)| Some((ip.parse().ok()?, p.parse().ok()?))).collect();
        }
    }
    netscan::local_ipv4_nets()
}

/// (디버그 빌드만) `CAPTURE_VMS_FAKE_DISCOVERY=<json 파일>`이면 네트워크에 묻지 않고 그 파일의 목록을 찾은 카메라로 쓴다.
/// 실제 네트워크에 탐색을 보내지 않고 찾기 화면과 주소 바꾸기를 시험하려는 것. 릴리스 빌드에서는 항상 None.
fn fake_discovery() -> Option<Vec<Discovered>> {
    if !cfg!(debug_assertions) {
        return None;
    }
    let path = std::env::var("CAPTURE_VMS_FAKE_DISCOVERY").ok()?;
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// 같은 네트워크의 카메라를 찾는다 (약 3초). ONVIF 탐색에 답한 장치와, 탐색에는 답이 없어도 RTSP 포트가 답한 장치를
/// 합쳐서 돌려준다. 같은 대역의 장치는 MAC 주소도 같이 적는다.
pub async fn find(nets: Vec<(Ipv4Addr, u8)>) -> Vec<Discovered> {
    if let Some(f) = fake_discovery() {
        return f;
    }
    let for_onvif = nets.clone();
    let (by_onvif, by_rtsp) = tokio::join!(tokio::task::spawn_blocking(move || onvif::discover(FIND_WAIT, &for_onvif)), netscan::sweep_rtsp(&nets));
    let mut out = by_onvif.unwrap_or_default();
    for h in by_rtsp {
        let ip = h.ip.to_string();
        match out.iter_mut().find(|d| d.ip == ip) {
            Some(d) => d.rtsp_port = Some(h.port),
            None => out.push(Discovered { ip, name: h.server, rtsp_port: Some(h.port), via: "rtsp".into(), ..Default::default() }),
        }
    }
    // MAC: 방금 응답한 주소들이라 바로 나온다 (다른 대역의 장치는 알 수 없다)
    let ips: Vec<Ipv4Addr> = out.iter().filter_map(|d| d.ip.parse().ok()).collect();
    let macs: HashMap<String, Option<String>> =
        tokio::task::spawn_blocking(move || ips.into_iter().map(|ip| (ip.to_string(), netscan::mac_of(ip))).collect()).await.unwrap_or_default();
    for d in &mut out {
        if d.mac.is_none() {
            d.mac = macs.get(&d.ip).cloned().flatten();
        }
    }
    // 주소 여러 개가 같은 MAC으로 보이면(공유기가 대신 답하는 망) MAC으로는 구별할 수 없다 → 쓰지 않는다.
    // 두 개까지는 주소를 둘 가진 카메라일 수 있어 그대로 둔다
    let mut seen: HashMap<String, u32> = HashMap::new();
    for m in out.iter().filter_map(|d| d.mac.clone()) {
        *seen.entry(m).or_default() += 1;
    }
    for d in &mut out {
        if d.mac.as_ref().is_some_and(|m| seen.get(m).copied().unwrap_or(0) > 2) {
            d.mac = None;
        }
    }
    out.sort_by_key(|d| d.ip.parse::<Ipv4Addr>().map(u32::from).unwrap_or(u32::MAX));
    out
}

// ---------------- 같은 카메라인지 ----------------

/// 찾은 장치가 이 카메라와 같은 장치인지 (ONVIF 고유 값이나 MAC이 같으면)
pub fn same_device(c: &CameraConfig, d: &Discovered) -> bool {
    (!c.uid.is_empty() && d.uid.as_deref() == Some(c.uid.as_str())) || (!c.mac.is_empty() && d.mac.as_deref() == Some(c.mac.as_str()))
}


/// 이 카메라가 등록된 주소에서 그대로 발견됐는지
pub fn at_home(c: &CameraConfig, found: &[Discovered]) -> bool {
    let home = c.ip();
    found.iter().any(|d| d.ip == home && same_device(c, d))
}

/// 이 카메라가 등록된 주소가 아닌 다른 주소에서 발견됐으면 그 장치 (고유 값이 맞는 것 먼저, 없으면 MAC이 맞는 것).
///
/// 주소를 둘 가진 카메라가 있다 — 받은 주소와 함께 자동 주소(169.254.x.x)도 알리는 카메라가 있다. 그래서
///  - 등록된 주소에서도 이 카메라가 보이면 주소가 바뀐 것이 아니다 → None
///  - 자동 주소(169.254.x.x)는 옮겨 갈 곳으로 치지 않는다 (처음부터 그 대역으로 등록한 카메라가 아니면)
pub fn moved_to<'a>(c: &CameraConfig, found: &'a [Discovered]) -> Option<&'a Discovered> {
    let home = c.ip();
    if at_home(c, found) {
        return None;
    }
    let link_local = |ip: &str| ip.parse::<Ipv4Addr>().is_ok_and(|a| a.is_link_local());
    let home_is_link_local = link_local(&home);
    let pick = |same: &dyn Fn(&Discovered) -> bool| found.iter().filter(|d| d.ip != home && (home_is_link_local || !link_local(&d.ip))).find(|d| same(d));
    let by_uid = if c.uid.is_empty() { None } else { pick(&|d| d.uid.as_deref() == Some(c.uid.as_str())) };
    by_uid.or_else(|| if c.mac.is_empty() { None } else { pick(&|d| d.mac.as_deref() == Some(c.mac.as_str())) })
}

/// 찾은 장치에서 알게 된 값으로 빈 칸을 채운다. 바뀐 것이 있으면 true
pub fn learn_from(c: &mut CameraConfig, d: &Discovered) -> bool {
    let mut changed = false;
    if let Some(u) = d.uid.as_ref().filter(|u| c.uid.is_empty() && !u.is_empty()) {
        c.uid = u.clone();
        changed = true;
    }
    if let Some(m) = d.mac.as_ref().filter(|m| c.mac.is_empty() && !m.is_empty()) {
        c.mac = m.clone();
        changed = true;
    }
    changed
}

/// 등록된 주소에서 본 장치의 값으로 빈 칸을 채운다 — 단, 그 장치가 이 카메라라는 것을 믿을 수 있을 때만.
///  - 아는 값이 하나도 없는 카메라: 처음 배우는 것이라 그대로 적는다
///  - 아는 값이 있는 카메라: 그 값이 장치와 맞을 때만 나머지를 채운다
/// 아는 값으로 확인이 안 되는 장치(예: 고유 값만 아는데 장치는 MAC만 보임)의 값은 적지 않는다. 두 카메라의 주소가
/// 서로 바뀐 뒤에 이렇게 채우면 남의 값을 배워서, 그 뒤로는 주소가 바뀐 것을 알아보지 못한다.
pub fn learn_verified(c: &mut CameraConfig, d: &Discovered) -> bool {
    let nothing_known = c.uid.is_empty() && c.mac.is_empty();
    (nothing_known || same_device(c, d)) && learn_from(c, d)
}

/// 등록된 카메라 가운데 주소가 바뀐 것: (카메라 id, 새 IP).
/// 두 카메라의 주소가 서로 바뀐 경우도 같이 나온다(한꺼번에 적용해야 한다). 옮겨 갈 주소를 다른 등록 카메라가 그대로
/// 쓰고 있으면(그 카메라는 안 옮김) 같은 주소가 두 대가 되므로 그 이동은 뺀다.
pub fn plan_moves(configs: &[CameraConfig], found: &[Discovered]) -> Vec<(String, String)> {
    let mut moves: Vec<(usize, String)> = configs.iter().enumerate().filter_map(|(i, c)| moved_to(c, found).map(|d| (i, d.ip.clone()))).collect();
    loop {
        let moving: HashSet<usize> = moves.iter().map(|(i, _)| *i).collect();
        let before = moves.len();
        let targets: Vec<String> = moves.iter().map(|(_, ip)| ip.clone()).collect();
        moves.retain(|(i, ip)| {
            let taken_by_staying = configs.iter().enumerate().any(|(j, o)| j != *i && o.ip() == *ip && !moving.contains(&j));
            let wanted_twice = targets.iter().filter(|t| *t == ip).count() > 1;
            !taken_by_staying && !wanted_twice
        });
        if moves.len() == before {
            break;
        }
    }
    moves.into_iter().map(|(i, ip)| (configs[i].id.clone(), ip)).collect()
}

// ---------------- 설정 바꾸기 ----------------

pub enum ApplyError {
    /// 값이 잘못됨 (형식 오류 등)
    Invalid(String),
    /// 설정 파일에 쓰지 못함
    Save(String),
}

impl ApplyError {
    pub fn text(&self) -> &str {
        match self {
            ApplyError::Invalid(s) | ApplyError::Save(s) => s,
        }
    }
}

/// 카메라 목록을 검증·저장하고 적용한다 (바뀐 카메라는 연결 감시를 새로 시작). `camera_edit_lock`을 잡은 채로 부른다.
pub fn apply_list(app: &Arc<App>, list: Vec<CameraConfig>) -> Result<(), ApplyError> {
    config::validate_cameras(&list).map_err(|e| ApplyError::Invalid(format!("{e:#}")))?;
    config::save_cameras(&app.config_path, &list).map_err(|e| ApplyError::Save(format!("설정 파일 저장 실패: {e:#}")))?;
    for cam in app.replace_cameras(list) {
        netmon::start_probe(app, &cam);
    }
    Ok(())
}

/// 주소 `url`의 호스트가 `old`이면 `new`로 바꾼다 (계정·포트·경로는 그대로)
pub fn replace_url_host(url: &str, old: &str, new: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else { return url.to_string() };
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let (user, hostport) = authority.rsplit_once('@').map_or(("", authority), |(u, h)| (u, h));
    let (host, port) = hostport.rsplit_once(':').filter(|(_, p)| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit())).map_or((hostport, ""), |(h, p)| (h, p));
    if host != old {
        return url.to_string();
    }
    format!("{scheme}://{}{new}{}{path}", if user.is_empty() { String::new() } else { format!("{user}@") }, if port.is_empty() { String::new() } else { format!(":{port}") })
}

/// 카메라의 주소만 새 IP로 바꾼 설정 (이름·계정·비밀번호·RTSP 경로·포트는 그대로)
pub fn moved_camera(c: &CameraConfig, new_ip: &str) -> CameraConfig {
    let old_ip = c.ip();
    let port = c.host.rsplit_once(':').filter(|(_, p)| !p.is_empty() && p.chars().all(|ch| ch.is_ascii_digit())).map(|(_, p)| p);
    CameraConfig {
        host: port.map_or(new_ip.to_string(), |p| format!("{new_ip}:{p}")),
        rtsp_main: replace_url_host(&c.rtsp_main, &old_ip, new_ip),
        rtsp_sub: replace_url_host(&c.rtsp_sub, &old_ip, new_ip),
        ..c.clone()
    }
}

/// 주소 옮기기를 한꺼번에 적용한다 (두 카메라의 주소가 서로 바뀐 경우도 한 번에). `camera_edit_lock`을 잡은 채로 부른다.
/// 옮긴 내용을 "이름 (옛 주소 → 새 주소)"로 돌려준다. 받고 있던 수신은 설정의 주소가 바뀐 것을 보고 스스로 새 주소로 붙는다.
pub fn apply_moves(app: &Arc<App>, moves: &[(String, String)], found: &[Discovered], why: &str) -> Result<Vec<String>, ApplyError> {
    let mut list = app.camera_configs();
    let mut done: Vec<(String, String)> = Vec::new();
    for (id, new_ip) in moves {
        let Some(c) = list.iter_mut().find(|c| &c.id == id) else { continue };
        let old = c.host.clone();
        *c = moved_camera(c, new_ip);
        if let Some(d) = found.iter().find(|d| &d.ip == new_ip) {
            learn_from(c, d);
        }
        done.push((id.clone(), format!("{} ({old} → {})", c.name, c.host)));
    }
    if done.is_empty() {
        return Ok(Vec::new());
    }
    apply_list(app, list)?;
    for (id, note) in &done {
        tracing::info!(camera = %id, "{why}: {note}");
        app.store.record_event(Event::new("camera_moved", Some(id), format!("{why}: {note}")));
    }
    Ok(done.into_iter().map(|(_, n)| n).collect())
}

// ---------------- 등록할 때: 알아보는 값 적기 ----------------

async fn port_alive(ip: Ipv4Addr, port: u16, wait: Duration) -> bool {
    matches!(tokio::time::timeout(wait, tokio::net::TcpStream::connect((ip, port))).await, Ok(Ok(_)))
}

/// 이 주소의 장치를 알아보는 값: (ONVIF 고유 값, MAC). 카메라에 탐색 질문 하나와 ARP 하나만 보낸다.
/// 장치가 꺼져 있으면(포트가 안 열리면) 기다리지 않고 (None, None).
pub async fn identity_at(ip: Ipv4Addr, rtsp_port: u16) -> (Option<String>, Option<String>) {
    let (a, b) = tokio::join!(port_alive(ip, rtsp_port, Duration::from_millis(1200)), port_alive(ip, 80, Duration::from_millis(1200)));
    if !a && !b {
        return (None, None);
    }
    tokio::task::spawn_blocking(move || (onvif::probe_unicast(ip, Duration::from_millis(700)).and_then(|d| d.uid), netscan::mac_of(ip))).await.unwrap_or((None, None))
}

/// 사람이 "이 주소가 이 카메라"라고 정한 때(추가·수정·주소 바꾸기): 그 주소의 장치에서 읽은 값으로 적는다.
/// 읽힌 값만 바꾼다 (장치가 꺼져 있거나 답이 없으면 그대로).
pub async fn learn(cfg: &mut CameraConfig) {
    let Ok(ip) = cfg.ip().parse::<Ipv4Addr>() else { return };
    let (uid, mac) = identity_at(ip, cfg.rtsp_port()).await;
    if let Some(u) = uid {
        cfg.uid = u;
    }
    if let Some(m) = mac {
        cfg.mac = m;
    }
}

// ---------------- 영상 주소 채우기 ----------------

/// 카메라에 접속해 영상 주소(메인, 서브 — 서브가 없으면 빈 문자열)를 찾는다. [연결 테스트]와 같은 일을 한다.
pub async fn detect_urls(app: &Arc<App>, cfg: &CameraConfig) -> Result<(String, String), String> {
    let input = onvif::TestInput { host: cfg.host.clone(), username: cfg.username.clone(), password: cfg.password.clone(), rtsp_main: None, rtsp_sub: None, ffmpeg: app.ffmpeg.clone() };
    let r = onvif::test_camera(&app.client, input).await;
    match r.main.filter(|m| m.ok) {
        Some(m) => Ok((m.url, r.sub.filter(|s| s.ok).map(|s| s.url).unwrap_or_default())),
        None => Err(r.error.unwrap_or_else(|| "영상 주소를 찾지 못했습니다".into())),
    }
}

/// 영상 주소가 비어 있는 카메라(연결 테스트 없이 저장한 카메라)의 주소를 찾아 적는다. 이미 있으면 아무것도 하지 않는다.
/// `need_sub`: 서브로 받으려는데 서브 주소가 비어 있을 때도 찾는다. 편집 잠금을 잡지 않은 채로 부른다.
pub async fn fill_urls(app: &Arc<App>, camera_id: &str, need_sub: bool) -> Result<(), String> {
    let Some(cam) = app.camera(camera_id) else { return Ok(()) };
    let cfg = cam.cfg.clone();
    let missing = |c: &CameraConfig| c.rtsp_main.trim().is_empty() || (need_sub && c.rtsp_sub.trim().is_empty());
    if !missing(&cfg) {
        return Ok(());
    }
    let (main, sub) = detect_urls(app, &cfg).await?;
    let _one = app.camera_edit_lock.lock().await;
    let mut list = app.camera_configs();
    // 찾는 동안 사람이 고쳤으면(주소가 달라졌거나 이미 채워졌으면) 건드리지 않는다
    let Some(c) = list.iter_mut().find(|c| c.id == camera_id && c.host == cfg.host && missing(c)) else { return Ok(()) };
    let before = c.clone();
    if c.rtsp_main.trim().is_empty() {
        c.rtsp_main = main;
    }
    if c.rtsp_sub.trim().is_empty() {
        c.rtsp_sub = sub;
    }
    let (name, got_main, got_sub, changed) = (c.name.clone(), c.rtsp_main.clone(), c.rtsp_sub.clone(), *c != before);
    if changed {
        apply_list(app, list).map_err(|e| e.text().to_string())?;
        tracing::info!(camera = %camera_id, "영상 주소를 찾아 적음: 메인 {got_main} · 서브 {}", if got_sub.is_empty() { "없음" } else { &got_sub });
        app.store.record_event(Event::new("camera_updated", Some(camera_id), format!("영상 주소를 찾아 적었습니다: {name} (메인{})", if got_sub.is_empty() { "만 · 서브 없음" } else { "·서브" })));
    }
    if need_sub && got_sub.is_empty() {
        return Err("이 카메라에서 서브 스트림을 찾지 못했습니다. 메인으로 받아 주세요".into());
    }
    Ok(())
}

// ---------------- 주소 따라가기 ----------------

fn due(camera_id: &str) -> bool {
    static LAST: Mutex<Option<HashMap<String, Instant>>> = Mutex::new(None);
    let mut g = LAST.lock().unwrap();
    let m = g.get_or_insert_with(HashMap::new);
    let now = Instant::now();
    if m.get(camera_id).is_some_and(|t| now.duration_since(*t) < FOLLOW_EVERY) {
        return false;
    }
    m.insert(camera_id.to_string(), now);
    true
}

/// 등록된 주소에 지금 누가 있는지
#[derive(Debug, PartialEq)]
enum Who {
    /// 이 카메라 (또는 다른 장치라고 볼 근거가 없음)
    Same,
    /// 다른 장치가 그 주소를 쓰고 있다
    Other,
}

async fn who_is_at(ip: Ipv4Addr, cfg: &CameraConfig) -> Who {
    // MAC이 가장 빠르고 확실하다 (방금 연결된 주소라 바로 나온다)
    let mac_now = if cfg.mac.is_empty() { None } else { tokio::task::spawn_blocking(move || netscan::mac_of(ip)).await.ok().flatten() };
    if mac_now.as_deref() == Some(cfg.mac.as_str()) {
        return Who::Same;
    }
    // MAC이 다르거나 알 수 없으면 고유 값으로 한 번 더 본다 (카메라의 랜 포트를 바꿔 끼우면 MAC만 달라진다)
    let uid_now = if cfg.uid.is_empty() { None } else { tokio::task::spawn_blocking(move || onvif::probe_unicast(ip, Duration::from_millis(600))).await.ok().flatten().and_then(|d| d.uid) };
    judge_who(cfg, mac_now.as_deref(), uid_now.as_deref())
}

/// 등록된 주소의 장치에서 읽은 값(MAC, 고유 값 — 못 읽었으면 None)으로 이 카메라인지 판단한다.
/// 다르다는 근거가 있을 때만 다른 장치로 본다 (답이 없어 모르는 것은 그대로 쓴다).
fn judge_who(cfg: &CameraConfig, mac_now: Option<&str>, uid_now: Option<&str>) -> Who {
    let mac_same = !cfg.mac.is_empty() && mac_now == Some(cfg.mac.as_str());
    let mac_differs = !cfg.mac.is_empty() && mac_now.is_some_and(|m| m != cfg.mac);
    let uid_same = !cfg.uid.is_empty() && uid_now == Some(cfg.uid.as_str());
    let uid_differs = !cfg.uid.is_empty() && uid_now.is_some_and(|u| u != cfg.uid);
    if !mac_same && !uid_same && (mac_differs || uid_differs) {
        Who::Other
    } else {
        Who::Same
    }
}

/// 등록된 주소에 다른 장치가 있는 것으로 보이는데 이 카메라를 다른 곳에서 찾지도 못했을 때: 기록에 한 번 남긴다.
/// 연결은 막지 않는다 (카메라를 같은 주소의 다른 기기로 교체한 경우일 수 있다).
fn warn_other_device(app: &Arc<App>, cfg: &CameraConfig) {
    static TOLD: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    if !TOLD.lock().unwrap().get_or_insert_with(HashSet::new).insert(format!("{}@{}", cfg.id, cfg.host)) {
        return;
    }
    let msg = format!(
        "{}: 등록된 주소 {}의 장치가 등록할 때의 카메라와 다른 것으로 보입니다. 카메라를 교체했다면 설정 → 카메라 → [수정] → [저장]을 한 번 눌러 주세요. 주소가 바뀐 것이면 [카메라 찾기]에서 [주소 바꾸기]를 누르세요",
        cfg.name, cfg.host
    );
    tracing::warn!(camera = %cfg.id, "{msg}");
    app.store.record_event(Event::new("camera_mismatch", Some(&cfg.id), msg));
}

/// 알아보는 값이 비어 있는 카메라(예전에 등록한 카메라): 주소가 살아 있을 때 한 번 읽어서 적어 둔다 (뒤에서 조용히).
fn learn_in_background(app: &Arc<App>, cfg: &CameraConfig) {
    static TRIED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    if !TRIED.lock().unwrap().get_or_insert_with(HashSet::new).insert(format!("{}@{}", cfg.id, cfg.host)) {
        return;
    }
    let (app, before) = (app.clone(), cfg.clone());
    tokio::spawn(async move {
        let mut after = before.clone();
        let Ok(ip) = before.ip().parse::<Ipv4Addr>() else { return };
        let (uid, mac) = identity_at(ip, before.rtsp_port()).await;
        let seen = Discovered { ip: ip.to_string(), uid, mac, ..Default::default() };
        if !learn_verified(&mut after, &seen) {
            return;
        }
        let Ok(_one) = app.camera_edit_lock.try_lock() else { return };
        let mut list = app.camera_configs();
        // 그 사이 바뀌지 않았을 때만
        let Some(c) = list.iter_mut().find(|c| **c == before) else { return };
        *c = after;
        if apply_list(&app, list).is_ok() {
            tracing::info!(camera = %before.id, "카메라를 알아보는 값을 적어 둠 (주소가 바뀌면 따라가는 데 씀)");
        }
    });
}

/// 등록된 주소에 이 카메라가 없으면(응답이 없거나 다른 장치가 그 주소를 씀) 같은 카메라를 찾아 주소를 따라간다.
/// 주소를 바꿨으면 true. 카메라를 알아보는 값(ONVIF 고유 값·MAC)을 모르면 찾지 않는다.
///
/// 수신을 시작할 때·연결에 실패한 뒤·수신 없이 찍다 실패한 뒤에 부른다. 주소가 멀쩡하면 TCP 연결 한 번과 ARP 한 번으로 끝난다.
pub async fn follow(app: &Arc<App>, camera_id: &str) -> bool {
    if !app.cfg.monitor.follow_camera {
        return false;
    }
    let Some(cam) = app.camera(camera_id) else { return false };
    let cfg = cam.cfg.clone();
    let Ok(ip) = cfg.ip().parse::<Ipv4Addr>() else { return false };
    let known = !cfg.uid.is_empty() || !cfg.mac.is_empty();
    let alive = port_alive(ip, cfg.rtsp_port(), Duration::from_millis(1500)).await;
    if alive {
        if cfg.uid.is_empty() || cfg.mac.is_empty() {
            learn_in_background(app, &cfg);
        }
        if !known || who_is_at(ip, &cfg).await == Who::Same {
            return false;
        }
        tracing::warn!(camera = %camera_id, "등록된 주소 {ip}를 다른 장치가 쓰고 있는 것으로 보입니다 → 이 카메라를 찾아봅니다");
    } else if !known {
        return false;
    }
    if !due(camera_id) {
        return false;
    }
    // 카메라가 있던 대역부터 (그 대역에 이 PC가 없으면 — 공유기를 바꿨을 때 — PC가 붙어 있는 대역 전부)
    let all = nets();
    let near: Vec<(Ipv4Addr, u8)> = all.iter().copied().filter(|(local, prefix)| netscan::in_subnet(ip, *local, *prefix)).collect();
    let found = find(if near.is_empty() { all } else { near }).await;
    let Ok(_one) = app.camera_edit_lock.try_lock() else { return false };
    let moves = plan_moves(&app.camera_configs(), &found);
    if !moves.iter().any(|(id, _)| id == camera_id) {
        tracing::info!(camera = %camera_id, "등록된 주소 {ip}에 이 카메라가 없어 같은 카메라를 찾아봤지만 찾지 못함 (찾은 장치 {}대)", found.len());
        // 그 주소는 살아 있으니 연결은 한다. 다만 다른 장치일 수 있다는 것을 한 번 남긴다 (카메라를 교체했으면 [수정] → [저장]으로 새로 적는다).
        // 찾아보니 등록된 주소에서 이 카메라로 확인됐으면(아까는 답이 늦었을 뿐) 남기지 않는다
        if alive && !at_home(&cfg, &found) {
            warn_other_device(app, &cfg);
        }
        return false;
    }
    match apply_moves(app, &moves, &found, "카메라 주소가 바뀌어 새 주소로 따라갑니다") {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(camera = %camera_id, "카메라 주소를 바꾸지 못함: {}", e.text());
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cam(id: &str, ip: &str, uid: &str, mac: &str) -> CameraConfig {
        CameraConfig { id: id.into(), name: id.into(), host: ip.into(), rtsp_main: format!("rtsp://{ip}:554/media/1/1/Profile1"), uid: uid.into(), mac: mac.into(), ..Default::default() }
    }

    fn dev(ip: &str, uid: Option<&str>, mac: Option<&str>) -> Discovered {
        Discovered { ip: ip.into(), uid: uid.map(str::to_string), mac: mac.map(str::to_string), ..Default::default() }
    }

    fn pair(id: &str, ip: &str) -> (String, String) {
        (id.to_string(), ip.to_string())
    }

    #[test]
    fn follows_a_camera_to_its_new_address() {
        // 공유기를 껐다 켜서 cam01이 .3 → .7
        let cams = [cam("cam01", "192.168.1.3", "urn:uuid:a", ""), cam("cam02", "192.168.1.4", "urn:uuid:b", "")];
        let found = [dev("192.168.1.7", Some("urn:uuid:a"), None), dev("192.168.1.4", Some("urn:uuid:b"), None)];
        assert_eq!(plan_moves(&cams, &found), vec![pair("cam01", "192.168.1.7")]);
        // 아무것도 안 바뀌었으면 옮길 것도 없다
        assert!(plan_moves(&cams, &[dev("192.168.1.3", Some("urn:uuid:a"), None), dev("192.168.1.4", Some("urn:uuid:b"), None)]).is_empty());
    }

    #[test]
    fn untangles_two_cameras_that_swapped_addresses() {
        let cams = [cam("cam01", "192.168.1.3", "urn:uuid:a", ""), cam("cam02", "192.168.1.4", "urn:uuid:b", "")];
        let found = [dev("192.168.1.3", Some("urn:uuid:b"), None), dev("192.168.1.4", Some("urn:uuid:a"), None)];
        assert_eq!(plan_moves(&cams, &found), vec![pair("cam01", "192.168.1.4"), pair("cam02", "192.168.1.3")]);
        // cam01이 cam02의 옛 주소로, cam02는 또 다른 주소로
        let found = [dev("192.168.1.4", Some("urn:uuid:a"), None), dev("192.168.1.9", Some("urn:uuid:b"), None)];
        assert_eq!(plan_moves(&cams, &found), vec![pair("cam01", "192.168.1.4"), pair("cam02", "192.168.1.9")]);
    }

    #[test]
    fn never_puts_two_cameras_on_one_address() {
        // cam01이 .4에서 보이는데, .4에 등록된 cam02는 어디로 갔는지 모른다(알아보는 값 없음) → 건드리지 않는다
        let cams = [cam("cam01", "192.168.1.3", "urn:uuid:a", ""), cam("cam02", "192.168.1.4", "", "")];
        assert!(plan_moves(&cams, &[dev("192.168.1.4", Some("urn:uuid:a"), None)]).is_empty());
    }

    #[test]
    fn knows_a_camera_by_mac_when_it_does_not_answer_onvif() {
        let cams = [cam("cam01", "192.168.1.3", "", "00:00:5e:00:53:18"), cam("cam02", "192.168.1.4", "", "")];
        let found = [dev("192.168.1.8", None, Some("00:00:5e:00:53:18")), dev("192.168.1.9", None, Some("00:00:5e:00:53:01"))];
        assert_eq!(plan_moves(&cams, &found), vec![pair("cam01", "192.168.1.8")]);
        assert!(same_device(&cams[0], &found[0]) && !same_device(&cams[0], &found[1]));

        // 알아보는 값이 하나도 없는 카메라는 따라가지 않는다 (엉뚱한 카메라에 붙으면 안 된다)
        assert!(plan_moves(&[cam("cam02", "192.168.1.4", "", "")], &found).is_empty());
    }

    #[test]
    fn a_camera_with_two_addresses_has_not_moved() {
        let cams = [cam("cam01", "192.168.1.3", "urn:uuid:a", "00:00:5e:00:53:18")];
        // 받은 주소와 자동 주소(169.254.x.x)를 같이 알리는 경우: 등록된 주소에서 보이면 그대로 둔다
        let both = [dev("169.254.10.57", Some("urn:uuid:a"), None), dev("192.168.1.3", Some("urn:uuid:a"), Some("00:00:5e:00:53:18"))];
        assert!(plan_moves(&cams, &both).is_empty());
        assert!(moved_to(&cams[0], &both).is_none());
        // 주소가 바뀌었을 때도 자동 주소가 아니라 새로 받은 주소로 간다 (목록 순서와 상관없이)
        let moved = [dev("169.254.10.57", Some("urn:uuid:a"), None), dev("192.168.1.7", Some("urn:uuid:a"), None)];
        assert_eq!(plan_moves(&cams, &moved), vec![pair("cam01", "192.168.1.7")]);
        // 자동 주소로만 보이면 옮기지 않는다
        assert!(plan_moves(&cams, &[dev("169.254.10.57", Some("urn:uuid:a"), None)]).is_empty());
        // 처음부터 자동 주소 대역에 등록한 카메라(공유기 없이 직접 연결)는 그 대역 안에서 따라간다
        let direct = [cam("cam01", "169.254.10.5", "urn:uuid:a", "")];
        assert_eq!(plan_moves(&direct, &[dev("169.254.77.1", Some("urn:uuid:a"), None)]), vec![pair("cam01", "169.254.77.1")]);
    }

    #[test]
    fn says_other_device_only_with_evidence() {
        let c = cam("cam01", "192.168.1.3", "urn:uuid:a", "00:00:5e:00:53:18");
        // MAC이 같으면 같은 카메라 (고유 값이 달라졌어도 — 켤 때마다 고유 값을 새로 만드는 카메라가 있을 수 있다)
        assert_eq!(judge_who(&c, Some("00:00:5e:00:53:18"), Some("urn:uuid:other")), Who::Same);
        // MAC이 달라도 고유 값이 같으면 같은 카메라 (랜 포트를 바꿔 끼운 경우)
        assert_eq!(judge_who(&c, Some("00:00:5e:00:53:01"), Some("urn:uuid:a")), Who::Same);
        // 둘 다 다르거나, 하나가 다르고 다른 하나는 못 읽었으면 다른 장치
        assert_eq!(judge_who(&c, Some("00:00:5e:00:53:01"), Some("urn:uuid:b")), Who::Other);
        assert_eq!(judge_who(&c, Some("00:00:5e:00:53:01"), None), Who::Other);
        assert_eq!(judge_who(&c, None, Some("urn:uuid:b")), Who::Other);
        // 아무것도 못 읽었으면 모르는 것이므로 그대로 쓴다
        assert_eq!(judge_who(&c, None, None), Who::Same);
        // 아는 값이 없는 카메라는 무엇이 읽혀도 다른 장치라고 하지 않는다
        assert_eq!(judge_who(&cam("cam02", "192.168.1.4", "", ""), Some("00:00:5e:00:53:01"), Some("urn:uuid:b")), Who::Same);
    }

    #[test]
    fn does_not_learn_from_a_device_it_cannot_confirm() {
        // cam01은 고유 값만 알고, cam02는 MAC만 안다. 두 카메라의 주소가 서로 바뀐 뒤: cam01의 주소(.9)에서는 MAC만 보이고(cam02의 것),
        // cam02의 주소(.10)에서는 고유 값만 보인다(cam01의 것). 여기서 빈 칸을 채우면 서로의 값을 배워 버린다
        let mut cam01 = cam("cam01", "192.0.2.9", "urn:uuid:a", "");
        let mut cam02 = cam("cam02", "192.0.2.10", "", "00:00:5e:00:53:c2");
        let found = [dev("192.0.2.10", Some("urn:uuid:a"), None), dev("192.0.2.9", None, Some("00:00:5e:00:53:c2"))];
        assert!(!learn_verified(&mut cam01, &found[1]) && cam01.mac.is_empty());
        assert!(!learn_verified(&mut cam02, &found[0]) && cam02.uid.is_empty());
        // 그래서 서로 바뀐 것을 그대로 알아본다
        assert_eq!(plan_moves(&[cam01.clone(), cam02.clone()], &found), vec![pair("cam01", "192.0.2.10"), pair("cam02", "192.0.2.9")]);
        // 아는 값으로 확인된 장치에서는 나머지를 채운다
        assert!(learn_verified(&mut cam01, &dev("192.0.2.9", Some("urn:uuid:a"), Some("00:00:5e:00:53:18"))));
        assert_eq!(cam01.mac, "00:00:5e:00:53:18");
        // 아는 값이 하나도 없는 카메라는 처음 본 값을 적는다
        let mut fresh = cam("cam03", "192.0.2.30", "", "");
        assert!(learn_verified(&mut fresh, &dev("192.0.2.30", None, Some("00:00:5e:00:53:30"))));
    }

    #[test]
    fn fills_only_blank_identity() {
        let mut c = cam("cam01", "192.168.1.3", "", "");
        assert!(learn_from(&mut c, &dev("192.168.1.3", Some("urn:uuid:a"), Some("00:00:5e:00:53:18"))));
        assert_eq!((c.uid.as_str(), c.mac.as_str()), ("urn:uuid:a", "00:00:5e:00:53:18"));
        // 이미 아는 값은 다른 장치의 값으로 덮어쓰지 않는다
        assert!(!learn_from(&mut c, &dev("192.168.1.3", Some("urn:uuid:zzz"), Some("ff:ff:ff:00:00:01"))));
        assert_eq!(c.uid, "urn:uuid:a");
    }

    #[test]
    fn changes_only_the_address() {
        let c = CameraConfig { id: "cam01".into(), name: "정차 A".into(), host: "192.168.1.3".into(), rtsp_main: "rtsp://192.168.1.3:554/media/1/1/Profile1".into(), rtsp_sub: "rtsp://192.168.1.3/media/1/2/Profile2".into(), username: "admin".into(), password: "pw".into(), ..Default::default() };
        let m = moved_camera(&c, "192.168.1.7");
        assert_eq!(m.host, "192.168.1.7");
        assert_eq!(m.rtsp_main, "rtsp://192.168.1.7:554/media/1/1/Profile1");
        assert_eq!(m.rtsp_sub, "rtsp://192.168.1.7/media/1/2/Profile2");
        assert_eq!((m.name.as_str(), m.username.as_str(), m.password.as_str()), ("정차 A", "admin", "pw"));
    }
}
