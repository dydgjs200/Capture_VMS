//! config.toml 로드 / 검증 / 저장. 파일이 없으면 기본 템플릿을 만든다.

use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// 녹화 파일 하나의 최대 길이(분)
pub const SEGMENT_MINUTES_MAX: u32 = 60;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub monitor: MonitorConfig,
    pub stream: StreamConfig,
    pub trigger: TriggerConfig,
    pub cameras: Vec<CameraConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind: String,
    pub open_browser: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { bind: "127.0.0.1:8787".into(), open_browser: true }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct StorageConfig {
    /// 기본 저장 폴더. 상대 경로면 프로그램 폴더 기준.
    pub data_dir: String,
    /// 녹화 폴더. 비우면 `<data_dir>/recordings`.
    pub recordings_dir: String,
    /// JSON 로그 폴더. 비우면 `<data_dir>/logs`.
    pub logs_dir: String,
    /// 이 일수보다 오래된 녹화/로그는 자동 삭제. 0이면 삭제 안 함.
    pub retention_days: u32,
    /// 녹화 폴더 드라이브의 남은 공간이 이 값(GB) 아래로 내려가면 녹화 중인 수신을 멈춘다.
    pub min_free_gb: f64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self { data_dir: "data".into(), recordings_dir: String::new(), logs_dir: String::new(), retention_days: 30, min_free_gb: 2.0 }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MonitorConfig {
    pub probe_interval_secs: u64,
    pub icmp_timeout_ms: u32,
    pub tcp_timeout_ms: u64,
    pub wifi_interval_secs: u64,
    /// 연속 몇 번 응답이 없으면 '카메라 끊김'으로 볼지.
    pub down_after_failures: u32,
    /// Wi-Fi 고정: 수신 중에 PC가 다른 Wi-Fi로 넘어가(또는 Wi-Fi가 끊긴 채 남아) 영상이 안 오면,
    /// 영상을 받고 있던 Wi-Fi로 다시 연결한다. 수신 중이 아닐 때는 아무것도 하지 않는다.
    pub wifi_hold: bool,
    /// 카메라 따라가기: 등록된 주소에서 카메라가 응답하지 않으면(공유기를 껐다 켜 IP가 바뀜) 같은 대역에서
    /// 같은 카메라를 찾아 주소를 바꾼다. 수신 시작·촬영을 눌렀을 때와 수신 중 연결이 안 될 때만 찾는다.
    pub follow_camera: bool,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self { probe_interval_secs: 5, icmp_timeout_ms: 1000, tcp_timeout_ms: 2000, wifi_interval_secs: 5, down_after_failures: 3, wifi_hold: true, follow_camera: true }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct StreamConfig {
    /// 비우면 프로그램 폴더의 ffmpeg\ffmpeg.exe → PATH 순으로 찾는다.
    pub ffmpeg_path: String,
    /// 녹화 파일 하나의 길이(분, 1~`SEGMENT_MINUTES_MAX`). 끊겨도 앞 파일은 온전히 남는다.
    pub segment_minutes: u32,
    /// 프레임이 이 시간 이상 안 들어오면 '끊김'으로 기록 (ms)
    pub stall_ms: u64,
    /// 프레임 간격이 이 값을 넘으면 '튐'으로 센다 (ms)
    pub jank_ms: u64,
    /// 버퍼 지연이 이 값 이상이면 '지연 중'(주의), `delay_bad_ms` 이상이면 심각 (ms)
    pub delay_warn_ms: u64,
    pub delay_bad_ms: u64,
    pub preview_width: u32,
    pub preview_fps: u32,
    /// 미리보기 디코딩에 GPU(d3d11va) 사용
    pub hwaccel: bool,
    /// 촬영 시간 기준: 누름(신호) → 저장이 `capture_goal_ms` 안이면 '목표 달성',
    /// `capture_limit_ms`(전체 판정 3초 등) 안이면 '허용'. 요약·보고서에 비율로 나온다.
    pub capture_goal_ms: u64,
    pub capture_limit_ms: u64,
    /// 수신 중에 제때 온 프레임이 이 시간 동안 하나도 없으면, 기다리지 않고 연결을 새로 맺는다 (ms, 0 = 끔).
    /// Wi-Fi가 몇 초 끊겼다 돌아올 때 기존 연결을 붙들고 있으면 영상이 더 늦게 돌아오고 영상 시각도 틀어진다.
    pub stall_reconnect_ms: u64,
    /// 영상 트랙만 받는다 (음성·메타데이터 트랙은 요청하지 않음). 끊겼다 이어질 때 영상 시각이 튀는 것을 줄인다.
    pub video_only: bool,
    /// 프로그램이 갑자기 꺼졌다(강제 종료·PC 꺼짐) 다시 켜졌을 때, 꺼진 지 이 시간(분) 안이면 받고 있던 수신을 이어서 시작한다 (0 = 끔).
    /// 사람이 다시 누를 때까지 녹화가 비지 않게 한다. [프로그램 종료]로 정상 종료한 경우에는 잇지 않는다.
    pub resume_within_minutes: u32,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            ffmpeg_path: String::new(),
            segment_minutes: 10,
            stall_ms: 500,
            jank_ms: 100,
            delay_warn_ms: 150,
            delay_bad_ms: 500,
            preview_width: 960,
            preview_fps: 10,
            hwaccel: true,
            capture_goal_ms: 1000,
            capture_limit_ms: 3000,
            stall_reconnect_ms: 3000,
            video_only: true,
            resume_within_minutes: 30,
        }
    }
}

/// 동시 촬영 신호(트리거). 버튼·HTTP는 항상 쓸 수 있고, PLC 연결 방식(TCP·Modbus)은 켜야 동작한다.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct TriggerConfig {
    /// 이전 신호 뒤 이 시간 안에 들어온 신호는 무시 (접점 채터링·중복 신호 방지, ms)
    pub min_interval_ms: u64,
    /// 수신 중이 아닌 카메라를 찍을 때 쓸 스트림 (main | sub)과 전송 방식 (tcp | udp)
    pub profile: String,
    pub transport: String,
    /// PLC → 이 PC로 TCP 메시지: 한 줄(또는 한 덩어리)이 촬영 1회
    pub tcp_enabled: bool,
    pub tcp_bind: String,
    /// 비우면 모든 메시지가 신호. 적으면 이 글자가 들어 있는 메시지만 신호 (하트비트 무시용)
    pub tcp_keyword: String,
    /// 촬영이 끝나면 결과 한 줄을 PLC에 돌려준다 ("OK,번호,ms" / "NG,번호,ms")
    pub tcp_reply: bool,
    /// 이 PC가 PLC(Modbus TCP 서버)를 주기적으로 읽어 0→1로 바뀌면 신호
    pub modbus_enabled: bool,
    /// PLC IP[:포트] (포트 기본 502)
    pub modbus_host: String,
    pub modbus_unit: u8,
    /// coil | discrete | holding | input
    pub modbus_kind: String,
    /// 0부터 시작하는 주소 (PLC 문서의 00001/40001 표기면 1을 뺀 값)
    pub modbus_address: u16,
    pub modbus_poll_ms: u64,
    /// 촬영 결과를 쓸 holding register 주소 (1 = 성공, 2 = 실패). -1이면 쓰지 않음
    pub modbus_ack_address: i32,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            min_interval_ms: 300,
            profile: "main".into(),
            transport: "tcp".into(),
            tcp_enabled: false,
            tcp_bind: "0.0.0.0:9100".into(),
            tcp_keyword: String::new(),
            tcp_reply: true,
            modbus_enabled: false,
            modbus_host: String::new(),
            modbus_unit: 1,
            modbus_kind: "coil".into(),
            modbus_address: 0,
            modbus_poll_ms: 50,
            modbus_ack_address: -1,
        }
    }
}

impl TriggerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.min_interval_ms > 60_000 {
            bail!("신호 무시 간격은 0~60000ms 사이로 정해 주세요");
        }
        if !matches!(self.profile.as_str(), "main" | "sub") || !matches!(self.transport.as_str(), "tcp" | "udp") {
            bail!("스트림은 main/sub, 전송은 tcp/udp 중 하나입니다");
        }
        if self.tcp_enabled {
            self.tcp_bind
                .trim()
                .parse::<SocketAddr>()
                .with_context(|| format!("TCP 수신 주소 형식 오류 (예: 0.0.0.0:9100): {}", self.tcp_bind))?;
        }
        if self.modbus_enabled {
            if self.modbus_host.trim().is_empty() {
                bail!("Modbus를 쓰려면 PLC IP를 입력하세요");
            }
            if !matches!(self.modbus_kind.as_str(), "coil" | "discrete" | "holding" | "input") {
                bail!("Modbus 읽을 영역은 coil | discrete | holding | input 중 하나입니다");
            }
            if !(10..=5_000).contains(&self.modbus_poll_ms) {
                bail!("Modbus 읽기 주기는 10~5000ms 사이로 정해 주세요");
            }
            if self.modbus_ack_address > 65_535 || self.modbus_ack_address < -1 {
                bail!("결과 쓰기 주소는 -1(안 씀) 또는 0~65535입니다");
            }
        }
        Ok(())
    }

    /// Modbus PLC 주소 (포트가 없으면 502)
    pub fn modbus_addr(&self) -> String {
        let h = self.modbus_host.trim();
        match h.rsplit_once(':') {
            Some((_, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => h.to_string(),
            _ => format!("{h}:502"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct CameraConfig {
    /// 파일 경로에 쓰이므로 영문/숫자/-/_ 만 허용.
    pub id: String,
    pub name: String,
    /// 카메라 IP[:포트] (ONVIF·연결 확인용)
    pub host: String,
    /// 계정이 빠진 RTSP 주소 (계정은 username/password로 붙인다)
    pub rtsp_main: String,
    pub rtsp_sub: String,
    pub username: String,
    pub password: String,
    /// 트리거(동시 촬영) 때 이 카메라도 찍는지
    pub trigger: bool,
    /// 장치 고유 값 (카메라 찾기에서 얻은 것, 없으면 빈 문자열). 공유기를 껐다 켜 카메라 IP가 바뀌어도
    /// 같은 카메라인지 알아보고 [주소 바꾸기]를 권하는 데 쓴다.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub uid: String,
    /// 카메라의 MAC 주소 (같은 대역일 때 등록하면서 적어 둔다, 없으면 빈 문자열). ONVIF 탐색에 답하지 않는
    /// 카메라도 IP가 바뀐 뒤 같은 카메라인지 알아볼 수 있다.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub mac: String,
    /// 이전 버전(스냅샷) 설정 호환용. 읽기만 하고 저장하지 않는다.
    #[serde(skip_serializing)]
    pub snapshot_url: String,
}

impl Default for CameraConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            host: String::new(),
            rtsp_main: String::new(),
            rtsp_sub: String::new(),
            username: String::new(),
            password: String::new(),
            trigger: true,
            uid: String::new(),
            mac: String::new(),
            snapshot_url: String::new(),
        }
    }
}

impl CameraConfig {
    /// 계정을 붙인 RTSP 주소
    pub fn rtsp_url(&self, profile: &str) -> Option<String> {
        let raw = if profile == "sub" { &self.rtsp_sub } else { &self.rtsp_main };
        if raw.trim().is_empty() {
            return None;
        }
        let mut u = reqwest::Url::parse(raw.trim()).ok()?;
        if !self.username.is_empty() {
            let _ = u.set_username(&self.username);
            let _ = u.set_password(Some(&self.password));
        }
        Some(u.to_string())
    }

    /// IP만 (ping용)
    pub fn ip(&self) -> String {
        match self.host.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => h.to_string(),
            _ => self.host.clone(),
        }
    }

    /// RTSP 포트 (TCP 연결 확인용)
    pub fn rtsp_port(&self) -> u16 {
        reqwest::Url::parse(&self.rtsp_main).ok().and_then(|u| u.port()).unwrap_or(554)
    }
}

const DEFAULT_TOML: &str = r#"# Capture VMS 설정 파일 (무선 환경 RTSP 수신 안정성 검증)
# 카메라와 저장 위치는 대시보드 [설정]에서 바꾸는 것을 권합니다 (바로 적용, 이 파일에 자동 저장).
# 그 밖의 값은 여기서 고친 뒤 프로그램을 다시 시작하면 적용됩니다.

[server]
bind = "127.0.0.1:8787"      # 대시보드 주소
open_browser = true          # 시작할 때 브라우저 자동 열기

[storage]
data_dir = "data"            # 기본 저장 폴더 (프로그램 폴더 기준)
recordings_dir = ""          # 녹화 폴더 (비우면 data/recordings)
logs_dir = ""                # JSON 로그 폴더 (비우면 data/logs)
retention_days = 30          # 이보다 오래된 녹화·로그 자동 삭제 (0 = 삭제 안 함)
min_free_gb = 2.0            # 남은 공간이 이보다 적으면 녹화 중인 수신을 멈춤 (GB)

[monitor]
probe_interval_secs = 5      # 카메라 ping/TCP 확인 주기
icmp_timeout_ms = 1000
tcp_timeout_ms = 2000
wifi_interval_secs = 5       # Wi-Fi 신호/BSSID 확인 주기
down_after_failures = 3
wifi_hold = true             # 수신 중 PC가 다른 Wi-Fi로 넘어가 영상이 멈추면, 영상을 받던 Wi-Fi로 다시 연결
follow_camera = true         # 카메라 IP가 바뀌면(공유기 재시작) 같은 카메라를 찾아 주소를 따라감

[stream]
ffmpeg_path = ""             # 비우면 프로그램 폴더의 ffmpeg\ffmpeg.exe → PATH 순으로 찾음
segment_minutes = 10         # 녹화 파일 하나의 길이(분, 1~60) · 뷰어 [설정] → 수신·녹화에서도 바꿀 수 있음
stall_ms = 500               # 프레임이 이 시간 이상 안 오면 '끊김'
jank_ms = 100                # 프레임 간격이 이보다 길면 '튐'
preview_width = 960          # 대시보드 미리보기 크기
preview_fps = 10
hwaccel = true               # 미리보기 디코딩에 GPU 사용
capture_goal_ms = 1000       # 촬영 목표: 누름 → 저장이 이 안이면 '목표 달성' (요약·보고서에 비율)
capture_limit_ms = 3000      # 촬영 허용 한도 (예: 판정 전체 3초)
stall_reconnect_ms = 3000    # 제때 온 프레임이 이 시간 동안 없으면 기다리지 않고 새로 연결 (0 = 끔)
video_only = true            # 영상 트랙만 받기 (음성·메타데이터 트랙은 요청하지 않음)
resume_within_minutes = 30   # 프로그램이 갑자기 꺼졌다 다시 켜지면, 꺼진 지 이 시간(분) 안일 때 받던 수신을 이어서 시작 (0 = 끔)

[trigger]                    # 동시 촬영 신호 (대시보드 [설정] → 트리거에서 바꾸는 것을 권합니다)
min_interval_ms = 300        # 이 시간 안에 다시 온 신호는 무시 (채터링 방지)
profile = "main"             # 수신 중이 아닌 카메라를 찍을 때 스트림 (main | sub)
transport = "tcp"
tcp_enabled = false          # PLC가 TCP로 메시지를 보내면 촬영 (한 줄 = 1회)
tcp_bind = "0.0.0.0:9100"
tcp_keyword = ""             # 비우면 모든 메시지, 적으면 이 글자가 든 메시지만 신호
tcp_reply = true             # 촬영 결과를 PLC에 한 줄로 돌려줌
modbus_enabled = false       # 이 PC가 PLC를 Modbus TCP로 읽어 0→1이 되면 촬영
modbus_host = ""             # PLC IP[:포트] (기본 502)
modbus_unit = 1
modbus_kind = "coil"         # coil | discrete | holding | input
modbus_address = 0           # 0부터 시작하는 주소
modbus_poll_ms = 50
modbus_ack_address = -1      # 결과를 쓸 holding register (1=성공, 2=실패, -1=안 씀)

# 카메라는 대시보드 [설정] → 카메라에서 [카메라 찾기]로 추가하세요.
"#;

impl Config {
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if !path.exists() {
            fs::write(path, DEFAULT_TOML)
                .with_context(|| format!("기본 설정 파일을 만들 수 없습니다: {}", path.display()))?;
        }
        let text = fs::read_to_string(path)
            .with_context(|| format!("설정 파일을 읽을 수 없습니다: {}", path.display()))?;
        // 메모장 등이 붙이는 UTF-8 BOM 제거
        let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
        let mut cfg: Config = toml::from_str(text).with_context(|| format!("설정 파일 형식 오류: {}", path.display()))?;
        for cam in &mut cfg.cameras {
            // 이전 버전 설정: host가 없으면 스냅샷/RTSP 주소에서 뽑는다
            if cam.host.trim().is_empty() {
                cam.host = [&cam.rtsp_main, &cam.snapshot_url]
                    .iter()
                    .find_map(|u| reqwest::Url::parse(u).ok().and_then(|u| u.host_str().map(str::to_string)))
                    .unwrap_or_default();
            }
            if cam.name.trim().is_empty() {
                cam.name = cam.id.clone();
            }
        }
        cfg.server
            .bind
            .parse::<SocketAddr>()
            .with_context(|| format!("server.bind 주소 형식 오류: {}", cfg.server.bind))?;
        validate_cameras(&cfg.cameras)?;
        if let Err(e) = cfg.trigger.validate() {
            // 트리거 설정이 잘못돼도 프로그램은 뜨게 하고, PLC 연결만 꺼 둔다
            eprintln!("트리거 설정 오류로 PLC 연결을 끕니다: {e:#}");
            cfg.trigger.tcp_enabled = false;
            cfg.trigger.modbus_enabled = false;
        }
        Ok(cfg)
    }
}

/// 상대 경로는 `home` 기준으로 바꾼다.
pub fn resolve_path(home: &Path, p: &str) -> PathBuf {
    let pb = PathBuf::from(p.trim());
    if pb.is_absolute() {
        pb
    } else {
        home.join(pb)
    }
}

impl StorageConfig {
    pub fn default_recordings_dir(&self, home: &Path) -> PathBuf {
        resolve_path(home, &self.data_dir).join("recordings")
    }

    pub fn default_logs_dir(&self, home: &Path) -> PathBuf {
        resolve_path(home, &self.data_dir).join("logs")
    }

    pub fn recordings_path(&self, home: &Path) -> PathBuf {
        if self.recordings_dir.trim().is_empty() {
            self.default_recordings_dir(home)
        } else {
            resolve_path(home, &self.recordings_dir)
        }
    }

    pub fn logs_path(&self, home: &Path) -> PathBuf {
        if self.logs_dir.trim().is_empty() {
            self.default_logs_dir(home)
        } else {
            resolve_path(home, &self.logs_dir)
        }
    }
}

/// 카메라 목록 검증. 카메라가 0대여도 된다. RTSP 주소는 비어 있어도 된다 (연결 테스트 전).
pub fn validate_cameras(cameras: &[CameraConfig]) -> Result<()> {
    let mut ids = HashSet::new();
    for cam in cameras {
        let valid = !cam.id.is_empty() && cam.id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid {
            bail!("카메라 id는 영문/숫자/-/_ 만 쓸 수 있습니다: '{}'", cam.id);
        }
        if !ids.insert(cam.id.as_str()) {
            bail!("카메라 id가 중복됩니다: '{}'", cam.id);
        }
        if cam.host.trim().is_empty() {
            bail!("[{}] 카메라 IP 주소가 비어 있습니다", cam.id);
        }
        for (label, url) in [("메인", &cam.rtsp_main), ("서브", &cam.rtsp_sub)] {
            if url.trim().is_empty() {
                continue;
            }
            let u = reqwest::Url::parse(url.trim()).with_context(|| format!("[{}] {label} RTSP 주소 형식 오류: {url}", cam.id))?;
            if !matches!(u.scheme(), "rtsp" | "rtsps") || u.host_str().is_none() {
                bail!("[{}] {label} 스트림 주소는 rtsp://로 시작해야 합니다: {url}", cam.id);
            }
        }
    }
    Ok(())
}

fn read_doc(path: &Path) -> Result<toml_edit::DocumentMut> {
    let text = fs::read_to_string(path).with_context(|| format!("설정 파일을 읽을 수 없습니다: {}", path.display()))?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    text.parse().context("설정 파일 형식 오류")
}

/// 쓰다가 끊겨도 설정 파일이 깨지지 않도록 임시 파일에 쓰고 바꿔치기
fn write_atomic(path: &Path, text: &str) -> Result<()> {
    let tmp = path.with_extension("toml.tmp");
    fs::write(&tmp, text).with_context(|| format!("설정 파일을 쓸 수 없습니다: {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("설정 파일을 바꿀 수 없습니다: {}", path.display()))?;
    Ok(())
}

/// 대시보드에서 바꾼 카메라 목록을 저장. 다른 설정과 주석은 그대로 둔다.
pub fn save_cameras(path: &Path, cameras: &[CameraConfig]) -> Result<()> {
    let mut doc = read_doc(path)?;
    let mut list = toml_edit::ArrayOfTables::new();
    for c in cameras {
        let mut t = toml_edit::Table::new();
        t["id"] = toml_edit::value(c.id.as_str());
        t["name"] = toml_edit::value(c.name.as_str());
        t["host"] = toml_edit::value(c.host.as_str());
        t["rtsp_main"] = toml_edit::value(c.rtsp_main.as_str());
        t["rtsp_sub"] = toml_edit::value(c.rtsp_sub.as_str());
        t["username"] = toml_edit::value(c.username.as_str());
        t["password"] = toml_edit::value(c.password.as_str());
        t["trigger"] = toml_edit::value(c.trigger);
        if !c.uid.is_empty() {
            t["uid"] = toml_edit::value(c.uid.as_str());
        }
        if !c.mac.is_empty() {
            t["mac"] = toml_edit::value(c.mac.as_str());
        }
        list.push(t);
    }
    doc["cameras"] = toml_edit::Item::ArrayOfTables(list);
    write_atomic(path, &doc.to_string())
}

/// 대시보드에서 바꾼 저장 위치를 저장. 다른 설정과 주석은 그대로 둔다.
pub fn save_storage(path: &Path, recordings_dir: &str, logs_dir: &str, retention_days: u32) -> Result<()> {
    let mut doc = read_doc(path)?;
    if !doc.contains_table("storage") {
        doc["storage"] = toml_edit::table();
    }
    let s = &mut doc["storage"];
    s["recordings_dir"] = toml_edit::value(recordings_dir);
    s["logs_dir"] = toml_edit::value(logs_dir);
    s["retention_days"] = toml_edit::value(retention_days as i64);
    write_atomic(path, &doc.to_string())
}

/// 대시보드에서 바꾼 녹화 파일 길이(분)를 저장. 다른 설정과 주석은 그대로 둔다.
pub fn save_segment_minutes(path: &Path, minutes: u32) -> Result<()> {
    let mut doc = read_doc(path)?;
    if !doc.contains_table("stream") {
        doc["stream"] = toml_edit::table();
    }
    // 값만 바꿔 줄 끝 주석을 남긴다
    match doc["stream"].get_mut("segment_minutes").and_then(|i| i.as_value_mut()) {
        Some(v) => {
            let decor = v.decor().clone();
            *v = toml_edit::Value::from(minutes as i64);
            *v.decor_mut() = decor;
        }
        None => doc["stream"]["segment_minutes"] = toml_edit::value(minutes as i64),
    }
    write_atomic(path, &doc.to_string())
}

/// 대시보드에서 바꾼 트리거 설정을 저장. 다른 설정과 주석은 그대로 둔다.
pub fn save_trigger(path: &Path, t: &TriggerConfig) -> Result<()> {
    let mut doc = read_doc(path)?;
    if !doc.contains_table("trigger") {
        doc["trigger"] = toml_edit::table();
    }
    let s = &mut doc["trigger"];
    s["min_interval_ms"] = toml_edit::value(t.min_interval_ms as i64);
    s["profile"] = toml_edit::value(t.profile.as_str());
    s["transport"] = toml_edit::value(t.transport.as_str());
    s["tcp_enabled"] = toml_edit::value(t.tcp_enabled);
    s["tcp_bind"] = toml_edit::value(t.tcp_bind.trim());
    s["tcp_keyword"] = toml_edit::value(t.tcp_keyword.as_str());
    s["tcp_reply"] = toml_edit::value(t.tcp_reply);
    s["modbus_enabled"] = toml_edit::value(t.modbus_enabled);
    s["modbus_host"] = toml_edit::value(t.modbus_host.trim());
    s["modbus_unit"] = toml_edit::value(t.modbus_unit as i64);
    s["modbus_kind"] = toml_edit::value(t.modbus_kind.as_str());
    s["modbus_address"] = toml_edit::value(t.modbus_address as i64);
    s["modbus_poll_ms"] = toml_edit::value(t.modbus_poll_ms as i64);
    s["modbus_ack_address"] = toml_edit::value(t.modbus_ack_address as i64);
    write_atomic(path, &doc.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saves_trigger_and_keeps_comments() {
        let dir = std::env::temp_dir().join(format!("capture-vms-trig-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("config.toml");
        fs::write(&p, "# 주석\n[server]\nbind = \"127.0.0.1:8788\" # 포트\n").unwrap();
        let t = TriggerConfig { tcp_enabled: true, tcp_bind: "0.0.0.0:9200".into(), modbus_host: "10.0.0.5".into(), ..Default::default() };
        t.validate().unwrap();
        save_trigger(&p, &t).unwrap();
        let text = fs::read_to_string(&p).unwrap();
        assert!(text.contains("# 주석") && text.contains("# 포트"));
        let cfg = Config::load_or_create(&p).unwrap();
        assert_eq!(cfg.trigger, t);
        assert_eq!(t.modbus_addr(), "10.0.0.5:502");
        assert!(TriggerConfig { modbus_enabled: true, ..Default::default() }.validate().is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn saves_segment_minutes_and_keeps_comments() {
        let dir = std::env::temp_dir().join(format!("capture-vms-seg-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        // 예전 버전이 만든 설정 (10분이 적혀 있음)
        let p = dir.join("config.toml");
        fs::write(&p, "[stream]\nsegment_minutes = 10         # 녹화 파일 하나의 길이(분)\nstall_ms = 500 # 끊김\n").unwrap();
        save_segment_minutes(&p, 1).unwrap();
        let text = fs::read_to_string(&p).unwrap();
        assert!(text.contains("segment_minutes = 1 ") && text.contains("# 녹화 파일 하나의 길이(분)") && text.contains("# 끊김"), "{text}");
        assert_eq!(Config::load_or_create(&p).unwrap().stream.segment_minutes, 1);
        // [stream]이 없는 설정에도 쓸 수 있다
        let q = dir.join("empty.toml");
        fs::write(&q, "[server]\nbind = \"127.0.0.1:8788\"\n").unwrap();
        save_segment_minutes(&q, 5).unwrap();
        assert_eq!(Config::load_or_create(&q).unwrap().stream.segment_minutes, 5);
        // 새로 만드는 설정의 기본값은 1분
        assert_eq!(Config::load_or_create(&dir.join("new.toml")).unwrap().stream.segment_minutes, 10);
        assert_eq!(StreamConfig::default().segment_minutes, 10);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn injects_credentials_into_rtsp() {
        let c = CameraConfig {
            id: "cam01".into(),
            host: "192.168.3.27".into(),
            rtsp_main: "rtsp://192.168.3.27:554/media/1/1/Profile1".into(),
            username: "admin".into(),
            password: "p@ss:1".into(),
            ..Default::default()
        };
        assert_eq!(c.rtsp_url("main").unwrap(), "rtsp://admin:p%40ss%3A1@192.168.3.27:554/media/1/1/Profile1");
        assert!(c.rtsp_url("sub").is_none());
        assert_eq!(c.rtsp_port(), 554);
    }

    #[test]
    fn migrates_old_snapshot_config() {
        let old = "[[cameras]]\nid = \"cam01\"\nname = \"x\"\nsnapshot_url = \"http://192.168.3.27/snapshot/1/snapshot.jpg\"\nauth = \"digest\"\ntimeout_ms = 5000\n";
        let dir = std::env::temp_dir().join(format!("capture-vms-cfg-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("config.toml");
        fs::write(&p, old).unwrap();
        let cfg = Config::load_or_create(&p).unwrap();
        assert_eq!(cfg.cameras[0].host, "192.168.3.27");
        assert!(cfg.cameras[0].rtsp_main.is_empty());
        // 예전 설정에는 trigger가 없다 → 기본값(촬영 대상)
        assert!(cfg.cameras[0].trigger);
        assert!(!cfg.trigger.tcp_enabled);
        let _ = fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod uid_tests {
    use super::*;
    use std::fs;

    #[test]
    fn camera_device_id_is_saved_only_when_known() {
        let dir = std::env::temp_dir().join(format!("capture-vms-cfg-uid-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("c.toml");
        fs::write(&p, "").unwrap();
        let a = CameraConfig { id: "cam01".into(), name: "a".into(), host: "192.168.0.2".into(), uid: "urn:uuid:abc".into(), mac: "00:00:5e:00:53:18".into(), ..Default::default() };
        let b = CameraConfig { id: "cam02".into(), name: "b".into(), host: "192.168.0.3".into(), ..Default::default() };
        save_cameras(&p, &[a, b]).unwrap();
        let text = fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().filter(|l| l.trim_start().starts_with("uid ")).count(), 1, "{text}");
        let cfg = Config::load_or_create(&p).unwrap();
        assert_eq!(cfg.cameras[0].uid, "urn:uuid:abc");
        assert_eq!(cfg.cameras[1].uid, "");
        assert_eq!(text.lines().filter(|l| l.trim_start().starts_with("mac ")).count(), 1, "{text}");
        assert_eq!((cfg.cameras[0].mac.as_str(), cfg.cameras[1].mac.as_str()), ("00:00:5e:00:53:18", ""));
        // 예전 설정 파일(값이 없음)도 그대로 읽히고, 카메라 따라가기는 기본으로 켜져 있다
        assert!(cfg.monitor.follow_camera);
        let _ = fs::remove_dir_all(&dir);
    }
}
