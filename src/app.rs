//! 프로그램 전역 상태.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Instant,
};

use chrono::{DateTime, Local};
use serde::Serialize;
use tokio::sync::Notify;

use crate::{
    applog::LogSwitch,
    config::{self, CameraConfig, Config},
    store::{Store, WifiInfo},
    stream::Session,
    trigger::TriggerRuntime,
};

pub struct App {
    /// 시작할 때 읽은 설정. 카메라 목록은 실행 중에 바뀔 수 있으므로 `cameras()`를 쓴다.
    pub cfg: Config,
    pub home: PathBuf,
    pub config_path: PathBuf,
    pub store: Store,
    pub retention_days: AtomicU32,
    /// 녹화 파일 하나의 길이(분). 설정 화면에서 바꾸면 새로 시작(또는 다시 연결)하는 수신부터 적용된다.
    pub segment_minutes: AtomicU32,
    pub app_log: Option<Arc<LogSwitch>>,
    pub storage_lock: tokio::sync::Mutex<()>,
    pub camera_edit_lock: tokio::sync::Mutex<()>,
    pub client: reqwest::Client,
    pub ffmpeg: Option<PathBuf>,
    pub ffmpeg_version: Option<String>,
    cams: RwLock<Vec<Arc<CamRuntime>>>,
    /// 진행 중인 수신 세션 (카메라당 하나)
    pub sessions: Mutex<HashMap<String, Arc<Session>>>,
    pub wifi_latest: RwLock<Option<WifiInfo>>,
    /// 프로그램이 마지막으로 멈칫했던 때와 그 길이(ms) — 끊김 진단이 'PC가 멈춘 것'을 가릴 때 쓴다 (`stalldiag::heartbeat`)
    pub last_freeze: Mutex<Option<(Instant, f64)>>,
    pub started_at: DateTime<Local>,
    /// 동시 촬영 신호 (버튼·PLC)
    pub trigger: TriggerRuntime,
    pub trigger_edit_lock: tokio::sync::Mutex<()>,
    /// 원시 데이터 조회 캐시 (같은 조건으로 쪽만 넘길 때 파일을 다시 읽지 않게)
    pub raw_cache: Mutex<Option<(String, Arc<Vec<serde_json::Value>>)>>,
    /// 지금 측정 중인 위치 (예: "정차 지점 A"). 촬영·트리거·세션 기록에 붙는다.
    location: RwLock<String>,
    shutdown: Notify,
    stopping: AtomicBool,
}

pub struct CamRuntime {
    pub cfg: CameraConfig,
    pub status: Mutex<CamStatus>,
    /// 삭제·수정으로 교체되면 true (모니터링 작업이 스스로 멈춘다)
    pub removed: AtomicBool,
    probe: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CamStatus {
    pub online: Option<bool>,
    pub ip: Option<String>,
    pub last_probe: Option<DateTime<Local>>,
    pub last_icmp_ms: Option<f64>,
    pub last_tcp_ms: Option<f64>,
    pub consecutive_fail: u32,
}

impl CamRuntime {
    pub fn new(cfg: CameraConfig) -> Arc<Self> {
        Arc::new(Self { cfg, status: Mutex::new(CamStatus::default()), removed: AtomicBool::new(false), probe: Mutex::new(None) })
    }

    pub fn set_probe(&self, handle: tokio::task::JoinHandle<()>) {
        if let Some(old) = self.probe.lock().unwrap().replace(handle) {
            old.abort();
        }
    }

    pub fn retire(&self) {
        self.removed.store(true, Ordering::SeqCst);
        if let Some(h) = self.probe.lock().unwrap().take() {
            h.abort();
        }
    }

    pub fn is_removed(&self) -> bool {
        self.removed.load(Ordering::SeqCst)
    }
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Config,
        home: PathBuf,
        config_path: PathBuf,
        store: Store,
        client: reqwest::Client,
        app_log: Option<Arc<LogSwitch>>,
        ffmpeg: Option<PathBuf>,
        ffmpeg_version: Option<String>,
    ) -> Self {
        let cams = cfg.cameras.iter().cloned().map(CamRuntime::new).collect();
        let last = store.triggers(None, 50).into_iter().find(|t| t.status != "ignored");
        let trigger = TriggerRuntime::new(cfg.trigger.clone(), store.last_trigger_seq(), last);
        // 다시 켜도 마지막으로 쓰던 위치를 이어서 쓴다
        let location = store.captures(None, None, None, 1).into_iter().next().and_then(|c| c.location).unwrap_or_default();
        Self {
            trigger,
            location: RwLock::new(location),
            trigger_edit_lock: tokio::sync::Mutex::new(()),
            raw_cache: Mutex::new(None),
            retention_days: AtomicU32::new(cfg.storage.retention_days),
            segment_minutes: AtomicU32::new(cfg.stream.segment_minutes.clamp(1, config::SEGMENT_MINUTES_MAX)),
            cfg,
            home,
            config_path,
            store,
            app_log,
            storage_lock: tokio::sync::Mutex::new(()),
            camera_edit_lock: tokio::sync::Mutex::new(()),
            client,
            ffmpeg,
            ffmpeg_version,
            cams: RwLock::new(cams),
            sessions: Mutex::new(HashMap::new()),
            wifi_latest: RwLock::new(None),
            last_freeze: Mutex::new(None),
            started_at: Local::now(),
            shutdown: Notify::new(),
            stopping: AtomicBool::new(false),
        }
    }

    pub fn cameras(&self) -> Vec<Arc<CamRuntime>> {
        self.cams.read().unwrap().clone()
    }

    pub fn camera(&self, id: &str) -> Option<Arc<CamRuntime>> {
        self.cams.read().unwrap().iter().find(|c| c.cfg.id == id).cloned()
    }

    pub fn camera_configs(&self) -> Vec<CameraConfig> {
        self.cams.read().unwrap().iter().map(|c| c.cfg.clone()).collect()
    }

    /// 카메라 목록 교체. 그대로인 카메라는 상태를 유지하고, 바뀌거나 빠진 카메라는 모니터링을 멈춘다.
    /// 새로 만들어진 카메라들을 돌려준다 (호출한 쪽에서 모니터링 시작).
    pub fn replace_cameras(&self, configs: Vec<CameraConfig>) -> Vec<Arc<CamRuntime>> {
        let mut cams = self.cams.write().unwrap();
        let mut next = Vec::with_capacity(configs.len());
        let mut created = Vec::new();
        for cfg in configs {
            match cams.iter().find(|c| c.cfg == cfg) {
                Some(same) => next.push(same.clone()),
                None => {
                    let rt = CamRuntime::new(cfg);
                    created.push(rt.clone());
                    next.push(rt);
                }
            }
        }
        for old in cams.iter() {
            if !next.iter().any(|n| Arc::ptr_eq(n, old)) {
                old.retire();
            }
        }
        *cams = next;
        created
    }

    pub fn session(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.lock().unwrap().values().find(|s| s.id == id).cloned()
    }

    pub fn session_for_camera(&self, camera_id: &str) -> Option<Arc<Session>> {
        self.sessions.lock().unwrap().get(camera_id).cloned()
    }

    pub fn active_sessions(&self) -> Vec<Arc<Session>> {
        self.sessions.lock().unwrap().values().cloned().collect()
    }

    pub fn location(&self) -> Option<String> {
        let l = self.location.read().unwrap();
        (!l.trim().is_empty()).then(|| l.trim().to_string())
    }

    pub fn set_location(&self, v: &str) {
        *self.location.write().unwrap() = v.trim().chars().take(40).collect();
    }

    pub fn wifi(&self) -> Option<WifiInfo> {
        self.wifi_latest.read().unwrap().clone()
    }

    pub fn request_shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.shutdown.notify_one();
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    pub async fn wait_shutdown(&self) {
        self.shutdown.notified().await;
    }
}
