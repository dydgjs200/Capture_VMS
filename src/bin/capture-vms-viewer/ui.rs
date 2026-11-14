//! 뷰어 화면: 카메라 화면(1대·2대), 수신 시작·중지, 촬영·동시 촬영, 수치·작은 그래프, 실제 지연 측정, 설정(카메라 등록 포함).

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use eframe::egui::{self, pos2, text::LayoutJob, vec2, Align, Align2, Color32, FontFamily, FontId, Layout, Rect, RichText, Rounding, Sense, Stroke, TextFormat, TextureHandle, TextureOptions};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    api::{CamEdit, Camera, Cmd, DiagCheck, DiagReport, Job, Live, Point, Shared, StartOpts, Status},
    e2e,
    video::{Open, Video},
};

// 색
const BG: Color32 = Color32::from_rgb(13, 15, 19);
const PANEL: Color32 = Color32::from_rgb(19, 22, 29);
const CARD: Color32 = Color32::from_rgb(25, 29, 38);
const LINE: Color32 = Color32::from_rgb(40, 46, 60);
const TEXT: Color32 = Color32::from_rgb(232, 235, 241);
const MUTED: Color32 = Color32::from_rgb(141, 150, 168);
const GOOD: Color32 = Color32::from_rgb(64, 204, 128);
const WARN: Color32 = Color32::from_rgb(244, 184, 64);
const BAD: Color32 = Color32::from_rgb(244, 96, 96);
/// 화면이 수신보다 10% 넘게 적게 그려지는 상태가 이만큼(초) 이어지면 화면 위에 알린다
const BEHIND_SECS: u32 = 4;
const ACCENT: Color32 = Color32::from_rgb(88, 150, 255);
const VIDEO_BG: Color32 = Color32::from_rgb(6, 7, 9);

/// 뷰어 쪽 설정 (기준 폴더의 viewer.toml). 카메라·녹화 같은 수신 설정은 엔진의 config.toml에 있다
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct Prefs {
    /// 수신을 시작할 때: main | sub
    profile: String,
    /// tcp | udp
    transport: String,
    record: bool,
    /// 화면용 영상을 GPU(d3d11va)로 푼다
    hwaccel: bool,
    /// 화면용으로 풀 때의 가로 크기 (작을수록 가볍다)
    width: u32,
}

impl Default for Prefs {
    fn default() -> Self {
        Self { profile: "main".into(), transport: "tcp".into(), record: true, hwaccel: true, width: 960 }
    }
}

impl Prefs {
    fn load(home: &PathBuf) -> Self {
        std::fs::read_to_string(home.join("viewer.toml")).ok().and_then(|t| toml::from_str(&t).ok()).unwrap_or_default()
    }

    fn save(&self, home: &PathBuf) {
        if let Ok(t) = toml::to_string(self) {
            let _ = std::fs::write(home.join("viewer.toml"), format!("# Capture VMS Viewer 설정 (뷰어의 [설정]에서 바꾸면 자동 저장)\n{t}"));
        }
    }

    fn opts(&self) -> StartOpts {
        StartOpts { profile: self.profile.clone(), transport: self.transport.clone(), record: self.record }
    }
}

/// 실제 지연 측정 한 번 (화면을 흑백으로 깜빡이고, 카메라 영상에 그 변화가 나오기까지를 잰다)
struct E2eRun {
    camera: String,
    session: String,
    e: e2e::E2e,
    white: bool,
    next_flip: Instant,
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Cameras,
    Receive,
    Screen,
    Program,
}

/// 카메라 추가·수정 입력
#[derive(Default)]
struct CamForm {
    /// 수정 중인 카메라 (없으면 새로 추가)
    id: Option<String>,
    name: String,
    host: String,
    user: String,
    pass: String,
    main: String,
    sub: String,
    trigger: bool,
    /// 카메라 찾기에서 고른 카메라의 장치 고유 값·MAC (같이 저장해 두면 주소가 바뀌어도 알아본다)
    uid: Option<String>,
    mac: Option<String>,
    has_password: bool,
    /// RTSP 주소를 직접 적는다 (ONVIF가 안 되는 카메라)
    manual: bool,
    /// 이 입력을 연 뒤의 테스트 결과를 주소 칸에 옮겼는지
    applied: bool,
    tested: bool,
    /// 열 때의 저장 횟수 (늘어나면 저장된 것 → 닫는다)
    mark: u64,
}

pub struct ViewerApp {
    base: String,
    home: PathBuf,
    shared: Arc<Shared>,
    cmd: UnboundedSender<Cmd>,
    prefs: Prefs,
    saved_prefs: Prefs,
    videos: HashMap<String, Video>,
    tex: HashMap<String, (TextureHandle, u64)>,
    /// 카메라별 화면 갱신 속도 (잰 시각, 그때까지의 프레임 수, 초당 장수)
    shown: HashMap<String, (Instant, u64, f32)>,
    /// 카메라별: 받은 것보다 눈에 띄게 적게 그리는 상태가 몇 초째인지 (화면 위 경고용)
    behind: HashMap<String, u32>,
    /// 엔진에 닿고 있는지. 끊겨 있는 동안은 화면이 안 나오는 것이 뷰어가 느려서가 아니므로 '화면이 밀림'으로 세지 않는다
    engine_ok: bool,
    loc_buf: String,
    loc_server: String,
    /// 크게 보는 카메라
    solo: Option<String>,
    e2e: Option<E2eRun>,
    show_settings: bool,
    show_diag: bool,
    tab: Tab,
    form: Option<CamForm>,
    confirm_delete: Option<String>,
    scan_target: String,
    seg_buf: u32,
    seg_loaded: bool,
    confirm_quit: bool,
    /// 처음 뜰 때 창이 화면보다 크면 한 번 줄여 맞춘다 (확인한 프레임 수)
    fit_tries: u8,
}

fn bold(size: f32) -> FontId {
    FontId::new(size, FontFamily::Name("bold".into()))
}

pub fn setup(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    let load = |fonts: &mut egui::FontDefinitions, key: &str, paths: &[&str]| {
        paths.iter().find_map(|p| std::fs::read(p).ok()).map(|b| fonts.font_data.insert(key.into(), egui::FontData::from_owned(b))).is_some()
    };
    let regular = load(&mut fonts, "korean", &[r"C:\Windows\Fonts\malgun.ttf", r"C:\Windows\Fonts\malgunsl.ttf"]);
    let heavy = load(&mut fonts, "korean-bold", &[r"C:\Windows\Fonts\malgunbd.ttf"]);
    let base = fonts.families.get(&FontFamily::Proportional).cloned().unwrap_or_default();
    if regular {
        fonts.families.entry(FontFamily::Proportional).or_default().insert(0, "korean".into());
        fonts.families.entry(FontFamily::Monospace).or_default().push("korean".into());
    }
    // 굵은 글씨용 (없으면 보통 글씨로)
    let mut bold_family: Vec<String> = Vec::new();
    if heavy {
        bold_family.push("korean-bold".into());
    }
    if regular {
        bold_family.push("korean".into());
    }
    bold_family.extend(base);
    fonts.families.insert(FontFamily::Name("bold".into()), bold_family);
    ctx.set_fonts(fonts);

    // Windows가 밝은 테마여도 영상이 잘 보이는 어두운 화면으로 고정한다
    ctx.options_mut(|o| o.theme_preference = egui::ThemePreference::Dark);
    let mut v = egui::Visuals::dark();
    v.panel_fill = BG;
    v.window_fill = CARD;
    v.window_stroke = Stroke::new(1.0_f32, LINE);
    v.window_rounding = Rounding::same(12.0);
    v.extreme_bg_color = Color32::from_rgb(10, 12, 16);
    v.selection.bg_fill = ACCENT.gamma_multiply(0.5);
    v.selection.stroke = Stroke::new(1.0_f32, Color32::WHITE);
    v.widgets.noninteractive.bg_stroke = Stroke::new(1.0_f32, LINE);
    // 글자색 (입력 칸의 예시 글자는 이보다 흐리게 나온다)
    v.widgets.noninteractive.fg_stroke = Stroke::new(1.0_f32, TEXT);
    v.widgets.inactive.fg_stroke = Stroke::new(1.0_f32, TEXT);
    v.widgets.hovered.fg_stroke = Stroke::new(1.5_f32, Color32::WHITE);
    v.widgets.active.fg_stroke = Stroke::new(1.5_f32, Color32::WHITE);
    for (w, fill) in [
        (&mut v.widgets.inactive, Color32::from_rgb(40, 46, 60)),
        (&mut v.widgets.hovered, Color32::from_rgb(54, 62, 80)),
        (&mut v.widgets.active, Color32::from_rgb(66, 76, 98)),
        (&mut v.widgets.open, Color32::from_rgb(54, 62, 80)),
    ] {
        w.weak_bg_fill = fill;
        w.bg_fill = fill;
        w.rounding = Rounding::same(8.0);
        w.bg_stroke = Stroke::NONE;
    }
    v.widgets.hovered.bg_stroke = Stroke::new(1.0_f32, ACCENT.gamma_multiply(0.6));
    ctx.set_visuals(v);
    ctx.style_mut(|s| {
        use egui::TextStyle::*;
        s.text_styles.insert(Body, FontId::proportional(15.0));
        s.text_styles.insert(Button, FontId::proportional(15.0));
        s.text_styles.insert(Heading, bold(19.0));
        s.text_styles.insert(Small, FontId::proportional(12.5));
        s.spacing.button_padding = vec2(14.0, 7.0);
        s.spacing.item_spacing = vec2(8.0, 8.0);
        s.spacing.interact_size.y = 30.0;
    });
}

fn state_text(state: &str) -> (&'static str, Color32) {
    match state {
        "receiving" => ("수신 중", GOOD),
        "delayed" => ("지연 중", WARN),
        "stalled" => ("영상 끊김", BAD),
        "reconnecting" => ("다시 연결하는 중", BAD),
        "connecting" => ("연결 중", WARN),
        "stopping" => ("멈추는 중", MUTED),
        _ => ("수신 안 함", MUTED),
    }
}

fn chip(ui: &mut egui::Ui, text: &str, color: Color32) {
    egui::Frame::none().fill(color.gamma_multiply(0.16)).rounding(Rounding::same(10.0)).inner_margin(egui::Margin::symmetric(9.0, 3.0)).show(ui, |ui| {
        ui.label(RichText::new(text).color(color).size(13.0));
    });
}

fn card() -> egui::Frame {
    egui::Frame::none().fill(CARD).stroke(Stroke::new(1.0_f32, LINE)).rounding(Rounding::same(12.0)).inner_margin(egui::Margin::same(10.0))
}

/// 색을 채운 큰 버튼
fn action(ui: &mut egui::Ui, text: &str, fill: Option<Color32>, enabled: bool) -> egui::Response {
    let mut b = egui::Button::new(RichText::new(text).font(bold(15.0))).min_size(vec2(0.0, 36.0));
    if let Some(f) = fill {
        b = b.fill(f);
    }
    ui.add_enabled(enabled, b)
}

/// 영상 위에 얹는 알약 모양 표시. 그린 너비를 돌려준다
fn pill(p: &egui::Painter, at: egui::Pos2, right: bool, text: &str, color: Color32) -> f32 {
    let g = p.layout_no_wrap(text.to_string(), bold(13.0), color);
    let size = g.size() + vec2(18.0, 8.0);
    let min = if right { at - vec2(size.x, 0.0) } else { at };
    let r = Rect::from_min_size(min, size);
    p.rect_filled(r, Rounding::same(size.y / 2.0), Color32::from_black_alpha(170));
    p.galley(r.min + vec2(9.0, 4.0), g, color);
    size.x
}

/// 작은 그래프 하나: 이름, 지금 값, 최근 2분 선
fn spark(ui: &mut egui::Ui, label: &str, values: &[f64], now: String, color: Color32, width: f32) {
    let (rect, _) = ui.allocate_exact_size(vec2(width, 50.0), Sense::hover());
    let p = ui.painter_at(rect);
    p.rect_filled(rect, 8.0, Color32::from_rgb(17, 20, 27));
    p.text(rect.left_top() + vec2(9.0, 6.0), Align2::LEFT_TOP, label, FontId::proportional(12.0), MUTED);
    p.text(rect.right_top() + vec2(-9.0, 5.0), Align2::RIGHT_TOP, now, bold(13.0), TEXT);
    if values.len() < 2 {
        return;
    }
    let area = Rect::from_min_max(rect.left_top() + vec2(8.0, 25.0), rect.right_bottom() - vec2(8.0, 6.0));
    let max = values.iter().cloned().fold(0.0f64, f64::max).max(1e-9);
    let x = |i: usize| area.left() + area.width() * i as f32 / (values.len() - 1) as f32;
    let y = |v: f64| area.bottom() - area.height() * (v / max).clamp(0.0, 1.0) as f32;
    let pts: Vec<egui::Pos2> = values.iter().enumerate().map(|(i, v)| pos2(x(i), y(*v))).collect();
    // 선 아래를 옅게 채운다
    let mut fill = pts.clone();
    fill.push(pos2(area.right(), area.bottom()));
    fill.push(pos2(area.left(), area.bottom()));
    p.add(egui::Shape::convex_polygon(fill, color.gamma_multiply(0.10), Stroke::NONE));
    p.add(egui::Shape::line(pts, Stroke::new(1.6_f32, color)));
}

fn seg(job: &mut LayoutJob, text: String, color: Color32, strong: bool) {
    job.append(&text, 0.0, TextFormat { font_id: if strong { bold(14.0) } else { FontId::proportional(13.5) }, color, ..Default::default() });
}

/// 영상 아래쪽에 얹는 수치 한 줄
fn stats_job(l: &Live, shown_fps: Option<f32>) -> LayoutJob {
    let mut j = LayoutJob::default();
    let fps_col = match l.camera_fps {
        Some(c) if l.fps < c * 0.9 => WARN,
        _ => Color32::WHITE,
    };
    seg(&mut j, format!("수신 {:.0}fps", l.fps), fps_col, true);
    if let Some(f) = shown_fps {
        seg(&mut j, format!("  화면 {f:.0}fps"), Color32::from_gray(200), false);
    }
    seg(&mut j, format!("  {:.1}Mbps", l.kbps / 1000.0), Color32::from_gray(200), false);
    if let Some(d) = l.delay_3s_ms {
        seg(&mut j, format!("  지연 {d:.0}ms"), if d >= 500.0 { BAD } else if d >= 150.0 { WARN } else { Color32::from_gray(200) }, false);
    }
    seg(&mut j, format!("  끊김 {}", l.stalls), if l.stalls > 0 { WARN } else { Color32::from_gray(200) }, false);
    seg(&mut j, format!("  재연결 {}", l.reconnects), if l.reconnects > 0 { BAD } else { Color32::from_gray(200) }, false);
    if let Some(m) = l.missing_pct {
        seg(&mut j, format!("  누락 {m:.1}%"), if m >= 1.0 { BAD } else { Color32::from_gray(200) }, false);
    }
    j
}

impl ViewerApp {
    pub fn new(base: String, home: PathBuf, shared: Arc<Shared>, cmd: UnboundedSender<Cmd>) -> Self {
        let prefs = Prefs::load(&home);
        let mut app = Self::build(base, home, shared, cmd, prefs);
        // 화면 확인용: `--show cameras|find|add|receive|screen|program` 으로 설정 창을 연 채 시작한다
        // (find = 카메라 탭에서 [카메라 찾기]를 한 번 누른 상태)
        let args: Vec<String> = std::env::args().collect();
        if let Some(view) = args.iter().position(|a| a == "--show").and_then(|i| args.get(i + 1)) {
            match view.as_str() {
                "receive" => app.open_settings(Tab::Receive),
                "screen" => app.open_settings(Tab::Screen),
                "program" => app.open_settings(Tab::Program),
                "diag" => app.open_diag(),
                "find" => {
                    app.open_settings(Tab::Cameras);
                    app.send(Cmd::Discover);
                }
                "add" => {
                    app.open_settings(Tab::Cameras);
                    app.form = Some(CamForm { user: "admin".into(), trigger: true, ..Default::default() });
                }
                _ => app.open_settings(Tab::Cameras),
            }
        }
        app
    }

    fn build(base: String, home: PathBuf, shared: Arc<Shared>, cmd: UnboundedSender<Cmd>, prefs: Prefs) -> Self {
        Self {
            base,
            home,
            shared,
            cmd,
            saved_prefs: prefs.clone(),
            prefs,
            videos: HashMap::new(),
            tex: HashMap::new(),
            shown: HashMap::new(),
            behind: HashMap::new(),
            engine_ok: true,
            loc_buf: String::new(),
            loc_server: String::new(),
            solo: None,
            e2e: None,
            show_settings: false,
            show_diag: false,
            tab: Tab::Cameras,
            form: None,
            confirm_delete: None,
            scan_target: String::new(),
            seg_buf: 1,
            seg_loaded: false,
            confirm_quit: false,
            fit_tries: 0,
        }
    }

    /// 작은 화면(노트북·배율 150% 등)에서는 기본 크기의 창이 화면 밖으로 나가 아래쪽 버튼이 안 보인다 → 화면 안으로 줄여 가운데에 둔다
    fn fit_to_screen(&mut self, ctx: &egui::Context) {
        if self.fit_tries >= 30 {
            return;
        }
        self.fit_tries += 1;
        let (mon, inner) = ctx.input(|i| (i.viewport().monitor_size, i.viewport().inner_rect));
        let (Some(mon), Some(inner)) = (mon, inner) else { return };
        self.fit_tries = 30;
        // 작업 표시줄과 창 제목줄 몫을 뺀다
        let max = vec2(mon.x - 24.0, mon.y - 110.0);
        if inner.width() > max.x || inner.height() > max.y {
            let size = vec2(inner.width().min(max.x), inner.height().min(max.y));
            ctx.send_viewport_cmd(egui::ViewportCommand::InnerSize(size));
            ctx.send_viewport_cmd(egui::ViewportCommand::OuterPosition(pos2(((mon.x - size.x) / 2.0).max(0.0), ((mon.y - size.y - 80.0) / 2.0).max(0.0))));
        }
    }

    fn send(&self, c: Cmd) {
        let _ = self.cmd.send(c);
    }

    fn open_web(&self, tab: &str) {
        let _ = open::that(format!("{}/#{tab}", self.base));
    }

    fn open_settings(&mut self, tab: Tab) {
        self.show_settings = true;
        self.tab = tab;
        self.seg_loaded = false;
        self.send(Cmd::LoadSettings);
        self.send(Cmd::LoadCameras);
    }

    fn open_diag(&mut self) {
        self.show_diag = true;
        self.send(Cmd::Diagnose);
    }

    /// 수신 중인 카메라마다 화면용 디코더를 붙이고, 수신이 끝난 카메라의 것은 닫는다
    fn sync_videos(&mut self, ctx: &egui::Context, st: &Status) {
        let ffmpeg = st.ffmpeg.path.clone().map(PathBuf::from);
        let mut want: HashMap<String, Open> = HashMap::new();
        if let Some(ffmpeg) = ffmpeg {
            for c in &st.cameras {
                let Some(l) = &c.live else { continue };
                let (Some(sw), Some(sh)) = (l.width, l.height) else { continue };
                if sw == 0 || sh == 0 {
                    continue;
                }
                let w = (self.prefs.width.clamp(320, 1920).min(sw) / 2 * 2) as usize;
                let h = ((w as f64 * sh as f64 / sw as f64 / 2.0).round() as usize * 2).max(2);
                let key = format!("{}|{w}x{h}|{}", l.session_id, self.prefs.hwaccel);
                want.insert(
                    c.id.clone(),
                    Open { key, ffmpeg: ffmpeg.clone(), url: format!("{}/api/sessions/{}/live.ts", self.base, l.session_id), w, h, hwaccel: self.prefs.hwaccel },
                );
            }
        }
        self.videos.retain(|id, v| want.get(id).is_some_and(|o| o.key == v.key));
        self.tex.retain(|id, _| self.videos.contains_key(id));
        for (id, o) in want {
            if !self.videos.contains_key(&id) {
                self.shown.remove(&id);
                self.behind.remove(&id);
                self.videos.insert(id, Video::start(o, ctx.clone()));
            }
        }
    }

    /// 새 프레임이 있으면 텍스처에 올린다. (텍스처, 화면 갱신 fps)
    /// `recv_fps`: 엔진이 받고 있는 초당 장수 (화면이 그만큼 따라가는지 보려고)
    fn texture(&mut self, ctx: &egui::Context, cam: &str, recv_fps: Option<f64>) -> Option<(egui::TextureId, f32)> {
        let v = self.videos.get(cam)?;
        let have = self.tex.get(cam).map_or(0, |t| t.1);
        let fresh = {
            let f = v.shared.frame.lock().unwrap();
            if f.seq == 0 {
                return None;
            }
            (f.seq != have && f.rgba.len() == v.w * v.h * 4).then(|| {
                let pixels = f.rgba.chunks_exact(4).map(|p| Color32::from_rgb(p[0], p[1], p[2])).collect();
                (egui::ColorImage { size: [v.w, v.h], pixels }, f.seq)
            })
        };
        if let Some((img, seq)) = fresh {
            match self.tex.get_mut(cam) {
                // 크기가 같을 때만 그 자리에 덮어쓴다
                Some(t) if t.0.size() == [v.w, v.h] => {
                    t.0.set(img, TextureOptions::LINEAR);
                    t.1 = seq;
                }
                _ => {
                    self.tex.insert(cam.to_string(), (ctx.load_texture(format!("video-{cam}"), img, TextureOptions::LINEAR), seq));
                }
            }
        }
        // 화면 갱신 속도 (1초마다 다시 잰다)
        let total = v.shared.frames.load(Ordering::Relaxed);
        let s = self.shown.entry(cam.to_string()).or_insert((Instant::now(), total, 0.0));
        let dt = s.0.elapsed().as_secs_f32();
        if dt >= 1.0 {
            *s = (Instant::now(), total, total.saturating_sub(s.1) as f32 / dt);
            let b = self.behind.entry(cam.to_string()).or_insert(0);
            let was = *b;
            *b = match recv_fps {
                Some(r) if r > 5.0 && (s.2 as f64) < r * 0.9 => *b + 1,
                _ => 0,
            };
            // 경고가 뜨고 사라지는 때를 뷰어 로그에 남긴다
            if *b == BEHIND_SECS && was < BEHIND_SECS {
                crate::vlog::log(format!("화면이 수신을 못 따라감 [{cam}] 화면 {:.0}fps / 수신 {:.0}fps", s.2, recv_fps.unwrap_or(0.0)));
            } else if *b == 0 && was >= BEHIND_SECS {
                crate::vlog::log(format!("화면이 다시 수신을 따라감 [{cam}] 화면 {:.0}fps", s.2));
            }
        }
        let fps = s.2;
        self.tex.get(cam).map(|t| (t.0.id(), fps))
    }

    fn top_bar(&mut self, ctx: &egui::Context, st: &Status) {
        egui::TopBottomPanel::top("top").frame(egui::Frame::none().fill(PANEL).inner_margin(egui::Margin::symmetric(14.0, 9.0))).show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new("Capture VMS").font(bold(20.0)).color(ACCENT));
                ui.label(RichText::new("Viewer").font(bold(20.0)));
                ui.label(RichText::new(format!("v{}", st.version)).color(MUTED).small());
                ui.add_space(16.0);
                ui.label(RichText::new("측정 위치").color(MUTED));
                let server = st.location.clone().unwrap_or_default();
                let resp = ui.add(egui::TextEdit::singleline(&mut self.loc_buf).desired_width(190.0).hint_text("예: 정차 지점 A").char_limit(40));
                if resp.lost_focus() && self.loc_buf.trim() != server {
                    self.send(Cmd::SetLocation(self.loc_buf.trim().to_string()));
                    self.loc_server = self.loc_buf.trim().to_string();
                } else if !resp.has_focus() && server != self.loc_server {
                    // 다른 곳(웹)에서 바뀐 값을 따라간다
                    self.loc_buf = server.clone();
                    self.loc_server = server;
                }
                ui.add_space(12.0);
                match &st.wifi {
                    Some(w) if w.state == "connected" => {
                        let sig = w.signal_pct.unwrap_or(0.0);
                        let col = if sig >= 60.0 { GOOD } else if sig >= 35.0 { WARN } else { BAD };
                        chip(ui, &format!("Wi-Fi {} {} · {sig:.0}%", w.ssid.as_deref().unwrap_or("?"), w.band.as_deref().unwrap_or("")), col);
                    }
                    Some(_) => chip(ui, "Wi-Fi 끊김", BAD),
                    None => {}
                }
                if let Some(free) = st.free_bytes {
                    chip(ui, &format!("남은 공간 {:.0}GB", free as f64 / 1e9), if st.low_disk { BAD } else { MUTED });
                }
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.button("기록 보기 (웹)").on_hover_text("세션 기록·촬영 기록·녹화 보기·원시 데이터는 웹에서 봅니다").clicked() {
                        self.open_web("sessions");
                    }
                    if ui.button("설정").clicked() {
                        self.open_settings(Tab::Cameras);
                    }
                    if ui.button("문제 진단").on_hover_text("이 PC·네트워크·카메라·수신·녹화를 한 번에 점검해 어디가 문제인지 보여 줍니다").clicked() {
                        self.open_diag();
                    }
                });
            });
        });
    }

    fn bottom_bar(&mut self, ctx: &egui::Context, st: &Status) {
        let any_live = st.cameras.iter().any(|c| c.session.is_some());
        let all_live = !st.cameras.is_empty() && st.cameras.iter().all(|c| c.session.is_some());
        let note = {
            let i = self.shared.inner.lock().unwrap();
            i.notes.back().filter(|n| n.at.elapsed() < Duration::from_secs(8)).map(|n| (n.text.clone(), n.ok))
        };
        egui::TopBottomPanel::bottom("bottom").frame(egui::Frame::none().fill(PANEL).inner_margin(egui::Margin::symmetric(14.0, 10.0))).show(ctx, |ui| {
            ui.horizontal(|ui| {
                if action(ui, "모두 수신 시작", Some(GOOD.gamma_multiply(0.42)), !all_live && !st.cameras.is_empty()).clicked() {
                    self.send(Cmd::StartAll(self.prefs.opts()));
                }
                if action(ui, "모두 중지", None, any_live).clicked() {
                    self.send(Cmd::StopAll);
                }
                ui.add_space(8.0);
                if action(ui, "동시 촬영 (Space)", Some(ACCENT.gamma_multiply(0.62)), !st.cameras.is_empty()).on_hover_text("트리거 대상 카메라를 모두 같은 순간에 찍습니다 (스페이스바)").clicked() {
                    self.send(Cmd::Trigger);
                }
                ui.add_space(16.0);
                ui.label(RichText::new("시작할 때").color(MUTED));
                egui::ComboBox::from_id_salt("profile").width(96.0).selected_text(if self.prefs.profile == "sub" { "서브" } else { "메인" }).show_ui(ui, |ui| {
                    ui.selectable_value(&mut self.prefs.profile, "main".into(), "메인 (고화질)");
                    ui.selectable_value(&mut self.prefs.profile, "sub".into(), "서브 (저화질)");
                });
                egui::ComboBox::from_id_salt("transport").width(72.0).selected_text(self.prefs.transport.to_uppercase()).show_ui(ui, |ui| {
                    ui.selectable_value(&mut self.prefs.transport, "tcp".into(), "TCP");
                    ui.selectable_value(&mut self.prefs.transport, "udp".into(), "UDP");
                });
                ui.checkbox(&mut self.prefs.record, "녹화 저장");
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| match note {
                    Some((t, ok)) => {
                        ui.label(RichText::new(t).color(if ok { GOOD } else { BAD }));
                    }
                    None => {
                        ui.label(RichText::new("창을 닫아도 수신·녹화는 계속됩니다").color(MUTED).small());
                    }
                });
            });
        });
    }

    fn pane(&mut self, ui: &mut egui::Ui, cam: &Camera, max_h: f32, many: bool, series: &[Point]) {
        let ctx = ui.ctx().clone();
        let live = cam.live.clone();
        card().show(ui, |ui| {
            // 영상
            let aspect = live.as_ref().and_then(|l| Some(l.width? as f32 / l.height?.max(1) as f32)).unwrap_or(4.0 / 3.0);
            let avail_w = ui.available_width();
            let below = if live.is_some() { 44.0 + 58.0 } else { 44.0 };
            let video_h = (avail_w / aspect).min((max_h - below - 34.0).max(140.0));
            let (rect, resp) = ui.allocate_exact_size(vec2(avail_w, video_h), Sense::click());
            let p = ui.painter_at(rect);
            p.rect_filled(rect, 8.0, VIDEO_BG);
            let recv_fps = live.as_ref().filter(|_| self.engine_ok).map(|l| l.fps);
            let shown = self.texture(&ctx, &cam.id, recv_fps);
            let name = if cam.name.is_empty() || cam.name == cam.id { cam.id.clone() } else { format!("{} · {}", cam.id, cam.name) };
            match (&live, shown) {
                (Some(_), Some((id, _))) => {
                    let img = Rect::from_center_size(rect.center(), vec2(video_h * aspect, video_h));
                    egui::Image::new(egui::load::SizedTexture::new(id, img.size())).rounding(Rounding::same(8.0)).paint_at(ui, img);
                }
                (Some(l), None) => {
                    let t = l.last_error.clone().filter(|_| l.state != "receiving").unwrap_or_else(|| "영상을 기다리는 중…".into());
                    p.text(rect.center(), Align2::CENTER_CENTER, t, FontId::proportional(16.0), MUTED);
                }
                (None, _) => {
                    p.text(rect.center() - vec2(0.0, 12.0), Align2::CENTER_CENTER, "수신하고 있지 않습니다", bold(17.0), MUTED);
                    let hint = if cam.status.online == Some(false) { "카메라가 응답하지 않습니다 · 네트워크를 확인하세요" } else { "[수신 시작]을 누르세요 · [촬영]은 수신하지 않아도 됩니다" };
                    p.text(rect.center() + vec2(0.0, 14.0), Align2::CENTER_CENTER, hint, FontId::proportional(13.5), if cam.status.online == Some(false) { BAD } else { MUTED });
                }
            }
            // 영상 위: 이름·상태(왼쪽), 녹화·주소(오른쪽), 수치(아래)
            let (t, col) = state_text(live.as_ref().map_or("", |l| l.state.as_str()));
            let mut x = rect.left() + 10.0;
            x += pill(&p, pos2(x, rect.top() + 10.0), false, &name, Color32::WHITE) + 6.0;
            pill(&p, pos2(x, rect.top() + 10.0), false, &format!("● {t}"), col);
            let mut rx = rect.right() - 10.0;
            if let Some(l) = live.as_ref().filter(|l| l.record) {
                rx -= pill(&p, pos2(rx, rect.top() + 10.0), true, &format!("● REC {:.0}MB", l.recorded_bytes as f64 / 1e6), BAD) + 6.0;
            }
            pill(&p, pos2(rx, rect.top() + 10.0), true, &cam.host, Color32::from_gray(190));
            // 화면이 수신을 못 따라가는 PC: 늦는 것은 화면뿐이고 수신·녹화는 그대로라는 것을 알린다
            if live.is_some() {
                let behind = self.behind.get(&cam.id).copied().unwrap_or(0) >= BEHIND_SECS;
                let on_cpu = self.videos.get(&cam.id).is_some_and(|v| v.shared.cpu_fallback.load(Ordering::Relaxed));
                let note = match (behind, on_cpu) {
                    (true, _) if avail_w > 620.0 => Some(("화면이 수신을 못 따라갑니다 · 이 PC가 영상을 푸는 속도 부족 (수신·녹화는 정상)", WARN)),
                    (true, _) => Some(("화면이 밀림 (PC 속도 부족)", WARN)),
                    (false, true) => Some(("GPU로 풀 수 없어 CPU로 푸는 중", Color32::from_gray(190))),
                    _ => None,
                };
                if let Some((text, col)) = note {
                    pill(&p, pos2(rect.left() + 10.0, rect.top() + 42.0), false, text, col);
                }
            }
            if let Some(l) = &live {
                let strip = Rect::from_min_max(pos2(rect.left(), rect.bottom() - 30.0), rect.right_bottom());
                p.rect_filled(strip, Rounding { nw: 0.0, ne: 0.0, sw: 8.0, se: 8.0 }, Color32::from_black_alpha(165));
                let g = ui.fonts(|f| f.layout_job(stats_job(l, shown.map(|s| s.1))));
                p.galley(strip.left_center() + vec2(12.0, -g.size().y / 2.0), g, Color32::WHITE);
                let (w, h) = (l.width.unwrap_or(0), l.height.unwrap_or(0));
                let info = format!("{} {w}×{h} · {:.0}분", l.codec.as_deref().unwrap_or("").to_uppercase(), l.elapsed_s / 60.0);
                if avail_w > 620.0 {
                    p.text(strip.right_center() - vec2(12.0, 0.0), Align2::RIGHT_CENTER, info, FontId::proportional(13.0), Color32::from_gray(190));
                }
            }
            // 화면을 누르면 그 카메라만 크게, 다시 누르면 원래대로
            if many {
                let solo = self.solo.as_deref() == Some(cam.id.as_str());
                let resp = resp.on_hover_cursor(egui::CursorIcon::PointingHand).on_hover_text(if solo { "누르면 2대 화면으로 돌아갑니다" } else { "누르면 이 카메라만 크게 봅니다" });
                if resp.clicked() {
                    self.solo = if solo { None } else { Some(cam.id.clone()) };
                }
            }
            // 조작
            ui.horizontal(|ui| {
                match &cam.session {
                    Some(sid) => {
                        if action(ui, "수신 중지", Some(BAD.gamma_multiply(0.5)), true).clicked() {
                            self.send(Cmd::Stop(sid.clone()));
                        }
                    }
                    None => {
                        let has = if self.prefs.profile == "sub" { cam.has_sub } else { cam.has_main };
                        if action(ui, "수신 시작", Some(GOOD.gamma_multiply(0.42)), has).on_disabled_hover_text("이 카메라의 스트림 주소가 없습니다. 설정 → 카메라에서 [연결 테스트]를 하세요").clicked() {
                            self.send(Cmd::Start(cam.id.clone(), self.prefs.opts()));
                        }
                    }
                }
                if action(ui, "촬영", Some(ACCENT.gamma_multiply(0.62)), true).on_hover_text("누른 뒤 처음 도착한 프레임을 원본 해상도로 저장합니다").clicked() {
                    self.send(Cmd::Capture(cam.id.clone(), self.prefs.opts()));
                }
                let can_e2e = live.is_some() && shown.is_some();
                if action(ui, "실제 지연 측정", None, can_e2e)
                    .on_hover_text("카메라가 이 모니터를 비추게 한 뒤 누르세요. 화면을 흑백으로 깜빡이고, 그 변화가 이 화면에 나오기까지 걸리는 시간을 잽니다")
                    .clicked()
                {
                    if let Some(l) = &live {
                        let mut e = e2e::E2e::default();
                        e.start();
                        if let Some(v) = self.videos.get(&cam.id) {
                            v.shared.lum.lock().unwrap().clear();
                        }
                        self.e2e = Some(E2eRun { camera: cam.id.clone(), session: l.session_id.clone(), e, white: false, next_flip: Instant::now() + Duration::from_millis(1500) });
                    }
                }
            });
            // 작은 그래프 (최근 2분)
            if let Some(l) = &live {
                let w = ((ui.available_width() - 3.0 * ui.spacing().item_spacing.x) / 4.0).floor().max(60.0);
                let col = |f: fn(&Point) -> f64| series.iter().map(f).collect::<Vec<f64>>();
                let last_iv = series.last().and_then(|p| p.interval_max_ms);
                let last_dl = series.last().and_then(|p| p.delay_ms);
                ui.horizontal(|ui| {
                    spark(ui, "수신 fps", &col(|p| p.fps), format!("{:.0}", l.fps), GOOD, w);
                    spark(ui, "프레임 간격 최대", &col(|p| p.interval_max_ms.unwrap_or(0.0)), last_iv.map_or("—".into(), |v| format!("{v:.0}ms")), WARN, w);
                    spark(ui, "비트레이트", &col(|p| p.kbps), format!("{:.1}Mbps", l.kbps / 1000.0), ACCENT, w);
                    spark(ui, "버퍼 지연", &col(|p| p.delay_ms.unwrap_or(0.0)), last_dl.map_or("—".into(), |v| format!("{v:.0}ms")), BAD, w);
                });
            }
        });
    }

    fn center(&mut self, ctx: &egui::Context, st: &Status) {
        let series = self.shared.inner.lock().unwrap().series.clone();
        egui::CentralPanel::default().frame(egui::Frame::none().fill(BG).inner_margin(egui::Margin::same(12.0))).show(ctx, |ui| {
            if st.cameras.is_empty() {
                ui.vertical_centered(|ui| {
                    ui.add_space(110.0);
                    ui.label(RichText::new("등록된 카메라가 없습니다").font(bold(22.0)));
                    ui.add_space(4.0);
                    ui.label(RichText::new("카메라를 찾아 등록하면 여기에 화면이 나옵니다.").color(MUTED));
                    ui.add_space(12.0);
                    if action(ui, "카메라 등록하기", Some(ACCENT.gamma_multiply(0.62)), true).clicked() {
                        self.open_settings(Tab::Cameras);
                    }
                });
                return;
            }
            if !st.ffmpeg.found {
                ui.label(RichText::new("FFmpeg를 찾지 못해 영상을 받을 수 없습니다 (프로그램 폴더의 ffmpeg\\ffmpeg.exe)").color(BAD));
            }
            if self.solo.as_ref().is_some_and(|s| !st.cameras.iter().any(|c| &c.id == s)) {
                self.solo = None;
            }
            let cams: Vec<&Camera> = match &self.solo {
                Some(s) => st.cameras.iter().filter(|c| &c.id == s).collect(),
                None => st.cameras.iter().take(2).collect(),
            };
            let many = st.cameras.len() > 1;
            let max_h = ui.available_height();
            let empty: Vec<Point> = Vec::new();
            let of = |c: &Camera| c.session.as_ref().and_then(|s| series.get(s)).unwrap_or(&empty).clone();
            if cams.len() == 1 {
                let s = of(cams[0]);
                self.pane(ui, cams[0], max_h, many, &s);
            } else {
                ui.spacing_mut().item_spacing.x = 12.0;
                ui.columns(cams.len(), |cols| {
                    for (col, cam) in cols.iter_mut().zip(cams) {
                        let s = of(cam);
                        self.pane(col, cam, max_h, many, &s);
                    }
                });
            }
        });
    }

    // ---------------- 설정 ----------------

    fn settings_window(&mut self, ctx: &egui::Context, st: &Status) {
        let mut open = self.show_settings;
        egui::Window::new(RichText::new("설정").font(bold(17.0))).open(&mut open).collapsible(false).resizable(false).fixed_size([640.0, 520.0]).anchor(Align2::CENTER_CENTER, [0.0, 0.0]).show(ctx, |ui| {
            ui.horizontal(|ui| {
                for (t, label) in [(Tab::Cameras, "카메라"), (Tab::Receive, "수신 · 녹화"), (Tab::Screen, "화면"), (Tab::Program, "프로그램")] {
                    if ui.selectable_label(self.tab == t, RichText::new(label).font(bold(15.0))).clicked() {
                        self.tab = t;
                    }
                }
            });
            ui.separator();
            egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| match self.tab {
                Tab::Cameras => self.tab_cameras(ui, st),
                Tab::Receive => self.tab_receive(ui),
                Tab::Screen => self.tab_screen(ui),
                Tab::Program => self.tab_program(ui, st),
            });
        });
        if !open {
            self.form = None;
            self.confirm_delete = None;
        }
        self.show_settings = open;
    }

    fn tab_cameras(&mut self, ui: &mut egui::Ui, st: &Status) {
        let (list, found, test, saved, scan) = {
            let i = self.shared.inner.lock().unwrap();
            (i.cam_list.clone(), i.found.clone(), i.test.clone(), i.cam_saved, i.scan.clone())
        };
        // 저장이 끝났으면 입력을 닫는다
        if self.form.as_ref().is_some_and(|f| f.mark != saved) {
            self.form = None;
        }
        if self.form.is_some() {
            self.cam_form(ui, &test, saved);
            return;
        }
        let Some(list) = list else {
            ui.label(RichText::new("카메라 목록을 읽는 중…").color(MUTED));
            return;
        };
        let max = if st.max_cameras == 0 { 2 } else { st.max_cameras };
        let full = list.len() >= max;
        let full_tip = format!("카메라는 {max}대까지입니다. 카메라 주소가 바뀌었으면 [카메라 찾기]로 찾아 [주소 바꾸기]를 누르세요. 다른 카메라로 바꾸려면 쓰지 않는 카메라를 지운 뒤 추가하세요");
        // 등록된 카메라의 IP (주소에 포트가 붙어 있을 수 있다)
        let registered_ips: Vec<String> = list.iter().map(|c| c.host.rsplit_once(':').filter(|(_, p)| !p.is_empty() && p.chars().all(|ch| ch.is_ascii_digit())).map_or(c.host.clone(), |(h, _)| h.to_string())).collect();
        let is_registered = |ip: &str| registered_ips.iter().any(|r| r == ip);
        ui.horizontal(|ui| {
            ui.label(RichText::new(format!("등록된 카메라 {} / {max}대", list.len())).font(bold(15.0)));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui.add_enabled(!full, egui::Button::new("직접 추가")).on_disabled_hover_text(&full_tip).clicked() {
                    self.form = Some(CamForm { user: "admin".into(), trigger: true, mark: saved, ..Default::default() });
                    self.shared.inner.lock().unwrap().test = Job::Idle;
                }
                let finding = matches!(found, Job::Running);
                // 가득 등록돼 있어도 찾을 수 있어야 한다: 공유기를 껐다 켜면 카메라 주소가 바뀌어 다시 찾아야 한다
                if ui.add_enabled(!finding, egui::Button::new(if finding { "찾는 중…" } else { "카메라 찾기" })).on_hover_text("같은 네트워크의 카메라를 찾습니다 — ONVIF 탐색과 RTSP 포트 확인을 같이 합니다 (약 3초)").on_disabled_hover_text(&full_tip).clicked() {
                    self.send(Cmd::Discover);
                }
            });
        });
        if list.is_empty() {
            ui.add_space(6.0);
            ui.label(RichText::new("등록된 카메라가 없습니다. [카메라 찾기]나 [직접 추가]로 등록하세요.").color(MUTED));
        }
        for c in &list {
            let on = st.cameras.iter().find(|x| x.id == c.id).and_then(|x| x.status.online);
            card().show(ui, |ui| {
                ui.horizontal(|ui| {
                    let (dot, state) = if c.receiving { (GOOD, "수신 중") } else { match on { Some(true) => (GOOD, "연결됨"), Some(false) => (BAD, "응답 없음"), None => (MUTED, "확인 중") } };
                    ui.label(RichText::new("●").color(dot));
                    ui.vertical(|ui| {
                        ui.label(RichText::new(&c.name).font(bold(15.0)));
                        ui.label(RichText::new(format!("{} · {} · {state}{}", c.id, c.host, if c.subnet.as_deref() == Some("routed") { " · 다른 대역 (공유기 경유)" } else { "" })).color(MUTED).small());
                        let path = |u: &str| if u.is_empty() { "없음".to_string() } else { u.splitn(4, '/').nth(3).map_or(u.to_string(), |p| format!("/{p}")) };
                        ui.label(RichText::new(format!("메인 {} · 서브 {}{} · {}", path(&c.rtsp_main), path(&c.rtsp_sub), if c.username.is_empty() { String::new() } else { format!(" · 계정 {}", c.username) }, if c.trigger { "동시 촬영 대상" } else { "동시 촬영 제외" })).color(MUTED).small());
                        if c.rtsp_main.is_empty() {
                            ui.label(RichText::new("영상 주소를 아직 못 찾았습니다 — 카메라가 켜진 뒤 [수신 시작]을 누르면 자동으로 찾습니다").color(WARN).small());
                        } else if c.follows {
                            ui.label(RichText::new("공유기를 껐다 켜서 IP가 바뀌어도 자동으로 따라갑니다").color(MUTED).small());
                        }
                    });
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if self.confirm_delete.as_deref() == Some(c.id.as_str()) {
                            if ui.button("취소").clicked() {
                                self.confirm_delete = None;
                            }
                            if ui.add(egui::Button::new("지우기").fill(BAD.gamma_multiply(0.5))).clicked() {
                                self.send(Cmd::DeleteCamera(c.id.clone()));
                                self.confirm_delete = None;
                            }
                            ui.label(RichText::new(if c.receiving { "수신을 멈추고 지웁니다" } else { "정말 지울까요?" }).color(WARN).small());
                        } else {
                            if ui.button("삭제").clicked() {
                                self.confirm_delete = Some(c.id.clone());
                            }
                            if ui.add_enabled(!c.receiving, egui::Button::new("수정")).on_disabled_hover_text("수신 중인 카메라는 바꿀 수 없습니다. 먼저 수신을 중지하세요").clicked() {
                                self.form = Some(open_form(c, saved));
                                self.shared.inner.lock().unwrap().test = Job::Idle;
                            }
                        }
                    });
                });
            });
        }
        // 찾은 카메라
        match &found {
            Job::Idle => {}
            Job::Running => {
                ui.label(RichText::new("같은 네트워크에서 카메라를 찾는 중… (약 3초)").color(MUTED));
            }
            Job::Failed(e) => {
                ui.label(RichText::new(format!("카메라 찾기 실패: {e}")).color(BAD));
            }
            Job::Done(l) if l.is_empty() => {
                ui.label(RichText::new("찾은 카메라가 없습니다. 카메라 전원·랜선과, 이 PC가 카메라와 같은 공유기에 붙어 있는지 확인하세요.").color(WARN));
                ui.label(RichText::new("카메라가 켜진 지 1분이 안 됐으면 잠시 뒤 다시 눌러 보세요. 그래도 안 나오면 아래 [IP로 찾기]에 공유기 대역(예: 192.168.1.1-254)을 넣어 찾을 수 있습니다.").color(MUTED).small());
            }
            Job::Done(l) => {
                // 다른 주소로 등록돼 있는 같은 카메라: 공유기를 껐다 켜서 IP가 바뀐 것 → 지우지 않고 주소만 바꾼다
                // (두 카메라의 주소가 서로 바뀐 경우에는 그 주소가 다른 카메라로 등록돼 있다 → 한 번 누르면 둘을 맞바꾼다)
                let moved: Vec<_> = l.iter().filter(|d| d.same_as.as_ref().is_some_and(|id| d.registered.as_ref() != Some(id) && list.iter().any(|c| &c.id == id))).collect();
                for d in &moved {
                    let Some(c) = d.same_as.as_ref().and_then(|id| list.iter().find(|c| &c.id == id)) else { continue };
                    // 그 주소를 지금 쓰고 있는(등록된) 다른 카메라
                    let other = list.iter().zip(&registered_ips).find(|(o, ip)| o.id != c.id && **ip == d.ip).map(|(o, _)| o);
                    let busy = c.receiving || other.is_some_and(|o| o.receiving);
                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(format!("{} — 주소가 바뀌었습니다", c.name)).font(bold(14.5)).color(WARN));
                        ui.label(RichText::new(match other {
                            Some(o) => format!("{} → {} ({}와 서로 바뀜)", c.host, d.ip, o.id),
                            None => format!("{} → {}", c.host, d.ip),
                        }).color(MUTED).small());
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            let tip = if other.is_some() { "두 카메라의 주소를 서로 맞바꿉니다. 이름·계정·영상 주소는 그대로 둡니다" } else { "이름·계정·영상 주소는 그대로 두고 카메라의 IP만 새 주소로 바꿉니다" };
                            if ui.add_enabled(!busy, egui::Button::new("주소 바꾸기").fill(ACCENT.gamma_multiply(0.62))).on_disabled_hover_text("수신 중인 카메라는 바꿀 수 없습니다. 먼저 수신을 중지하세요").on_hover_text(tip).clicked() {
                                self.send(Cmd::MoveCamera(c.id.clone(), d.ip.clone(), d.uid.clone(), d.mac.clone()));
                            }
                        });
                    });
                }
                // 응답이 없는 등록 카메라: 같은 카메라인지 프로그램이 알아보지 못해도(예전에 등록한 카메라) 사람이 골라 주소를 옮길 수 있게 한다
                let offline: Vec<&CamEdit> = list
                    .iter()
                    .filter(|c| !c.receiving && st.cameras.iter().find(|x| x.id == c.id).and_then(|x| x.status.online) != Some(true))
                    // 새 주소를 이미 알아낸 카메라(위의 '주소가 바뀌었습니다')는 빼고
                    .filter(|c| !moved.iter().any(|m| m.same_as.as_deref() == Some(c.id.as_str())))
                    .collect();
                // 이미 등록한 카메라는 목록에서 뺀다 (찾은 뒤에 추가한 것도 바로 빠진다)
                let fresh: Vec<_> = l.iter().filter(|d| d.registered.is_none() && !is_registered(&d.ip) && !moved.iter().any(|m| m.ip == d.ip)).collect();
                let hidden = l.len() - fresh.len() - moved.len();
                ui.add_space(4.0);
                if fresh.is_empty() && moved.is_empty() {
                    ui.label(RichText::new(format!("찾은 카메라 {}대는 모두 이미 등록돼 있습니다.", l.len())).color(MUTED));
                } else if !fresh.is_empty() {
                    let note = if hidden > 0 { format!(" (이미 등록한 {hidden}대는 빼고 보여 줍니다)") } else { String::new() };
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(format!("새로 찾은 카메라 {}대", fresh.len())).font(bold(15.0)));
                        ui.label(RichText::new(note).color(MUTED).small());
                    });
                }
                for d in fresh {
                    let maker = [d.manufacturer.clone(), d.model.clone()].into_iter().flatten().collect::<Vec<_>>().join(" ");
                    // ONVIF 탐색에는 답이 없고 RTSP 포트만 응답한 장치 (ONVIF가 꺼진 카메라, 탐색이 막힌 네트워크)
                    let rtsp_only = d.via == "rtsp";
                    let title = d.name.clone().filter(|n| !n.is_empty()).unwrap_or_else(|| if !maker.is_empty() { maker.clone() } else if rtsp_only { "카메라 (RTSP 응답)".into() } else { "ONVIF 카메라".into() });
                    // 버튼: [추가]와, 응답 없는 등록 카메라의 주소를 이 카메라로 옮기는 [camNN 주소로]
                    let buttons = |ui: &mut egui::Ui, this: &mut Self| {
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            if ui.add_enabled(!full, egui::Button::new("추가").fill(ACCENT.gamma_multiply(0.62))).on_disabled_hover_text(&full_tip).clicked() {
                                let name = if rtsp_only { String::new() } else { title.clone() };
                                this.form = Some(CamForm { name, host: d.ip.clone(), user: "admin".into(), trigger: true, uid: d.uid.clone(), mac: d.mac.clone(), mark: saved, ..Default::default() });
                                this.shared.inner.lock().unwrap().test = Job::Idle;
                            }
                            for c in &offline {
                                if ui.button(format!("{} 주소로", c.id)).on_hover_text(format!("응답이 없는 {}({})의 주소를 이 카메라({})로 바꿉니다. 이름·계정·영상 주소는 그대로 둡니다", c.name, c.host, d.ip)).clicked() {
                                    this.send(Cmd::MoveCamera(c.id.clone(), d.ip.clone(), d.uid.clone(), d.mac.clone()));
                                }
                            }
                        });
                    };
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(&title).font(bold(14.5)));
                        ui.label(RichText::new(format!("{} {maker}{}", d.ip, if rtsp_only { "· ONVIF 탐색 응답 없음" } else { "" })).color(MUTED).small());
                        // 버튼이 [추가] 하나면 같은 줄에, [camNN 주소로]가 붙으면 문구를 가리므로 아래 줄에 둔다
                        if offline.is_empty() {
                            buttons(ui, self);
                        }
                    });
                    if !offline.is_empty() {
                        ui.horizontal(|ui| buttons(ui, self));
                    }
                }
            }
        }
        // IP로 찾기: 대역이 달라 [카메라 찾기]에 안 나오는 카메라
        ui.add_space(6.0);
        ui.label(RichText::new("IP로 찾기").font(bold(15.0)));
        ui.horizontal(|ui| {
            let scanning = matches!(scan, Job::Running);
            let resp = ui.add(egui::TextEdit::singleline(&mut self.scan_target).desired_width(360.0).hint_text("192.168.2.27 · 192.168.2.1-254 · 192.168.2.0/24"));
            let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
            if (ui.add_enabled(!scanning && !self.scan_target.trim().is_empty(), egui::Button::new(if scanning { "찾는 중…" } else { "찾기" })).clicked() || enter) && !scanning && !self.scan_target.trim().is_empty() {
                self.send(Cmd::Scan(self.scan_target.trim().to_string()));
            }
        });
        ui.label(RichText::new("카메라 IP를 알면 대역이 달라도 찾아봅니다. 포트 응답과 장치 이름만 읽고 비밀번호는 넣어 보지 않습니다 (최대 1024개).").color(MUTED).small());
        match &scan {
            Job::Idle => {}
            Job::Running => {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(RichText::new("찾는 중입니다…").color(MUTED));
                });
            }
            Job::Failed(e) => {
                ui.label(RichText::new(format!("찾지 못했습니다: {e}")).color(BAD));
            }
            Job::Done(r) => {
                let hosts = r["hosts"].as_array().cloned().unwrap_or_default();
                ui.label(RichText::new(format!("{}개 주소를 확인해 응답한 장치 {}개", r["scanned"], hosts.len())).color(MUTED).small());
                for hst in &hosts {
                    let ip = hst["ip"].as_str().unwrap_or("").to_string();
                    let name = hst["onvif"]["name"].as_str().or(hst["onvif"]["model"].as_str()).or(hst["http_title"].as_str()).or(hst["rtsp_server"].as_str()).or(hst["http_realm"].as_str()).unwrap_or("").to_string();
                    let likely = hst["likely_camera"].as_bool() == Some(true);
                    let ports: Vec<String> = hst["open_ports"].as_array().map(|a| a.iter().map(|p| p.to_string()).collect()).unwrap_or_default();
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("●").color(if likely { GOOD } else { MUTED }));
                        ui.label(RichText::new(&ip).font(bold(14.5)));
                        ui.label(RichText::new(format!("{}{name} · 포트 {}{}", if likely { "카메라로 보임 · " } else { "" }, if ports.is_empty() { "없음".into() } else { ports.join(", ") }, if hst["same_subnet"].as_bool() == Some(false) { " · 다른 대역" } else { "" })).color(MUTED).small());
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            // 등록 여부는 지금 목록으로 본다 (찾은 뒤에 추가한 것도 바로 '등록됨'으로 바뀐다)
                            if hst["registered"].is_string() || is_registered(&ip) {
                                ui.label(RichText::new("등록됨").color(MUTED).small());
                            } else if ui.add_enabled(!full, egui::Button::new("추가").fill(ACCENT.gamma_multiply(0.62))).on_disabled_hover_text(&full_tip).clicked() {
                                self.form = Some(CamForm { name: name.clone(), host: ip.clone(), user: "admin".into(), trigger: true, mark: saved, ..Default::default() });
                                self.shared.inner.lock().unwrap().test = Job::Idle;
                            }
                        });
                    });
                }
                let d = &r["diagnosis"];
                if !d.is_null() {
                    ui.label(RichText::new(d["summary"].as_str().unwrap_or("")).color(if matches!(d["verdict"].as_str(), Some("ok")) { GOOD } else { WARN }));
                    for a in d["advice"].as_array().cloned().unwrap_or_default() {
                        ui.label(RichText::new(format!("→ {}", a.as_str().unwrap_or(""))).color(MUTED).small());
                    }
                }
            }
        }
    }

    fn cam_form(&mut self, ui: &mut egui::Ui, test: &Job<Value>, saved: u64) {
        let saving = self.shared.inner.lock().unwrap().cam_saving;
        let Some(f) = self.form.as_mut() else { return };
        // 테스트가 성공하면 찾은 주소를 칸에 옮긴다
        if let (Job::Done(r), false) = (test, f.applied) {
            f.applied = true;
            f.tested = true;
            if r["ok"].as_bool() == Some(true) {
                f.main = r["main"]["url"].as_str().unwrap_or("").to_string();
                f.sub = r["sub"]["url"].as_str().unwrap_or("").to_string();
                if f.name.trim().is_empty() {
                    f.name = [r["manufacturer"].as_str(), r["model"].as_str()].into_iter().flatten().collect::<Vec<_>>().join(" ");
                }
            }
        }
        let title = match &f.id {
            Some(id) => format!("카메라 수정 · {id}"),
            None => "카메라 추가".into(),
        };
        ui.label(RichText::new(title).font(bold(16.0)));
        let mut do_test = false;
        let mut do_save = false;
        let mut cancel = false;
        egui::Grid::new("g-cam").num_columns(2).spacing([14.0, 9.0]).show(ui, |ui| {
            ui.label("이름");
            ui.add(egui::TextEdit::singleline(&mut f.name).desired_width(340.0).hint_text("예: 정차 지점 A 카메라 (비우면 자동)"));
            ui.end_row();
            ui.label("IP 주소");
            ui.add(egui::TextEdit::singleline(&mut f.host).desired_width(340.0).hint_text("예: 192.168.2.27"));
            ui.end_row();
            ui.label("아이디");
            ui.add(egui::TextEdit::singleline(&mut f.user).desired_width(200.0));
            ui.end_row();
            ui.label("비밀번호");
            ui.add(egui::TextEdit::singleline(&mut f.pass).password(true).desired_width(200.0).hint_text(if f.has_password { "바꾸지 않으려면 비워 두세요" } else { "" }));
            ui.end_row();
            ui.label("동시 촬영");
            ui.checkbox(&mut f.trigger, "동시 촬영(트리거) 때 이 카메라도 찍기");
            ui.end_row();
            ui.label("주소 직접 입력");
            ui.checkbox(&mut f.manual, "RTSP 주소를 직접 적기 (ONVIF가 안 되는 카메라)");
            ui.end_row();
            if f.manual || !f.main.is_empty() {
                ui.label("메인 RTSP");
                ui.add_enabled(f.manual, egui::TextEdit::singleline(&mut f.main).desired_width(440.0).hint_text("rtsp://192.168.2.27:554/…"));
                ui.end_row();
                ui.label("서브 RTSP");
                ui.add_enabled(f.manual, egui::TextEdit::singleline(&mut f.sub).desired_width(440.0));
                ui.end_row();
            }
        });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            let running = matches!(test, Job::Running);
            if ui.add_enabled(!running && !f.host.trim().is_empty(), egui::Button::new(if running { "테스트 중…" } else { "연결 테스트" })).on_hover_text("카메라에 접속해 RTSP 주소와 메인·서브 스트림의 해상도·fps를 찾습니다").clicked() {
                do_test = true;
            }
            if ui.add_enabled(!saving && !f.host.trim().is_empty(), egui::Button::new(RichText::new(if saving { "저장 중…" } else { "저장" }).font(bold(15.0))).fill(ACCENT.gamma_multiply(0.62))).clicked() {
                do_save = true;
            }
            if ui.button("취소").clicked() {
                cancel = true;
            }
            if saving {
                ui.spinner();
            }
        });
        // 안내는 버튼 아래 줄에 (옆에 두면 긴 문구가 창을 넓힌다)
        if saving {
            ui.label(RichText::new("저장하는 중입니다… 영상 주소가 비어 있으면 카메라에 접속해 찾습니다 (수 초)").color(MUTED).small());
        } else if f.main.trim().is_empty() {
            ui.label(RichText::new("영상 주소는 저장할 때 자동으로 찾습니다. [연결 테스트]로 먼저 확인할 수 있습니다").color(MUTED).small());
        }
        ui.add_space(4.0);
        match test {
            Job::Idle => {}
            Job::Running => {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(RichText::new("카메라에 연결하는 중입니다… ONVIF 확인과 스트림 접속에 수 초 걸립니다").color(MUTED));
                });
            }
            Job::Failed(e) => {
                ui.label(RichText::new(format!("테스트 요청 실패: {e}")).color(BAD));
            }
            Job::Done(r) => test_result(ui, r),
        }
        if do_test {
            f.applied = false;
            let opt = |s: &str| if f.manual && !s.trim().is_empty() { json!(s.trim()) } else { Value::Null };
            let body = json!({
                "camera_id": f.id, "host": f.host.trim(), "username": f.user.trim(),
                "password": if f.pass.is_empty() { Value::Null } else { json!(f.pass) },
                "rtsp_main": opt(&f.main), "rtsp_sub": opt(&f.sub),
            });
            let _ = self.cmd.send(Cmd::TestCamera(body));
        }
        if do_save {
            f.mark = saved;
            let body = json!({
                "name": f.name.trim(), "host": f.host.trim(), "rtsp_main": f.main.trim(), "rtsp_sub": f.sub.trim(), "username": f.user.trim(),
                "password": if f.pass.is_empty() { Value::Null } else { json!(f.pass) }, "trigger": f.trigger, "uid": f.uid, "mac": f.mac,
            });
            let _ = self.cmd.send(Cmd::SaveCamera(f.id.clone(), body));
        }
        if cancel {
            self.form = None;
        }
    }

    fn tab_receive(&mut self, ui: &mut egui::Ui) {
        let settings = self.shared.inner.lock().unwrap().settings.clone();
        if let (Some(s), false) = (&settings, self.seg_loaded) {
            self.seg_buf = s.segment_minutes.clamp(1, 60);
            self.seg_loaded = true;
        }
        egui::Grid::new("g-recv").num_columns(2).spacing([14.0, 10.0]).show(ui, |ui| {
            ui.label("스트림");
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.prefs.profile, "main".into(), "메인 (고화질)");
                ui.selectable_value(&mut self.prefs.profile, "sub".into(), "서브 (저화질)");
            });
            ui.end_row();
            ui.label("전송");
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.prefs.transport, "tcp".into(), "TCP");
                ui.selectable_value(&mut self.prefs.transport, "udp".into(), "UDP");
            });
            ui.end_row();
            ui.label("녹화");
            ui.checkbox(&mut self.prefs.record, "수신하면서 녹화 저장 (원본 그대로)");
            ui.end_row();
            ui.label("녹화 파일 길이");
            ui.horizontal(|ui| {
                ui.add_enabled(settings.is_some(), egui::DragValue::new(&mut self.seg_buf).range(1..=60).suffix("분"));
                let changed = settings.as_ref().is_some_and(|s| s.segment_minutes != self.seg_buf);
                if ui.add_enabled(changed, egui::Button::new("저장")).clicked() {
                    self.send(Cmd::SetSegment(self.seg_buf));
                }
            });
            ui.end_row();
            if let Some(s) = &settings {
                ui.label("녹화 저장 위치");
                ui.label(RichText::new(&s.recordings_dir).color(MUTED).small());
                ui.end_row();
            }
        });
        ui.add_space(4.0);
        ui.label(RichText::new("스트림·전송·녹화는 다음에 수신을 시작할 때부터, 녹화 파일 길이는 새로 시작하는 수신부터 적용됩니다.").color(MUTED).small());
        ui.separator();
        ui.horizontal(|ui| {
            if ui.button("저장 위치 · 보존 기간 · 트리거(PLC) 설정 (웹)").clicked() {
                self.open_web("settings");
            }
            ui.label(RichText::new("저장 위치·보존 기간·PLC 트리거는 웹 설정에 있습니다.").color(MUTED).small());
        });
    }

    fn tab_screen(&mut self, ui: &mut egui::Ui) {
        egui::Grid::new("g-view").num_columns(2).spacing([14.0, 10.0]).show(ui, |ui| {
            ui.label("영상 풀기");
            ui.checkbox(&mut self.prefs.hwaccel, "GPU 사용 (끄면 CPU)");
            ui.end_row();
            ui.label("화면용 크기");
            ui.horizontal(|ui| {
                for w in [640u32, 960, 1280, 1600] {
                    ui.selectable_value(&mut self.prefs.width, w, format!("{w}"));
                }
            });
            ui.end_row();
        });
        ui.add_space(4.0);
        ui.label(RichText::new("화면이 버벅이면 크기를 줄이세요. 화면 설정은 녹화·촬영 화질과 상관없습니다 (녹화·촬영은 항상 원본 해상도).").color(MUTED).small());
        ui.label(RichText::new("영상 화면을 누르면 그 카메라만 크게 보고, 다시 누르면 2대 화면으로 돌아갑니다.").color(MUTED).small());
    }

    fn tab_program(&mut self, ui: &mut egui::Ui, st: &Status) {
        ui.label(RichText::new(format!("Capture VMS Viewer · 엔진 v{}", st.version)).font(bold(15.0)));
        ui.label(RichText::new("이 창(뷰어)을 닫아도 수신·녹화는 계속됩니다. 다시 열면 이어서 보입니다.").color(MUTED));
        ui.label(RichText::new("세션 기록·촬영 기록·녹화 보기·원시 데이터·보고서는 웹에서 봅니다.").color(MUTED));
        ui.add_space(6.0);
        if ui.button("기록 보기 (웹)").clicked() {
            self.open_web("sessions");
        }
        ui.separator();
        ui.horizontal(|ui| {
            if !self.confirm_quit {
                if ui.button("프로그램 종료 (수신·녹화까지 끔)").clicked() {
                    self.confirm_quit = true;
                }
            } else {
                ui.label(RichText::new("수신·녹화를 모두 멈추고 엔진을 끕니다.").color(WARN));
                if ui.add(egui::Button::new("종료").fill(BAD.gamma_multiply(0.5))).clicked() {
                    self.send(Cmd::Shutdown);
                    self.confirm_quit = false;
                }
                if ui.button("취소").clicked() {
                    self.confirm_quit = false;
                }
            }
        });
    }

    /// 실제 지연 측정 화면: 창 전체를 흑백으로 깜빡인다
    fn e2e_screen(&mut self, ctx: &egui::Context, st: &Status) {
        let Some(run) = self.e2e.as_mut() else { return };
        let alive = st.cameras.iter().any(|c| c.id == run.camera && c.session.as_deref() == Some(run.session.as_str()));
        // 그동안 들어온 프레임의 밝기를 넣는다
        if let Some(v) = self.videos.get(&run.camera) {
            let q: Vec<(f64, f32)> = v.shared.lum.lock().unwrap().drain(..).collect();
            for (t, lum) in q {
                run.e.on_frame(t, lum);
            }
        }
        if Instant::now() >= run.next_flip {
            run.white = !run.white;
            run.e.mark(e2e::now_ms(), run.white);
            let jitter = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.subsec_nanos() % 1000) as u64;
            run.next_flip = Instant::now() + Duration::from_millis(2000 + jitter);
        }
        let live = run.e.live();
        let (white, camera) = (run.white, run.camera.clone());
        let fill = if white { Color32::WHITE } else { Color32::BLACK };
        egui::CentralPanel::default().frame(egui::Frame::none().fill(fill)).show(ctx, |_| {});
        let shown = self.texture(ctx, &camera, None);
        if let (Some((id, _)), Some(v)) = (shown, self.videos.get(&camera)) {
            egui::Area::new("e2e-video".into()).anchor(Align2::RIGHT_TOP, [-16.0, 16.0]).show(ctx, |ui| {
                let size = vec2(300.0, 300.0 * v.h as f32 / v.w.max(1) as f32);
                let (rect, _) = ui.allocate_exact_size(size, Sense::hover());
                ui.painter().image(id, rect, Rect::from_min_max(pos2(0.0, 0.0), pos2(1.0, 1.0)), Color32::WHITE);
                ui.painter().rect_stroke(rect, 0.0, Stroke::new(1.0_f32, Color32::GRAY));
            });
        }
        let mut finish = None;
        egui::Area::new("e2e-info".into()).anchor(Align2::LEFT_BOTTOM, [16.0, -16.0]).show(ctx, |ui| {
            egui::Frame::none().fill(CARD).stroke(Stroke::new(1.0_f32, LINE)).rounding(Rounding::same(12.0)).inner_margin(egui::Margin::same(16.0)).show(ui, |ui| {
                ui.set_max_width(540.0);
                ui.label(RichText::new(format!("실제 지연 측정 · {camera}")).font(bold(19.0)));
                ui.label(RichText::new("카메라가 이 화면을 비추게 하세요. 화면이 2~3초마다 흑↔백으로 바뀌고, 그 변화가 뷰어 화면에 나오기까지의 시간을 잽니다 (카메라 → 네트워크 → 수신 → 화면).").color(MUTED));
                ui.add_space(6.0);
                if !alive {
                    ui.label(RichText::new("수신이 끝나 측정을 이어갈 수 없습니다.").color(BAD));
                } else if !live.frames_ok {
                    ui.label(RichText::new("영상이 들어오지 않습니다.").color(BAD));
                } else if !live.signal_ok {
                    ui.label(RichText::new(format!("카메라가 화면 깜빡임을 보지 못하고 있습니다 (밝기 차이 {:.0}). 카메라를 모니터 쪽으로 돌리세요.", live.signal)).color(WARN));
                } else {
                    ui.label(RichText::new(format!("화면 깜빡임이 영상에 잡힙니다 (밝기 차이 {:.0})", live.signal)).color(GOOD));
                }
                let ms = |v: Option<f64>| v.map_or("—".to_string(), |v| format!("{v:.0}ms"));
                ui.label(RichText::new(format!("측정 {}회 · 방금 {} · 중앙값 {} · 최소 {} · 최대 {}", live.count, ms(live.last_ms), ms(live.p50_ms), ms(live.min_ms), ms(live.max_ms))).font(bold(17.0)));
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if action(ui, "측정 끝내기 (기록에 남김)", Some(ACCENT.gamma_multiply(0.62)), true).clicked() {
                        finish = Some(true);
                    }
                    if action(ui, "취소", None, true).clicked() {
                        finish = Some(false);
                    }
                });
            });
        });
        if let Some(save) = finish {
            if let Some(mut run) = self.e2e.take() {
                match (save, run.e.finish()) {
                    (true, Some(sum)) => {
                        self.shared.note(format!("실제 지연 측정 {}회 · 중앙값 {:.0}ms (최소 {:.0} · 최대 {:.0})", sum.count, sum.p50_ms, sum.min_ms, sum.max_ms), true);
                        if let Ok(v) = serde_json::to_value(&sum) {
                            self.send(Cmd::E2eReport(run.session.clone(), v));
                        }
                    }
                    (true, None) => self.shared.note("측정값이 없어 기록에 남기지 않았습니다", false),
                    _ => {}
                }
            }
        }
        // 색을 바꾸는 시각이 정확하도록 계속 다시 그린다
        ctx.request_repaint();
    }
}

fn level_style(level: &str) -> (&'static str, Color32) {
    match level {
        "bad" => ("문제", BAD),
        "warn" => ("주의", WARN),
        "ok" => ("정상", GOOD),
        _ => ("참고", MUTED),
    }
}

fn diag_row(ui: &mut egui::Ui, c: &DiagCheck) {
    let (label, col) = level_style(&c.level);
    ui.horizontal_top(|ui| {
        egui::Frame::none().fill(col.gamma_multiply(0.18)).rounding(Rounding::same(9.0)).inner_margin(egui::Margin::symmetric(9.0, 2.0)).show(ui, |ui| {
            ui.set_min_width(46.0);
            ui.label(RichText::new(format!("● {label}")).color(col).font(bold(13.0)));
        });
        ui.vertical(|ui| {
            ui.horizontal_wrapped(|ui| {
                ui.label(RichText::new(&c.title).font(bold(14.5)));
                ui.label(RichText::new(&c.summary).color(if c.level == "bad" { BAD } else { TEXT }));
            });
            for a in &c.advice {
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new("→").color(col));
                    ui.label(RichText::new(a).color(MUTED).size(13.5));
                });
            }
        });
    });
}

impl ViewerApp {
    /// 문제 진단 창: 엔진이 점검한 결과에, 이 뷰어가 화면을 제때 그리고 있는지를 덧붙여 보여 준다
    fn diag_window(&mut self, ctx: &egui::Context, st: &Status) {
        let diag = self.shared.inner.lock().unwrap().diag.clone();
        let mut open = self.show_diag;
        let mut again = false;
        let report: Option<DiagReport> = match &diag {
            Job::Done(r) => Some(self.with_screen_checks(r.clone(), st)),
            _ => None,
        };
        egui::Window::new(RichText::new("문제 진단").font(bold(17.0))).open(&mut open).collapsible(false).resizable(false).fixed_size([700.0, 580.0]).anchor(Align2::CENTER_CENTER, [0.0, 0.0]).show(ctx, |ui| {
            ui.horizontal(|ui| {
                match (&diag, &report) {
                    (Job::Failed(e), _) => {
                        ui.label(RichText::new(format!("점검하지 못했습니다: {e}")).color(BAD));
                    }
                    (Job::Done(_), Some(r)) => {
                        let all = || r.groups.iter().flat_map(|g| g.checks.iter());
                        let (bad, warn) = (all().filter(|c| c.level == "bad").count(), all().filter(|c| c.level == "warn").count());
                        let (text, col) = match (bad, warn) {
                            (0, 0) => ("문제가 없습니다".to_string(), GOOD),
                            (0, w) => (format!("확인할 것 {w}건"), WARN),
                            (b, 0) => (format!("문제 {b}건"), BAD),
                            (b, w) => (format!("문제 {b}건 · 확인할 것 {w}건"), BAD),
                        };
                        ui.label(RichText::new(text).font(bold(20.0)).color(col));
                        ui.label(RichText::new(format!("{} 점검", r.at.get(11..19).unwrap_or(""))).color(MUTED).small());
                    }
                    _ => {
                        ui.spinner();
                        ui.label(RichText::new("점검하는 중입니다… (1~2초)").color(MUTED));
                    }
                }
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.add_enabled(!matches!(diag, Job::Running), egui::Button::new("다시 점검")).clicked() {
                        again = true;
                    }
                });
            });
            ui.separator();
            egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                if let Some(r) = &report {
                    for g in &r.groups {
                        card().show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            ui.label(RichText::new(&g.title).font(bold(15.5)));
                            for c in &g.checks {
                                diag_row(ui, c);
                            }
                        });
                    }
                    ui.label(RichText::new("언제 끊겼는지 같은 지난 기록은 [기록 보기 (웹)]의 세션 상세·보고서에서 봅니다.").color(MUTED).small());
                }
            });
        });
        if again {
            self.send(Cmd::Diagnose);
        }
        self.show_diag = open;
    }

    /// 카메라별로 '화면 표시' 항목을 덧붙인다: 받은 만큼 화면에 그리고 있는지 (이 PC가 영상을 푸는 속도)
    fn with_screen_checks(&self, mut r: DiagReport, st: &Status) -> DiagReport {
        for g in &mut r.groups {
            let Some(cam) = g.camera_id.as_ref().and_then(|id| st.cameras.iter().find(|c| &c.id == id)) else { continue };
            let Some(l) = &cam.live else { continue };
            let shown = self.shown.get(&cam.id).map(|s| s.2).filter(|_| self.videos.contains_key(&cam.id));
            let waiting = l.state == "receiving" && l.elapsed_s > 10.0;
            let c = match shown {
                Some(f) if l.fps > 5.0 && (f as f64) < l.fps * 0.8 && f > 0.0 => DiagCheck {
                    title: "화면 표시".into(),
                    level: "warn".into(),
                    summary: format!("초당 {f:.0}장만 화면에 그리고 있습니다 (수신은 초당 {:.0}장)", l.fps),
                    advice: vec!["이 PC가 영상을 푸는 속도가 모자랍니다. 설정 → 화면에서 '화면용 크기'를 줄이고 GPU 사용이 켜져 있는지 확인하세요. 수신·녹화·촬영에는 영향이 없습니다".into()],
                },
                Some(f) if f > 0.0 => DiagCheck { title: "화면 표시".into(), level: "ok".into(), summary: format!("초당 {f:.0}장을 화면에 그리고 있습니다"), advice: vec![] },
                _ => DiagCheck {
                    title: "화면 표시".into(),
                    level: if waiting { "warn".into() } else { "info".into() },
                    summary: "아직 화면에 영상이 나오지 않았습니다".into(),
                    advice: if waiting { vec!["영상은 받고 있는데 화면에 못 그리고 있습니다. 설정 → 화면에서 GPU 사용을 껐다 켜 보세요".into()] } else { vec![] },
                },
            };
            g.checks.push(c);
        }
        r
    }
}

fn open_form(c: &CamEdit, saved: u64) -> CamForm {
    CamForm {
        id: Some(c.id.clone()),
        name: c.name.clone(),
        host: c.host.clone(),
        user: c.username.clone(),
        main: c.rtsp_main.clone(),
        sub: c.rtsp_sub.clone(),
        trigger: c.trigger,
        has_password: c.has_password,
        applied: true,
        mark: saved,
        ..Default::default()
    }
}

/// 연결 테스트 결과 (엔진이 준 그대로 풀어서 보여 준다)
fn test_result(ui: &mut egui::Ui, r: &Value) {
    let ok = r["ok"].as_bool() == Some(true);
    card().show(ui, |ui| {
        ui.set_width(ui.available_width());
        ui.label(
            RichText::new(if ok { "연결 성공 · 저장하면 바로 수신할 수 있습니다".to_string() } else { r["error"].as_str().unwrap_or("연결 실패").to_string() })
                .font(bold(15.0))
                .color(if ok { GOOD } else { BAD }),
        );
        let maker = [r["manufacturer"].as_str(), r["model"].as_str()].into_iter().flatten().collect::<Vec<_>>().join(" ");
        if !maker.is_empty() {
            ui.label(RichText::new(format!("{maker}{}", r["firmware"].as_str().map(|f| format!(" · 펌웨어 {f}")).unwrap_or_default())).color(MUTED).small());
        }
        for (label, key) in [("메인", "main"), ("서브", "sub")] {
            let c = &r[key];
            if c.is_null() {
                continue;
            }
            let good = c["ok"].as_bool() == Some(true);
            let size = match (c["width"].as_u64(), c["height"].as_u64()) {
                (Some(w), Some(h)) => format!("{w}×{h}"),
                _ => "—".into(),
            };
            let line = format!(
                "{label}: {} · {size} · {}fps · {}",
                c["codec"].as_str().map_or("—".into(), |s| s.to_uppercase()),
                c["fps"].as_f64().map_or("—".into(), |f| format!("{f:.0}")),
                if good { "수신 확인".to_string() } else { c["error"].as_str().unwrap_or("실패").to_string() }
            );
            ui.label(RichText::new(line).color(if good { TEXT } else { BAD }));
        }
        if let Some(steps) = r["steps"].as_array() {
            for s in steps {
                let (mark, col) = if s["warn"].as_bool() == Some(true) { ("!", WARN) } else if s["ok"].as_bool() == Some(true) { ("✔", GOOD) } else { ("✖", BAD) };
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new(mark).color(col));
                    ui.label(RichText::new(s["name"].as_str().unwrap_or("")).font(bold(13.5)));
                    ui.label(RichText::new(s["detail"].as_str().unwrap_or("")).color(MUTED).small());
                });
            }
        }
        let d = &r["diagnosis"];
        if !d.is_null() {
            ui.separator();
            ui.label(RichText::new(d["summary"].as_str().unwrap_or("")).color(WARN));
            if let Some(adv) = d["advice"].as_array() {
                for a in adv {
                    ui.label(RichText::new(format!("· {}", a.as_str().unwrap_or(""))).color(MUTED).small());
                }
            }
        }
    });
}

impl eframe::App for ViewerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.fit_to_screen(ctx);
        let (status, error) = {
            let i = self.shared.inner.lock().unwrap();
            (i.status.clone(), i.error.clone())
        };
        self.engine_ok = error.is_none();
        if self.prefs != self.saved_prefs {
            self.prefs.save(&self.home);
            self.saved_prefs = self.prefs.clone();
        }
        let Some(st) = status else {
            self.videos.clear();
            self.tex.clear();
            self.e2e = None;
            egui::CentralPanel::default().frame(egui::Frame::none().fill(BG)).show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(140.0);
                    ui.horizontal(|ui| {
                        ui.add_space(ui.available_width() / 2.0 - 150.0);
                        ui.label(RichText::new("Capture VMS").font(bold(28.0)).color(ACCENT));
                        ui.label(RichText::new("Viewer").font(bold(28.0)));
                    });
                    ui.add_space(10.0);
                    ui.label(RichText::new(error.unwrap_or_else(|| "엔진에 연결하는 중입니다…".into())).color(MUTED));
                });
            });
            ctx.request_repaint_after(Duration::from_millis(300));
            return;
        };
        self.sync_videos(ctx, &st);
        if self.e2e.is_some() {
            self.e2e_screen(ctx, &st);
            return;
        }
        if !ctx.wants_keyboard_input() && !self.show_settings && ctx.input(|i| i.key_pressed(egui::Key::Space)) && !st.cameras.is_empty() {
            self.send(Cmd::Trigger);
        }
        self.top_bar(ctx, &st);
        self.bottom_bar(ctx, &st);
        self.center(ctx, &st);
        if self.show_settings {
            self.settings_window(ctx, &st);
        }
        if self.show_diag {
            self.diag_window(ctx, &st);
        }
        // 영상이 없어도 수치·안내가 갱신되게
        ctx.request_repaint_after(Duration::from_millis(250));
    }
}
