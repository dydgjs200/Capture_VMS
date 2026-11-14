//! 카메라 한 대의 화면: 엔진이 넘겨주는 원본 영상(MPEG-TS)을 같이 배포하는 FFmpeg로 풀어 RGBA 프레임으로 받는다.
//!
//! 풀기는 뷰어 쪽 프로세스에서만 한다 (수신·녹화 쪽은 영상을 풀지 않는다). 화면에는 항상 가장 최근 프레임만 그리므로
//! 처음 받을 때 밀려 있던 분량이나 잠깐 느려진 뒤의 분량은 건너뛰고 지금 영상을 따라간다.

use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use eframe::egui;

use crate::e2e;

#[derive(Default)]
pub struct Frame {
    /// 0이면 아직 한 장도 없음
    pub seq: u64,
    pub rgba: Vec<u8>,
}

#[derive(Default)]
pub struct VideoShared {
    pub frame: Mutex<Frame>,
    /// 실제 지연 측정용: 프레임마다 (화면에 그릴 수 있게 된 시각 ms, 평균 밝기)
    pub lum: Mutex<VecDeque<(f64, f32)>>,
    pub frames: AtomicU64,
    /// GPU로 풀라고 했지만 이 PC의 GPU로는 풀리지 않아 CPU로 풀고 있다
    pub cpu_fallback: AtomicBool,
    stop: AtomicBool,
    child: Mutex<Option<Child>>,
}

pub struct Video {
    /// 이 값이 달라지면(세션·크기·디코딩 방식) 새로 연다
    pub key: String,
    pub w: usize,
    pub h: usize,
    pub shared: Arc<VideoShared>,
    thread: Option<JoinHandle<()>>,
}

pub struct Open {
    pub key: String,
    pub ffmpeg: PathBuf,
    pub url: String,
    pub w: usize,
    pub h: usize,
    /// GPU(d3d11va)로 풀기. 끄면 CPU
    pub hwaccel: bool,
}

impl Video {
    pub fn start(o: Open, ctx: egui::Context) -> Self {
        let shared = Arc::new(VideoShared::default());
        let (s, key, w, h) = (shared.clone(), o.key.clone(), o.w, o.h);
        let thread = std::thread::Builder::new().name("video".into()).spawn(move || run(o, s, ctx)).ok();
        Self { key, w, h, shared, thread }
    }
}

impl Drop for Video {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        if let Some(c) = self.shared.child.lock().unwrap().as_mut() {
            let _ = c.kill();
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// CPU로 풀 때 쓰는 스레드 수. 스레드 수 − 1장만큼 늦게 나온다 (3개면 60fps에서 약 33ms).
/// HEVC 2048×1536 60fps를 개발 PC에서 풀어 본 속도: 1개 230fps · 2개 430fps · 3개 570fps · 4개 600fps
const CPU_THREADS: &str = "3";

fn spawn_decoder(o: &Open, gpu: bool) -> std::io::Result<Child> {
    let mut cmd = Command::new(&o.ffmpeg);
    // 처음 받는 분량(최근 키프레임부터)을 버리지 않아야 바로 그림이 나온다 → nobuffer는 쓰지 않고 분석만 짧게.
    // discardcorrupt: 엔진이 밀린 분량을 건너뛴 자리의 잘린 프레임은 버린다 (바로 뒤가 키프레임이라 화면은 이어진다)
    cmd.args(["-hide_banner", "-fflags", "+discardcorrupt", "-probesize", "131072", "-analyzeduration", "0"]);
    if gpu {
        // GPU: 받는 대로 바로 내보낸다. 실제로 GPU가 푸는지는 로그로 확인한다 (`watch_log`)
        cmd.args(["-loglevel", "verbose", "-flags", "low_delay", "-hwaccel", "d3d11va"]);
    } else {
        // CPU: 프레임 단위로 여러 코어에 나눠 푼다. low_delay를 주면 FFmpeg가 이 병렬 처리를 꺼서
        // 스레드 수를 얼마로 주든 코어 1개만 쓰고, 느린 PC에서는 수신 속도를 못 따라가 화면이 계속 밀린다
        cmd.args(["-loglevel", "error", "-threads", CPU_THREADS]);
    }
    cmd.args(["-f", "mpegts", "-i", &o.url, "-map", "0:v:0", "-vf"])
        .arg(format!("scale={}:{}:flags=fast_bilinear,format=rgba", o.w, o.h))
        .args(["-fps_mode", "passthrough", "-f", "rawvideo", "pipe:1"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        // 로그는 GPU 확인용으로만 읽는다. 읽지 않을 때는 받지 않는다 (안 읽는 파이프가 꽉 차면 FFmpeg가 멈춘다)
        .stderr(if gpu { Stdio::piped() } else { Stdio::null() });
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // 콘솔 창 띄우지 않기
    }
    cmd.spawn()
}

/// 화면 가운데 부분의 평균 밝기 (0~255). 4픽셀마다 하나씩만 본다
fn mean_luma(rgba: &[u8], w: usize, h: usize) -> f32 {
    let (mut sum, mut n) = (0u64, 0u64);
    for y in (h / 8..h * 7 / 8).step_by(4) {
        let row = &rgba[y * w * 4..(y + 1) * w * 4];
        for x in (w / 8..w * 7 / 8).step_by(4) {
            let p = &row[x * 4..x * 4 + 3];
            sum += (p[0] as u64 * 2 + p[1] as u64 * 5 + p[2] as u64) / 8;
            n += 1;
        }
    }
    if n == 0 { 0.0 } else { sum as f32 / n as f32 }
}

/// 디코딩 방식 (FFmpeg 로그에서 읽어 낸 것)
const DEC_UNKNOWN: u8 = 0;
const DEC_GPU: u8 = 1;
const DEC_CPU: u8 = 2;

/// 로그의 "… input from stream 0:0 … pixfmt:nv12 …" 줄로 GPU가 푸는지 알아낸다.
/// d3d11va로 푼 프레임은 nv12(10비트는 p010)로 내려오고, CPU로 푼 프레임은 yuv420p 계열이다.
/// GPU가 이 영상을 못 풀면 FFmpeg는 아무 경고 없이 CPU로 풀기 때문에 이 줄 말고는 알 방법이 없다.
fn decoder_kind(line: &str) -> Option<u8> {
    if !line.contains("input from stream") {
        return None;
    }
    let fmt = line.split("pixfmt:").nth(1)?.split_whitespace().next()?;
    Some(if fmt.starts_with("nv12") || fmt.starts_with("p01") || fmt.starts_with("d3d11") { DEC_GPU } else { DEC_CPU })
}

/// FFmpeg 로그를 끝까지 읽으며(안 읽으면 FFmpeg가 멈춘다) 디코딩 방식을 `kind`에 적는다
fn watch_log(err: impl Read, kind: Arc<AtomicU8>) {
    let mut r = BufReader::new(err);
    let mut line = Vec::new();
    loop {
        line.clear();
        match r.read_until(b'\n', &mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if let Some(k) = decoder_kind(&String::from_utf8_lossy(&line)) {
            kind.store(k, Ordering::Relaxed);
        }
    }
}

fn run(o: Open, s: Arc<VideoShared>, ctx: egui::Context) {
    let size = o.w * o.h * 4;
    let mut buf = vec![0u8; size];
    // GPU로 풀라고 했어도 이 PC에서 안 되면 CPU로 바꾼다
    let mut gpu = o.hwaccel;
    // GPU 방식으로 한 장도 못 얻고 끝난 연속 횟수, 그래서 CPU를 시험해 보는 중인지
    let (mut gpu_empty, mut cpu_trial) = (0u32, false);
    while !s.stop.load(Ordering::SeqCst) {
        let kind = Arc::new(AtomicU8::new(DEC_UNKNOWN));
        let (mut got, mut not_gpu) = (0u64, false);
        crate::vlog::log(format!("화면 디코더 시작 [{}] {}", o.key, if gpu { "GPU" } else { "CPU" }));
        let (mut out, log) = match spawn_decoder(&o, gpu) {
            Ok(mut c) => {
                let out = c.stdout.take();
                let log = c.stderr.take().and_then(|e| {
                    let k = kind.clone();
                    std::thread::Builder::new().name("video-log".into()).spawn(move || watch_log(e, k)).ok()
                });
                *s.child.lock().unwrap() = Some(c);
                (out, log)
            }
            Err(_) => (None, None),
        };
        if let Some(out) = out.as_mut() {
            while !s.stop.load(Ordering::SeqCst) && out.read_exact(&mut buf).is_ok() {
                got += 1;
                if gpu && kind.load(Ordering::Relaxed) == DEC_CPU {
                    // GPU로 푸는 줄 알았는데 CPU(코어 1개)로 풀리고 있다 → CPU 방식으로 다시 연다
                    not_gpu = true;
                    break;
                }
                let t = e2e::now_ms();
                let lum = mean_luma(&buf, o.w, o.h);
                {
                    let mut q = s.lum.lock().unwrap();
                    q.push_back((t, lum));
                    while q.len() > 600 {
                        q.pop_front();
                    }
                }
                {
                    let mut f = s.frame.lock().unwrap();
                    if f.rgba.len() != size {
                        f.rgba = vec![0u8; size];
                    }
                    std::mem::swap(&mut f.rgba, &mut buf);
                    f.seq += 1;
                }
                s.frames.fetch_add(1, Ordering::Relaxed);
                ctx.request_repaint();
            }
        }
        if let Some(mut c) = s.child.lock().unwrap().take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        if let Some(l) = log {
            let _ = l.join();
        }
        if !s.stop.load(Ordering::SeqCst) {
            crate::vlog::log(format!("화면 디코더 끝 [{}] 프레임 {got}장{}", o.key, if not_gpu { " · GPU로 풀리지 않아 CPU로 바꿈" } else { "" }));
        }
        if gpu {
            if not_gpu {
                (gpu, cpu_trial) = (false, false);
                s.cpu_fallback.store(true, Ordering::Relaxed);
                continue;
            }
            // GPU 장치를 못 여는 PC에서는 FFmpeg가 한 장도 못 내고 끝난다 (영상이 안 오는 것과 구분이 안 되므로 CPU로 시험해 본다)
            gpu_empty = if got == 0 { gpu_empty + 1 } else { 0 };
            if gpu_empty >= 3 {
                crate::vlog::log(format!("GPU 방식으로 3번 연속 화면이 나오지 않아 CPU로 시험 [{}]", o.key));
                (gpu, cpu_trial, gpu_empty) = (false, true, 0);
            }
        } else if cpu_trial {
            cpu_trial = false;
            if got > 0 {
                s.cpu_fallback.store(true, Ordering::Relaxed);
            } else {
                // CPU로도 안 나온다 = 영상이 안 오는 것 → 원래대로
                gpu = true;
            }
        }
        // 세션이 끝났거나 잠깐 끊긴 것: 잠시 뒤 다시 붙는다 (세션이 없어지면 뷰어가 이 화면을 닫는다).
        // 영상이 나오다 끊긴 것이면 금방, 한 장도 못 받았으면 조금 더 쉬었다가
        for _ in 0..if got > 0 { 2 } else { 7 } {
            if s.stop.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_decoder_kind_from_ffmpeg_log() {
        let gpu = "[graph -1 input from stream 0:0 @ 000001efd9a8f000] w:2048 h:1536 pixfmt:nv12 tb:1/90000 fr:60/1 sar:1/1 csp:unknown range:tv alpha:unspecified";
        let cpu = "[graph -1 input from stream 0:0 @ 0000013a6e7400c0] w:2048 h:1536 pixfmt:yuv420p tb:1/90000 fr:60/1 sar:1/1";
        assert_eq!(decoder_kind(gpu), Some(DEC_GPU));
        assert_eq!(decoder_kind(cpu), Some(DEC_CPU));
        assert_eq!(decoder_kind(&gpu.replace("nv12", "p010le")), Some(DEC_GPU));
        // 다른 줄에는 반응하지 않는다
        assert_eq!(decoder_kind("[vist#0:0/hevc @ 0] Selecting decoder 'hevc' because of requested hwaccel method d3d11va"), None);
    }
}
