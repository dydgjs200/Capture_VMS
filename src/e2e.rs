//! 실제 지연(카메라 촬영 → PC 수신) 측정.
//!
//! 대시보드가 화면의 사각형을 흑↔백으로 바꾸고 그 시각을 알려 주면, 카메라 영상(64×48 흑백으로 줄인 것)의
//! 평균 밝기가 바뀌는 순간을 찾아 그 차이를 지연으로 본다. 화면과 수신이 같은 PC 시계를 쓰므로
//! 카메라 시계를 맞출 필요가 없다. 화면 표시 지연(모니터·브라우저, 보통 10~30ms)이 포함된다.

use std::collections::VecDeque;

use chrono::Local;
use serde::Serialize;

use crate::store::E2eSummary;

/// 밝기 범위(가장 어두움~밝음)를 잡는 구간. 깜빡임 두 번 이상이 들어가야 한다.
const LEVEL_WINDOW_MS: f64 = 8_000.0;
/// 바뀐 시각을 같은 색 표시와 짝짓는 최대 지연. 같은 색 표시 간격(깜빡임 최소 간격 2초 × 2)보다 짧아야 헷갈리지 않는다.
pub const MATCH_WINDOW_MS: f64 = 3_900.0;
/// 이보다 밝기 차이가 작으면 카메라가 사각형을 제대로 못 보는 것으로 본다 (0~255)
const MIN_RANGE: f32 = 6.0;
/// 잡음에 흔들리지 않도록 가운데에서 이만큼(범위 비율) 넘어가야 바뀐 것으로 본다
const HYSTERESIS: f32 = 0.12;
const MAX_SAMPLES: usize = 1_000;

struct Mark {
    t: f64,
    white: bool,
    matched: bool,
}

#[derive(Default)]
pub struct E2e {
    pub active: bool,
    marks: VecDeque<Mark>,
    lum: VecDeque<(f64, f32)>,
    side: Option<bool>,
    samples: Vec<f64>,
    last_lum: Option<f32>,
    last_frame_t: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct E2eLive {
    pub active: bool,
    /// 밝기 신호가 들어오고 있는지 (흑백 분석 출력이 살아 있는지)
    pub frames_ok: bool,
    /// 최근 8초 밝기 차이 (0~255). 클수록 카메라가 사각형을 잘 보고 있다
    pub signal: f32,
    pub signal_ok: bool,
    pub lum: Option<f32>,
    pub count: usize,
    pub last_ms: Option<f64>,
    pub p50_ms: Option<f64>,
    pub min_ms: Option<f64>,
    pub max_ms: Option<f64>,
    /// 최근 측정값 (오래된 것 → 최근)
    pub recent: Vec<f64>,
}

pub fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}

impl E2e {
    pub fn start(&mut self) {
        self.active = true;
        self.marks.clear();
        self.samples.clear();
        self.side = None;
    }

    /// 대시보드가 사각형 색을 바꾼 시각 (PC 시계, ms)
    pub fn mark(&mut self, t: f64, white: bool) {
        if !self.active {
            return;
        }
        self.marks.push_back(Mark { t, white, matched: false });
        while self.marks.len() > 16 {
            self.marks.pop_front();
        }
    }

    fn range(&self) -> (f32, f32) {
        self.lum.iter().fold((f32::MAX, f32::MIN), |(lo, hi), (_, v)| (lo.min(*v), hi.max(*v)))
    }

    /// 흑백 프레임 하나가 도착: 밝기가 바뀌었고 짝이 맞는 표시가 있으면 지연(ms)을 돌려준다
    pub fn on_frame(&mut self, t: f64, lum: f32) -> Option<f64> {
        self.last_frame_t = Some(t);
        self.last_lum = Some(lum);
        self.lum.push_back((t, lum));
        while self.lum.front().map_or(false, |(ft, _)| *ft < t - LEVEL_WINDOW_MS) {
            self.lum.pop_front();
        }
        if !self.active {
            return None;
        }
        let (lo, hi) = self.range();
        let range = hi - lo;
        if range < MIN_RANGE {
            self.side = None;
            return None;
        }
        let mid = (lo + hi) / 2.0;
        let band = HYSTERESIS * range;
        let new_side = match self.side {
            None => lum > mid,
            Some(true) => lum >= mid - band,
            Some(false) => lum > mid + band,
        };
        let prev = self.side.replace(new_side);
        if prev.is_none() || prev == Some(new_side) {
            return None;
        }
        let m = self
            .marks
            .iter_mut()
            .rev()
            .find(|m| m.white == new_side && !m.matched && t >= m.t && t - m.t <= MATCH_WINDOW_MS)?;
        m.matched = true;
        let lat = t - m.t;
        if self.samples.len() < MAX_SAMPLES {
            self.samples.push(lat);
        }
        Some(lat)
    }

    pub fn live(&self) -> E2eLive {
        let (lo, hi) = self.range();
        let signal = if self.lum.is_empty() { 0.0 } else { hi - lo };
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        E2eLive {
            active: self.active,
            frames_ok: self.last_frame_t.map_or(false, |t| now_ms() - t < 2_000.0),
            signal,
            signal_ok: signal >= MIN_RANGE,
            lum: self.last_lum,
            count: self.samples.len(),
            last_ms: self.samples.last().copied(),
            p50_ms: median(&sorted),
            min_ms: sorted.first().copied(),
            max_ms: sorted.last().copied(),
            recent: self.samples.iter().rev().take(30).rev().copied().collect(),
        }
    }

    /// 측정을 끝내고 요약 (측정값이 없으면 None)
    pub fn finish(&mut self) -> Option<E2eSummary> {
        self.active = false;
        self.marks.clear();
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        Some(E2eSummary {
            count: sorted.len(),
            avg_ms: sorted.iter().sum::<f64>() / sorted.len() as f64,
            p50_ms: median(&sorted).unwrap_or(0.0),
            min_ms: sorted[0],
            max_ms: sorted[sorted.len() - 1],
            measured_at: Some(Local::now()),
        })
    }
}

fn median(sorted: &[f64]) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let n = sorted.len();
    Some(if n % 2 == 1 { sorted[n / 2] } else { (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2.5초마다 흑백이 바뀌는 화면을 카메라가 230ms 늦게, 30fps로, 밝기가 두 프레임에 걸쳐 바뀌며 보는 경우
    #[test]
    fn measures_flash_latency() {
        let mut e = E2e::default();
        e.start();
        let latency = 230.0;
        let flips: Vec<f64> = (1..=8).map(|i| 1_000.0 + i as f64 * 2_500.0).collect();
        let color_at = |t: f64| flips.iter().filter(|f| **f <= t).count() % 2 == 1; // 처음은 검정
        let mut got = Vec::new();
        let mut next_flip = 0;
        let mut t = 0.0;
        while t < 23_000.0 {
            while next_flip < flips.len() && flips[next_flip] <= t {
                e.mark(flips[next_flip], next_flip % 2 == 0);
                next_flip += 1;
            }
            // 카메라가 본 화면: latency 전의 색, 바뀌는 순간 한 프레임은 중간 밝기
            let now_white = color_at(t - latency);
            let before_white = color_at(t - latency - 33.3);
            let base = if now_white { 200.0 } else { 30.0 };
            let lum = if now_white != before_white { 115.0 } else { base } + ((t / 7.0).sin() * 2.0) as f32 as f64;
            if let Some(l) = e.on_frame(t, lum as f32) {
                got.push(l);
            }
            t += 1000.0 / 30.0;
        }
        assert!(got.len() >= 6, "측정 {got:?}");
        for l in &got {
            // 30fps 한 프레임(33ms) + 중간 밝기 한 프레임만큼의 오차 안
            assert!((*l - latency).abs() <= 70.0, "측정 {l} (기대 {latency})");
        }
        let s = e.finish().unwrap();
        assert_eq!(s.count, got.len());
    }

    #[test]
    fn ignores_changes_without_marks() {
        let mut e = E2e::default();
        e.start();
        let mut t = 0.0;
        let mut n = 0;
        while t < 10_000.0 {
            // 사람이 지나가듯 밝기가 바뀌지만 화면 표시는 없음
            let lum = if (t as i64 / 3000) % 2 == 0 { 40.0 } else { 160.0 };
            if e.on_frame(t, lum).is_some() {
                n += 1;
            }
            t += 33.3;
        }
        assert_eq!(n, 0);
    }
}
