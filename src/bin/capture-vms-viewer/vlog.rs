//! 뷰어 로그(viewer.YYYY-MM-DD.log).
//!
//! 엔진의 기록 폴더에 같이 둔다. 화면이 끊겨 보였을 때 뷰어 쪽에서 무슨 일이 있었는지(엔진에 닿았는지, 화면용 디코더가
//! 언제 열리고 닫혔는지, GPU로 풀었는지, 화면이 수신을 못 따라갔는지)를 나중에 기록만 보고 알 수 있게 한다.
//! 엔진의 기록(수신·녹화)과 시각을 맞춰 볼 수 있게 같은 현지 시각으로 적는다.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

use chrono::{Duration, Local, NaiveDate};

/// 이보다 오래된 뷰어 로그는 지운다
const KEEP_DAYS: i64 = 14;
/// 기록 폴더를 알기 전(엔진에 처음 닿기 전)에 생긴 줄은 이만큼까지 들고 있다가 쓴다
const PENDING_MAX: usize = 200;

static STATE: Mutex<(Option<PathBuf>, Vec<String>)> = Mutex::new((None, Vec::new()));

fn append(dir: &Path, lines: &[String]) {
    // 화면 스레드·디코더 스레드·통신 스레드가 같이 쓴다. 한 번에 하나씩, 줄과 줄바꿈을 한 번의 쓰기로 넣어 줄이 섞이지 않게 한다
    static WRITE: Mutex<()> = Mutex::new(());
    let _one = WRITE.lock().unwrap();
    let path = dir.join(format!("viewer.{}.log", Local::now().format("%Y-%m-%d")));
    let _ = fs::create_dir_all(dir);
    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
        let _ = f.write_all(text.as_bytes());
    }
}

/// "viewer.2026-11-14.log" → 날짜
fn log_date(name: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(name.strip_prefix("viewer.")?.strip_suffix(".log")?, "%Y-%m-%d").ok()
}

fn cleanup(dir: &Path) {
    let cutoff = Local::now().date_naive() - Duration::days(KEEP_DAYS);
    let Ok(rd) = fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        if log_date(&e.file_name().to_string_lossy()).is_some_and(|d| d < cutoff) {
            let _ = fs::remove_file(e.path());
        }
    }
}

/// 기록 폴더를 알려 준다 (엔진 상태에서 읽은 값). 바뀌었을 때만 처리한다
pub fn set_dir(dir: &str) {
    if dir.is_empty() {
        return;
    }
    let dir = PathBuf::from(dir);
    let pending = {
        let mut st = STATE.lock().unwrap();
        if st.0.as_ref() == Some(&dir) {
            return;
        }
        st.0 = Some(dir.clone());
        std::mem::take(&mut st.1)
    };
    cleanup(&dir);
    append(&dir, &pending);
}

/// 한 줄 남긴다
pub fn log(msg: impl AsRef<str>) {
    let line = format!("{} {}", Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"), msg.as_ref());
    let dir = {
        let mut st = STATE.lock().unwrap();
        match &st.0 {
            Some(d) => d.clone(),
            None => {
                if st.1.len() < PENDING_MAX {
                    st.1.push(line);
                }
                return;
            }
        }
    };
    append(&dir, &[line]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_date_from_log_name() {
        assert_eq!(log_date("viewer.2026-11-14.log"), NaiveDate::from_ymd_opt(2026, 11, 14));
        // 엔진의 로그나 다른 파일은 건드리지 않는다
        assert_eq!(log_date("app.2026-11-14.log"), None);
        assert_eq!(log_date("events-2026-11-14.jsonl"), None);
    }
}
