//! 탐색기로 폴더를 열거나 파일 위치를 보여 준다 (웹 대시보드의 [폴더 열기]).
//!
//! 두 가지를 챙긴다.
//! 1. `explorer /select,경로`는 탐색기가 명령줄을 스스로 풀기 때문에 **경로만** 따옴표로 감싸야 한다. 인자 전체를
//!    감싸면(`"/select,C:\Program Files\a.jpg"` — 경로에 공백이 있을 때 Rust가 자동으로 그렇게 한다) 탐색기가
//!    알아듣지 못하고 '문서' 폴더를 연다. 설치 폴더 이름(`Capture VMS`)에 공백이 있어 설치된 PC에서는 늘 그랬다.
//! 2. 버튼을 누른 곳은 브라우저인데 탐색기를 띄우는 것은 창 없는 엔진이다. Windows는 지금 쓰고 있지 않은 프로그램이
//!    띄운 창을 앞으로 내주지 않아서(작업 표시줄만 깜빡임) 폴더 창이 브라우저 뒤에 열린다 → 새로 뜬 폴더 창을
//!    찾아 맨 위로 올린다.

use std::{io, path::Path, process::Command};

/// 폴더를 탐색기로 연다.
pub fn folder(dir: &Path) -> io::Result<()> {
    let before = raise::snapshot();
    Command::new("explorer").arg(dir).spawn()?;
    raise::new_window_later(before);
    Ok(())
}

/// 파일이 든 폴더를 열고 그 파일을 선택해 둔다.
pub fn select(file: &Path) -> io::Result<()> {
    let before = raise::snapshot();
    let mut cmd = Command::new("explorer");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.raw_arg(select_arg(file));
    }
    #[cfg(not(windows))]
    cmd.arg(select_arg(file));
    cmd.spawn()?;
    raise::new_window_later(before);
    Ok(())
}

/// 연결된 프로그램(엑셀·PDF 보기 등)으로 파일을 연다.
pub fn file(path: &Path) -> io::Result<()> {
    Command::new("explorer").arg(path).spawn().map(|_| ())
}

/// `/select,"경로"` — 경로만 따옴표로 감싼다 (Windows 경로에는 따옴표가 들어갈 수 없다).
fn select_arg(file: &Path) -> String {
    format!("/select,\"{}\"", file.display())
}

/// 폴더 창 하나: 창 핸들과 제목 (제목은 지금 보이는 탭의 폴더 이름).
type Win = (isize, String);

/// 탐색기를 띄운 뒤 앞으로 올릴 창을 고른다: 새로 생긴 폴더 창, 없으면 제목이 바뀐 창(있던 창에 새 탭으로 열린 경우).
fn pick(before: &[Win], now: &[Win]) -> Option<isize> {
    if let Some((h, _)) = now.iter().find(|(h, _)| !before.iter().any(|(b, _)| b == h)) {
        return Some(*h);
    }
    now.iter().find(|(h, t)| before.iter().any(|(b, bt)| b == h && bt != t)).map(|(h, _)| *h)
}

#[cfg(windows)]
mod raise {
    use std::{ptr::null_mut, thread, time::Duration};

    use windows_sys::Win32::{
        Foundation::{BOOL, FALSE, HWND, LPARAM, TRUE},
        System::Threading::{AttachThreadInput, GetCurrentThreadId},
        UI::WindowsAndMessaging::{
            BringWindowToTop, EnumWindows, GetClassNameW, GetForegroundWindow, GetWindowTextW, GetWindowThreadProcessId, IsIconic,
            IsWindowVisible, PeekMessageW, SetForegroundWindow, SetWindowPos, ShowWindow, HWND_NOTOPMOST, HWND_TOPMOST, MSG, PM_NOREMOVE,
            SWP_NOMOVE, SWP_NOSIZE, SWP_SHOWWINDOW, SW_RESTORE,
        },
    };

    use super::{pick, Win};

    /// 새 창을 기다리는 시간 (느린 PC에서 탐색기가 처음 뜰 때를 넉넉히 본다).
    const WAIT: Duration = Duration::from_secs(6);
    const STEP: Duration = Duration::from_millis(100);

    unsafe extern "system" fn each(h: HWND, l: LPARAM) -> BOOL {
        let list = &mut *(l as *mut Vec<Win>);
        if IsWindowVisible(h) != 0 {
            let mut class = [0u16; 64];
            let n = GetClassNameW(h, class.as_mut_ptr(), class.len() as i32).max(0) as usize;
            // 탐색기의 폴더 창
            if matches!(String::from_utf16_lossy(&class[..n]).as_str(), "CabinetWClass" | "ExploreWClass") {
                let mut title = [0u16; 260];
                let t = GetWindowTextW(h, title.as_mut_ptr(), title.len() as i32).max(0) as usize;
                list.push((h as isize, String::from_utf16_lossy(&title[..t])));
            }
        }
        TRUE
    }

    /// 지금 열려 있는 폴더 창들 (위에 있는 것부터).
    pub fn snapshot() -> Vec<Win> {
        let mut list: Vec<Win> = Vec::new();
        unsafe { EnumWindows(Some(each), &mut list as *mut Vec<Win> as LPARAM) };
        list
    }

    /// 탐색기가 창을 띄울 때까지 기다렸다가 그 창을 맨 위로 올린다 (부른 쪽은 기다리지 않는다).
    pub fn new_window_later(before: Vec<Win>) {
        let _ = thread::Builder::new().name("raise-explorer".into()).spawn(move || {
            let mut waited = Duration::ZERO;
            while waited < WAIT {
                thread::sleep(STEP);
                waited += STEP;
                if let Some(h) = pick(&before, &snapshot()) {
                    // 창이 다 그려지기 전에 올리면 다시 뒤로 가는 일이 있어 잠깐 뒤에 한 번 더 올린다
                    to_front(h as HWND);
                    thread::sleep(Duration::from_millis(250));
                    to_front(h as HWND);
                    return;
                }
            }
            tracing::debug!("탐색기 창을 찾지 못해 앞으로 올리지 못했습니다");
        });
    }

    fn to_front(h: HWND) {
        unsafe {
            if IsIconic(h) != 0 {
                ShowWindow(h, SW_RESTORE);
            }
            // '항상 위'로 올렸다가 바로 풀면 창 순서의 맨 위에 남는다 — 다른 프로그램이 앞에 있어도 된다
            let flags = SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW;
            SetWindowPos(h, HWND_TOPMOST, 0, 0, 0, 0, flags);
            SetWindowPos(h, HWND_NOTOPMOST, 0, 0, 0, 0, flags);
            // 키보드 입력도 그 창으로 넘겨 본다. 안 돼도 창은 이미 맨 위에 보인다
            let mut msg: MSG = std::mem::zeroed();
            PeekMessageW(&mut msg, null_mut(), 0, 0, PM_NOREMOVE); // 이 스레드에 메시지 대기열을 만든다
            let me = GetCurrentThreadId();
            let front = GetForegroundWindow();
            let other = if front.is_null() { 0 } else { GetWindowThreadProcessId(front, null_mut()) };
            let attached = other != 0 && other != me && AttachThreadInput(me, other, TRUE) != 0;
            SetForegroundWindow(h);
            BringWindowToTop(h);
            if attached {
                AttachThreadInput(me, other, FALSE);
            }
        }
    }
}

#[cfg(not(windows))]
mod raise {
    use super::Win;

    pub fn snapshot() -> Vec<Win> {
        Vec::new()
    }

    pub fn new_window_later(_before: Vec<Win>) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_quotes_only_the_path() {
        let p = Path::new(r"C:\Users\user\AppData\Local\Programs\Capture VMS\data\recordings\20261114\captures\a_메인.jpg");
        assert_eq!(select_arg(p), r#"/select,"C:\Users\user\AppData\Local\Programs\Capture VMS\data\recordings\20261114\captures\a_메인.jpg""#);
        // 인자 전체를 감싼 꼴(탐색기가 '문서'를 여는 꼴)이 아니어야 한다
        assert!(!select_arg(p).starts_with('"'));
    }

    fn w(h: isize, t: &str) -> Win {
        (h, t.to_string())
    }

    #[test]
    fn raises_the_window_that_just_appeared() {
        let before = [w(1, "문서"), w(2, "다운로드")];
        assert_eq!(pick(&before, &[w(3, "recordings"), w(1, "문서"), w(2, "다운로드")]), Some(3));
    }

    #[test]
    fn raises_the_window_that_got_a_new_tab() {
        // 새 창이 아니라 있던 창의 새 탭으로 열리면 그 창의 제목이 바뀐다
        let before = [w(1, "문서"), w(2, "다운로드")];
        assert_eq!(pick(&before, &[w(1, "문서"), w(2, "recordings")]), Some(2));
    }

    #[test]
    fn leaves_other_windows_alone_until_something_changes() {
        let before = [w(1, "문서"), w(2, "다운로드")];
        assert_eq!(pick(&before, &[w(2, "다운로드"), w(1, "문서")]), None);
        assert_eq!(pick(&before, &[w(1, "문서")]), None); // 창 하나가 닫힌 것은 우리가 연 것이 아니다
        assert_eq!(pick(&[], &[]), None);
    }
}
