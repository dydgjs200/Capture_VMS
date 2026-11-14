//! 프로그램 동작 로그(app.YYYY-MM-DD.log). 실행 중에 로그 폴더를 바꿀 수 있게 감싼다.

use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

use tracing_appender::rolling::{self, RollingFileAppender};

pub struct LogSwitch {
    inner: Mutex<RollingFileAppender>,
}

fn build(dir: &Path) -> Result<RollingFileAppender, String> {
    rolling::Builder::new()
        .rotation(rolling::Rotation::DAILY)
        .filename_prefix("app")
        .filename_suffix("log")
        .max_log_files(14)
        .build(dir)
        .map_err(|e| e.to_string())
}

impl LogSwitch {
    pub fn new(dir: &Path) -> Result<Arc<Self>, String> {
        Ok(Arc::new(Self { inner: Mutex::new(build(dir)?) }))
    }

    /// 이후 로그를 새 폴더에 쓴다.
    pub fn switch(&self, dir: &Path) -> Result<(), String> {
        let appender = build(dir)?;
        *self.inner.lock().unwrap() = appender;
        Ok(())
    }

    pub fn writer(self: &Arc<Self>) -> SwitchWriter {
        SwitchWriter(self.clone())
    }
}

pub struct SwitchWriter(Arc<LogSwitch>);

impl io::Write for SwitchWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.inner.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.inner.lock().unwrap().flush()
    }
}
