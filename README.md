# Capture VMS

IP 카메라의 RTSP 영상을 받아 **녹화하고, 수신 품질을 재고, 원하는 순간의 프레임을 촬영**하는 Windows용 경량 VMS입니다.
무선(Wi-Fi)처럼 불안정한 네트워크에서 영상이 얼마나 안정적으로 들어오는지 숫자로 확인하려는 용도로 만들었습니다.

> A small Windows VMS written in Rust: receives RTSP from IP cameras through FFmpeg, records segmented MP4,
> measures frame intervals / stalls / delay / reconnects, takes frame captures (single or simultaneous),
> and diagnoses where a stall came from. Native live viewer (egui) + web dashboard for records.

## 주요 기능

- **수신·녹화** — 카메라마다 FFmpeg 하나로 영상을 풀지 않고 그대로(copy) 받아 분 단위 MP4로 저장합니다. 강제로 끊겨도 마지막 조각 전까지는 재생됩니다.
- **수신 품질 측정** — 프레임 도착 간격, 끊김, 버퍼 지연, 프레임 누락, 재연결 횟수를 초 단위로 기록하고 세션마다 판정합니다.
- **끊김 진단** — 영상이 끊긴 동안 카메라·공유기 ping, RTSP 포트, Wi-Fi 상태를 0.5초마다 보고 "PC↔공유기 / 공유기↔카메라 / 영상만 멈춤 / PC 멈춤"으로 가려 남깁니다.
- **자동 복구** — 제때 온 프레임이 3초 없으면 녹화를 닫고 새로 연결, 카메라 포트가 열리는 즉시 재연결, 프로그램이 갑자기 꺼졌다 켜지면 받던 수신을 이어받습니다.
- **프레임 촬영** — 버튼·HTTP·TCP 메시지·Modbus TCP 신호로 한 대 또는 여러 대를 동시에 찍고, 누름→저장 시간과 카메라 간 시각 차이를 기록합니다.
- **카메라 등록** — ONVIF 탐색(WS-Discovery)과 RTSP 포트 확인으로 카메라를 찾고, 연결 테스트가 ONVIF·흔한 RTSP 주소로 메인/서브 스트림 주소를 알아냅니다.
- **주소 따라가기** — 공유기를 껐다 켜서 카메라 IP가 바뀌면 ONVIF 고유 값·MAC으로 같은 카메라를 찾아 스스로 주소를 바꿉니다.
- **기록 보기·내보내기** — 웹 대시보드에서 세션 비교, 촬영 기록, 녹화 재생, 원시 데이터 조회, Excel·PDF 보고서.

## 구조

```
                       ┌──────────────────────────────────────────────┐
 IP 카메라 ── RTSP ──▶ │ 엔진  capture-vms.exe  (창 없음)              │
                       │  · 카메라마다 FFmpeg 자식 프로세스 1개         │
                       │     ├ 녹화 (segment MP4, 재인코딩 없음)        │
                       │     ├ 프레임 기록 (도착 시각 측정)             │
                       │     └ MPEG-TS (촬영·화면 전달용)               │
                       │  · 측정·판정·끊김 진단·JSONL 기록              │
                       │  · HTTP API + 웹 대시보드 (127.0.0.1:8787)     │
                       └───────────────┬───────────────┬──────────────┘
                                       │ API · live.ts │ 웹
                    ┌──────────────────▼──────┐  ┌─────▼────────────────┐
                    │ 뷰어 capture-vms-viewer │  │ 웹 대시보드 (브라우저) │
                    │  라이브 화면·조작·설정   │  │  기록·녹화·보고서     │
                    └─────────────────────────┘  └──────────────────────┘
```

- 뷰어와 웹은 엔진 API만 부르고 카메라에 직접 붙지 않습니다. 뷰어를 닫아도 수신·녹화는 계속됩니다.
- 수신 쪽 FFmpeg는 영상을 디코딩하지 않습니다. 화면용 디코딩은 뷰어 프로세스에서만 합니다.

| 경로 | 내용 |
|---|---|
| `src/main.rs` | 엔진 시작, 기준 폴더(`--home`), FFmpeg 찾기, 웹 서버 |
| `src/stream.rs` | RTSP 세션: FFmpeg 실행, 지표, 재연결, 판정 |
| `src/capture.rs` | 프레임 촬영, 화면 전달(`live.ts`) |
| `src/stalldiag.rs` | 끊김 진단 |
| `src/onvif.rs` · `src/netscan.rs` · `src/camfix.rs` | 카메라 찾기, 연결 테스트, 주소 따라가기 |
| `src/netmon.rs` | ping/TCP/Wi-Fi 감시 |
| `src/trigger.rs` | 동시 촬영 (버튼·HTTP·TCP·Modbus TCP) |
| `src/store.rs` · `src/export.rs` | JSONL 기록, 요약, Excel·PDF |
| `src/web.rs` · `assets/dashboard.html` | HTTP API, 웹 대시보드 (실행 파일에 포함됨) |
| `src/bin/capture-vms-viewer/` | 네이티브 뷰어 (egui) |

## 준비물

- Windows 10/11 (Wi-Fi 상태 읽기·MAC 확인 등 일부 기능이 Windows API를 씁니다)
- [Rust](https://rustup.rs) (stable)
- [FFmpeg](https://ffmpeg.org/download.html) 7.1 이상 — `ffmpeg.exe`를 프로젝트의 `ffmpeg\` 폴더에 두거나 PATH에 넣습니다. 이 저장소에는 들어 있지 않습니다.

## 빌드와 실행

```powershell
cargo test                                         # 자동 테스트
cargo run --bin capture-vms -- --home .            # 엔진 (이 폴더의 config.toml / data 사용)
cargo run --bin capture-vms-viewer -- --home .     # 뷰어 (엔진이 꺼져 있으면 띄웁니다)
cargo build --release                              # 배포용 빌드
```

1. 뷰어의 **설정 → 카메라**에서 [카메라 찾기] 또는 [직접 추가]로 카메라를 등록합니다 (2대까지).
2. **[수신 시작]** 을 누르면 화면이 나오고 녹화·측정이 시작됩니다.
3. **[촬영]** / **[동시 촬영]** 으로 프레임을 저장합니다.
4. 기록은 `http://127.0.0.1:8787` 의 웹 대시보드에서 봅니다.

## 설정

처음 실행하면 `config.toml`이 기본값으로 만들어집니다. 항목 설명은 [`config.example.toml`](config.example.toml)을 보세요.

```toml
[server]
bind = "127.0.0.1:8787"

[stream]
segment_minutes = 10        # 녹화 파일 하나의 길이(분)
stall_reconnect_ms = 3000   # 제때 온 프레임이 이 시간 동안 없으면 새로 연결

[monitor]
wifi_hold = true            # 수신 중 다른 Wi-Fi로 넘어가면 되돌림
follow_camera = true        # 카메라 IP가 바뀌면 같은 카메라를 찾아 따라감
```

기록은 `data\logs\<종류>-YYYY-MM-DD.jsonl`, 녹화는 `data\recordings\<날짜>\<세션>\runNN_xxxxx.mp4`에 쌓입니다.

## 엔진 API (일부)

| 호출 | 하는 일 |
|---|---|
| `GET /api/status` | 엔진·카메라·수신 상태 |
| `POST /api/cameras/discover` | 같은 대역의 카메라 찾기 |
| `POST /api/cameras/test {host,username,password}` | 연결 테스트 (RTSP 주소 찾기) |
| `POST /api/cameras` · `PUT /api/cameras/{id}` · `POST /api/cameras/{id}/move` | 등록·수정·주소 바꾸기 |
| `POST /api/sessions {camera_id,profile,transport,record}` · `POST /api/sessions/{id}/stop` | 수신 시작·중지 |
| `POST /api/cameras/{id}/capture` · `POST /api/trigger` | 촬영 · 동시 촬영 |
| `GET /api/sessions/{id}/live.ts` | 받은 영상을 그대로 MPEG-TS로 전달 |
| `GET /api/raw?kind=…` · `POST /api/export/xlsx|pdf` | 원시 데이터 · 내보내기 |
| `GET /api/diagnose` | 문제 진단 |

## 알아 둘 점

- **카메라 계정은 `config.toml`에 평문으로 저장됩니다.** 이 파일은 저장소에 올리지 마세요 (`.gitignore`에 들어 있습니다).
- 엔진은 기본으로 `127.0.0.1`에만 열립니다. 다른 주소에 열면 인증 없이 조작할 수 있으니 주의하세요.
- 카메라 찾기와 주소 따라가기는 같은 대역에 ONVIF 탐색 질문과 RTSP 포트 확인을 보냅니다 (로그인 시도는 하지 않습니다). 허락된 네트워크에서만 쓰고, 자동 찾기를 원하지 않으면 `follow_camera = false`로 끄세요.
- 카메라는 2대까지 등록합니다 (`MAX_CAMERAS`).
- 자동 테스트와 가짜 카메라로 확인한 범위가 대부분입니다. 실제 카메라 기종에 따라 동작이 다를 수 있습니다.
