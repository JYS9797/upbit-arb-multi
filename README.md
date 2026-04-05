# upbit-arb-multi (monitor only)

여러 자산을 대상으로 업비트 실시간 오더북(WebSocket)으로 차익거래 **기회만** 감지합니다.
- 대상 자산: `top_krw` 모드(기본)에서 KRW 마켓 24h 거래대금 상위 N개
- 모니터링 마켓: KRW/USDT/BTC 페어를 구독(가능한 것만)
- 차익 계산: KRW-ASSET ↔ USDT-ASSET (실제로는 KRW-USDT를 포함한 3-leg 기준)
- 기회 포착 시: 콘솔 출력 + `data/snapshots.jsonl`에 3개 오더북 스냅샷 저장

## 준비
- Go 1.22+

## 설치
```powershell
go mod tidy
```

## 실행
```powershell
go run ./cmd/arb-monitor -config configs/config.yaml
```

## 설정
- `configs/config.yaml`에서 mode, top_krw_n, max_qty_asset, min_profit_krw 등을 조절하세요.

## 출력
- 콘솔: 각 평가 주기에서 가장 큰 기회 1개를 출력
- JSONL: 기회가 출력될 때마다 당시 `KRW-asset`, `USDT-asset`, `KRW-USDT` 오더북 상위 N호가 저장


---
## ⚠️ 실거래(주문 실행) 기능

이 프로젝트는 기본적으로 모니터링/스냅샷 저장 용도이며, **실거래는 `configs/config.yaml`의 `trading.enabled`를 true로 바꿨을 때만** 주문을 보냅니다.

### 1) 환경변수로 API 키 설정 (Windows PowerShell)
```powershell
$env:UPBIT_ACCESS_KEY="여기에_액세스키"
$env:UPBIT_SECRET_KEY="여기에_시크릿키"
```

### 2) config 설정
`configs/config.yaml`
- `trading.enabled: true` 로 변경
- `trading.max_krw_per_trade`: 1회 트레이드 최대 사용 KRW
- `trading.safety_krw_reserve`: 잔고에서 남겨둘 KRW

### 3) 실행
```powershell
go mod tidy
go run ./cmd/arb-monitor -config configs/config.yaml
```

### 4) 동작 방식 (실거래)
- best signal(가장 좋은 기회) 발견 시, 방향에 따라 3-leg를 순차 실행합니다.
- **Dir A**: KRW-asset 최유리 매수 → USDT-asset 최유리 매도 → KRW-USDT 최유리 매도(USDT→KRW 환전)
- **Dir B**: KRW-USDT 최유리 매수(KRW→USDT) → USDT-asset 최유리 매수 → KRW-asset 최유리 매도

각 leg의 주문은 `ord_type=best` + `time_in_force=ioc|fok`로 실행됩니다. (기본 ioc)

### 5) 출력/저장
- 콘솔에 **실제 실행 전후 KRW 잔고**와 **실현 손익(realized)** 를 로그로 출력합니다.
- `data/trades.jsonl`에 트레이드 레코드를 JSONL로 저장합니다.
- 기존처럼 `data/snapshots.jsonl`에는 “기회 포착 시점” 오더북 3개 스냅샷이 저장됩니다.

### 6) 주의
- 이 코드는 교육/PoC 목적이며, 실거래에서 손실이 발생할 수 있습니다.
- `ioc`는 부분체결 후 잔량 취소가 될 수 있어, 3-leg에서 잔고가 남을 수 있습니다.
- 충분한 리스크 관리(최대 포지션, 실패 처리, 재시도 정책, 잔고 정리 로직)를 추가한 뒤 사용하세요.


---
## WebSocket 안정화
- public/private WS 모두 ping을 별도 ticker로 보내고, pong 수신 시 read deadline을 연장하도록 수정했습니다.
- 네트워크가 잠깐 흔들려도 자동 재연결(backoff)합니다.
