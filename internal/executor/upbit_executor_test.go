package executor

import (
	"sync"
	"testing"
	"time"

	"upbit-arb/internal/exchange"
	"upbit-arb/internal/state"
)

// ──────────────────────────────────────────────
// Bug 1: TryLock – 거래 중 재진입 차단
// ──────────────────────────────────────────────

func TestTradeMu_TryLockBlocksConcurrent(t *testing.T) {
	var tradeMu sync.Mutex

	if !tradeMu.TryLock() {
		t.Fatal("첫 TryLock 실패")
	}

	// 락 보유 중: 두 번째 TryLock은 즉시 false 반환해야 함
	if tradeMu.TryLock() {
		t.Fatal("거래 중임에도 두 번째 TryLock이 성공 – 재진입 차단 실패")
	}

	tradeMu.Unlock()

	// 락 해제 후: 다시 TryLock 가능해야 함
	if !tradeMu.TryLock() {
		t.Fatal("언락 후 TryLock 실패")
	}
	tradeMu.Unlock()
}

// ──────────────────────────────────────────────
// Bug 2: waitAvailPositive – select break 버그 수정 확인
// ──────────────────────────────────────────────
// 이전 코드: select { case <-deadline.C: break } → for 루프를 탈출 못 해 무한 루프
// 수정 코드: select { case <-deadline.C: return ... } → 타임아웃 시 즉시 반환

func TestWaitAvailPositive_TimeoutReturnsError(t *testing.T) {
	// Private WS 경로: 잔고가 0인 채로 시그널 없이 타임아웃 발생
	e := &UpbitExecutor{
		UsePrivateWS: true,
		pwsReady:     true,
	}
	e.Balances = newTestBalanceStore()
	e.availSig = map[string]chan struct{}{}
	e.orderCh = map[string]chan orderEvent{}
	e.orderLast = map[string]orderEvent{}

	start := time.Now()
	_, _, err := e.waitAvailPositive("BTC", 200*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("잔고 0인데도 에러가 반환되지 않음")
	}
	// 무한 루프가 아니라 타임아웃(200ms) 근처에서 끝나야 함
	if elapsed > 600*time.Millisecond {
		t.Fatalf("타임아웃 후에도 오래 걸림: %v (무한 루프 의심)", elapsed)
	}
}

// ──────────────────────────────────────────────
// Bug 3: waitOrderDonePWS – mu 해제 후 채널 대기 (데드락 없음)
// ──────────────────────────────────────────────
// Private WS 콜백(OnMyOrder)이 mu를 획득해 ch에 이벤트를 전달할 수 있어야 한다.

func TestWaitOrderDonePWS_NoDeadlock(t *testing.T) {
	e := &UpbitExecutor{
		UsePrivateWS: true,
	}
	e.orderCh = map[string]chan orderEvent{}
	e.orderLast = map[string]orderEvent{}
	e.availSig = map[string]chan struct{}{}
	e.pwsReady = true

	uuid := "test-uuid-001"

	// waitOrderDonePWS를 goroutine에서 실행
	result := make(chan *OrderResp, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := e.waitOrderDonePWS(uuid, 2*time.Second)
		result <- resp
		errCh <- err
	}()

	// 채널이 등록될 때까지 잠깐 대기
	time.Sleep(30 * time.Millisecond)

	// 실제 OnMyOrder 콜백과 동일한 방식으로 이벤트 주입
	// (mu 획득 → orderLast 저장 → mu 해제 → ch 전송)
	fakeResp := &exchange.OrderResp{UUID: uuid, State: "done"}
	ev := orderEvent{UUID: uuid, Resp: fakeResp}

	e.mu.Lock()
	e.orderLast[uuid] = ev
	ch := e.orderCh[uuid]
	e.mu.Unlock()

	if ch != nil {
		ch <- ev
	}

	select {
	case resp := <-result:
		if err := <-errCh; err != nil {
			t.Fatalf("데드락 없이 완료됐지만 에러 반환: %v", err)
		}
		if resp == nil || resp.State != "done" {
			t.Fatalf("잘못된 응답: %+v", resp)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("타임아웃 – 데드락 발생 또는 이벤트 미수신")
	}
}

// ──────────────────────────────────────────────
// helpers
// ──────────────────────────────────────────────

type OrderResp = exchange.OrderResp

var _ = sync.Mutex{} // ensure sync import used

func newTestBalanceStore() *state.BalanceStore { return state.NewBalanceStore() }
