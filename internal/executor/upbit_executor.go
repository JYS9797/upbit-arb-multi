package executor

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"upbit-arb/internal/exchange"
	"upbit-arb/internal/marketdata"
	"upbit-arb/internal/state"
	"upbit-arb/internal/storage"
	"upbit-arb/internal/strategy"
)

type UpbitExecutor struct {
	Enabled          bool
	TimeInForce      string
	MaxKRWPerTrade   float64
	SafetyKRWReserve float64

	REST        *exchange.RESTClient
	Cache       *marketdata.Cache
	Balances    *state.BalanceStore
	TradeWriter *storage.JSONLWriter

	// ---- private WS options ----
	UsePrivateWS     bool
	PrivateWSURL     string
	AccessKey        string
	SecretKey        string
	PingInterval     time.Duration
	ReconnectBackoff time.Duration
	MyOrderCodes     []string

	// tradeMu: 한 번에 하나의 거래만 실행 (TryLock으로 이미 진행 중이면 스킵)
	tradeMu sync.Mutex
	// mu: orderCh / orderLast / availSig 등 공유 맵 보호 (짧게만 보유)
	mu sync.Mutex

	// ---- private WS runtime state ----
	pwsOnce   sync.Once
	pwsReady  bool
	orderCh   map[string]chan orderEvent
	orderLast map[string]orderEvent
	availSig  map[string]chan struct{}
	stopCh    chan struct{}
}

type orderEvent struct {
	UUID string
	Resp *exchange.OrderResp
	Raw  json.RawMessage
}

type myOrderMsg struct {
	Type           string   `json:"type"`
	Code           string   `json:"code"`
	UUID           string   `json:"uuid"`
	AskBid         string   `json:"ask_bid"`
	OrderType      string   `json:"order_type"`
	State          string   `json:"state"`
	ExecutedVolume *float64 `json:"executed_volume"`
	ExecutedFunds  *float64 `json:"executed_funds"`
	PaidFee        *float64 `json:"paid_fee"`
}

type myAssetEntry struct {
	Currency string  `json:"currency"`
	Balance  float64 `json:"balance"`
	Locked   float64 `json:"locked"`
}

type myAssetMsg struct {
	Type      string         `json:"type"`
	AssetUUID string         `json:"asset_uuid"`
	Assets    []myAssetEntry `json:"assets"`
}

func (e *UpbitExecutor) OnSignal(sig *strategy.Signal) error {
	if !e.Enabled {
		return nil
	}

	// 이미 거래 중이면 스킵 (큐잉 없음)
	if !e.tradeMu.TryLock() {
		log.Printf("[trade] 이전 거래 진행 중, 스킵")
		return nil
	}
	defer e.tradeMu.Unlock()

	// private WS lazy init
	e.ensurePrivateWS()

	// 잔고 갱신: Private WS가 추적 중이면 캐시 사용 (REST 생략으로 ~150ms 절약)
	// 단, 잔고가 0이거나 WS 미사용이면 REST로 재확인
	e.mu.Lock()
	wsReady := e.pwsReady
	e.mu.Unlock()

	krwBefore := e.Balances.Get("KRW")
	if !wsReady || krwBefore <= 0 {
		accts, err := e.REST.GetAccounts()
		if err != nil {
			return err
		}
		e.Balances.UpdateFromAccounts(accts)
		krwBefore = e.Balances.Get("KRW")
	}

	// Apply reserve + max per trade
	usable := krwBefore - e.SafetyKRWReserve
	if usable <= 0 {
		log.Printf("[trade] KRW 부족: before=%.0f reserve=%.0f", krwBefore, e.SafetyKRWReserve)
		return nil
	}
	if usable > e.MaxKRWPerTrade {
		usable = e.MaxKRWPerTrade
	}

	op := sig.Opportunity
	asset := op.Asset

	var orders []*exchange.OrderResp
	var summaries []storage.OrderSummary
	var tradeErr error

	snapCodes := []string{"KRW-" + asset, "USDT-" + asset, "KRW-USDT"}
	snap := e.Cache.Snapshot(snapCodes)

	switch op.BestDirection {
	case strategy.DirA:
		orders, tradeErr = e.execDirA(sig, usable, snap)
	case strategy.DirB:
		orders, tradeErr = e.execDirB(sig, usable, snap)
	default:
		return fmt.Errorf("unknown direction")
	}
	if tradeErr != nil {
		log.Printf("[trade] 실행 실패: %v", tradeErr)
	}

	// After balances: 거래 완료 후 최종 KRW 확인 (REST로 정확한 값)
	if accts2, err2 := e.REST.GetAccounts(); err2 == nil {
		e.Balances.UpdateFromAccounts(accts2)
	}
	krwAfter := e.Balances.Get("KRW")
	realized := krwAfter - krwBefore

	for _, o := range orders {
		if o == nil {
			continue
		}
		summaries = append(summaries, storage.OrderSummary{
			Market:         o.Market,
			Side:           o.Side,
			OrdType:        o.OrdType,
			UUID:           o.UUID,
			State:          o.State,
			ExecutedVolume: o.ExecutedVolume,
			ExecutedFunds:  o.ExecutedFunds,
			PaidFee:        o.PaidFee,
		})
	}

	log.Printf("[trade] KRW before=%.0f after=%.0f realized=%.0f planned=%.0f dir=%s asset=%s",
		krwBefore, krwAfter, realized, op.BestProfitKRW, op.BestDirection, asset)

	if e.TradeWriter != nil {
		rec := storage.TradeRecord{
			TimeMs:               sig.TimeMs,
			TimeKST:              sig.TimeKST,
			Asset:                asset,
			Direction:            string(op.BestDirection),
			KRWBefore:            krwBefore,
			KRWAfter:             krwAfter,
			RealizedProfitKRW:    realized,
			PlannedBestProfitKRW: op.BestProfitKRW,
			PlannedQtyAsset:      op.BestQtyAsset,
			Orders:               summaries,
		}
		_ = e.TradeWriter.Write(rec)
	}

	return tradeErr
}

// minUnwindQty: 긴급 청산 주문을 낼 최소 수량 임계값 (너무 소량이면 API 오류 발생)
const minUnwindQty = 1e-6

// unwindAsset: leg 실패 후 남은 ASSET을 KRW-ASSET 시장에 긴급 매도.
// 반드시 IOC로 실행 (FoK 설정과 무관하게 부분이라도 팔아야 함).
func (e *UpbitExecutor) unwindAsset(asset string, qty float64) *exchange.OrderResp {
	if qty < minUnwindQty {
		return nil
	}
	log.Printf("[unwind] %s %.8f 긴급 청산 시작", asset, qty)
	req := exchange.BestSell("KRW-"+asset, trimFloat(qty, 8), "ioc")
	o, err := e.REST.CreateOrder(req)
	if err != nil {
		log.Printf("[unwind] %s 주문 실패: %v", asset, err)
		return nil
	}
	final, _ := e.waitOrderFilled(o.UUID, 10*time.Second)
	if final != nil {
		log.Printf("[unwind] %s 완료: state=%s vol=%s", asset, final.State, final.ExecutedVolume)
	}
	return final
}

// unwindUSDT: leg 실패 후 남은 USDT를 KRW-USDT 시장에 긴급 매도.
func (e *UpbitExecutor) unwindUSDT(qty float64) *exchange.OrderResp {
	if qty < minUnwindQty {
		return nil
	}
	log.Printf("[unwind] USDT %.8f 긴급 청산 시작", qty)
	req := exchange.BestSell("KRW-USDT", trimFloat(qty, 8), "ioc")
	o, err := e.REST.CreateOrder(req)
	if err != nil {
		log.Printf("[unwind] USDT 주문 실패: %v", err)
		return nil
	}
	final, _ := e.waitOrderFilled(o.UUID, 10*time.Second)
	if final != nil {
		log.Printf("[unwind] USDT 완료: state=%s vol=%s", final.State, final.ExecutedVolume)
	}
	return final
}

func (e *UpbitExecutor) execDirA(sig *strategy.Signal, maxKRW float64, snap map[string]struct {
	TS    int64
	Units []marketdata.OrderbookUnit
}) ([]*exchange.OrderResp, error) {
	asset := sig.Opportunity.Asset
	krwAsset := "KRW-" + asset
	usdtAsset := "USDT-" + asset
	krwUsdt := "KRW-USDT"

	krwBook, ok := snap[krwAsset]
	if !ok || len(krwBook.Units) == 0 {
		return nil, fmt.Errorf("missing %s", krwAsset)
	}
	bestAsk := krwBook.Units[0].AskPrice

	qty := sig.Opportunity.BestQtyAsset
	need := bestAsk * qty
	if need > maxKRW {
		qty = maxKRW / bestAsk
		need = maxKRW
	}

	// ── Leg 1: KRW → ASSET ──
	buy, err := e.REST.CreateOrder(exchange.BestBuy(krwAsset, fmt.Sprintf("%.0f", need), e.TimeInForce))
	if err != nil {
		return []*exchange.OrderResp{buy}, err
	}
	buyFinal, err := e.waitOrderFilled(buy.UUID, 6*time.Second)
	if err != nil {
		return []*exchange.OrderResp{buyFinal}, err
	}
	volAsset := parseVol(buyFinal.ExecutedVolume)
	if volAsset <= 0 {
		// FoK cancel 또는 0 체결 → 자산 없음, 안전하게 종료
		return []*exchange.OrderResp{buyFinal}, fmt.Errorf("leg1: no fill (state=%s)", buyFinal.State)
	}

	// Leg 1 완료 후 ASSET 잔고 정산 대기
	assetAvail, _, _ := e.waitAvailPositive(asset, 2*time.Second)
	volToSell := volAsset
	if assetAvail > 0 && assetAvail < volToSell {
		volToSell = assetAvail
	}

	// ── Leg 2: ASSET → USDT ──
	sell, err := e.REST.CreateOrder(exchange.BestSell(usdtAsset, trimFloat(volToSell, 8), e.TimeInForce))
	if err != nil {
		e.unwindAsset(asset, volToSell)
		return []*exchange.OrderResp{buyFinal, sell}, fmt.Errorf("leg2 create failed, unwind: %w", err)
	}
	sellFinal, _ := e.waitOrderFilled(sell.UUID, 6*time.Second)

	leg2Sold := parseVol(sellFinal.ExecutedVolume)   // ASSET 판 양
	usdtReceived := parseVol(sellFinal.ExecutedFunds) // USDT 받은 양

	// 미체결 ASSET 잔량 긴급 청산
	if remaining := volToSell - leg2Sold; remaining >= minUnwindQty {
		uw := e.unwindAsset(asset, remaining)
		if uw != nil {
			// unwind 주문도 기록에 포함
			return []*exchange.OrderResp{buyFinal, sellFinal, uw},
				fmt.Errorf("leg2: partial fill (sold=%.8f remain=%.8f), unwind executed", leg2Sold, remaining)
		}
	}
	if usdtReceived <= 0 {
		return []*exchange.OrderResp{buyFinal, sellFinal}, fmt.Errorf("leg2: no USDT received")
	}

	// Leg 2 완료 후 USDT 잔고 정산 대기
	usdtAvail, _, _ := e.waitAvailPositive("USDT", 2*time.Second)
	usdtToConv := usdtReceived
	if usdtAvail > 0 && usdtAvail < usdtToConv {
		usdtToConv = usdtAvail
	}

	// ── Leg 3: USDT → KRW ──
	conv, err := e.REST.CreateOrder(exchange.BestSell(krwUsdt, trimFloat(usdtToConv, 8), e.TimeInForce))
	if err != nil {
		e.unwindUSDT(usdtToConv)
		return []*exchange.OrderResp{buyFinal, sellFinal, conv}, fmt.Errorf("leg3 create failed, unwind: %w", err)
	}
	convFinal, convErr := e.waitOrderFilled(conv.UUID, 6*time.Second)

	// 미체결 USDT 잔량 긴급 청산
	leg3SoldUSDT := parseVol(convFinal.ExecutedVolume)
	if remaining := usdtToConv - leg3SoldUSDT; remaining >= minUnwindQty {
		uw := e.unwindUSDT(remaining)
		if uw != nil {
			return []*exchange.OrderResp{buyFinal, sellFinal, convFinal, uw},
				fmt.Errorf("leg3: partial fill (sold=%.8f remain=%.8f), unwind executed", leg3SoldUSDT, remaining)
		}
	}
	return []*exchange.OrderResp{buyFinal, sellFinal, convFinal}, convErr
}

func (e *UpbitExecutor) execDirB(
	sig *strategy.Signal,
	maxKRW float64,
	snap map[string]struct {
		TS    int64
		Units []marketdata.OrderbookUnit
	},
) ([]*exchange.OrderResp, error) {

	asset := sig.Opportunity.Asset
	krwAsset := "KRW-" + asset
	usdtAsset := "USDT-" + asset
	krwUsdt := "KRW-USDT"

	// ── Leg 1: KRW → USDT ──
	buyUsdt, err := e.REST.CreateOrder(exchange.BestBuy(krwUsdt, fmt.Sprintf("%.0f", maxKRW), e.TimeInForce))
	if err != nil {
		return []*exchange.OrderResp{buyUsdt}, err
	}
	buyUsdtFinal, err := e.waitOrderFilled(buyUsdt.UUID, 6*time.Second)
	if err != nil {
		return []*exchange.OrderResp{buyUsdtFinal}, err
	}
	usdtGot := parseVol(buyUsdtFinal.ExecutedVolume)
	if usdtGot <= 0 {
		return []*exchange.OrderResp{buyUsdtFinal}, fmt.Errorf("leg1: no USDT received (state=%s)", buyUsdtFinal.State)
	}

	// Leg 1 완료 후 USDT 잔고 정산 대기
	usdtAvail, _, _ := e.waitAvailPositive("USDT", 1500*time.Millisecond)
	usdtToSpend := usdtAvail
	if usdtToSpend > usdtGot {
		usdtToSpend = usdtGot
	}
	usdtToSpend *= 0.997 // 0.3% safety margin
	if usdtToSpend <= 0 {
		return []*exchange.OrderResp{buyUsdtFinal},
			fmt.Errorf("leg1: USDT not available (avail=%.8f got=%.8f)", usdtAvail, usdtGot)
	}

	// ── Leg 2: USDT → ASSET ──
	buyAsset, err := e.REST.CreateOrder(exchange.BestBuy(usdtAsset, trimFloat(usdtToSpend, 6), e.TimeInForce))
	if err != nil {
		e.unwindUSDT(usdtToSpend)
		return []*exchange.OrderResp{buyUsdtFinal, buyAsset}, fmt.Errorf("leg2 create failed, unwind: %w", err)
	}
	buyAssetFinal, _ := e.waitOrderFilled(buyAsset.UUID, 6*time.Second)

	usdtSpent := parseVol(buyAssetFinal.ExecutedFunds)  // USDT 사용한 양
	assetVol := parseVol(buyAssetFinal.ExecutedVolume)  // ASSET 받은 양

	// 미사용 USDT 잔량 긴급 청산
	if remaining := usdtToSpend - usdtSpent; remaining >= minUnwindQty {
		uw := e.unwindUSDT(remaining)
		if assetVol <= 0 {
			// ASSET을 하나도 못 받은 경우
			return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, uw},
				fmt.Errorf("leg2: no asset received, unwind USDT executed")
		}
		if uw != nil {
			log.Printf("[trade] DirB leg2: USDT %.8f unwind 완료", remaining)
		}
	}
	if assetVol <= 0 {
		return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal}, fmt.Errorf("leg2: no asset received")
	}

	// Leg 2 완료 후 ASSET 잔고 정산 대기
	assetAvail, _, _ := e.waitAvailPositive(asset, 2*time.Second)
	volToSell := assetVol
	if assetAvail > 0 && assetAvail < volToSell {
		volToSell = assetAvail
	}

	// ── Leg 3: ASSET → KRW ──
	sell, err := e.REST.CreateOrder(exchange.BestSell(krwAsset, trimFloat(volToSell, 8), e.TimeInForce))
	if err != nil {
		e.unwindAsset(asset, volToSell)
		return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sell}, fmt.Errorf("leg3 create failed, unwind: %w", err)
	}
	sellFinal, sellErr := e.waitOrderFilled(sell.UUID, 6*time.Second)

	// 미체결 ASSET 잔량 긴급 청산
	leg3Sold := parseVol(sellFinal.ExecutedVolume)
	if remaining := volToSell - leg3Sold; remaining >= minUnwindQty {
		uw := e.unwindAsset(asset, remaining)
		if uw != nil {
			return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sellFinal, uw},
				fmt.Errorf("leg3: partial fill (sold=%.8f remain=%.8f), unwind executed", leg3Sold, remaining)
		}
	}
	return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sellFinal}, sellErr
}

// parseVol: 주문 응답의 string 필드를 float64로 변환 (파싱 실패 시 0)
func parseVol(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func (e *UpbitExecutor) waitOrderFilled(uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	// private WS 사용 시 우선 이벤트 기반으로 대기 (mu 미보유 상태로 호출)
	if e.UsePrivateWS {
		if od, err := e.waitOrderDonePWS(uuid, timeout); err == nil && od != nil {
			return od, nil
		}
		// 실패하면 아래 REST fallback
	}

	deadline := time.Now().Add(timeout)
	var last *exchange.OrderResp
	for time.Now().Before(deadline) {
		o, err := e.REST.GetOrder(uuid)
		if err != nil {
			time.Sleep(120 * time.Millisecond)
			continue
		}
		last = o
		if o.State == "done" || o.State == "cancel" {
			return o, nil
		}
		time.Sleep(120 * time.Millisecond)
	}
	if last != nil {
		return last, fmt.Errorf("order timeout state=%s", last.State)
	}
	return nil, fmt.Errorf("order timeout")
}

func (e *UpbitExecutor) ensurePrivateWS() {
	if !e.UsePrivateWS {
		return
	}
	e.pwsOnce.Do(func() {
		e.mu.Lock()
		e.orderCh = map[string]chan orderEvent{}
		e.orderLast = map[string]orderEvent{}
		e.availSig = map[string]chan struct{}{}
		e.stopCh = make(chan struct{})
		e.mu.Unlock()

		// 초기 잔고 스냅샷
		if ac, err := e.REST.GetAccounts(); err == nil {
			e.Balances.UpdateFromAccounts(ac)
		}

		pws := &exchange.PrivateWSClient{
			URL:          e.PrivateWSURL,
			PingInterval: e.PingInterval,
			Backoff:      e.ReconnectBackoff,
			MyOrderCodes: e.MyOrderCodes,
			AuthFunc: func() (string, error) {
				return exchange.BuildJWT(e.AccessKey, e.SecretKey, "")
			},
			OnMyAsset: func(raw json.RawMessage) {
				var m myAssetMsg
				if err := json.Unmarshal(raw, &m); err != nil {
					return
				}

				// mu를 잠깐만 보유하여 잔고 업데이트 + 시그널 수집
				var sigs []chan struct{}
				e.mu.Lock()
				for _, a := range m.Assets {
					cur := strings.ToUpper(strings.TrimSpace(a.Currency))
					e.Balances.Set(cur, a.Balance)
					if ch, ok := e.availSig[cur]; ok {
						sigs = append(sigs, ch)
					}
				}
				e.mu.Unlock()

				// mu 해제 후 시그널 전송 (블로킹 없음)
				for _, ch := range sigs {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
			},
			OnMyOrder: func(raw json.RawMessage) {
				var m myOrderMsg
				if err := json.Unmarshal(raw, &m); err != nil {
					return
				}

				resp := &exchange.OrderResp{
					UUID:    m.UUID,
					State:   m.State,
					Market:  m.Code,
					Side:    normalizeSide(m.AskBid),
					OrdType: m.OrderType,
				}
				if m.ExecutedVolume != nil {
					resp.ExecutedVolume = trimFloat(*m.ExecutedVolume, 12)
				}
				if m.ExecutedFunds != nil {
					resp.ExecutedFunds = trimFloat(*m.ExecutedFunds, 12)
				}
				if m.PaidFee != nil {
					resp.PaidFee = trimFloat(*m.PaidFee, 12)
				}

				ev := orderEvent{UUID: m.UUID, Resp: resp, Raw: raw}

				// mu를 잠깐만 보유하여 상태 저장 + 채널 참조 획득
				e.mu.Lock()
				e.orderLast[m.UUID] = ev
				ch := e.orderCh[m.UUID]
				e.mu.Unlock()

				// mu 해제 후 채널 전송 (waitOrderDonePWS가 대기 중)
				if ch != nil {
					select {
					case ch <- ev:
					default:
					}
				}
			},
		}

		go pws.Run()

		e.mu.Lock()
		e.pwsReady = true
		e.mu.Unlock()
	})
}

func (e *UpbitExecutor) waitOrderDonePWS(uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	e.mu.Lock()
	if !e.pwsReady {
		e.mu.Unlock()
		return nil, fmt.Errorf("private ws not ready")
	}

	ch := make(chan orderEvent, 8)
	e.orderCh[uuid] = ch

	// 이미 완료된 이벤트가 캐시에 있으면 즉시 반환
	if ev, ok := e.orderLast[uuid]; ok && ev.Resp != nil {
		if ev.Resp.State == "done" || ev.Resp.State == "cancel" {
			delete(e.orderCh, uuid)
			e.mu.Unlock()
			return ev.Resp, nil
		}
	}
	e.mu.Unlock() // ← 채널 대기 전에 반드시 해제

	defer func() {
		e.mu.Lock()
		delete(e.orderCh, uuid)
		e.mu.Unlock()
	}()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case <-deadline.C:
			return nil, fmt.Errorf("pws wait timeout uuid=%s", uuid)
		case ev := <-ch:
			if ev.Resp != nil && (ev.Resp.State == "done" || ev.Resp.State == "cancel") {
				return ev.Resp, nil
			}
		}
	}
}

func (e *UpbitExecutor) waitAvailPositive(currency string, timeout time.Duration) (avail float64, locked float64, err error) {
	cur := strings.ToUpper(currency)

	// private WS 우선
	if e.UsePrivateWS {
		e.mu.Lock()
		sig, ok := e.availSig[cur]
		if !ok {
			sig = make(chan struct{}, 8)
			e.availSig[cur] = sig
		}
		avail = e.Balances.Get(cur)
		e.mu.Unlock() // ← 채널 대기 전에 반드시 해제

		if avail > 0 {
			return avail, 0, nil
		}

		deadline := time.NewTimer(timeout)
		defer deadline.Stop()

		for {
			select {
			case <-deadline.C:
				// 타임아웃: 현재 잔고 그대로 반환
				return e.Balances.Get(cur), 0, fmt.Errorf("avail not positive for %s within %s", cur, timeout)
			case <-sig:
				avail = e.Balances.Get(cur)
				if avail > 0 {
					return avail, 0, nil
				}
			}
		}
	}

	// REST fallback
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		accts, aerr := e.REST.GetAccounts()
		if aerr == nil {
			e.Balances.UpdateFromAccounts(accts)
			avail = e.Balances.Get(cur)
			if avail > 0 {
				return avail, 0, nil
			}
		}
		time.Sleep(120 * time.Millisecond)
	}
	return avail, locked, fmt.Errorf("avail not positive for %s within %s", cur, timeout)
}

func normalizeSide(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "BID":
		return "bid"
	case "ASK":
		return "ask"
	default:
		return strings.ToLower(strings.TrimSpace(s))
	}
}

func trimFloat(v float64, decimals int) string {
	fmtStr := "%0." + strconv.Itoa(decimals) + "f"
	s := fmt.Sprintf(fmtStr, v)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		s = "0"
	}
	return s
}
