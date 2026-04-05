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

	// Refresh balances (REST)
	accts, err := e.REST.GetAccounts()
	if err != nil {
		return err
	}
	e.Balances.UpdateFromAccounts(accts)
	krwBefore := e.Balances.Get("KRW")

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

	snapCodes := []string{"KRW-" + asset, "USDT-" + asset, "KRW-USDT"}
	snap := e.Cache.Snapshot(snapCodes)

	switch op.BestDirection {
	case strategy.DirA:
		orders, err = e.execDirA(sig, usable, snap)
	case strategy.DirB:
		orders, err = e.execDirB(sig, usable, snap)
	default:
		return fmt.Errorf("unknown direction")
	}
	if err != nil {
		log.Printf("[trade] 실행 실패: %v", err)
	}

	// After balances (최종 실현 손익은 REST 1회로 안전하게 확인)
	accts2, err2 := e.REST.GetAccounts()
	if err2 != nil {
		return err2
	}
	e.Balances.UpdateFromAccounts(accts2)
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

	return err
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

	// try to buy planned qty but cap by maxKRW at bestAsk
	qty := sig.Opportunity.BestQtyAsset
	need := bestAsk * qty
	if need > maxKRW {
		qty = maxKRW / bestAsk
		need = maxKRW
	}

	// ── Leg 1: KRW → ASSET ──
	krwTotal := fmt.Sprintf("%.0f", need)
	buyReq := exchange.BestBuy(krwAsset, krwTotal, e.TimeInForce)
	buy, err := e.REST.CreateOrder(buyReq)
	if err != nil {
		return []*exchange.OrderResp{buy}, err
	}
	buyFinal, err := e.waitOrderFilled(buy.UUID, 6*time.Second)
	if err != nil {
		return []*exchange.OrderResp{buyFinal}, err
	}
	volAsset, _ := strconv.ParseFloat(buyFinal.ExecutedVolume, 64)
	if volAsset <= 0 {
		return []*exchange.OrderResp{buyFinal}, fmt.Errorf("buy executed_volume=0")
	}

	// Leg 1 완료 후 ASSET 잔고 정산 대기
	assetAvail, _, waitErr := e.waitAvailPositive(asset, 2*time.Second)
	if waitErr != nil {
		log.Printf("[trade] DirA: asset avail wait: %v (fallback to executed_volume)", waitErr)
		assetAvail = volAsset
	}
	volToSell := volAsset
	if assetAvail > 0 && assetAvail < volToSell {
		volToSell = assetAvail
	}

	// ── Leg 2: ASSET → USDT ──
	sellReq := exchange.BestSell(usdtAsset, trimFloat(volToSell, 8), e.TimeInForce)
	sell, err := e.REST.CreateOrder(sellReq)
	if err != nil {
		return []*exchange.OrderResp{buyFinal, sell}, err
	}
	sellFinal, err := e.waitOrderFilled(sell.UUID, 6*time.Second)
	if err != nil {
		return []*exchange.OrderResp{buyFinal, sellFinal}, err
	}
	usdtFunds, _ := strconv.ParseFloat(sellFinal.ExecutedFunds, 64)
	if usdtFunds <= 0 {
		return []*exchange.OrderResp{buyFinal, sellFinal}, fmt.Errorf("sell executed_funds=0")
	}

	// Leg 2 완료 후 USDT 잔고 정산 대기
	usdtAvail, _, waitErr2 := e.waitAvailPositive("USDT", 2*time.Second)
	if waitErr2 != nil {
		log.Printf("[trade] DirA: USDT avail wait: %v (fallback to executed_funds)", waitErr2)
		usdtAvail = usdtFunds
	}
	usdtToConv := usdtFunds
	if usdtAvail > 0 && usdtAvail < usdtToConv {
		usdtToConv = usdtAvail
	}

	// ── Leg 3: USDT → KRW ──
	convReq := exchange.BestSell(krwUsdt, trimFloat(usdtToConv, 8), e.TimeInForce)
	conv, err := e.REST.CreateOrder(convReq)
	if err != nil {
		return []*exchange.OrderResp{buyFinal, sellFinal, conv}, err
	}
	convFinal, err := e.waitOrderFilled(conv.UUID, 6*time.Second)
	return []*exchange.OrderResp{buyFinal, sellFinal, convFinal}, err
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
	krwTotal := fmt.Sprintf("%.0f", maxKRW)
	buyUsdtReq := exchange.BestBuy(krwUsdt, krwTotal, e.TimeInForce)
	buyUsdt, err := e.REST.CreateOrder(buyUsdtReq)
	if err != nil {
		return []*exchange.OrderResp{buyUsdt}, err
	}

	buyUsdtFinal, err := e.waitOrderFilled(buyUsdt.UUID, 6*time.Second)
	if err != nil {
		return []*exchange.OrderResp{buyUsdtFinal}, err
	}

	usdtGot, _ := strconv.ParseFloat(buyUsdtFinal.ExecutedVolume, 64)
	if usdtGot <= 0 {
		return []*exchange.OrderResp{buyUsdtFinal}, fmt.Errorf("buy usdt executed_volume=0")
	}

	// Leg 1 완료 후 USDT 잔고 정산 대기
	usdtAvail, usdtLocked, waitErr := e.waitAvailPositive("USDT", 1500*time.Millisecond)
	if waitErr != nil {
		log.Printf("[trade] DirB: waitAvailPositive USDT: %v", waitErr)
	}

	safety := 0.003 // 0.3%
	usdtToSpend := usdtAvail
	if usdtToSpend > usdtGot {
		usdtToSpend = usdtGot
	}
	usdtToSpend = usdtToSpend * (1.0 - safety)

	if usdtToSpend <= 0 {
		return []*exchange.OrderResp{buyUsdtFinal},
			fmt.Errorf("no USDT available after step1 (avail=%.8f locked=%.8f got=%.8f)", usdtAvail, usdtLocked, usdtGot)
	}

	// ── Leg 2: USDT → ASSET ──
	usdtTotal := trimFloat(usdtToSpend, 6)
	buyAssetReq := exchange.BestBuy(usdtAsset, usdtTotal, e.TimeInForce)
	buyAsset, err := e.REST.CreateOrder(buyAssetReq)
	if err != nil {
		return []*exchange.OrderResp{buyUsdtFinal, buyAsset}, err
	}

	buyAssetFinal, err := e.waitOrderFilled(buyAsset.UUID, 6*time.Second)
	if err != nil {
		return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal}, err
	}

	assetVol, _ := strconv.ParseFloat(buyAssetFinal.ExecutedVolume, 64)
	if assetVol <= 0 {
		return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal}, fmt.Errorf("buy asset executed_volume=0")
	}

	// Leg 2 완료 후 ASSET 잔고 정산 대기
	assetAvail, _, waitErr2 := e.waitAvailPositive(asset, 2*time.Second)
	if waitErr2 != nil {
		log.Printf("[trade] DirB: asset avail wait: %v (fallback to executed_volume)", waitErr2)
		assetAvail = assetVol
	}
	volToSell := assetVol
	if assetAvail > 0 && assetAvail < volToSell {
		volToSell = assetAvail
	}

	// ── Leg 3: ASSET → KRW ──
	sellReq := exchange.BestSell(krwAsset, trimFloat(volToSell, 8), e.TimeInForce)
	sell, err := e.REST.CreateOrder(sellReq)
	if err != nil {
		return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sell}, err
	}

	sellFinal, err := e.waitOrderFilled(sell.UUID, 6*time.Second)
	return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sellFinal}, err
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
