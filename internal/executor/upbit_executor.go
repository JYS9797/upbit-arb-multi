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
	e.mu.Lock()
	defer e.mu.Unlock()

	// private WS lazy init
	e.ensurePrivateWSLocked()

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

	sellReq := exchange.BestSell(usdtAsset, trimFloat(volAsset, 8), e.TimeInForce)
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

	convReq := exchange.BestSell(krwUsdt, trimFloat(usdtFunds, 8), e.TimeInForce)
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

	// Step1: KRW -> USDT
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

	// ✅ private WS 기반으로 USDT available 대기
	var usdtAvail, usdtLocked float64
	usdtAvail, usdtLocked, err = e.waitAvailPositive("USDT", 1500*time.Millisecond)
	if err != nil {
		// fallback 성격이라 로그만 남기고 진행
		log.Printf("[trade] waitAvailPositive fallback: %v", err)
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

	// Step2: USDT -> ASSET
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

	// Step3: ASSET -> KRW
	sellReq := exchange.BestSell(krwAsset, trimFloat(assetVol, 8), e.TimeInForce)
	sell, err := e.REST.CreateOrder(sellReq)
	if err != nil {
		return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sell}, err
	}

	sellFinal, err := e.waitOrderFilled(sell.UUID, 6*time.Second)
	return []*exchange.OrderResp{buyUsdtFinal, buyAssetFinal, sellFinal}, err
}

func (e *UpbitExecutor) waitOrderFilled(uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	// private WS 사용 시 우선 이벤트 기반으로 대기
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

func (e *UpbitExecutor) ensurePrivateWSLocked() {
	if !e.UsePrivateWS {
		return
	}
	e.pwsOnce.Do(func() {
		e.orderCh = map[string]chan orderEvent{}
		e.orderLast = map[string]orderEvent{}
		e.availSig = map[string]chan struct{}{}
		e.stopCh = make(chan struct{})

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

				e.mu.Lock()
				defer e.mu.Unlock()

				for _, a := range m.Assets {
					e.Balances.Set(strings.ToUpper(strings.TrimSpace(a.Currency)), a.Balance)
					cur := strings.ToUpper(strings.TrimSpace(a.Currency))
					if ch, ok := e.availSig[cur]; ok {
						select {
						case ch <- struct{}{}:
						default:
						}
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

				e.mu.Lock()
				e.orderLast[m.UUID] = ev
				ch := e.orderCh[m.UUID]
				e.mu.Unlock()

				if ch != nil {
					select {
					case ch <- ev:
					default:
					}
				}
			},
		}

		go pws.Run()
		e.pwsReady = true
	})
}

func (e *UpbitExecutor) waitOrderDonePWS(uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	e.ensurePrivateWSLocked()
	if !e.pwsReady {
		return nil, fmt.Errorf("private ws not ready")
	}

	ch := make(chan orderEvent, 8)

	e.orderCh[uuid] = ch
	if ev, ok := e.orderLast[uuid]; ok && ev.Resp != nil {
		if ev.Resp.State == "done" || ev.Resp.State == "cancel" {
			delete(e.orderCh, uuid)
			return ev.Resp, nil
		}
	}

	defer delete(e.orderCh, uuid)

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
		e.ensurePrivateWSLocked()

		sig, ok := e.availSig[cur]
		if !ok {
			sig = make(chan struct{}, 8)
			e.availSig[cur] = sig
		}

		deadline := time.NewTimer(timeout)
		defer deadline.Stop()

		for {
			avail = e.Balances.Get(cur)
			if avail > 0 {
				return avail, 0, nil
			}

			select {
			case <-deadline.C:
				break
			case <-sig:
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
