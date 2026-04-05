package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"upbit-arb/internal/config"
	"upbit-arb/internal/exchange"
)

type StepMetrics struct {
	CreateMs int64
	WaitMs   int64
	TotalMs  int64
}

type RunMetrics struct {
	Mode     string
	Started  time.Time
	TotalMs  int64
	Step1    StepMetrics
	Step2    StepMetrics
	Step3    StepMetrics
	KRWDelta float64
}

type Waiter interface {
	WaitOrderDone(uuid string, timeout time.Duration) (*exchange.OrderResp, error)
	WaitAvailPositive(currency string, timeout time.Duration) (avail float64, locked float64, err error)
	Close()
}

//
// --------------------
// POLL waiter (REST polling)
// --------------------
//

type PollWaiter struct {
	rest  *exchange.RESTClient
	sleep time.Duration
}

func NewPollWaiter(rest *exchange.RESTClient, sleep time.Duration) *PollWaiter {
	return &PollWaiter{rest: rest, sleep: sleep}
}

func (w *PollWaiter) WaitOrderDone(uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		od, err := w.rest.GetOrder(uuid)
		if err != nil {
			lastErr = err

			// 주문 직후 잠깐 order_not_found가 날 수 있으므로 재시도
			msg := err.Error()
			if strings.Contains(msg, "order_not_found") || strings.Contains(msg, `"name":"order_not_found"`) {
				time.Sleep(w.sleep)
				continue
			}
			return nil, err
		}

		if od.State == "done" || od.State == "cancel" {
			return od, nil
		}
		time.Sleep(w.sleep)
	}

	if lastErr != nil {
		return nil, fmt.Errorf("timeout waiting order %s (last err: %v)", uuid, lastErr)
	}
	return nil, fmt.Errorf("timeout waiting order %s", uuid)
}

func (w *PollWaiter) WaitAvailPositive(currency string, timeout time.Duration) (float64, float64, error) {
	deadline := time.Now().Add(timeout)
	var lastAvail, lastLocked float64
	for time.Now().Before(deadline) {
		ac, err := w.rest.GetAccounts()
		if err == nil {
			lastAvail = findAvail(ac, currency)
			lastLocked = findLocked(ac, currency)
			if lastAvail > 0 {
				return lastAvail, lastLocked, nil
			}
		}
		time.Sleep(w.sleep)
	}
	return lastAvail, lastLocked, fmt.Errorf("avail not positive for %s within %s", currency, timeout)
}

func (w *PollWaiter) Close() {}

//
// --------------------
// Private WS waiter (event driven)
// --------------------
//

type PrivateWSWaiter struct {
	rest *exchange.RESTClient

	mu       sync.Mutex
	orderCh  map[string]chan orderEvent
	avail    map[string]float64
	locked   map[string]float64
	availSig map[string]chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}
}

type orderEvent struct {
	UUID  string
	State string
	Raw   json.RawMessage
}

type myOrderMsg struct {
	Type      string `json:"type"`
	Code      string `json:"code"`
	UUID      string `json:"uuid"`
	AskBid    string `json:"ask_bid"`
	OrderType string `json:"order_type"`
	State     string `json:"state"`
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

func NewPrivateWSWaiter(cfg *config.Config, rest *exchange.RESTClient) *PrivateWSWaiter {
	w := &PrivateWSWaiter{
		rest:     rest,
		orderCh:  map[string]chan orderEvent{},
		avail:    map[string]float64{},
		locked:   map[string]float64{},
		availSig: map[string]chan struct{}{},

		stopCh: make(chan struct{}),
	}

	pws := &exchange.PrivateWSClient{
		URL:          cfg.Upbit.PrivateWsURL,
		PingInterval: time.Duration(cfg.Upbit.PingIntervalSec) * time.Second,
		Backoff:      time.Duration(cfg.Upbit.ReconnectBackoffMs) * time.Millisecond,
		MyOrderCodes: cfg.PrivateStream.MyOrderCodes,
		AuthFunc: func() (string, error) {
			return exchange.BuildJWT(cfg.Upbit.AccessKey, cfg.Upbit.SecretKey, "")
		},
		OnMyAsset: func(raw json.RawMessage) {
			var m myAssetMsg
			if err := json.Unmarshal(raw, &m); err != nil {
				return
			}

			w.mu.Lock()
			defer w.mu.Unlock()

			for _, a := range m.Assets {
				cur := strings.ToUpper(strings.TrimSpace(a.Currency))
				w.avail[cur] = a.Balance
				w.locked[cur] = a.Locked

				if ch, ok := w.availSig[cur]; ok {
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
			ev := orderEvent{
				UUID:  m.UUID,
				State: m.State,
				Raw:   raw,
			}

			w.mu.Lock()
			ch := w.orderCh[m.UUID]
			w.mu.Unlock()

			if ch != nil {
				select {
				case ch <- ev:
				default:
				}
			}
		},
	}

	go pws.Run()

	// 초기 잔고 스냅샷 1회 로드 (WS 이벤트 오기 전에도 avail map 채우기)
	if ac, err := rest.GetAccounts(); err == nil {
		w.mu.Lock()
		for _, a := range ac {
			cur := strings.ToUpper(strings.TrimSpace(a.Currency))
			bal, _ := strconv.ParseFloat(a.Balance, 64)
			lk, _ := strconv.ParseFloat(a.Locked, 64)
			w.avail[cur] = bal
			w.locked[cur] = lk
		}
		w.mu.Unlock()
	}

	return w
}

func (w *PrivateWSWaiter) Close() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

func (w *PrivateWSWaiter) WaitOrderDone(uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	ch := make(chan orderEvent, 8)

	w.mu.Lock()
	w.orderCh[uuid] = ch
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		delete(w.orderCh, uuid)
		w.mu.Unlock()
	}()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case <-w.stopCh:
			return nil, fmt.Errorf("pws stopped")
		case <-deadline.C:
			// timeout이면 REST 보정
			return w.rest.GetOrder(uuid)
		case ev := <-ch:
			if ev.State == "done" || ev.State == "cancel" {
				return w.rest.GetOrder(uuid)
			}
		}
	}
}

func (w *PrivateWSWaiter) WaitAvailPositive(currency string, timeout time.Duration) (float64, float64, error) {
	cur := strings.ToUpper(currency)

	w.mu.Lock()
	sig, ok := w.availSig[cur]
	if !ok {
		sig = make(chan struct{}, 8)
		w.availSig[cur] = sig
	}
	w.mu.Unlock()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		w.mu.Lock()
		av := w.avail[cur]
		lk := w.locked[cur]
		w.mu.Unlock()

		if av > 0 {
			return av, lk, nil
		}

		select {
		case <-w.stopCh:
			return av, lk, fmt.Errorf("pws stopped")
		case <-deadline.C:
			// timeout이면 REST로 보정
			ac, err := w.rest.GetAccounts()
			if err != nil {
				return 0, 0, err
			}
			return findAvail(ac, cur), findLocked(ac, cur), fmt.Errorf("timeout waiting avail positive for %s", cur)
		case <-sig:
			// loop
		}
	}
}

//
// --------------------
// main benchmark
// --------------------
//

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config path")
	krw := flag.Float64("krw", 10000, "KRW amount to spend in step1")
	tif := flag.String("tif", "ioc", "time_in_force: ioc or fok")
	mode := flag.String("mode", "both", "poll | pws | both")
	repeat := flag.Int("repeat", 3, "number of runs per mode")
	pollSleep := flag.Duration("poll_sleep", 80*time.Millisecond, "polling interval for poll mode")
	stepTimeout := flag.Duration("step_timeout", 10*time.Second, "order done/cancel wait timeout per step")
	availTimeout := flag.Duration("avail_timeout", 4*time.Second, "wait USDT avail timeout (step3)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}
	if cfg.Upbit.AccessKey == "" || cfg.Upbit.SecretKey == "" {
		log.Fatalf("missing UPBIT keys: set env UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY")
	}

	rest := exchange.NewREST(cfg.Upbit.RESTBaseURL, cfg.Upbit.AccessKey, cfg.Upbit.SecretKey)

	runOne := func(waiter Waiter, modeName string) RunMetrics {
		m := RunMetrics{Mode: modeName, Started: time.Now()}

		ac0, _ := rest.GetAccounts()
		krw0 := findAvail(ac0, "KRW")

		startAll := time.Now()

		// --- Step1 KRW -> BTC
		s1Start := time.Now()
		req1 := exchange.BestBuy("KRW-BTC", fmt.Sprintf("%.0f", *krw), *tif)

		tCreate := time.Now()
		o1, err := rest.CreateOrder(req1)
		m.Step1.CreateMs = time.Since(tCreate).Milliseconds()
		if err != nil {
			log.Fatalf("[%s step1] CreateOrder failed: %v", modeName, err)
		}

		od1, err := waiter.WaitOrderDone(o1.UUID, *stepTimeout)
		m.Step1.WaitMs = time.Since(tCreate).Milliseconds() - m.Step1.CreateMs
		m.Step1.TotalMs = time.Since(s1Start).Milliseconds()
		if err != nil {
			log.Fatalf("[%s step1] wait failed: %v", modeName, err)
		}

		btcGot, _ := strconv.ParseFloat(od1.ExecutedVolume, 64)
		if btcGot <= 0 {
			log.Fatalf("[%s step1] executed_volume=0 state=%s", modeName, od1.State)
		}

		// --- Step2 BTC -> USDT
		s2Start := time.Now()
		req2 := exchange.BestSell("USDT-BTC", trimFloat(btcGot, 8), *tif)

		tCreate = time.Now()
		o2, err := rest.CreateOrder(req2)
		m.Step2.CreateMs = time.Since(tCreate).Milliseconds()
		if err != nil {
			log.Fatalf("[%s step2] CreateOrder failed: %v", modeName, err)
		}

		_, err = waiter.WaitOrderDone(o2.UUID, *stepTimeout)
		m.Step2.WaitMs = time.Since(tCreate).Milliseconds() - m.Step2.CreateMs
		m.Step2.TotalMs = time.Since(s2Start).Milliseconds()
		if err != nil {
			log.Fatalf("[%s step2] wait failed: %v", modeName, err)
		}

		// --- Step3 USDT -> KRW
		s3Start := time.Now()
		usdtAvail, usdtLocked, _ := waiter.WaitAvailPositive("USDT", *availTimeout)

		safety := 0.003
		usdtToSell := usdtAvail * (1.0 - safety)
		if usdtToSell <= 0 {
			log.Fatalf("[%s step3] no USDT available (avail=%.6f locked=%.6f)", modeName, usdtAvail, usdtLocked)
		}

		req3 := exchange.BestSell("KRW-USDT", trimFloat(usdtToSell, 6), *tif)
		tCreate = time.Now()
		o3, err := rest.CreateOrder(req3)
		m.Step3.CreateMs = time.Since(tCreate).Milliseconds()
		if err != nil {
			log.Fatalf("[%s step3] CreateOrder failed: %v", modeName, err)
		}

		_, err = waiter.WaitOrderDone(o3.UUID, *stepTimeout)
		m.Step3.WaitMs = time.Since(tCreate).Milliseconds() - m.Step3.CreateMs
		m.Step3.TotalMs = time.Since(s3Start).Milliseconds()
		if err != nil {
			log.Fatalf("[%s step3] wait failed: %v", modeName, err)
		}

		m.TotalMs = time.Since(startAll).Milliseconds()

		acf, _ := rest.GetAccounts()
		krwF := findAvail(acf, "KRW")
		m.KRWDelta = krwF - krw0

		return m
	}

	runMode := func(modeName string) []RunMetrics {
		var waiter Waiter
		if modeName == "poll" {
			waiter = NewPollWaiter(rest, *pollSleep)
		} else {
			if !cfg.PrivateStream.Enabled {
				log.Fatalf("config: PrivateStream.Enabled must be true for pws mode")
			}
			waiter = NewPrivateWSWaiter(cfg, rest)
			time.Sleep(800 * time.Millisecond) // WS 초기화 여유
		}
		defer waiter.Close()

		out := make([]RunMetrics, 0, *repeat)
		for i := 0; i < *repeat; i++ {
			m := runOne(waiter, modeName)
			out = append(out, m)
			fmt.Printf("[%s #%d] total=%dms (s1=%d s2=%d s3=%d) KRWΔ=%.0f\n",
				modeName, i+1, m.TotalMs, m.Step1.TotalMs, m.Step2.TotalMs, m.Step3.TotalMs, m.KRWDelta)

			time.Sleep(600 * time.Millisecond)
		}
		return out
	}

	var pollRes, pwsRes []RunMetrics
	switch strings.ToLower(*mode) {
	case "poll":
		pollRes = runMode("poll")
	case "pws":
		pwsRes = runMode("pws")
	default:
		pollRes = runMode("poll")
		fmt.Println("----")
		pwsRes = runMode("pws")
	}

	if len(pollRes) > 0 {
		printSummary("poll", pollRes)
	}
	if len(pwsRes) > 0 {
		printSummary("pws", pwsRes)
	}
}

func printSummary(name string, runs []RunMetrics) {
	get := func(f func(RunMetrics) int64) (avg, p50, p95 int64) {
		arr := make([]int64, 0, len(runs))
		var sum int64
		for _, r := range runs {
			v := f(r)
			arr = append(arr, v)
			sum += v
		}
		sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
		avg = sum / int64(len(arr))
		p50 = arr[int(math.Floor(0.50*float64(len(arr)-1)))]
		p95 = arr[int(math.Floor(0.95*float64(len(arr)-1)))]
		return
	}

	tAvg, tP50, tP95 := get(func(r RunMetrics) int64 { return r.TotalMs })
	s1Avg, s1P50, s1P95 := get(func(r RunMetrics) int64 { return r.Step1.TotalMs })
	s2Avg, s2P50, s2P95 := get(func(r RunMetrics) int64 { return r.Step2.TotalMs })
	s3Avg, s3P50, s3P95 := get(func(r RunMetrics) int64 { return r.Step3.TotalMs })

	fmt.Printf("\n[%s summary] runs=%d\n", name, len(runs))
	fmt.Printf(" total: avg=%dms p50=%dms p95=%dms\n", tAvg, tP50, tP95)
	fmt.Printf(" step1: avg=%dms p50=%dms p95=%dms\n", s1Avg, s1P50, s1P95)
	fmt.Printf(" step2: avg=%dms p50=%dms p95=%dms\n", s2Avg, s2P50, s2P95)
	fmt.Printf(" step3: avg=%dms p50=%dms p95=%dms\n", s3Avg, s3P50, s3P95)
}

// -------- balance helpers (available vs locked)

func findAvail(accts []exchange.Account, currency string) float64 {
	cur := strings.ToUpper(currency)
	for _, a := range accts {
		if strings.ToUpper(a.Currency) == cur {
			v, _ := strconv.ParseFloat(a.Balance, 64)
			return v
		}
	}
	return 0
}

func findLocked(accts []exchange.Account, currency string) float64 {
	cur := strings.ToUpper(currency)
	for _, a := range accts {
		if strings.ToUpper(a.Currency) == cur {
			v, _ := strconv.ParseFloat(a.Locked, 64)
			return v
		}
	}
	return 0
}

func trimFloat(x float64, decimals int) string {
	fmtStr := "%." + strconv.Itoa(decimals) + "f"
	s := fmt.Sprintf(fmtStr, x)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}
