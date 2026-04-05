package app

import (
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"upbit-arb/internal/config"
	"upbit-arb/internal/exchange"
	"upbit-arb/internal/executor"
	"upbit-arb/internal/marketdata"
	"upbit-arb/internal/notify"
	"upbit-arb/internal/state"
	"upbit-arb/internal/storage"
	"upbit-arb/internal/strategy"
)

type App struct {
	Cfg   *config.Config
	Cache *marketdata.Cache
	WS    *marketdata.WSClient

	Assets []string // 모니터링/평가 대상 자산 심볼들

	Detector *strategy.Detector
	Notify   *notify.Console
	Exec     executor.Executor
	Saver    *storage.JSONLWriter

	REST        *exchange.RESTClient
	Balances    *state.BalanceStore
	TradeWriter *storage.JSONLWriter

	lastPrint int64
	lastEval  int64
}

func New(cfg *config.Config) *App {
	cache := marketdata.NewCache()

	// 1) 자산 선정 + 구독 코드 빌드
	rest := marketdata.NewREST(cfg.Upbit.RESTBaseURL)

	markets, err := rest.ListMarkets(false)
	if err != nil {
		log.Fatalf("market/all 실패: %v", err)
	}

	assets := selectAssets(cfg, rest, markets)
	codes := buildSubscribeCodes(cfg, markets, assets)

	ws := &marketdata.WSClient{
		URL:              cfg.Upbit.WsURL,
		Codes:            codes,
		TopN:             cfg.Markets.TopNLevels,
		PingInterval:     time.Duration(cfg.Upbit.PingIntervalSec) * time.Second,
		ReconnectBackoff: time.Duration(cfg.Upbit.ReconnectBackoffMs) * time.Millisecond,
		Cache:            cache,
	}

	det := &strategy.Detector{
		FeeKRW:      cfg.TradeModel.FeeKRW,
		FeeUSDT:     cfg.TradeModel.FeeUSDT,
		MinProfit:   cfg.Signal.MinProfitKRW,
		MaxQtyAsset: cfg.TradeModel.MaxQtyAsset,
	}

	var saver *storage.JSONLWriter
	if cfg.Storage.Enabled {
		w, err := storage.NewJSONLWriter(cfg.Storage.OutDir, cfg.Storage.JSONLFilename)
		if err != nil {
			log.Printf("스토리지 초기화 실패(스냅샷 저장 비활성화): %v", err)
		} else {
			saver = w
			log.Printf("스냅샷 저장 활성화: %s/%s", cfg.Storage.OutDir, cfg.Storage.JSONLFilename)
		}
	}

	// --- trading / executor setup ---
	balances := state.NewBalanceStore()
	restTrade := exchange.NewREST(cfg.Upbit.RESTBaseURL, cfg.Upbit.AccessKey, cfg.Upbit.SecretKey)

	var tradeWriter *storage.JSONLWriter
	if cfg.Storage.Enabled {
		w2, err := storage.NewJSONLWriter(cfg.Storage.OutDir, "trades.jsonl")
		if err != nil {
			log.Printf("trade 로그 초기화 실패: %v", err)
		} else {
			tradeWriter = w2
			log.Printf("trade 로그 저장: %s/%s", cfg.Storage.OutDir, "trades.jsonl")
		}
	}

	log.Printf("선정 자산(%d): %s", len(assets), strings.Join(assets, ","))
	log.Printf("구독 코드(%d): %s", len(codes), strings.Join(codes, ","))

	return &App{
		Cfg:      cfg,
		Cache:    cache,
		WS:       ws,
		Assets:   assets,
		Detector: det,
		Notify:   &notify.Console{},

		Exec: &executor.UpbitExecutor{
			Enabled:          cfg.Trading.Enabled,
			TimeInForce:      cfg.Trading.TimeInForce,
			MaxKRWPerTrade:   cfg.Trading.MaxKRWPerTrade,
			SafetyKRWReserve: cfg.Trading.SafetyKRWReserve,

			REST:        restTrade,
			Cache:       cache,
			Balances:    balances,
			TradeWriter: tradeWriter,

			UsePrivateWS:     cfg.PrivateStream.Enabled,
			PrivateWSURL:     cfg.Upbit.PrivateWsURL,
			AccessKey:        cfg.Upbit.AccessKey,
			SecretKey:        cfg.Upbit.SecretKey,
			PingInterval:     time.Duration(cfg.Upbit.PingIntervalSec) * time.Second,
			ReconnectBackoff: time.Duration(cfg.Upbit.ReconnectBackoffMs) * time.Millisecond,
			MyOrderCodes:     cfg.PrivateStream.MyOrderCodes,
		},

		Saver: saver,

		REST:        restTrade,
		Balances:    balances,
		TradeWriter: tradeWriter,
	}
}

func (a *App) Run() {
	minPrint := time.Duration(a.Cfg.Signal.MinPrintIntervalMs) * time.Millisecond
	// 메시지마다 평가하지 않고 최소 간격마다만 수행
	evalEvery := 100 * time.Millisecond

	go a.WS.Run(nil)

	for {
		now := time.Now().UnixMilli()
		lastEval := atomic.LoadInt64(&a.lastEval)
		if now-lastEval < evalEvery.Milliseconds() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		atomic.StoreInt64(&a.lastEval, now)

		// 각 자산별 기회 평가 → 가장 좋은 것 1개만 출력/저장
		bestSig := (*strategy.Signal)(nil)
		for _, asset := range a.Assets {
			sig, ok := a.Detector.EvaluateAsset(a.Cache, asset)
			if !ok {
				continue
			}
			a.Notify.Print(sig)
			if bestSig == nil || sig.Opportunity.BestProfitKRW > bestSig.Opportunity.BestProfitKRW {
				bestSig = sig
			}
		}

		if bestSig == nil {
			continue
		}

		lastPrint := atomic.LoadInt64(&a.lastPrint)
		if now-lastPrint < minPrint.Milliseconds() {
			continue
		}
		atomic.StoreInt64(&a.lastPrint, now)

		a.Notify.Print(bestSig)

		if a.Saver != nil {
			rec := a.buildSnapshot(bestSig)
			if err := a.Saver.Write(rec); err != nil {
				log.Printf("스냅샷 저장 실패: %v", err)
			}
		}

		_ = a.Exec.OnSignal(bestSig)
	}
}

func (a *App) buildSnapshot(sig *strategy.Signal) *storage.SnapshotRecord {
	asset := sig.Opportunity.Asset
	legs := []string{"KRW-" + asset, "USDT-" + asset, "KRW-USDT"}
	snap := a.Cache.Snapshot(legs)

	outLegs := make([]storage.LegSnapshot, 0, len(legs))
	for _, code := range legs {
		v, ok := snap[code]
		if !ok {
			continue
		}
		outLegs = append(outLegs, storage.LegSnapshot{
			Code:      code,
			Timestamp: v.TS,
			TopN:      a.Cfg.Markets.TopNLevels,
			Units:     v.Units,
		})
	}

	sort.Slice(outLegs, func(i, j int) bool { return outLegs[i].Code < outLegs[j].Code })

	op := sig.Opportunity
	return &storage.SnapshotRecord{
		TimeMs:        sig.TimeMs,
		TimeKST:       sig.TimeKST,
		Asset:         asset,
		BestDirection: string(op.BestDirection),
		BestProfitKRW: op.BestProfitKRW,
		BestQtyAsset:  op.BestQtyAsset,
		ProfitA:       op.ProfitA,
		QtyA:          op.QtyA,
		ProfitB:       op.ProfitB,
		QtyB:          op.QtyB,
		Legs:          outLegs,
	}
}

// ----- asset selection / subscribe building -----

func selectAssets(cfg *config.Config, rest *marketdata.RESTClient, markets []marketdata.MarketInfo) []string {
	mode := strings.ToLower(strings.TrimSpace(cfg.Markets.Mode))
	if mode == "static" {
		out := make([]string, 0, len(cfg.Markets.StaticAssets))
		for _, a := range cfg.Markets.StaticAssets {
			a = strings.TrimSpace(strings.ToUpper(a))
			if a != "" {
				out = append(out, a)
			}
		}
		return unique(out)
	}

	// top_krw: KRW 마켓 티커로 24h 거래대금 상위 N 계산
	krwMarkets := []string{}
	for _, m := range markets {
		if strings.HasPrefix(m.Market, "KRW-") {
			krwMarkets = append(krwMarkets, m.Market)
		}
	}

	tickers := []marketdata.Ticker{}
	chunk := 100
	for i := 0; i < len(krwMarkets); i += chunk {
		end := i + chunk
		if end > len(krwMarkets) {
			end = len(krwMarkets)
		}
		part, err := rest.Tickers(krwMarkets[i:end])
		if err != nil {
			log.Printf("ticker 실패(일부): %v", err)
			continue
		}
		tickers = append(tickers, part...)
	}
	assets := marketdata.TopKRWAssetsByValue24h(markets, tickers, cfg.Markets.TopKRWN)
	return unique(assets)
}

func buildSubscribeCodes(cfg *config.Config, markets []marketdata.MarketInfo, assets []string) []string {
	avail := map[string]bool{}
	for _, m := range markets {
		avail[m.Market] = true
	}

	codes := []string{}
	// 변환 마켓은 항상 포함
	if avail["KRW-USDT"] {
		codes = append(codes, "KRW-USDT")
	}

	for _, asset := range assets {
		if cfg.Markets.IncludeKRW {
			code := "KRW-" + asset
			if avail[code] {
				codes = append(codes, code)
			}
		}
		if cfg.Markets.IncludeUSDT {
			code := "USDT-" + asset
			if avail[code] {
				codes = append(codes, code)
			}
		}
		if cfg.Markets.IncludeBTC {
			code := "BTC-" + asset
			if avail[code] {
				codes = append(codes, code)
			}
		}
	}

	return unique(codes)
}

func unique(in []string) []string {
	m := map[string]bool{}
	out := []string{}
	for _, s := range in {
		if s == "" {
			continue
		}
		if !m[s] {
			m[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
