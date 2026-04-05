package strategy

import (
	"math"

	"upbit-arb/internal/marketdata"
	"upbit-arb/internal/pricing"
	"upbit-arb/pkg/timeutil"
)

type Detector struct {
	FeeKRW      float64
	FeeUSDT     float64
	MinProfit   float64
	MaxQtyAsset float64
}

// 특정 asset에 대해 A/B 방향 각각의 "최적 qty"와 profit을 계산
func (d *Detector) EvaluateAsset(cache *marketdata.Cache, asset string) (*Signal, bool) {
	krwAsset := "KRW-" + asset
	usdtAsset := "USDT-" + asset
	krwUsdt := "KRW-USDT"

	krwBook, _, ok1 := cache.Get(krwAsset)
	usdtBook, _, ok2 := cache.Get(usdtAsset)
	usdtKrwBook, _, ok3 := cache.Get(krwUsdt)
	if !(ok1 && ok2 && ok3) {
		return nil, false
	}

	askPriceA, bidPriceA, costA, profitA, qtyA, okA := d.bestA(krwBook, usdtBook, usdtKrwBook)

	if !okA {
		return nil, false
	}

	if profitA < d.MinProfit {
		return nil, false
	}

	nowMs := timeutil.NowUnixMilli()
	return &Signal{
		TimeMs:  nowMs,
		TimeKST: timeutil.NowKSTString(),
		Opportunity: Opportunity{
			Asset:         asset,
			BestDirection: DirA,
			BestAskPrice:  askPriceA,
			BestBidPrice:  bidPriceA,
			BestCost:      costA,
			BestProfitKRW: profitA,
			BestQtyAsset:  qtyA,
			ProfitA:       profitA, QtyA: qtyA,
			ProfitB: 0, QtyB: 0,
		},
	}, true
}

// A: KRW로 자산 매수 → USDT로 매도 → KRW로 환전
//
// 최적화:
//   - prefix sum으로 누적 qty O(1) 조회 (기존 O(i) 내부 루프 제거)
//   - two-pointer로 bidLv 탐색 O(N+M) (기존 매 i마다 j=0 재스캔 O(N×M) 제거)
func (d *Detector) bestA(
	krwAsks []marketdata.OrderbookUnit,
	usdtBids []marketdata.OrderbookUnit,
	usdtKrwBids []marketdata.OrderbookUnit,
) (bestAskPrice, bestBidPrice, maxCost, bestProfit, bestQty float64, ok bool) {

	if len(krwAsks) == 0 || len(usdtBids) == 0 || len(usdtKrwBids) == 0 {
		return 0, 0, 0, 0, 0, false
	}
	usdtKrwPrice := usdtKrwBids[0].BidPrice

	// ── prefix sum: 누적 qty (O(N) 전처리) ──
	askCumQty := make([]float64, len(krwAsks))
	{
		s := 0.0
		for i, a := range krwAsks {
			s += a.AskSize
			askCumQty[i] = s
		}
	}
	bidCumQty := make([]float64, len(usdtBids))
	{
		s := 0.0
		for j, b := range usdtBids {
			s += b.BidSize
			bidCumQty[j] = s
		}
	}

	// ── two-pointer: bidLv는 단조 감소 (ask 상승 → profitable bid 감소) ──
	// bidLv = usdtBids[0..bidLv]까지가 profitable한 최대 인덱스
	bidLv := len(usdtBids) - 1

	bestProfit = math.Inf(-1)
	bestAskLv := -1
	bestBidLv := -1

	for i, ask := range krwAsks {
		// ask price가 올라갈수록 profitable bid 범위 줄어듦 → bidLv를 뒤에서 앞으로만 이동
		threshold := ask.AskPrice * (1 + d.FeeKRW)
		for bidLv >= 0 && usdtBids[bidLv].BidPrice*usdtKrwPrice*(1-d.FeeUSDT)*(1-d.FeeKRW) <= threshold {
			bidLv--
		}
		if bidLv < 0 {
			break // 이후 ask는 더 비싸므로 profitable bid 없음
		}

		// 누적 qty O(1) 조회 (기존 O(i) + O(bidLv) inner loop 대체)
		askQty := askCumQty[i]
		bQty := bidCumQty[bidLv]
		qty := math.Min(askQty, bQty)

		askCost, ok1 := pricing.BuyCost(krwAsks, qty)
		bidProceeds, ok2 := pricing.SellProceeds(usdtBids, qty)
		if !(ok1 && ok2) {
			continue
		}
		askCost *= (1 + d.FeeKRW)
		bidProceeds *= (1 - d.FeeUSDT)

		krwFromUsdt, ok3 := pricing.SellProceeds(usdtKrwBids, bidProceeds)
		if !ok3 {
			continue
		}
		krwFromUsdt *= (1 - d.FeeKRW)
		profit := krwFromUsdt - askCost

		if profit > bestProfit {
			maxCost = askCost
			bestProfit = profit
			bestQty = qty
			bestAskLv = i
			bestBidLv = bidLv
		}
	}

	if math.IsInf(bestProfit, -1) {
		return 0, 0, 0, 0, 0, false
	}
	return krwAsks[bestAskLv].AskPrice, usdtBids[bestBidLv].BidPrice, maxCost, bestProfit, bestQty, true
}

// B: KRW로 USDT 매수 → USDT로 자산 매수 → KRW로 매도
//
// 최적화: bestA와 동일한 prefix sum + two-pointer 적용
func (d *Detector) bestB(
	krwBids []marketdata.OrderbookUnit,
	usdtAsks []marketdata.OrderbookUnit,
	usdtKrwAsks []marketdata.OrderbookUnit,
) (bestAskPrice, bestBidPrice, maxCost, bestProfit, bestQty float64, ok bool) {

	if len(krwBids) == 0 || len(usdtAsks) == 0 || len(usdtKrwAsks) == 0 {
		return 0, 0, 0, 0, 0, false
	}
	usdtKrwPrice := usdtKrwAsks[0].AskPrice

	askCumQty := make([]float64, len(usdtAsks))
	{
		s := 0.0
		for i, a := range usdtAsks {
			s += a.AskSize
			askCumQty[i] = s
		}
	}
	bidCumQty := make([]float64, len(krwBids))
	{
		s := 0.0
		for j, b := range krwBids {
			s += b.BidSize
			bidCumQty[j] = s
		}
	}

	bidLv := len(krwBids) - 1
	bestProfit = math.Inf(-1)
	bestAskLv := -1
	bestBidLv := -1

	for i, ask := range usdtAsks {
		threshold := ask.AskPrice * usdtKrwPrice * (1 + d.FeeUSDT) * (1 + d.FeeKRW)
		for bidLv >= 0 && krwBids[bidLv].BidPrice*(1-d.FeeKRW) <= threshold {
			bidLv--
		}
		if bidLv < 0 {
			break
		}

		askQty := askCumQty[i]
		bQty := bidCumQty[bidLv]
		qty := math.Min(askQty, bQty)

		askCost, ok1 := pricing.BuyCost(usdtAsks, qty)
		bidProceeds, ok2 := pricing.SellProceeds(krwBids, qty)
		if !(ok1 && ok2) {
			continue
		}
		askCost *= (1 + d.FeeUSDT)
		bidProceeds *= (1 - d.FeeKRW)

		krwForUsdt, ok3 := pricing.BuyCost(usdtKrwAsks, askCost)
		if !ok3 {
			continue
		}
		krwForUsdt *= (1 + d.FeeKRW)
		profit := bidProceeds - krwForUsdt

		if profit > bestProfit {
			maxCost = krwForUsdt
			bestProfit = profit
			bestQty = qty
			bestAskLv = i
			bestBidLv = bidLv
		}
	}

	if math.IsInf(bestProfit, -1) {
		return 0, 0, 0, 0, 0, false
	}
	return usdtAsks[bestAskLv].AskPrice, krwBids[bestBidLv].BidPrice, maxCost, bestProfit, bestQty, true
}
