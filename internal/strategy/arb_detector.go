package strategy

import (
	"math"
	"sort"

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
	// askPriceB, bidPriceB, costB, profitB, qtyB, okB := d.bestB(krwBook, usdtBook, usdtKrwBook)
	// askPriceB, bidPriceB, costB, profitB, qtyB, okB := 0, 0, 0, 0, 0, false

	// if !(okA || okB) {
	// 	return nil, false
	// }
	if !(okA) {
		return nil, false
	}

	bestAskPrice := askPriceA
	bestBidPrice := bidPriceA
	bestCost := costA
	bestDir := DirA
	bestProfit := profitA
	bestQty := qtyA
	// if profitB > bestProfit {
	// 	bestAskPrice = askPriceB
	// 	bestBidPrice = bidPriceB
	// 	bestCost = costB
	// 	bestDir = DirB
	// 	bestProfit = profitB
	// 	bestQty = qtyB
	// }

	if bestProfit < d.MinProfit {
		return nil, false
	}

	nowMs := timeutil.NowUnixMilli()
	return &Signal{
		TimeMs:  nowMs,
		TimeKST: timeutil.NowKSTString(),
		Opportunity: Opportunity{
			Asset:         asset,
			BestDirection: bestDir,
			BestAskPrice:  bestAskPrice,
			BestBidPrice:  bestBidPrice,
			BestCost:      bestCost,
			BestProfitKRW: bestProfit,
			BestQtyAsset:  bestQty,
			ProfitA:       profitA, QtyA: qtyA,
			ProfitB: 0, QtyB: 0,
			// ProfitB: profitB, QtyB: qtyB,
		},
	}, true
}

// A: KRW로 자산 매수 -> USDT로 매도 -> KRW로 환전
func (d *Detector) bestA(krwAsks []marketdata.OrderbookUnit, usdtBids []marketdata.OrderbookUnit, usdtKrwBids []marketdata.OrderbookUnit) (bestAskPrice float64, bestBidPrice float64, maxCost float64, bestProfit float64, bestQty float64, ok bool) {
	usdtKrwPrice := usdtKrwBids[0].BidPrice
	bestAskLv := -1
	bestBidLv := -1
	maxCost = 0.0
	bestQty = 0.0
	bestProfit = math.Inf(-1)
	for i, ask := range krwAsks {
		bidLv := -1
		qty := 0.0
		for j, bid := range usdtBids {
			if ask.AskPrice*(1+d.FeeKRW) < bid.BidPrice*usdtKrwPrice*(1-d.FeeUSDT)*(1-d.FeeKRW) {
				bidLv = j
			} else {
				break
			}
		}
		if bidLv == -1 {
			break
		} else {
			askQty := 0.0
			bidQty := 0.0
			for k, tempAsk := range krwAsks {
				if k > i {
					break
				}
				askQty += tempAsk.AskSize

			}
			for l, tempBid := range usdtBids {
				if l > bidLv {
					break
				}
				bidQty += tempBid.BidSize
			}
			qty = askQty
			if askQty > bidQty {
				qty = bidQty
			}
			askCost, ok1 := pricing.BuyCost(krwAsks, qty)
			bidProceeds, ok2 := pricing.SellProceeds(usdtBids, qty)
			if !(ok1 && ok2) {
				continue
			}
			askCost *= (1 + d.FeeKRW)
			bidProceeds *= (1 - d.FeeUSDT)
			krwFromUsdt, ok3 := pricing.SellProceeds(usdtKrwBids, bidProceeds) // sell USDT for KRW
			if !ok3 {
				continue
			}
			krwFromUsdt *= (1 - d.FeeKRW)
			profit := krwFromUsdt - askCost

			if bestProfit < profit {
				maxCost = askCost
				bestProfit = profit
				bestQty = qty
				bestAskLv = i
				bestBidLv = bidLv
			}
		}
	}

	if !math.IsInf(bestProfit, -1) {
		return krwAsks[bestAskLv].AskPrice, usdtBids[bestBidLv].BidPrice, maxCost, bestProfit, bestQty, true
	}
	return 0, 0, 0, 0, 0, false

	// maxQty := d.MaxQtyAsset
	// cands := candidateQtys(
	// 	pricing.QtyBreakpointsFromAsks(krwAsks, maxQty),
	// 	pricing.QtyBreakpointsFromBids(usdtBids, maxQty),
	// )
	// bestProfit = math.Inf(-1)
	// bestQty = 0

	// for _, q := range cands {
	// 	if q <= 1e-9 { continue }
	// 	krwCost, ok1 := pricing.BuyCost(krwAsks, q)
	// 	usdtProceeds, ok2 := pricing.SellProceeds(usdtBids, q)
	// 	if !(ok1 && ok2) { continue }

	// 	krwCost *= (1 + d.FeeKRW)
	// 	usdtProceeds *= (1 - d.FeeUSDT)

	// 	krwFromUsdt, ok3 := pricing.SellProceeds(usdtKrwBids, usdtProceeds) // sell USDT for KRW
	// 	if !ok3 { continue }
	// 	krwFromUsdt *= (1 - d.FeeKRW)

	// 	profit := krwFromUsdt - krwCost
	// 	if profit > bestProfit {
	// 		bestProfit = profit
	// 		bestQty = q
	// 	}
	// }

	// if !math.IsInf(bestProfit, -1) {
	// 	return bestProfit, bestQty, true
	// }
	// return 0, 0, false
}

// B: KRW로 USDT 매수 -> USDT로 자산 매수 -> KRW로 매도
func (d *Detector) bestB(krwBids []marketdata.OrderbookUnit, usdtAsks []marketdata.OrderbookUnit, usdtKrwAsks []marketdata.OrderbookUnit) (bestAskPrice float64, bestBidPrice float64, maxCost float64, bestProfit float64, bestQty float64, ok bool) {
	usdtKrwPrice := usdtKrwAsks[0].AskPrice
	bestAskLv := -1
	bestBidLv := -1
	bestQty = 0.0
	bestProfit = math.Inf(-1)
	maxCost = 0.0
	for i, ask := range usdtAsks {
		bidLv := -1
		qty := 0.0
		for j, bid := range krwBids {
			if ask.AskPrice*usdtKrwPrice*(1+d.FeeUSDT)*(1+d.FeeKRW) < bid.BidPrice*(1-d.FeeKRW) {
				bidLv = j
			} else {
				break
			}
		}
		if bidLv == -1 {
			break
		} else {
			askQty := 0.0
			bidQty := 0.0
			for k, tempAsk := range usdtAsks {
				if k > i {
					break
				}
				askQty += tempAsk.AskSize

			}
			for l, tempBid := range krwBids {
				if l > bidLv {
					break
				}
				bidQty += tempBid.BidSize
			}
			qty = askQty
			if askQty > bidQty {
				qty = bidQty
			}
			askCost, ok1 := pricing.BuyCost(usdtAsks, qty)
			bidProceeds, ok2 := pricing.SellProceeds(krwBids, qty)
			if !(ok1 && ok2) {
				continue
			}
			askCost *= (1 + d.FeeUSDT)
			bidProceeds *= (1 - d.FeeKRW)
			krwForUsdt, ok3 := pricing.BuyCost(usdtKrwAsks, askCost) // buy USDT with KRW
			if !ok3 {
				continue
			}
			krwForUsdt *= (1 + d.FeeKRW)
			profit := bidProceeds - krwForUsdt

			if bestProfit < profit {
				maxCost = krwForUsdt
				bestProfit = profit
				bestQty = qty
				bestAskLv = i
				bestBidLv = bidLv
			}
		}
	}

	if !math.IsInf(bestProfit, -1) {
		return usdtAsks[bestAskLv].AskPrice, krwBids[bestBidLv].BidPrice, maxCost, bestProfit, bestQty, true
	}
	return 0, 0, 0, 0, 0, false

	// 주의: 인자 이름만 헷갈리지 않게. 여기선:
	// - krwBids: KRW-ASSET의 "bids" (매도 시 사용)
	// - usdtAsks: USDT-ASSET의 "asks" (매수 시 사용)
	// - usdtKrwAsks: KRW-USDT의 "asks" (USDT 매수 시 사용)

	// maxQty := d.MaxQtyAsset
	// cands := candidateQtys(
	// 	pricing.QtyBreakpointsFromAsks(usdtAsks, maxQty),
	// 	pricing.QtyBreakpointsFromBids(krwBids, maxQty),
	// )
	// bestProfit = math.Inf(-1)
	// bestQty = 0

	// for _, q := range cands {
	// 	if q <= 1e-9 { continue }

	// 	usdtNeed, ok1 := pricing.BuyCost(usdtAsks, q) // buy asset with USDT
	// 	krwProceeds, ok2 := pricing.SellProceeds(krwBids, q) // sell asset for KRW
	// 	if !(ok1 && ok2) { continue }

	// 	usdtNeed *= (1 + d.FeeUSDT)
	// 	krwProceeds *= (1 - d.FeeKRW)

	// 	krwForUsdt, ok3 := pricing.BuyCost(usdtKrwAsks, usdtNeed) // buy USDT with KRW
	// 	if !ok3 { continue }
	// 	krwForUsdt *= (1 + d.FeeKRW)

	// 	profit := krwProceeds - krwForUsdt
	// 	if profit > bestProfit {
	// 		bestProfit = profit
	// 		bestQty = q
	// 	}
	// }

	// if !math.IsInf(bestProfit, -1) {
	// 	return bestProfit, bestQty, true
	// }
	// return 0, 0, false
}

func candidateQtys(a []float64, b []float64) []float64 {
	m := map[float64]bool{}
	for _, x := range a {
		m[roundQty(x)] = true
	}
	for _, x := range b {
		m[roundQty(x)] = true
	}
	out := make([]float64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Float64s(out)
	return out
}

func roundQty(x float64) float64 {
	// 너무 촘촘한 float 후보 방지 (1e-6 단위로 반올림)
	return math.Round(x*1e6) / 1e6
}
