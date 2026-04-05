package pricing

import (
	"math"
	"upbit-arb/internal/marketdata"
)

// asks로 qty(자산)를 살 때 드는 통화 비용
func BuyCost(asks []marketdata.OrderbookUnit, qty float64) (float64, bool) {
	remain := qty
	cost := 0.0
	for _, lv := range asks {
		if remain <= 0 { break }
		take := math.Min(remain, lv.AskSize)
		cost += take * lv.AskPrice
		remain -= take
	}
	if remain > 1e-9 { return 0, false }
	return cost, true
}

// bids로 qty(자산)를 팔 때 얻는 통화 수익
func SellProceeds(bids []marketdata.OrderbookUnit, qty float64) (float64, bool) {
	remain := qty
	proceeds := 0.0
	for _, lv := range bids {
		if remain <= 0 { break }
		take := math.Min(remain, lv.BidSize)
		proceeds += take * lv.BidPrice
		remain -= take
	}
	if remain > 1e-9 { return 0, false }
	return proceeds, true
}

// levels에서 "자산 기준" 누적 가능 qty 후보(레벨 경계) 생성
func QtyBreakpointsFromAsks(asks []marketdata.OrderbookUnit, maxQty float64) []float64 {
	out := []float64{}
	sum := 0.0
	for _, lv := range asks {
		sum += lv.AskSize
		if sum > maxQty { sum = maxQty }
		out = append(out, sum)
		if sum >= maxQty-1e-9 { break }
	}
	return out
}

func QtyBreakpointsFromBids(bids []marketdata.OrderbookUnit, maxQty float64) []float64 {
	out := []float64{}
	sum := 0.0
	for _, lv := range bids {
		sum += lv.BidSize
		if sum > maxQty { sum = maxQty }
		out = append(out, sum)
		if sum >= maxQty-1e-9 { break }
	}
	return out
}
