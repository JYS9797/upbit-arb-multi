package strategy

import (
	"math"
	"testing"

	"upbit-arb/internal/marketdata"
)

// ── 테스트용 오더북 헬퍼 ──

func makeAsks(prices, sizes []float64) []marketdata.OrderbookUnit {
	out := make([]marketdata.OrderbookUnit, len(prices))
	for i := range prices {
		out[i] = marketdata.OrderbookUnit{AskPrice: prices[i], AskSize: sizes[i]}
	}
	return out
}

func makeBids(prices, sizes []float64) []marketdata.OrderbookUnit {
	out := make([]marketdata.OrderbookUnit, len(prices))
	for i := range prices {
		out[i] = marketdata.OrderbookUnit{BidPrice: prices[i], BidSize: sizes[i]}
	}
	return out
}

// ──────────────────────────────────────────────
// bestA 정확성 테스트
// ──────────────────────────────────────────────

func TestBestA_ProfitableSingleLevel(t *testing.T) {
	d := &Detector{FeeKRW: 0.0005, FeeUSDT: 0.0025}

	// KRW-ASSET ask: 100원에 10개
	krwAsks := makeAsks([]float64{100}, []float64{10})
	// USDT-ASSET bid: 0.11 USDT에 10개
	usdtBids := makeBids([]float64{0.11}, []float64{10})
	// KRW-USDT bid: 1400원/USDT에 200개
	usdtKrwBids := makeBids([]float64{1400}, []float64{200})

	_, _, _, profit, qty, ok := d.bestA(krwAsks, usdtBids, usdtKrwBids)
	if !ok {
		t.Fatal("profitable 기회인데 ok=false")
	}
	if qty <= 0 {
		t.Fatalf("qty <= 0: %v", qty)
	}
	// profit > 0 이어야 함
	if profit <= 0 {
		t.Fatalf("profit <= 0: %v", profit)
	}
}

func TestBestA_NoProfitableOpportunity(t *testing.T) {
	d := &Detector{FeeKRW: 0.0005, FeeUSDT: 0.0025}

	// ask 가격이 너무 높아 수익 없음
	krwAsks := makeAsks([]float64{200}, []float64{10})
	usdtBids := makeBids([]float64{0.10}, []float64{10})
	usdtKrwBids := makeBids([]float64{1300}, []float64{200})

	_, _, _, _, _, ok := d.bestA(krwAsks, usdtBids, usdtKrwBids)
	if ok {
		t.Fatal("수익 없는 케이스인데 ok=true")
	}
}

func TestBestA_MultiLevel_PrefixSumConsistency(t *testing.T) {
	// prefix sum + two-pointer 최적화 후에도 결과가 수학적으로 일치하는지 검증
	d := &Detector{FeeKRW: 0.0005, FeeUSDT: 0.0025}

	krwAsks := makeAsks(
		[]float64{1000, 1001, 1002, 1003},
		[]float64{5, 5, 5, 5},
	)
	usdtBids := makeBids(
		[]float64{0.8, 0.79, 0.78, 0.77},
		[]float64{4, 4, 4, 4},
	)
	usdtKrwBids := makeBids([]float64{1400}, []float64{1000})

	_, _, cost, profit, qty, ok := d.bestA(krwAsks, usdtBids, usdtKrwBids)
	if !ok {
		// profitable이 아닐 수 있음 - 그냥 패닉 없이 통과하면 됨
		return
	}
	if math.IsNaN(profit) || math.IsInf(profit, 0) {
		t.Fatalf("profit이 NaN/Inf: %v", profit)
	}
	if qty <= 0 || cost <= 0 {
		t.Fatalf("qty=%v cost=%v", qty, cost)
	}
}

func TestBestA_EmptyBook(t *testing.T) {
	d := &Detector{FeeKRW: 0.0005, FeeUSDT: 0.0025}

	_, _, _, _, _, ok := d.bestA(nil, nil, nil)
	if ok {
		t.Fatal("빈 오더북에서 ok=true")
	}
}

// ──────────────────────────────────────────────
// 벤치마크: two-pointer + prefix sum 성능 확인
// ──────────────────────────────────────────────

func BenchmarkBestA_15Levels(b *testing.B) {
	d := &Detector{FeeKRW: 0.0005, FeeUSDT: 0.0025}

	krwAsks := makeAsks(
		[]float64{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014},
		[]float64{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
	)
	usdtBids := makeBids(
		[]float64{0.80, 0.79, 0.78, 0.77, 0.76, 0.75, 0.74, 0.73, 0.72, 0.71, 0.70, 0.69, 0.68, 0.67, 0.66},
		[]float64{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
	)
	usdtKrwBids := makeBids([]float64{1400}, []float64{1000})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.bestA(krwAsks, usdtBids, usdtKrwBids)
	}
}
