package storage

import "upbit-arb/internal/marketdata"

type SnapshotRecord struct {
	TimeMs  int64  `json:"time_ms"`
	TimeKST string `json:"time_kst"`

	Asset string `json:"asset"`

	BestDirection string  `json:"best_direction"`
	BestProfitKRW float64 `json:"best_profit_krw"`
	BestQtyAsset  float64 `json:"best_qty_asset"`

	ProfitA float64 `json:"profit_a"`
	QtyA    float64 `json:"qty_a"`
	ProfitB float64 `json:"profit_b"`
	QtyB    float64 `json:"qty_b"`

	Legs []LegSnapshot `json:"legs"`
}

type LegSnapshot struct {
	Code      string                     `json:"code"`
	Timestamp int64                      `json:"timestamp"`
	TopN      int                        `json:"top_n"`
	Units     []marketdata.OrderbookUnit `json:"units"`
}

type TradeRecord struct {
	TimeMs  int64  `json:"time_ms"`
	TimeKST string `json:"time_kst"`

	Asset     string `json:"asset"`
	Direction string `json:"direction"`

	KRWBefore         float64 `json:"krw_before"`
	KRWAfter          float64 `json:"krw_after"`
	RealizedProfitKRW float64 `json:"realized_profit_krw"`

	PlannedBestProfitKRW float64 `json:"planned_best_profit_krw"`
	PlannedQtyAsset      float64 `json:"planned_qty_asset"`

	Orders []OrderSummary `json:"orders"`
}

type OrderSummary struct {
	Market         string `json:"market"`
	Side           string `json:"side"`
	OrdType        string `json:"ord_type"`
	UUID           string `json:"uuid"`
	State          string `json:"state"`
	ExecutedVolume string `json:"executed_volume"`
	ExecutedFunds  string `json:"executed_funds"`
	PaidFee        string `json:"paid_fee"`
}
