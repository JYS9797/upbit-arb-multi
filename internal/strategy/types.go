package strategy

type Direction string

const (
	DirA Direction = "A(KRW->ASSET->USDT->KRW)"
	DirB Direction = "B(KRW->USDT->ASSET->KRW)"
)

type Opportunity struct {
	Asset string

	BestDirection Direction
	BestAskPrice  float64
	BestBidPrice  float64
	BestCost      float64
	BestProfitKRW float64
	BestQtyAsset  float64

	ProfitA float64
	QtyA    float64
	ProfitB float64
	QtyB    float64
}

type Signal struct {
	TimeMs  int64
	TimeKST string

	Opportunity Opportunity
}
