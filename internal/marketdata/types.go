package marketdata

type OrderbookUnit struct {
	AskPrice float64 `json:"ask_price"`
	BidPrice float64 `json:"bid_price"`
	AskSize  float64 `json:"ask_size"`
	BidSize  float64 `json:"bid_size"`
}

type OrderbookMsg struct {
	Code           string          `json:"code"`
	Timestamp      int64           `json:"timestamp"`
	OrderbookUnits []OrderbookUnit `json:"orderbook_units"`
}

type MarketInfo struct {
	Market       string `json:"market"`
	KoreanName   string `json:"korean_name"`
	EnglishName  string `json:"english_name"`
	MarketWarning string `json:"market_warning"`
}

type Ticker struct {
	Market            string  `json:"market"`
	AccTradePrice24h  float64 `json:"acc_trade_price_24h"`
	AccTradeVolume24h float64 `json:"acc_trade_volume_24h"`
	TradePrice        float64 `json:"trade_price"`
}
