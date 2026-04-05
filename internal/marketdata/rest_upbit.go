package marketdata

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type RESTClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewREST(base string) *RESTClient {
	return &RESTClient{
		BaseURL: strings.TrimRight(base, "/"),
		HTTP: &http.Client{Timeout: 8 * time.Second},
	}
}

func (c *RESTClient) getJSON(path string, q url.Values, out any) error {
	u := c.BaseURL + path
	if q != nil && len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *RESTClient) ListMarkets(isDetails bool) ([]MarketInfo, error) {
	q := url.Values{}
	if isDetails {
		q.Set("isDetails", "true")
	} else {
		q.Set("isDetails", "false")
	}
	var out []MarketInfo
	if err := c.getJSON("/v1/market/all", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *RESTClient) Tickers(markets []string) ([]Ticker, error) {
	q := url.Values{}
	q.Set("markets", strings.Join(markets, ","))
	var out []Ticker
	if err := c.getJSON("/v1/ticker", q, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// KRW 마켓 24h 거래대금(acc_trade_price_24h) 상위 N개의 "자산 심볼"을 반환
func TopKRWAssetsByValue24h(markets []MarketInfo, tickers []Ticker, n int) []string {
	// KRW-XXX => XXX
	krwSet := map[string]bool{}
	for _, m := range markets {
		if strings.HasPrefix(m.Market, "KRW-") {
			krwSet[m.Market] = true
		}
	}

	filtered := make([]Ticker, 0, len(tickers))
	for _, t := range tickers {
		if krwSet[t.Market] {
			filtered = append(filtered, t)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].AccTradePrice24h > filtered[j].AccTradePrice24h
	})

	out := []string{}
	seen := map[string]bool{}
	for _, t := range filtered {
		parts := strings.SplitN(t.Market, "-", 2)
		if len(parts) != 2 { continue }
		asset := parts[1]
		if seen[asset] { continue }
		seen[asset] = true
		out = append(out, asset)
		if len(out) >= n { break }
	}
	return out
}
