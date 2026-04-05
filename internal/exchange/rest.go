package exchange

import (
	// "bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RESTClient struct {
	BaseURL   string
	AccessKey string
	SecretKey string
	HTTP      *http.Client
}

func NewREST(baseURL, accessKey, secretKey string) *RESTClient {
	return &RESTClient{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		AccessKey: accessKey,
		SecretKey: secretKey,
		HTTP:      &http.Client{Timeout: 10 * time.Second},
	}
}

type Account struct {
	Currency     string `json:"currency"`
	Balance      string `json:"balance"`
	Locked       string `json:"locked"`
	AvgBuyPrice  string `json:"avg_buy_price"`
	UnitCurrency string `json:"unit_currency"`
}

func (c *RESTClient) GetAccounts() ([]Account, error) {
	auth, err := BuildJWT(c.AccessKey, c.SecretKey, "")
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest("GET", c.BaseURL+"/v1/accounts", nil)
	req.Header.Set("Authorization", auth)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	var out []Account
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

type NewOrderReq struct {
	Market      string  `json:"market"`
	Side        string  `json:"side"`     // bid or ask
	OrdType     string  `json:"ord_type"` // limit / price / market / best
	Volume      *string `json:"volume,omitempty"`
	Price       *string `json:"price,omitempty"`
	TimeInForce *string `json:"time_in_force,omitempty"` // ioc/fok/post_only
}

type OrderResp struct {
	UUID            string `json:"uuid"`
	Side            string `json:"side"`
	OrdType         string `json:"ord_type"`
	State           string `json:"state"`
	Market          string `json:"market"`
	CreatedAt       string `json:"created_at"`
	Volume          string `json:"volume"`
	RemainingVolume string `json:"remaining_volume"`
	ExecutedVolume  string `json:"executed_volume"`
	PaidFee         string `json:"paid_fee"`
	Locked          string `json:"locked"`
	ExecutedFunds   string `json:"executed_funds"`
}

func strPtr(s string) *string { return &s }

func LimitBuy(market string, price string, volume string, tif string) NewOrderReq {
	r := NewOrderReq{
		Market:  market,
		Side:    "bid",
		OrdType: "limit",
	}
	r.Price = &price
	r.Volume = &volume
	if tif != "" {
		r.TimeInForce = &tif
	}
	return r
}

func LimitSell(market string, price string, volume string, tif string) NewOrderReq {
	r := NewOrderReq{
		Market:  market,
		Side:    "ask",
		OrdType: "limit",
	}
	r.Price = &price
	r.Volume = &volume
	if tif != "" {
		r.TimeInForce = &tif
	}
	return r
}

func BestBuy(market string, total string, tif string) NewOrderReq {
	return NewOrderReq{
		Market: market, Side: "bid", OrdType: "best",
		Price: strPtr(total), TimeInForce: strPtr(tif),
	}
}
func BestSell(market string, volume string, tif string) NewOrderReq {
	return NewOrderReq{
		Market: market, Side: "ask", OrdType: "best",
		Volume: strPtr(volume), TimeInForce: strPtr(tif),
	}
}

// For query_hash: use URL-encoded query string from form values (Upbit spec).
func normalizeQueryString(encoded string) string {
	// Upbit는 URL 인코딩되지 않은(=unquoted) 쿼리 문자열을 기준으로 query_hash를 검증합니다.
	// 공식 예시: unquote(urlencode(...))
	dec, err := url.QueryUnescape(encoded)
	if err != nil {
		return encoded
	}
	return dec
}

func buildQueryForHashOrder(r NewOrderReq) string {
	v := url.Values{}
	v.Set("market", r.Market)
	v.Set("side", r.Side)
	v.Set("ord_type", r.OrdType)
	if r.Volume != nil {
		v.Set("volume", *r.Volume)
	}
	if r.Price != nil {
		v.Set("price", *r.Price)
	}
	if r.TimeInForce != nil {
		v.Set("time_in_force", *r.TimeInForce)
	}
	return normalizeQueryString(v.Encode())
}

func (c *RESTClient) CreateOrder(r NewOrderReq) (*OrderResp, error) {
	// 1) form body를 만들고(= 실제로 서버에 보낼 payload)
	// 2) 그 문자열을 unescape한 값을 query_hash에 사용
	formEncoded := buildFormEncodedOrder(r)           // e.g., "market=KRW-ETH&side=bid&ord_type=best&price=1000&time_in_force=ioc"
	queryForHash := normalizeQueryString(formEncoded) // unquoted version for hash

	auth, err := BuildJWT(c.AccessKey, c.SecretKey, queryForHash)
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/orders", strings.NewReader(formEncoded))
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		rb, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(rb))
	}
	var out OrderResp
	return &out, json.NewDecoder(resp.Body).Decode(&out)
}

// 실제로 서버에 보낼 form-encoded 문자열 생성
func buildFormEncodedOrder(r NewOrderReq) string {
	v := url.Values{}
	v.Set("market", r.Market)
	v.Set("side", r.Side)
	v.Set("ord_type", r.OrdType)
	if r.Volume != nil {
		v.Set("volume", *r.Volume)
	}
	if r.Price != nil {
		v.Set("price", *r.Price)
	}
	if r.TimeInForce != nil {
		v.Set("time_in_force", *r.TimeInForce)
	}
	return v.Encode()
}

func (c *RESTClient) GetOrder(uuidStr string) (*OrderResp, error) {
	q := url.Values{}
	q.Set("uuid", uuidStr)
	query := normalizeQueryString(q.Encode())
	auth, err := BuildJWT(c.AccessKey, c.SecretKey, query)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest("GET", c.BaseURL+"/v1/order?"+query, nil)
	req.Header.Set("Authorization", auth)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	var out OrderResp
	return &out, json.NewDecoder(resp.Body).Decode(&out)
}
