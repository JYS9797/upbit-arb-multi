package marketdata

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type WSClient struct {
	URL              string
	Codes            []string
	TopN             int
	PingInterval     time.Duration
	ReconnectBackoff time.Duration
	Cache            *Cache
}

func (c *WSClient) Run(onUpdate func()) {
	for {
		if err := c.runOnce(onUpdate); err != nil {
			log.Printf("ws 종료: %v (재연결 대기 %s)", err, c.ReconnectBackoff)
			time.Sleep(c.ReconnectBackoff)
		}
	}
}

func (c *WSClient) runOnce(onUpdate func()) error {
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 10 * time.Second,
		// 읽기/쓰기 버퍼 명시 (gorilla 기본값 4096 → 16K로 확대해 fragmentation 감소)
		ReadBufferSize:  16384,
		WriteBufferSize: 4096,
	}

	conn, _, err := dialer.Dial(c.URL, http.Header{})
	if err != nil {
		return err
	}
	defer conn.Close()

	// ---- Heartbeat ----
	pongWait := c.PingInterval*2 + 5*time.Second
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	sub := []map[string]any{
		{"ticket": "arb-monitor"},
		{"type": "orderbook", "codes": c.Codes},
	}
	b, _ := json.Marshal(sub)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return err
	}

	pingTicker := time.NewTicker(c.PingInterval)
	defer pingTicker.Stop()

	// 버퍼: WS goroutine이 parse goroutine より先行できるように余裕を持たせる
	msgCh := make(chan []byte, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(msgCh)
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
				continue
			}
			// bytes.TrimSpace 제거: Upbit WS 메시지는 padding 없음
			msgCh <- msg
		}
	}()

	topN := c.TopN
	for {
		select {
		case <-pingTicker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(2*time.Second)); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			var ob OrderbookMsg
			if err := json.Unmarshal(msg, &ob); err != nil {
				continue
			}
			if ob.Code == "" || len(ob.OrderbookUnits) == 0 {
				continue
			}

			units := ob.OrderbookUnits
			if topN > 0 && len(units) > topN {
				units = units[:topN]
			}
			c.Cache.Set(ob.Code, ob.Timestamp, units)

			if onUpdate != nil {
				onUpdate()
			}
		}
	}
}
