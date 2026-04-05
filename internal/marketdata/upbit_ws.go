package marketdata

import (
	"bytes"
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

	msgCh := make(chan []byte, 512)
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
			msg = bytes.TrimSpace(msg)
			msgCh <- msg
		}
	}()

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
			if c.TopN > 0 && len(units) > c.TopN {
				units = units[:c.TopN]
			}
			c.Cache.Set(ob.Code, ob.Timestamp, units)

			if onUpdate != nil {
				onUpdate()
			}
		}
	}
}
