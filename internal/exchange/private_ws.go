package exchange

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type PrivateWSClient struct {
	URL          string
	PingInterval time.Duration
	Backoff      time.Duration
	MyOrderCodes []string

	// 재연결 때마다 새 JWT를 만들기 위한 함수
	AuthFunc func() (string, error)

	OnMyAsset func(raw json.RawMessage)
	OnMyOrder func(raw json.RawMessage)
}

func (c *PrivateWSClient) Run() {
	for {
		if err := c.runOnce(); err != nil {
			log.Printf("[private-ws] 종료: %v (재연결 %s)", err, c.Backoff)
			time.Sleep(c.Backoff)
		}
	}
}

func (c *PrivateWSClient) runOnce() error {
	auth := ""
	if c.AuthFunc != nil {
		a, err := c.AuthFunc()
		if err != nil {
			return err
		}
		auth = a
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	header := http.Header{}
	if auth != "" {
		header.Set("Authorization", auth)
	}

	conn, _, err := dialer.Dial(c.URL, header)
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
		{"ticket": "arb-private"},
		{"type": "myAsset"},
	}
	if len(c.MyOrderCodes) == 0 {
		sub = append(sub, map[string]any{"type": "myOrder"})
	} else {
		sub = append(sub, map[string]any{"type": "myOrder", "codes": c.MyOrderCodes})
	}
	sub = append(sub, map[string]any{"format": "DEFAULT"})

	b, _ := json.Marshal(sub)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return err
	}

	// ping은 ticker로, read는 goroutine으로 분리해서 안정적으로 유지
	pingTicker := time.NewTicker(c.PingInterval)
	defer pingTicker.Stop()

	msgCh := make(chan []byte, 128)
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
			var probe struct{ Type string `json:"type"` }
			if err := json.Unmarshal(msg, &probe); err != nil {
				continue
			}
			switch probe.Type {
			case "myAsset":
				if c.OnMyAsset != nil {
					c.OnMyAsset(json.RawMessage(msg))
				}
			case "myOrder":
				if c.OnMyOrder != nil {
					c.OnMyOrder(json.RawMessage(msg))
				}
			}
		}
	}
}
