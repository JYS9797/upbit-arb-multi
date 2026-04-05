package main

import (
	"encoding/json"
	"flag"
	"log"
	"time"

	"upbit-arb/internal/config"
	"upbit-arb/internal/exchange"
)

func main() {

	cfgPath := flag.String("config", "configs/config.yaml", "config path")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	if cfg.Upbit.AccessKey == "" || cfg.Upbit.SecretKey == "" {
		log.Fatal("UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY required")
	}

	log.Println("Starting private WS dump...")

	pws := &exchange.PrivateWSClient{
		URL:          cfg.Upbit.PrivateWsURL,
		PingInterval: time.Duration(cfg.Upbit.PingIntervalSec) * time.Second,
		Backoff:      time.Duration(cfg.Upbit.ReconnectBackoffMs) * time.Millisecond,
		MyOrderCodes: cfg.PrivateStream.MyOrderCodes,

		AuthFunc: func() (string, error) {
			return exchange.BuildJWT(cfg.Upbit.AccessKey, cfg.Upbit.SecretKey, "")
		},

		OnMyAsset: func(raw json.RawMessage) {
			log.Printf("========== MY ASSET ==========")
			log.Printf("%s", string(raw))
		},

		OnMyOrder: func(raw json.RawMessage) {
			log.Printf("========== MY ORDER ==========")
			log.Printf("%s", string(raw))
		},
	}

	pws.Run()
}
