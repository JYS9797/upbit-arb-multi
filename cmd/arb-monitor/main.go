package main

import (
	"flag"
	"log"

	"upbit-arb/internal/app"
	"upbit-arb/internal/config"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config 파일 경로")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config 로드 실패: %v", err)
	}

	a := app.New(cfg)
	a.Run()
}
