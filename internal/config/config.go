package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Upbit struct {
		WsURL              string `yaml:"ws_url"`
		PrivateWsURL       string `yaml:"private_ws_url"`
		RESTBaseURL        string `yaml:"rest_base_url"`
		AccessKey          string `yaml:"access_key"`
		SecretKey          string `yaml:"secret_key"`
		PingIntervalSec    int    `yaml:"ping_interval_sec"`
		ReconnectBackoffMs int    `yaml:"reconnect_backoff_ms"`
	} `yaml:"upbit"`

	Markets struct {
		Mode         string   `yaml:"mode"` // static | top_krw
		StaticAssets []string `yaml:"static_assets"`
		TopKRWN      int      `yaml:"top_krw_n"`

		IncludeKRW bool `yaml:"include_krw"`
		IncludeUSDT bool `yaml:"include_usdt"`
		IncludeBTC bool `yaml:"include_btc"`

		TopNLevels int `yaml:"top_n_levels"`
	} `yaml:"markets"`

	TradeModel struct {
		MaxQtyAsset float64 `yaml:"max_qty_asset"`
		FeeKRW      float64 `yaml:"fee_krw"`
		FeeUSDT     float64 `yaml:"fee_usdt"`
	} `yaml:"trade_model"`

	Signal struct {
		MinProfitKRW       float64 `yaml:"min_profit_krw"`
		MinPrintIntervalMs int     `yaml:"min_print_interval_ms"`
	} `yaml:"signal"`

	PrivateStream struct {
		Enabled    bool     `yaml:"enabled"`
		MyOrderCodes []string `yaml:"myorder_codes"`
	} `yaml:"private_stream"`

	Trading struct {
		Enabled bool   `yaml:"enabled"`
		TimeInForce string `yaml:"time_in_force"` // ioc or fok
		MaxKRWPerTrade float64 `yaml:"max_krw_per_trade"`
		SafetyKRWReserve float64 `yaml:"safety_krw_reserve"`
	} `yaml:"trading"`

	Storage struct {
		Enabled       bool   `yaml:"enabled"`
		OutDir        string `yaml:"out_dir"`
		JSONLFilename string `yaml:"jsonl_filename"`
	} `yaml:"storage"`

	Runtime struct {
		LogLevel string `yaml:"log_level"`
	} `yaml:"runtime"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	expandEnv(&cfg)
	return &cfg, nil
}

func expandEnv(cfg *Config) {
	// ${ENV} 형태면 환경변수로 치환
	repl := func(s string) string {
		if len(s) >= 4 && s[0:2] == "${" && s[len(s)-1:] == "}" {
			key := s[2:len(s)-1]
			if v := os.Getenv(key); v != "" {
				return v
			}
		}
		return s
	}
	cfg.Upbit.AccessKey = repl(cfg.Upbit.AccessKey)
	cfg.Upbit.SecretKey = repl(cfg.Upbit.SecretKey)
}

