package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"upbit-arb/internal/config"
	"upbit-arb/internal/exchange"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config path")
	krw := flag.Float64("krw", 10000, "KRW amount to spend in step1")
	tif := flag.String("tif", "ioc", "time_in_force: ioc or fok (recommend ioc)")
	balanceWait := flag.Duration("balance_wait", 4*time.Second, "max wait per step for balance reflect")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}
	if cfg.Upbit.AccessKey == "" || cfg.Upbit.SecretKey == "" {
		log.Fatalf("missing UPBIT keys: set env UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY")
	}

	rest := exchange.NewREST(cfg.Upbit.RESTBaseURL, cfg.Upbit.AccessKey, cfg.Upbit.SecretKey)

	// ---------------------------
	// Before balances
	// ---------------------------
	ac0, err := rest.GetAccounts()
	if err != nil {
		log.Fatalf("GetAccounts(before) failed: %v", err)
	}
	krw0 := findAvail(ac0, "KRW")
	usdt0 := findAvail(ac0, "USDT")
	btc0 := findAvail(ac0, "BTC")
	fmt.Printf("[before] KRW=%.0f USDT=%.6f BTC=%.8f\n", krw0, usdt0, btc0)

	// ---------------------------
	// Cleanup (BTC/USDT -> KRW)
	// ---------------------------
	cleanup := func(tag string) {
		log.Printf("[%s] cleanup start", tag)

		// 1) BTC -> KRW
		ac, err := rest.GetAccounts()
		if err == nil {
			btcAvail := findAvail(ac, "BTC")
			if btcAvail > 0 {
				req := exchange.BestSell("KRW-BTC", trimFloat(btcAvail, 8), *tif)
				o, e := rest.CreateOrder(req)
				if e != nil {
					log.Printf("[%s][cleanup] BTC->KRW CreateOrder failed: %v", tag, e)
				} else {
					_, _ = waitDoneOrCancel(rest, o.UUID, 10*time.Second)
					log.Printf("[%s][cleanup] BTC->KRW attempted (btc=%.8f)", tag, btcAvail)
				}
			}
		}

		// 2) USDT -> KRW (available가 풀릴 때까지 기다렸다가 팔기)
		usdtToSell := waitAvailPositive(rest, "USDT", 4*time.Second)
		if usdtToSell > 0 {
			usdtToSell = usdtToSell * (1.0 - 0.003) // safety
			if usdtToSell > 0 {
				req := exchange.BestSell("KRW-USDT", trimFloat(usdtToSell, 6), *tif)
				o, e := rest.CreateOrder(req)
				if e != nil {
					log.Printf("[%s][cleanup] USDT->KRW CreateOrder failed: %v", tag, e)
				} else {
					_, _ = waitDoneOrCancel(rest, o.UUID, 10*time.Second)
					log.Printf("[%s][cleanup] USDT->KRW attempted (usdt=%.6f)", tag, usdtToSell)
				}
			}
		}

		acF, _ := rest.GetAccounts()
		log.Printf("[%s] cleanup end (KRW=%.0f USDT(avail)=%.6f locked=%.6f BTC=%.8f)",
			tag,
			findAvail(acF, "KRW"),
			findAvail(acF, "USDT"),
			findLocked(acF, "USDT"),
			findAvail(acF, "BTC"),
		)
	}

	// ---------------------------
	// Step1: KRW -> BTC  (KRW-BTC best buy; price=KRW 총액)
	// ---------------------------
	ac1pre, _ := rest.GetAccounts()
	krw1pre := findAvail(ac1pre, "KRW")
	btc1pre := findAvail(ac1pre, "BTC")

	req1 := exchange.BestBuy("KRW-BTC", fmt.Sprintf("%.0f", *krw), *tif)
	o1, err := rest.CreateOrder(req1)
	if err != nil {
		log.Fatalf("[step1] CreateOrder failed: %v", err)
	}
	o1f, err := waitDoneOrCancel(rest, o1.UUID, 10*time.Second)
	if err != nil {
		log.Fatalf("[step1] wait failed: %v", err)
	}
	btcExec1, _ := strconv.ParseFloat(o1f.ExecutedVolume, 64)

	_, _, _ = waitBalanceDelta(rest, "BTC", btc1pre, 1e-10, *balanceWait)

	ac1post, _ := rest.GetAccounts()
	krw1post := findAvail(ac1post, "KRW")
	btc1post := findAvail(ac1post, "BTC")

	btcGot := btc1post - btc1pre
	if btcGot <= 0 {
		log.Printf("[step1 raw] state=%s execVol=%.10f", o1f.State, btcExec1)
		log.Fatalf("[step1] BTC balance did not increase (before=%.10f after=%.10f)", btc1pre, btc1post)
	}
	fmt.Printf("[step1] state=%s execVol=%.10f BTCdelta=%.10f KRWdelta=%.0f\n",
		o1f.State, btcExec1, btcGot, krw1post-krw1pre)

	// ---------------------------
	// Step2: BTC -> USDT (USDT-BTC best sell; volume=BTC)
	// ---------------------------
	ac2pre, _ := rest.GetAccounts()
	usdt2pre := findAvail(ac2pre, "USDT")
	btc2pre := findAvail(ac2pre, "BTC")

	btcToSell := btc2pre
	if btcToSell <= 0 {
		log.Fatalf("[step2] no BTC to sell (btc=%.10f)", btc2pre)
	}

	req2 := exchange.BestSell("USDT-BTC", trimFloat(btcToSell, 8), *tif)
	o2, err := rest.CreateOrder(req2)
	if err != nil {
		log.Printf("[step2] CreateOrder failed: %v", err)
		cleanup("step2")
		log.Fatal("[step2] abort (cleanup attempted)")
	}
	o2f, err := waitDoneOrCancel(rest, o2.UUID, 10*time.Second)
	if err != nil {
		log.Printf("[step2] wait failed: %v", err)
		cleanup("step2")
		log.Fatal("[step2] abort (cleanup attempted)")
	}

	_, _, _ = waitBalanceDelta(rest, "USDT", usdt2pre, 1e-9, *balanceWait)

	ac2post, _ := rest.GetAccounts()
	usdt2post := findAvail(ac2post, "USDT")
	btc2post := findAvail(ac2post, "BTC")

	usdtGot := usdt2post - usdt2pre
	if usdtGot <= 0 {
		log.Printf("[step2 raw] state=%s execVol=%s execFunds=%s", o2f.State, o2f.ExecutedVolume, o2f.ExecutedFunds)
		cleanup("step2")
		log.Fatalf("[step2] USDT balance did not increase (before=%.6f after=%.6f) (cleanup attempted)", usdt2pre, usdt2post)
	}
	fmt.Printf("[step2] state=%s USDTdelta=%.6f BTCdelta=%.10f (USDT locked=%.6f)\n",
		o2f.State, usdtGot, btc2post-btc2pre, findLocked(ac2post, "USDT"))

	// ---------------------------
	// Step3: USDT -> KRW (KRW-USDT best sell; volume=USDT available)
	// ---------------------------
	ac3pre, _ := rest.GetAccounts()
	krw3pre := findAvail(ac3pre, "KRW")

	// 핵심: available(balance)가 실제로 풀릴 때까지 잠깐 기다린다
	usdtAvail := waitAvailPositive(rest, "USDT", 4*time.Second)
	usdtLocked := func() float64 {
		ac, _ := rest.GetAccounts()
		return findLocked(ac, "USDT")
	}()

	safety := 0.003
	usdtToSell := usdtAvail * (1.0 - safety)

	if usdtToSell <= 0 {
		log.Printf("[step3] no available USDT to sell (avail=%.6f locked=%.6f)", usdtAvail, usdtLocked)
		cleanup("step3")
		log.Fatal("[step3] abort (cleanup attempted)")
	}

	req3 := exchange.BestSell("KRW-USDT", trimFloat(usdtToSell, 6), *tif)
	o3, err := rest.CreateOrder(req3)
	if err != nil {
		log.Printf("[step3] CreateOrder failed: %v (avail=%.6f locked=%.6f trySell=%.6f)", err, usdtAvail, usdtLocked, usdtToSell)
		cleanup("step3")
		log.Fatal("[step3] abort (cleanup attempted)")
	}
	o3f, err := waitDoneOrCancel(rest, o3.UUID, 10*time.Second)
	if err != nil {
		log.Printf("[step3] wait failed: %v", err)
		cleanup("step3")
		log.Fatal("[step3] abort (cleanup attempted)")
	}

	_, _, _ = waitBalanceDelta(rest, "KRW", krw3pre, 0.5, *balanceWait)

	ac3post, _ := rest.GetAccounts()
	krw3post := findAvail(ac3post, "KRW")
	usdt3post := findAvail(ac3post, "USDT")
	fmt.Printf("[step3] state=%s KRWdelta=%.0f USDT(avail)delta=%.6f locked=%.6f\n",
		o3f.State, krw3post-krw3pre, usdt3post-usdtAvail, findLocked(ac3post, "USDT"))

	// ---------------------------
	// After balances
	// ---------------------------
	acF, _ := rest.GetAccounts()
	krwF := findAvail(acF, "KRW")
	usdtF := findAvail(acF, "USDT")
	btcF := findAvail(acF, "BTC")
	fmt.Printf("[after ] KRW=%.0f USDT=%.6f BTC=%.8f\n", krwF, usdtF, btcF)
	fmt.Printf("[delta ] KRW=%.0f USDT=%.6f BTC=%.8f\n", krwF-krw0, usdtF-usdt0, btcF-btc0)
}

// ---------------------------
// Helpers
// ---------------------------

func waitDoneOrCancel(rest *exchange.RESTClient, uuid string, timeout time.Duration) (*exchange.OrderResp, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		od, err := rest.GetOrder(uuid)
		if err != nil {
			return nil, err
		}
		if od.State == "done" || od.State == "cancel" {
			return od, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, fmt.Errorf("timeout waiting order %s", uuid)
}

// currency available(balance)가 0보다 커질 때까지 폴링
func waitAvailPositive(rest *exchange.RESTClient, currency string, timeout time.Duration) float64 {
	deadline := time.Now().Add(timeout)
	var last float64
	for time.Now().Before(deadline) {
		ac, err := rest.GetAccounts()
		if err == nil {
			last = findAvail(ac, currency)
			if last > 0 {
				return last
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return last
}

func waitBalanceDelta(rest *exchange.RESTClient, currency string, base float64, minDelta float64, timeout time.Duration) (float64, float64, error) {
	deadline := time.Now().Add(timeout)
	last := base
	for time.Now().Before(deadline) {
		ac, err := rest.GetAccounts()
		if err == nil {
			cur := findAvail(ac, currency)
			last = cur
			if math.Abs(cur-base) >= minDelta {
				return base, cur, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return base, last, fmt.Errorf("balance not reflected for %s within %s (base=%.10f last=%.10f)", currency, timeout, base, last)
}

func findAvail(accts []exchange.Account, currency string) float64 {
	for _, a := range accts {
		if strings.EqualFold(a.Currency, currency) {
			v, _ := strconv.ParseFloat(a.Balance, 64) // available
			return v
		}
	}
	return 0
}

func findLocked(accts []exchange.Account, currency string) float64 {
	for _, a := range accts {
		if strings.EqualFold(a.Currency, currency) {
			v, _ := strconv.ParseFloat(a.Locked, 64)
			return v
		}
	}
	return 0
}

func trimFloat(x float64, decimals int) string {
	fmtStr := "%." + strconv.Itoa(decimals) + "f"
	s := fmt.Sprintf(fmtStr, x)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}
