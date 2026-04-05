package state

import (
	"strconv"
	"sync"

	"upbit-arb/internal/exchange"
)

type BalanceStore struct {
	mu sync.RWMutex
	// currency => available balance
	avail map[string]float64
}

func NewBalanceStore() *BalanceStore {
	return &BalanceStore{avail: map[string]float64{}}
}

func (s *BalanceStore) UpdateFromAccounts(accts []exchange.Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range accts {
		b, _ := strconv.ParseFloat(a.Balance, 64)
		s.avail[a.Currency] = b
	}
}

func (s *BalanceStore) Set(currency string, bal float64) {
	s.mu.Lock()
	s.avail[currency] = bal
	s.mu.Unlock()
}

func (s *BalanceStore) Get(currency string) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.avail[currency]
}
