package marketdata

import "sync"

type Cache struct {
	mu    sync.RWMutex
	books map[string][]OrderbookUnit
	ts    map[string]int64
}

func NewCache() *Cache {
	return &Cache{
		books: make(map[string][]OrderbookUnit),
		ts:    make(map[string]int64),
	}
}

func (c *Cache) Set(code string, ts int64, units []OrderbookUnit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.books[code] = units
	c.ts[code] = ts
}

func (c *Cache) Get(code string) ([]OrderbookUnit, int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	u, ok := c.books[code]
	if !ok {
		return nil, 0, false
	}
	return u, c.ts[code], true
}

// 스냅샷: 현재 캐시에서 code들의 오더북을 복사해서 반환 (저장/계산용)
func (c *Cache) Snapshot(codes []string) map[string]struct {
	TS    int64
	Units []OrderbookUnit
} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]struct {
		TS    int64
		Units []OrderbookUnit
	}, len(codes))

	for _, code := range codes {
		u, ok := c.books[code]
		if !ok {
			continue
		}
		cp := make([]OrderbookUnit, len(u))
		copy(cp, u)
		out[code] = struct {
			TS    int64
			Units []OrderbookUnit
		}{TS: c.ts[code], Units: cp}
	}
	return out
}
