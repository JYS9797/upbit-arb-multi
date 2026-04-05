package executor

import "upbit-arb/internal/strategy"

type Executor interface {
	OnSignal(sig *strategy.Signal) error
}
