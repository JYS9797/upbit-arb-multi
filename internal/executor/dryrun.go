package executor

import "upbit-arb/internal/strategy"

type DryRun struct{}

func (d *DryRun) OnSignal(sig *strategy.Signal) error {
	return nil
}
