package notify

import (
	"fmt"
	"upbit-arb/internal/strategy"
)

type Console struct{}

func (c *Console) Print(sig *strategy.Signal) {
	op := sig.Opportunity
	fmt.Printf("[%s] %s %s cost=%.0fKRW profit=%.0fKRW qty=%.6f \n",
		sig.TimeKST,
		op.Asset,
		op.BestDirection,
		op.BestCost,
		op.BestProfitKRW,
		op.BestQtyAsset,
	)
}
