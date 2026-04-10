package risk

import (
	"fmt"
	"math"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type riskOpenPlan struct {
	DecisionOrderType string
	EntryPrice        float64
	StopLossPrice     float64
	TakeProfitPrice   float64
	Leverage          int
	MarginUSDT        float64
	Size              float64
}

type riskOpenSizeFunc func(perTradeUSDT float64, leverage int, entryPrice float64) (float64, error)

func evaluateRiskOpenPlan(
	cfg RiskConfig,
	signal models.Signal,
	signalOrderType string,
	side string,
	entryPrice float64,
	available float64,
	accountState models.RiskAccountState,
	sizeFn riskOpenSizeFunc,
) (riskOpenPlan, error) {
	if side == "" {
		return riskOpenPlan{}, fmt.Errorf("missing position side")
	}
	if signalOrderType == models.OrderTypeLimit && entryPrice <= 0 {
		return riskOpenPlan{}, fmt.Errorf("limit order requires entry price")
	}
	if entryPrice <= 0 {
		return riskOpenPlan{}, fmt.Errorf("invalid entry price")
	}
	sl := signal.SL
	if sl <= 0 && cfg.SL.RequireSignal {
		return riskOpenPlan{}, fmt.Errorf("SL required")
	}
	if sl <= 0 {
		return riskOpenPlan{}, fmt.Errorf("invalid SL")
	}
	adverseRate, err := adverseMoveRate(side, entryPrice, sl)
	if err != nil {
		return riskOpenPlan{}, err
	}
	if adverseRate > cfg.SL.MaxLossPct {
		return riskOpenPlan{}, fmt.Errorf(
			"SL too large, max SL -%.2f%%, signal SL -%.2f%%",
			cfg.SL.MaxLossPct*100,
			adverseRate*100,
		)
	}

	leverage := int(math.Floor(cfg.SL.MaxLossPct / adverseRate))
	if leverage < cfg.Leverage.Min {
		return riskOpenPlan{}, fmt.Errorf(
			"SL too large, max SL -%.2f%%, signal SL -%.2f%%",
			cfg.SL.MaxLossPct*100,
			adverseRate*100,
		)
	}
	if leverage < 1 {
		leverage = 1
	}
	if leverage > cfg.Leverage.Max {
		leverage = cfg.Leverage.Max
	}

	perTradeUSDT := accountState.PerTradeUSDT
	if perTradeUSDT <= 0 {
		perTradeUSDT = accountState.TradingUSDT * cfg.PerTrade.Ratio
	}
	if perTradeUSDT <= 0 {
		return riskOpenPlan{}, fmt.Errorf("invalid per-trade budget")
	}
	if signal.Amount < 0 {
		return riskOpenPlan{}, fmt.Errorf("invalid amount")
	}
	if signal.Amount > 0 && signal.Amount < perTradeUSDT {
		perTradeUSDT = signal.Amount
	}
	if available < perTradeUSDT {
		return riskOpenPlan{}, fmt.Errorf("insufficient available usdt %.4f < %.4f", available, perTradeUSDT)
	}

	tp := resolveOpenTakeProfit(cfg.TP, side, entryPrice, signal.TP, float64(leverage))
	if err := validateTPSLAgainstPrice(side, entryPrice, tp, sl); err != nil {
		return riskOpenPlan{}, err
	}

	size, err := sizeFn(perTradeUSDT, leverage, entryPrice)
	if err != nil {
		return riskOpenPlan{}, err
	}
	if size <= 0 {
		return riskOpenPlan{}, fmt.Errorf("invalid quantity")
	}

	return riskOpenPlan{
		DecisionOrderType: decisionOpenOrderType(signalOrderType),
		EntryPrice:        entryPrice,
		StopLossPrice:     sl,
		TakeProfitPrice:   tp,
		Leverage:          leverage,
		MarginUSDT:        perTradeUSDT,
		Size:              size,
	}, nil
}
