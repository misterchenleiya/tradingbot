package models

const (
	DecisionActionIgnore    = "ignore"
	DecisionActionOpenLong  = "open-long"
	DecisionActionOpenShort = "open-short"
	DecisionActionClose     = "close"
	DecisionActionUpdate    = "update"
)

const (
	PositionStatusOpen   = "open"
	PositionStatusClosed = "closed"
)

const (
	MarginModeIsolated = "isolated"
	MarginModeCross    = "cross"
)

const (
	SignalHasNoPosition       = 0
	SignalHasOpenPosition     = 8
	SignalHasPartialClose     = 32
	SignalHasClosedProfit     = 64
	SignalHasClosedLoss       = -64
	SignalChangeStatusNew     = 1
	SignalChangeStatusUpdated = 2
	SignalChangeStatusGone    = 3
)

const (
	SignalProfitProtectStageNone = iota
	SignalProfitProtectStageBreakEven
	SignalProfitProtectStagePartial
)
