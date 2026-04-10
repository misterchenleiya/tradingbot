package models

const (
	ManualOrderStatusPending  = "pending"
	ManualOrderStatusFilled   = "filled"
	ManualOrderStatusCanceled = "canceled"
	ManualOrderStatusRejected = "rejected"
	ManualOrderStatusExpired  = "expired"
)

const (
	ManualOrderOwnerManual = "manual"
)

type ManualOrder struct {
	ID                  int64
	Mode                string
	Exchange            string
	Symbol              string
	InstID              string
	Timeframe           string
	PositionSide        string
	MarginMode          string
	OrderType           string
	Status              string
	StrategyName        string
	StrategyVersion     string
	StrategyTimeframes  []string
	ComboKey            string
	GroupID             string
	LeverageMultiplier  float64
	Amount              float64
	Size                float64
	Price               float64
	TakeProfitPrice     float64
	StopLossPrice       float64
	ClientOrderID       string
	ExchangeOrderID     string
	ExchangeAlgoOrderID string
	PositionID          int64
	EntryPrice          float64
	FilledSize          float64
	ErrorMessage        string
	DecisionJSON        string
	MetadataJSON        string
	CreatedAtMS         int64
	SubmittedAtMS       int64
	FilledAtMS          int64
	LastCheckedAtMS     int64
	UpdatedAtMS         int64
}
