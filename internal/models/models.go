package models

const (
	OrderTypeMarket = "market"
	OrderTypeLimit  = "limit"
)

type OHLCV struct {
	TS     int64
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

type Exchange struct {
	Name         string
	APIKey       string
	RateLimitMS  int
	OHLCVLimit   int
	VolumeFilter float64
	MarketProxy  string
	TradeProxy   string
	Timeframes   string
	Active       bool
}

type MarketData struct {
	Exchange  string
	Symbol    string
	Timeframe string
	OHLCV     OHLCV
	Closed    bool
	Source    string
	Seq       int64
}

type SeriesMeta struct {
	LastClosedTS int64
	LastIndex    int
}

type MarketSnapshot struct {
	Exchange       string
	Symbol         string
	EventTimeframe string
	EventTS        int64
	EventClosed    bool
	Series         map[string][]OHLCV
	Meta           map[string]SeriesMeta
}

type RiskEvalContext struct {
	MarketData MarketData
	Snapshot   *MarketSnapshot
}

type TriggerHistoryRecord struct {
	Action           int `json:"action"`
	MidSide          int `json:"mid_side"`
	TriggerTimestamp int `json:"trigger_timestamp"`
}

type SignalGroupedCandidate struct {
	CandidateKey    string  `json:"candidate_key"`
	CandidateState  string  `json:"candidate_state"`
	IsSelected      bool    `json:"is_selected"`
	PriorityScore   float64 `json:"priority_score"`
	HasOpenPosition bool    `json:"has_open_position"`
}

type SignalGroupedInfo struct {
	GroupID                   string                   `json:"group_id"`
	Strategy                  string                   `json:"strategy"`
	PrimaryTimeframe          string                   `json:"primary_timeframe"`
	Side                      string                   `json:"side"`
	AnchorTrendingTimestampMS int64                    `json:"anchor_trending_timestamp_ms"`
	State                     string                   `json:"state"`
	LockStage                 string                   `json:"lock_stage,omitempty"`
	SelectedCandidateKey      string                   `json:"selected_candidate_key,omitempty"`
	EntryCount                int                      `json:"entry_count"`
	CandidateKey              string                   `json:"candidate_key"`
	CandidateState            string                   `json:"candidate_state"`
	IsSelected                bool                     `json:"is_selected"`
	PriorityScore             float64                  `json:"priority_score"`
	HasOpenPosition           bool                     `json:"has_open_position"`
	Candidates                []SignalGroupedCandidate `json:"candidates,omitempty"`
}

type StrategyComboConfig struct {
	Timeframes   []string `json:"timeframes"`
	TradeEnabled bool     `json:"trade_enabled"`
}

const (
	SignalPostHighPullbackFirstEntryNone = iota
	SignalPostHighPullbackFirstEntryPending
	SignalPostHighPullbackFirstEntryArmed
)

const (
	SignalActionOpenRiskRejected       = -8
	SignalActionOpenTrendGuardRejected = -12
)

type Signal struct {
	Exchange                        string                 `json:"exchange"` // 由strategy.manager负责填充
	Symbol                          string                 `json:"symbol"`   // 由strategy.manager负责填充
	Timeframe                       string                 `json:"timeframe"`
	ComboKey                        string                 `json:"combo_key,omitempty"`
	GroupID                         string                 `json:"group_id,omitempty"`   // 由exporter输出阶段填充；命中 grouped trend_guard 时返回所属 group_id
	HasPosition                     int                    `json:"has_position"`         // 仓位状态，0:无持仓/8:有持仓/32:部分平仓/64:全部平仓且盈利/-64:全部平仓且亏损
	OrderType                       string                 `json:"order_type,omitempty"` // 开仓订单类型：market/limit；空值走历史兼容逻辑
	Amount                          float64                `json:"amount"`               // 开仓保证金预算（USDT），仅 action=8 时参与 risk 评估
	Entry                           float64                `json:"entry"`
	Exit                            float64                `json:"exit"`
	SL                              float64                `json:"sl"`
	TP                              float64                `json:"tp"`
	InitialSL                       float64                `json:"initial_sl"`
	InitialRiskPct                  float64                `json:"initial_risk_pct"`
	MaxFavorableProfitPct           float64                `json:"max_favorable_profit_pct"`
	ProfitProtectStage              int                    `json:"profit_protect_stage"`
	Plan1LastProfitLockMFER         float64                `json:"plan1_last_profit_lock_mfer"`
	Plan1LastProfitLockHighBucketTS int                    `json:"plan1_last_profit_lock_high_bucket_ts"`
	Plan1LastProfitLockStructPrice  float64                `json:"plan1_last_profit_lock_struct_price"`
	Action                          int                    `json:"action"`             // 当前动作。-12：开仓被trend guard拒绝（临时态）/-8：开仓被risk拒绝（临时态）/4：小周期armed状态/8：开仓/16：更新移动止盈止损/32：部分平仓/64：全部平仓
	HighSide                        int                    `json:"high_side"`          // 大周期方向/趋势。当前策略仅使用 0/1/-1；历史版本归档数据中可能出现其他旧状态值。
	MidSide                         int                    `json:"mid_side"`           // 中周期方向/趋势。当前策略仅使用 0/1/-1；历史版本归档数据中可能出现其他旧状态值。
	TrendingTimestamp               int                    `json:"trending_timestamp"` // 趋势起始K线的时间戳
	TrendEntryCount                 int                    `json:"trend_entry_count"`  // 同一趋势(TrendingTimestamp+方向)下累计开仓次数，0:首笔趋势单；>=1:再入场单
	MidPullbackCount                int                    `json:"mid_pullback_count"`
	LastMidPullbackTS               int                    `json:"last_mid_pullback_ts"`
	RequireHighPullbackReset        bool                   `json:"require_high_pullback_reset"`
	StageEntryUsed                  bool                   `json:"stage_entry_used"`
	PostHighPullbackFirstEntryState int                    `json:"post_high_pullback_first_entry_state"`
	EntryWatchTimestamp             int                    `json:"entry_watch_timestamp"`
	TriggerTimestamp                int                    `json:"trigger_timestamp"` // 触发信号K线的时间戳
	OHLCV                           []OHLCV                `json:"ohlcv,omitempty"`   // 最近10根已收盘K线，按近到远排序；由exporter输出阶段填充
	TriggerHistory                  []TriggerHistoryRecord `json:"trigger_history,omitempty"`
	Strategy                        string                 `json:"strategy"`                      // 由strategy.manager负责填充
	StrategyVersion                 string                 `json:"strategy_version"`              // 由strategy.manager负责填充
	StrategyTimeframes              []string               `json:"strategy_timeframes,omitempty"` // 由strategy.Get负责填充；strategy.Update阶段保持不变
	StrategyIndicators              map[string][]string    `json:"strategy_indicators,omitempty"` // 由strategy.Get负责填充；strategy.Update阶段保持不变
}

type SignalChangeRecord struct {
	ID              int64
	SingletonID     int64
	Mode            string
	ExchangeID      int64
	SymbolID        int64
	Exchange        string
	Symbol          string
	Timeframe       string
	Strategy        string
	StrategyVersion string
	ChangeStatus    int
	ChangedFields   string
	SignalJSON      string
	EventAtMS       int64
	CreatedAtMS     int64
}

type Position struct {
	PositionID              int64
	SingletonID             int64
	ExchangeID              int64
	SymbolID                int64
	Exchange                string
	Symbol                  string
	Timeframe               string
	PositionSide            string
	GroupID                 string
	MarginMode              string
	LeverageMultiplier      float64
	MarginAmount            float64
	EntryPrice              float64
	EntryQuantity           float64
	EntryValue              float64
	EntryTime               string
	TakeProfitPrice         float64
	StopLossPrice           float64
	CurrentPrice            float64
	UnrealizedProfitAmount  float64
	UnrealizedProfitRate    float64
	HoldingDurationMS       int64
	ExitPrice               float64
	ExitQuantity            float64
	ExitValue               float64
	ExitTime                string
	FeeAmount               float64
	ProfitAmount            float64
	ProfitRate              float64
	MaxFloatingProfitAmount float64
	MaxFloatingLossAmount   float64
	Status                  string
	StrategyName            string
	StrategyVersion         string
	StrategyTimeframes      []string
	ComboKey                string
	UpdatedTime             string
}

type SingletonRecord struct {
	ID           int64   `json:"id"`
	UUID         string  `json:"uuid"`
	Version      string  `json:"version"`
	Mode         string  `json:"mode"`
	Source       *string `json:"source"`
	Status       string  `json:"status"`
	Created      int64   `json:"created"`
	Updated      int64   `json:"updated"`
	Closed       *int64  `json:"closed"`
	Heartbeat    *int64  `json:"heartbeat"`
	LeaseExpires *int64  `json:"lease_expires"`
	StartTime    *string `json:"start_time"`
	EndTime      *string `json:"end_time"`
	Runtime      *string `json:"runtime"`
}

const (
	BacktestTaskStatusPending   = "pending"
	BacktestTaskStatusRunning   = "running"
	BacktestTaskStatusSucceeded = "succeeded"
	BacktestTaskStatusFailed    = "failed"
)

type BacktestTask struct {
	ID                 int64    `json:"id"`
	Status             string   `json:"status"`
	Exchange           string   `json:"exchange"`
	Symbol             string   `json:"symbol"`
	DisplaySymbol      string   `json:"display_symbol"`
	PositionSide       string   `json:"position_side,omitempty"`
	LeverageMultiplier float64  `json:"leverage_multiplier,omitempty"`
	OpenPrice          float64  `json:"open_price,omitempty"`
	ClosePrice         float64  `json:"close_price,omitempty"`
	RealizedProfitRate float64  `json:"realized_profit_rate,omitempty"`
	OpenTimeMS         int64    `json:"open_time_ms,omitempty"`
	CloseTimeMS        int64    `json:"close_time_ms,omitempty"`
	HoldingDurationMS  int64    `json:"holding_duration_ms,omitempty"`
	ChartTimeframe     string   `json:"chart_timeframe"`
	TradeTimeframes    []string `json:"trade_timeframes,omitempty"`
	RangeStartMS       int64    `json:"range_start_ms"`
	RangeEndMS         int64    `json:"range_end_ms"`
	PriceLow           float64  `json:"price_low,omitempty"`
	PriceHigh          float64  `json:"price_high,omitempty"`
	SelectionDirection string   `json:"selection_direction,omitempty"`
	Source             string   `json:"source"`
	HistoryBars        int      `json:"history_bars"`
	SingletonID        int64    `json:"singleton_id,omitempty"`
	SingletonUUID      string   `json:"singleton_uuid,omitempty"`
	PID                int      `json:"pid,omitempty"`
	ErrorMessage       string   `json:"error_message,omitempty"`
	CreatedAtMS        int64    `json:"created_at_ms"`
	StartedAtMS        int64    `json:"started_at_ms,omitempty"`
	FinishedAtMS       int64    `json:"finished_at_ms,omitempty"`
	UpdatedAtMS        int64    `json:"updated_at_ms"`
}

type Decision struct {
	PositionID         int64
	Exchange           string
	Symbol             string
	Timeframe          string
	EventTS            int64
	Action             string
	CloseReason        string
	Strategy           string
	OrderType          string
	PositionSide       string
	MarginMode         string
	Size               float64
	LeverageMultiplier float64
	Price              float64
	StopLossPrice      float64
	TakeProfitPrice    float64
	ClientOrderID      string
}

type Symbol struct {
	Exchange    string
	Symbol      string
	Base        string
	Quote       string
	Type        string
	ListTime    int64
	Timeframes  string
	Active      bool
	Dynamic     bool
	RateLimitMS int
}

type RiskAccountState struct {
	Mode                  string
	Exchange              string
	TradeDate             string
	TotalUSDT             float64
	FundingUSDT           float64
	TradingUSDT           float64
	PerTradeUSDT          float64
	DailyLossLimitUSDT    float64
	DailyRealizedUSDT     float64
	DailyClosedProfitUSDT float64
	UpdatedAtMS           int64
}

type RiskAccountFunds struct {
	Exchange           string
	Currency           string
	FundingUSDT        float64
	TradingUSDT        float64
	TotalUSDT          float64
	PerTradeUSDT       float64
	DailyProfitUSDT    float64
	ClosedProfitRate   float64
	FloatingProfitRate float64
	TotalProfitRate    float64
	UpdatedAtMS        int64
}

type RiskSymbolCooldownState struct {
	Mode                 string
	Exchange             string
	Symbol               string
	ConsecutiveStopLoss  int
	WindowStartAtMS      int64
	LastStopLossAtMS     int64
	CooldownUntilMS      int64
	LastProcessedCloseMS int64
	UpdatedAtMS          int64
}

type RiskTrendGroup struct {
	ID                        int64
	Mode                      string
	Strategy                  string
	PrimaryTimeframe          string
	Side                      string
	AnchorTrendingTimestampMS int64
	State                     string
	LockStage                 string
	SelectedCandidateKey      string
	IncumbentLeaderKey        string
	IncumbentLeaderScore      float64
	IncumbentLeaderClosedAtMS int64
	FirstEntryAtMS            int64
	LastEntryAtMS             int64
	EntryCount                int
	FinishReason              string
	CreatedAtMS               int64
	UpdatedAtMS               int64
	FinishedAtMS              int64
}

type RiskTrendGroupCandidate struct {
	ID                  int64
	Mode                string
	GroupID             int64
	CandidateKey        string
	Exchange            string
	Symbol              string
	CandidateState      string
	IsSelected          bool
	PriorityScore       float64
	ScoreJSON           string
	FirstSeenAtMS       int64
	LastSeenAtMS        int64
	EnteredCount        int
	FirstEntryAtMS      int64
	LastEntryAtMS       int64
	LastExitAtMS        int64
	HasOpenPosition     bool
	LastSignalAction    int
	LastHighSide        int
	LastMidSide         int
	TrendingTimestampMS int64
	ExitReason          string
	UpdatedAtMS         int64
}

type RiskOpenPosition struct {
	SingletonID             int64
	Mode                    string
	Exchange                string
	Symbol                  string
	InstID                  string
	Pos                     string
	PosSide                 string
	MgnMode                 string
	Margin                  string
	Lever                   string
	AvgPx                   string
	Upl                     string
	UplRatio                string
	NotionalUSD             string
	MarkPx                  string
	LiqPx                   string
	TPTriggerPx             string
	SLTriggerPx             string
	OpenTimeMS              int64
	UpdateTimeMS            int64
	RowJSON                 string
	MaxFloatingLossAmount   float64
	MaxFloatingProfitAmount float64
	UpdatedAtMS             int64
}

type RiskClosedPosition struct {
	Mode         string
	Exchange     string
	Symbol       string
	InstID       string
	PosSide      string
	MgnMode      string
	Lever        string
	OpenAvgPx    string
	CloseAvgPx   string
	RealizedPnl  string
	PnlRatio     string
	Fee          string
	FundingFee   string
	OpenTimeMS   int64
	CloseTimeMS  int64
	State        string
	CloseRowJSON string
	UpdatedAtMS  int64
}

type RiskHistoryPosition struct {
	SingletonID             int64
	Mode                    string
	Exchange                string
	Symbol                  string
	InstID                  string
	Pos                     string
	PosSide                 string
	MgnMode                 string
	Margin                  string
	Lever                   string
	AvgPx                   string
	NotionalUSD             string
	MarkPx                  string
	LiqPx                   string
	TPTriggerPx             string
	SLTriggerPx             string
	OpenTimeMS              int64
	OpenUpdateTimeMS        int64
	MaxFloatingLossAmount   float64
	MaxFloatingProfitAmount float64
	OpenRowJSON             string
	CloseAvgPx              string
	RealizedPnl             string
	PnlRatio                string
	Fee                     string
	FundingFee              string
	CloseTimeMS             int64
	State                   string
	CloseRowJSON            string
	CreatedAtMS             int64
	UpdatedAtMS             int64
}

type ExecutionStepResult struct {
	Stage   string `json:"stage"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type ExecutionOrderRecord struct {
	AttemptID string

	SingletonUUID string
	Mode          string
	Source        string

	Exchange     string
	Symbol       string
	InstID       string
	Action       string
	OrderType    string
	PositionSide string
	MarginMode   string

	Size               float64
	LeverageMultiplier float64
	Price              float64
	TakeProfitPrice    float64
	StopLossPrice      float64
	ClientOrderID      string
	Strategy           string

	ResultStatus string
	FailSource   string
	FailStage    string
	FailReason   string

	ExchangeCode        string
	ExchangeMessage     string
	ExchangeOrderID     string
	ExchangeAlgoOrderID string

	HasSideEffect   bool
	StepResultsJSON string
	RequestJSON     string
	ResponseJSON    string

	StartedAtMS  int64
	FinishedAtMS int64
	DurationMS   int64
	CreatedAtMS  int64
	UpdatedAtMS  int64
}

type RiskDecisionRecord struct {
	SingletonID   int64
	SingletonUUID string
	Mode          string

	Exchange  string
	Symbol    string
	Timeframe string
	Strategy  string
	ComboKey  string
	GroupID   string

	SignalAction   int
	HighSide       int
	DecisionAction string
	ResultStatus   string
	RejectReason   string

	EventAtMS           int64
	TriggerTimestampMS  int64
	TrendingTimestampMS int64
	SignalJSON          string
	DecisionJSON        string
	CreatedAtMS         int64
}
