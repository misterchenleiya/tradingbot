package exchange

import "encoding/json"

type Instrument struct {
	InstID     string
	CtVal      float64
	LotSz      float64
	TickSz     float64
	MinSz      float64
	State      string
	AssetID    int
	SzDecimals int
}

type OHLCV struct {
	TS     int64
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

type MarketSymbol struct {
	Symbol   string
	Base     string
	Quote    string
	Type     string
	ListTime int64
}

type Position struct {
	InstID      string
	Pos         string
	PosSide     string
	MgnMode     string
	Margin      string
	Lever       string
	AvgPx       string
	Upl         string
	UplRatio    string
	NotionalUsd string
	MarkPx      string
	LiqPx       string
	OpenTime    string `json:"cTime"`
	UpdateTime  string `json:"uTime"`
	TPTriggerPx string
	SLTriggerPx string
}

type PositionHistory struct {
	InstID      string
	PosSide     string
	Direction   string
	Pos         string
	MgnMode     string
	Lever       string
	OpenAvgPx   string
	CloseAvgPx  string
	RealizedPnl string
	PnlRatio    string
	Fee         string
	FundingFee  string
	OpenTime    string
	CloseTime   string
	State       string
}

func (p *PositionHistory) UnmarshalJSON(data []byte) error {
	type rawPositionHistory struct {
		InstID      string `json:"instId"`
		PosSide     string `json:"posSide"`
		Direction   string `json:"direction"`
		Pos         string `json:"pos"`
		MgnMode     string `json:"mgnMode"`
		Lever       string `json:"lever"`
		OpenAvgPx   string `json:"openAvgPx"`
		CloseAvgPx  string `json:"closeAvgPx"`
		RealizedPnl string `json:"pnl"`
		PnlRatio    string `json:"pnlRatio"`
		Fee         string `json:"fee"`
		FundingFee  string `json:"fundingFee"`
		OpenTime    string `json:"openTime"`
		CloseTime   string `json:"closeTime"`
		State       string `json:"state"`
		CTime       string `json:"cTime"`
		UTime       string `json:"uTime"`
	}
	var raw rawPositionHistory
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	p.InstID = raw.InstID
	p.PosSide = raw.PosSide
	p.Direction = raw.Direction
	p.Pos = raw.Pos
	p.MgnMode = raw.MgnMode
	p.Lever = raw.Lever
	p.OpenAvgPx = raw.OpenAvgPx
	p.CloseAvgPx = raw.CloseAvgPx
	p.RealizedPnl = raw.RealizedPnl
	p.PnlRatio = raw.PnlRatio
	p.Fee = raw.Fee
	p.FundingFee = raw.FundingFee
	p.OpenTime = raw.OpenTime
	if p.OpenTime == "" {
		p.OpenTime = raw.CTime
	}
	p.CloseTime = raw.CloseTime
	if p.CloseTime == "" {
		p.CloseTime = raw.UTime
	}
	p.State = raw.State
	return nil
}

type Balance struct {
	Ccy       string
	Bal       string
	AvailBal  string
	FrozenBal string
	Eq        string
	AvailEq   string
}

type BalanceSnapshot struct {
	Trading []Balance
	Funding []Balance
}

type Order struct {
	OrdID  string
	State  string
	AvgPx  string
	FillPx string
	Px     string
}

type OrderRequest struct {
	InstID         string
	TdMode         string
	Side           string
	PosSide        string
	OrdType        string
	Sz             string
	Px             string
	ReduceOnly     bool
	ClientOrderID  string
	AttachAlgoOrds []AttachAlgoOrder
}

type AttachAlgoOrder struct {
	TPTriggerPx string `json:"tpTriggerPx,omitempty"`
	TPOrdPx     string `json:"tpOrdPx,omitempty"`
	SLTriggerPx string `json:"slTriggerPx,omitempty"`
	SLOrdPx     string `json:"slOrdPx,omitempty"`
}

type TPSLOrder struct {
	OrderID     string `json:"algoId"`
	InstID      string `json:"instId"`
	OrdType     string `json:"ordType,omitempty"`
	Side        string `json:"side"`
	PosSide     string `json:"posSide"`
	TriggerPx   string `json:"triggerPx,omitempty"`
	ActivePx    string `json:"activePx,omitempty"`
	TPTriggerPx string `json:"tpTriggerPx,omitempty"`
	SLTriggerPx string `json:"slTriggerPx,omitempty"`
	State       string `json:"state"`
}

type TPSLOrderRequest struct {
	InstID        string
	TdMode        string
	Side          string
	PosSide       string
	Sz            string
	TPTriggerPx   string
	SLTriggerPx   string
	ReduceOnly    bool
	ClientOrderID string
}

type CancelTPSLOrderRequest struct {
	OrderID string
	InstID  string
}
