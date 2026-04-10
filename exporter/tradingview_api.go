package exporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

const tradingViewAPIPrefix = "/tradingview/api/v1"
const (
	tradingViewWSTailLimitDefault = 320
	tradingViewWSTailLimitMax     = 1024
)

type tradingViewRuntimeProvider interface {
	ListRuntimeSymbols(exchange string) []market.RuntimeSymbolSnapshot
	ExchangeRuntimeState(exchange string) (string, string)
}

type tradingViewRuntimeCandleProvider interface {
	LookupRuntimeOHLCV(exchange, symbol, timeframe string) (market.RuntimeOHLCVSnapshot, bool)
}

type tradingViewRuntimeOHLCVListProvider interface {
	ListRuntimeOHLCV(exchange, timeframe string) []market.RuntimeOHLCVSnapshot
}

type tradingViewRuntimeResponse struct {
	Exchanges         []tradingViewExchangeItem   `json:"exchanges"`
	Mode              string                      `json:"mode"`
	SelectedExchange  string                      `json:"selected_exchange"`
	DefaultSymbol     string                      `json:"default_symbol"`
	DefaultTimeframe  string                      `json:"default_timeframe"`
	BootstrapComplete bool                        `json:"bootstrap_complete"`
	Timeframes        []string                    `json:"timeframes"`
	StrategyOptions   []tradingViewStrategyOption `json:"strategy_options,omitempty"`
	Symbols           []tradingViewSymbolItem     `json:"symbols"`
	Orders            []tradingViewOrderItem      `json:"orders"`
	Positions         []positionItem              `json:"positions"`
	HistoryPositions  []historyPositionItem       `json:"history_positions"`
	Funds             tradingViewFundsSummary     `json:"funds"`
	ReadOnly          bool                        `json:"read_only"`
}

type tradingViewExchangeItem struct {
	Name              string `json:"name"`
	DisplayName       string `json:"display_name"`
	Active            bool   `json:"active"`
	RuntimeState      string `json:"runtime_state,omitempty"`
	RuntimeMessage    string `json:"runtime_message,omitempty"`
	ActiveSymbolCount int    `json:"active_symbol_count"`
}

type tradingViewSymbolItem struct {
	Exchange               string  `json:"exchange"`
	Symbol                 string  `json:"symbol"`
	DisplaySymbol          string  `json:"display_symbol"`
	Base                   string  `json:"base,omitempty"`
	Quote                  string  `json:"quote,omitempty"`
	MarketType             string  `json:"market_type,omitempty"`
	ActiveSync             bool    `json:"active_sync"`
	Dynamic                bool    `json:"dynamic,omitempty"`
	WSSubscribed           bool    `json:"ws_subscribed,omitempty"`
	LastWSAtMS             int64   `json:"last_ws_at_ms,omitempty"`
	IsHeld                 bool    `json:"is_held"`
	PositionSide           string  `json:"position_side,omitempty"`
	LeverageMultiplier     float64 `json:"leverage_multiplier,omitempty"`
	MarginAmount           float64 `json:"margin_amount,omitempty"`
	LastPrice              float64 `json:"last_price"`
	Change24hPct           float64 `json:"change_24h_pct"`
	High24h                float64 `json:"high_24h"`
	Low24h                 float64 `json:"low_24h"`
	Turnover24h            float64 `json:"turnover_24h"`
	UnrealizedProfitAmount float64 `json:"unrealized_profit_amount,omitempty"`
	UnrealizedProfitRate   float64 `json:"unrealized_profit_rate,omitempty"`
}

type tradingViewFundsSummary struct {
	Exchange           string  `json:"exchange"`
	Currency           string  `json:"currency"`
	TotalEquityUSDT    float64 `json:"total_equity_usdt"`
	FloatingProfitUSDT float64 `json:"floating_profit_usdt"`
	MarginInUseUSDT    float64 `json:"margin_in_use_usdt"`
	FundingUSDT        float64 `json:"funding_usdt"`
	TradingUSDT        float64 `json:"trading_usdt"`
	PerTradeUSDT       float64 `json:"per_trade_usdt"`
	DailyProfitUSDT    float64 `json:"daily_profit_usdt"`
	ClosedProfitRate   float64 `json:"closed_profit_rate"`
	FloatingProfitRate float64 `json:"floating_profit_rate"`
	TotalProfitRate    float64 `json:"total_profit_rate"`
	UpdatedAtMS        int64   `json:"updated_at_ms"`
}

type tradingViewOrderItem struct {
	ID                 int64   `json:"id"`
	Exchange           string  `json:"exchange"`
	Symbol             string  `json:"symbol"`
	DisplaySymbol      string  `json:"display_symbol"`
	Action             string  `json:"action"`
	OrderType          string  `json:"order_type,omitempty"`
	PositionSide       string  `json:"position_side,omitempty"`
	LeverageMultiplier float64 `json:"leverage_multiplier,omitempty"`
	Price              float64 `json:"price,omitempty"`
	Size               float64 `json:"size,omitempty"`
	TakeProfitPrice    float64 `json:"take_profit_price,omitempty"`
	StopLossPrice      float64 `json:"stop_loss_price,omitempty"`
	ResultStatus       string  `json:"result_status,omitempty"`
	ErrorMessage       string  `json:"error_message,omitempty"`
	StartedAtMS        int64   `json:"started_at_ms,omitempty"`
	UpdatedAtMS        int64   `json:"updated_at_ms,omitempty"`
}

type tradingViewCandlesResponse struct {
	Exchange      string                     `json:"exchange"`
	Symbol        string                     `json:"symbol"`
	DisplaySymbol string                     `json:"display_symbol"`
	MarketType    string                     `json:"market_type,omitempty"`
	Timeframe     string                     `json:"timeframe"`
	Candles       []tradingViewCandle        `json:"candles"`
	Indicators    []tradingViewIndicatorLine `json:"indicators"`
}

type tradingViewCandle struct {
	TS     int64   `json:"ts"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

type tradingViewIndicatorLine struct {
	ID          string                    `json:"id"`
	Label       string                    `json:"label"`
	Color       string                    `json:"color"`
	LegendColor string                    `json:"legend_color,omitempty"`
	Points      []tradingViewIndicatorBar `json:"points"`
}

type tradingViewIndicatorBar struct {
	TS    int64   `json:"ts"`
	Value float64 `json:"value"`
}

type tradingViewStrategyOption struct {
	StrategyName    string   `json:"strategy_name"`
	TradeTimeframes []string `json:"trade_timeframes"`
	ComboKey        string   `json:"combo_key"`
	DisplayLabel    string   `json:"display_label"`
}

type tradingViewWSMessage struct {
	Type string                     `json:"type"`
	Data tradingViewCandlesResponse `json:"data"`
	TS   int64                      `json:"ts"`
}

type tradingViewStrategyConfig struct {
	Live     []string                     `json:"live"`
	Paper    []string                     `json:"paper"`
	BackTest []string                     `json:"back-test"`
	Combo    []models.StrategyComboConfig `json:"combo"`
}

type tradingViewSymbolMeta struct {
	Exchange string
	Symbol   string
	Base     string
	Quote    string
	Type     string
}

type tradingViewSymbolStats struct {
	Symbol      string
	LastPrice   float64
	Open24h     float64
	High24h     float64
	Low24h      float64
	Turnover24h float64
	LastTS      int64
}

type tradingViewHistoryCloseOrder struct {
	ID                 int64
	Exchange           string
	Symbol             string
	InstID             string
	PositionSide       string
	MarginMode         string
	Size               float64
	LeverageMultiplier float64
	Price              float64
	ResultStatus       string
	RequestJSON        string
	StartedAtMS        int64
	FinishedAtMS       int64
	EventTS            int64
}

type tradingViewHistoryCloseMetrics struct {
	FirstCloseMS       int64
	LastCloseMS        int64
	ClosedQty          float64
	AverageClosePrice  float64
	RealizedProfit     float64
	LeverageMultiplier float64
}

type tradingViewHistoryPositionRecord struct {
	Key          string
	SortAnchorMS int64
	EntryMS      int64
	Item         historyPositionItem
}

func (s *Server) buildWSSymbolsResponse(exchange string) (wsSymbolsResponse, error) {
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return wsSymbolsResponse{}, fmt.Errorf("symbols exchange is required")
	}
	openPositions, err := s.loadOpenPositions(PositionFilter{Exchange: exchange})
	if err != nil {
		return wsSymbolsResponse{}, err
	}
	openPositions = tradingViewFilterPositionsByExchange(openPositions, exchange)
	runtimeSymbols, err := s.loadTradingViewRuntimeSymbols(exchange)
	if err != nil {
		return wsSymbolsResponse{}, err
	}
	metaBySymbol, err := s.loadTradingViewSymbolMeta(exchange)
	if err != nil {
		return wsSymbolsResponse{}, err
	}
	timeframes, err := s.loadTradingViewTimeframes()
	if err != nil {
		return wsSymbolsResponse{}, err
	}
	statsTimeframe := tradingViewStatsTimeframe(timeframes)
	stats, err := queryTradingViewSymbolStats(
		s.cfg.HistoryStore.DB,
		exchange,
		tradingViewUnionSymbols(runtimeSymbols, openPositions),
		statsTimeframe,
		time.Now().Add(-24*time.Hour).UnixMilli(),
		time.Now().UnixMilli(),
	)
	if err != nil {
		return wsSymbolsResponse{}, err
	}
	stats = s.overlayTradingViewRuntimeSymbolStats(exchange, statsTimeframe, stats)
	return wsSymbolsResponse{
		Exchange: exchange,
		Symbols:  tradingViewBuildSymbolItems(runtimeSymbols, metaBySymbol, stats, openPositions),
	}, nil
}

func (s *Server) buildWSCandlesStreamSnapshot(filter wsCandlesFilter) (tradingViewCandlesResponse, error) {
	if strings.TrimSpace(filter.Exchange) == "" || strings.TrimSpace(filter.Symbol) == "" || strings.TrimSpace(filter.Timeframe) == "" {
		return tradingViewCandlesResponse{}, fmt.Errorf("candles exchange, symbol and timeframe are required")
	}
	return s.buildTradingViewWSTail(filter.Exchange, filter.Symbol, filter.Timeframe, filter.Limit)
}

func (s *Server) handleTradingViewRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.SymbolProvider == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "symbol provider unavailable"})
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	if s.cfg.AccountProvider == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "account provider unavailable"})
		return
	}

	exchanges, exchangeMetas, err := s.loadTradingViewExchanges()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	strategyOptions, err := s.loadTradingViewStrategyOptions(strings.TrimSpace(s.cfg.Mode))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	selectedExchange := tradingViewSelectExchange(strings.TrimSpace(r.URL.Query().Get("exchange")), exchanges)
	if selectedExchange == "" {
		writeJSON(w, http.StatusOK, tradingViewRuntimeResponse{
			Exchanges:         exchanges,
			BootstrapComplete: true,
			StrategyOptions:   strategyOptions,
			ReadOnly:          !s.tradingViewManualTradeEnabled(),
		})
		return
	}

	timeframes, err := s.loadTradingViewTimeframes()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	defaultTimeframe := tradingViewDefaultTimeframe(timeframes)
	lite := tradingViewRuntimeLiteRequested(r)

	openPositions, err := s.loadOpenPositions(PositionFilter{Exchange: selectedExchange})
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	openPositions = tradingViewFilterPositionsByExchange(openPositions, selectedExchange)
	sort.SliceStable(openPositions, func(i, j int) bool {
		left := tradingViewPositionTimeMS(openPositions[i].UpdatedTime)
		right := tradingViewPositionTimeMS(openPositions[j].UpdatedTime)
		if left != right {
			return left > right
		}
		return openPositions[i].Symbol < openPositions[j].Symbol
	})

	runtimeSymbols, err := s.loadTradingViewRuntimeSymbols(selectedExchange)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	metaBySymbol, err := s.loadTradingViewSymbolMeta(selectedExchange)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	statsTimeframe := tradingViewStatsTimeframe(timeframes)
	stats := make(map[string]tradingViewSymbolStats)
	if !lite {
		stats, err = queryTradingViewSymbolStats(
			s.cfg.HistoryStore.DB,
			selectedExchange,
			tradingViewUnionSymbols(runtimeSymbols, openPositions),
			statsTimeframe,
			time.Now().Add(-24*time.Hour).UnixMilli(),
			time.Now().UnixMilli(),
		)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
	}
	stats = s.overlayTradingViewRuntimeSymbolStats(selectedExchange, statsTimeframe, stats)
	items := tradingViewBuildSymbolItems(runtimeSymbols, metaBySymbol, stats, openPositions)
	defaultSymbol := tradingViewDefaultSymbol(items)
	historyPositions := make([]historyPositionItem, 0)
	orders := make([]tradingViewOrderItem, 0)
	accountFunds := models.RiskAccountFunds{Currency: "USDT"}
	if !lite {
		historyPositions, err = s.loadTradingViewHistoryPositionItems(selectedExchange, openPositions)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
			return
		}
		orders, err = s.loadTradingViewOrderItems(selectedExchange, metaBySymbol)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}

		accountFunds, err = s.cfg.AccountProvider.GetAccountFunds(selectedExchange)
		if err != nil {
			s.logger.Warn("tradingview get account funds failed",
				zap.String("exchange", selectedExchange),
				zap.Error(err),
			)
			accountFunds = models.RiskAccountFunds{Currency: "USDT"}
		}
	}

	writeJSON(w, http.StatusOK, tradingViewRuntimeResponse{
		Exchanges:         exchangeMetas.withSelected(selectedExchange),
		Mode:              strings.TrimSpace(s.cfg.Mode),
		SelectedExchange:  selectedExchange,
		DefaultSymbol:     defaultSymbol,
		DefaultTimeframe:  defaultTimeframe,
		BootstrapComplete: !lite,
		Timeframes:        timeframes,
		StrategyOptions:   strategyOptions,
		Symbols:           items,
		Orders:            orders,
		Positions:         buildPositionItems(openPositions),
		HistoryPositions:  historyPositions,
		Funds:             tradingViewBuildFundsSummary(selectedExchange, accountFunds, openPositions),
		ReadOnly:          !s.tradingViewManualTradeEnabled(),
	})
}

func (s *Server) tradingViewManualTradeEnabled() bool {
	mode := strings.ToLower(strings.TrimSpace(s.cfg.Mode))
	return mode == "live" || mode == "paper"
}

func (s *Server) loadTradingViewOrderItems(exchange string, metaBySymbol map[string]tradingViewSymbolMeta) ([]tradingViewOrderItem, error) {
	if s == nil || s.cfg.HistoryStore == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	if !s.tradingViewManualTradeEnabled() {
		return []tradingViewOrderItem{}, nil
	}
	items, err := s.cfg.HistoryStore.ListManualOrders(strings.TrimSpace(s.cfg.Mode), exchange, models.ManualOrderStatusPending)
	if err != nil {
		return nil, err
	}
	out := make([]tradingViewOrderItem, 0, len(items))
	for _, item := range items {
		out = append(out, s.buildTradingViewOrderItem(item, metaBySymbol))
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].StartedAtMS != out[j].StartedAtMS {
			return out[i].StartedAtMS > out[j].StartedAtMS
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

func (s *Server) loadTradingViewHistoryPositionItems(exchange string, openPositions []models.Position) ([]historyPositionItem, error) {
	fallbackPositions, err := s.listHistoryPositions(exchange, "")
	if err != nil {
		return nil, err
	}
	fallbackItems := buildHistoryPositionItems(buildPositionItems(fallbackPositions))
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return tradingViewTrimHistoryPositionItems(fallbackItems), nil
	}

	mode := strings.TrimSpace(s.cfg.Mode)
	if mode == "" {
		mode = "live"
	}
	snapshots, err := s.cfg.HistoryStore.ListRiskHistorySnapshots(mode, exchange)
	if err != nil {
		return nil, err
	}
	closeOrders, err := s.queryTradingViewHistoryCloseOrders(mode, exchange)
	if err != nil {
		return nil, err
	}
	orderBuckets := buildTradingViewHistoryOrderBuckets(closeOrders)
	if len(snapshots) == 0 && len(orderBuckets) == 0 {
		return tradingViewTrimHistoryPositionItems(fallbackItems), nil
	}

	records := make([]tradingViewHistoryPositionRecord, 0, len(snapshots)+len(openPositions))
	seen := make(map[string]struct{}, len(snapshots)+len(openPositions))
	for _, row := range snapshots {
		record, ok := buildTradingViewHistoryRecordFromSnapshot(row, orderBuckets)
		if !ok {
			continue
		}
		if _, exists := seen[record.Key]; exists {
			continue
		}
		seen[record.Key] = struct{}{}
		records = append(records, record)
	}
	for _, pos := range openPositions {
		record, ok := buildTradingViewHistoryRecordFromOpenPosition(pos, orderBuckets)
		if !ok {
			continue
		}
		if _, exists := seen[record.Key]; exists {
			continue
		}
		seen[record.Key] = struct{}{}
		records = append(records, record)
	}
	if len(records) == 0 {
		return tradingViewTrimHistoryPositionItems(fallbackItems), nil
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].SortAnchorMS != records[j].SortAnchorMS {
			return records[i].SortAnchorMS > records[j].SortAnchorMS
		}
		if records[i].EntryMS != records[j].EntryMS {
			return records[i].EntryMS > records[j].EntryMS
		}
		if records[i].Item.Exchange != records[j].Item.Exchange {
			return records[i].Item.Exchange < records[j].Item.Exchange
		}
		if records[i].Item.Symbol != records[j].Item.Symbol {
			return records[i].Item.Symbol < records[j].Item.Symbol
		}
		return records[i].Item.PositionSide < records[j].Item.PositionSide
	})

	items := make([]historyPositionItem, 0, len(records))
	for _, record := range records {
		items = append(items, record.Item)
	}
	return tradingViewTrimHistoryPositionItems(items), nil
}

func (s *Server) queryTradingViewHistoryCloseOrders(mode, exchange string) ([]tradingViewHistoryCloseOrder, error) {
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = "live"
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	if exchange == "" {
		return nil, nil
	}
	rows, err := s.cfg.HistoryStore.DB.Query(
		`SELECT id, exchange, symbol, inst_id, position_side, margin_mode, size, leverage_multiplier,
		        price, result_status, request_json, started_at_ms, finished_at_ms
		   FROM orders
		  WHERE mode = ?
		    AND exchange = ?
		    AND action = ?
		  ORDER BY started_at_ms ASC, id ASC;`,
		mode,
		exchange,
		models.DecisionActionClose,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]tradingViewHistoryCloseOrder, 0, 32)
	for rows.Next() {
		var item tradingViewHistoryCloseOrder
		if err := rows.Scan(
			&item.ID,
			&item.Exchange,
			&item.Symbol,
			&item.InstID,
			&item.PositionSide,
			&item.MarginMode,
			&item.Size,
			&item.LeverageMultiplier,
			&item.Price,
			&item.ResultStatus,
			&item.RequestJSON,
			&item.StartedAtMS,
			&item.FinishedAtMS,
		); err != nil {
			return nil, err
		}
		item.EventTS = parseTradingViewBacktestOrderEventTS(item.RequestJSON)
		if !isTradingViewBacktestOrderSuccessful(item.ResultStatus) {
			continue
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) handleTradingViewCandles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}

	query := r.URL.Query()
	exchange := strings.TrimSpace(query.Get("exchange"))
	symbol := strings.TrimSpace(query.Get("symbol"))
	timeframe := strings.TrimSpace(query.Get("timeframe"))
	if exchange == "" || symbol == "" || timeframe == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "exchange, symbol and timeframe are required"})
		return
	}
	startMS, err := tradingViewParseOptionalMS(query.Get("start_ms"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	endMS, err := tradingViewParseOptionalMS(query.Get("end_ms"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	start := time.UnixMilli(0)
	if startMS > 0 {
		start = time.UnixMilli(startMS)
	}
	end := time.Now()
	if endMS > 0 {
		end = time.UnixMilli(endMS)
	}
	if end.Before(start) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "end_ms must be greater than or equal to start_ms"})
		return
	}

	candles, err := s.cfg.HistoryStore.ListOHLCVRange(exchange, symbol, timeframe, start, end)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	candles = s.mergeTradingViewRuntimeOHLCV(candles, exchange, symbol, timeframe, start, end, 0)
	metaBySymbol, err := s.loadTradingViewSymbolMeta(exchange)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	meta := metaBySymbol[symbol]
	writeJSON(w, http.StatusOK, tradingViewCandlesResponse{
		Exchange:      exchange,
		Symbol:        symbol,
		DisplaySymbol: tradingViewDisplaySymbol(symbol, meta.Type),
		MarketType:    strings.TrimSpace(meta.Type),
		Timeframe:     timeframe,
		Candles:       tradingViewBuildCandles(candles),
		Indicators:    tradingViewBuildEMALines(candles),
	})
}

func (s *Server) handleTradingViewWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		http.Error(w, "history store unavailable", http.StatusServiceUnavailable)
		return
	}

	query := r.URL.Query()
	exchange := strings.TrimSpace(query.Get("exchange"))
	symbol := strings.TrimSpace(query.Get("symbol"))
	timeframe := strings.TrimSpace(query.Get("timeframe"))
	if exchange == "" || symbol == "" || timeframe == "" {
		http.Error(w, "exchange, symbol and timeframe are required", http.StatusBadRequest)
		return
	}
	if _, ok := market.TimeframeDuration(timeframe); !ok {
		http.Error(w, "invalid timeframe", http.StatusBadRequest)
		return
	}
	limit := tradingViewParseWSLimit(query.Get("limit"))

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
		OriginPatterns:  s.cfg.WSOriginPatterns,
	})
	if err != nil {
		s.logger.Warn("tradingview ws accept failed", zap.Error(err))
		return
	}
	conn.SetReadLimit(wsMaxMessageBytes)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer func() {
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			s.logger.Debug("tradingview ws close failed", zap.Error(err))
		}
	}()

	var sendMu sync.Mutex

	current, err := s.buildTradingViewWSTail(exchange, symbol, timeframe, limit)
	if err != nil {
		_ = s.writeWSError(ctx, conn, &sendMu, "", err.Error())
		return
	}
	if err := s.writeWS(ctx, conn, &sendMu, tradingViewWSMessage{
		Type: "snapshot",
		Data: current,
		TS:   time.Now().UTC().UnixMilli(),
	}); err != nil {
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				cancel()
				return
			}
		}
	}()

	ticker := time.NewTicker(s.cfg.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			<-done
			return
		case <-ticker.C:
			next, err := s.buildTradingViewWSTail(exchange, symbol, timeframe, limit)
			if err != nil {
				s.logger.Warn("tradingview ws tail build failed",
					zap.String("exchange", exchange),
					zap.String("symbol", symbol),
					zap.String("timeframe", timeframe),
					zap.Error(err),
				)
				continue
			}
			if reflect.DeepEqual(current, next) {
				continue
			}
			if err := s.writeWS(ctx, conn, &sendMu, tradingViewWSMessage{
				Type: "diff",
				Data: next,
				TS:   time.Now().UTC().UnixMilli(),
			}); err != nil {
				<-done
				return
			}
			current = next
		}
	}
}

func (s *Server) loadTradingViewExchanges() ([]tradingViewExchangeItem, tradingViewExchangeList, error) {
	items, err := s.cfg.SymbolProvider.ListExchanges()
	if err != nil {
		return nil, nil, fmt.Errorf("list exchanges: %w", err)
	}
	counts := s.tradingViewActiveExchangeCounts()
	out := make([]tradingViewExchangeItem, 0, len(items))
	for _, item := range items {
		name := strings.TrimSpace(item.Name)
		if name == "" || !item.Active {
			continue
		}
		runtimeState := ""
		runtimeMessage := ""
		if s.cfg.TradingViewRuntime != nil {
			runtimeState, runtimeMessage = s.cfg.TradingViewRuntime.ExchangeRuntimeState(name)
		}
		out = append(out, tradingViewExchangeItem{
			Name:              name,
			DisplayName:       strings.ToUpper(name),
			Active:            item.Active,
			RuntimeState:      strings.TrimSpace(runtimeState),
			RuntimeMessage:    strings.TrimSpace(runtimeMessage),
			ActiveSymbolCount: counts[strings.ToLower(name)],
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ActiveSymbolCount != out[j].ActiveSymbolCount {
			return out[i].ActiveSymbolCount > out[j].ActiveSymbolCount
		}
		return out[i].Name < out[j].Name
	})
	return out, tradingViewExchangeList(out), nil
}

func (s *Server) tradingViewActiveExchangeCounts() map[string]int {
	out := make(map[string]int)
	if s.cfg.TradingViewRuntime == nil {
		return out
	}
	items := s.cfg.TradingViewRuntime.ListRuntimeSymbols("")
	for _, item := range items {
		if !item.Active {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(item.Exchange))
		if key == "" {
			continue
		}
		out[key]++
	}
	return out
}

func (s *Server) loadTradingViewRuntimeSymbols(exchange string) ([]market.RuntimeSymbolSnapshot, error) {
	if s.cfg.TradingViewRuntime != nil {
		items := s.cfg.TradingViewRuntime.ListRuntimeSymbols(exchange)
		filtered := make([]market.RuntimeSymbolSnapshot, 0, len(items))
		for _, item := range items {
			if strings.TrimSpace(item.Exchange) == "" || strings.TrimSpace(item.Symbol) == "" {
				continue
			}
			if !item.Active {
				continue
			}
			filtered = append(filtered, item)
		}
		return filtered, nil
	}
	if s.cfg.SymbolProvider == nil {
		return nil, fmt.Errorf("symbol provider unavailable")
	}
	items, err := s.cfg.SymbolProvider.ListSymbols()
	if err != nil {
		return nil, fmt.Errorf("list symbols: %w", err)
	}
	out := make([]market.RuntimeSymbolSnapshot, 0, len(items))
	for _, item := range items {
		if !item.Active {
			continue
		}
		if exchange != "" && !strings.EqualFold(item.Exchange, exchange) {
			continue
		}
		out = append(out, market.RuntimeSymbolSnapshot{
			Exchange: item.Exchange,
			Symbol:   item.Symbol,
			Dynamic:  item.Dynamic,
			Active:   true,
		})
	}
	return out, nil
}

func (s *Server) loadTradingViewSymbolMeta(exchange string) (map[string]tradingViewSymbolMeta, error) {
	items, err := s.cfg.SymbolProvider.ListSymbols()
	if err != nil {
		return nil, fmt.Errorf("list symbols: %w", err)
	}
	out := make(map[string]tradingViewSymbolMeta)
	for _, item := range items {
		if exchange != "" && !strings.EqualFold(item.Exchange, exchange) {
			continue
		}
		if strings.TrimSpace(item.Symbol) == "" {
			continue
		}
		out[item.Symbol] = tradingViewSymbolMeta{
			Exchange: item.Exchange,
			Symbol:   item.Symbol,
			Base:     strings.TrimSpace(item.Base),
			Quote:    strings.TrimSpace(item.Quote),
			Type:     strings.TrimSpace(item.Type),
		}
	}
	return out, nil
}

func (s *Server) loadTradingViewTimeframes() ([]string, error) {
	if s.cfg.HistoryStore == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	value, found, err := s.cfg.HistoryStore.GetConfigValue("strategy")
	if err != nil {
		return nil, fmt.Errorf("get config.strategy: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("missing config.strategy")
	}
	var cfg tradingViewStrategyConfig
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return nil, fmt.Errorf("invalid config.strategy: %w", err)
	}
	set := make(map[string]time.Duration)
	for i, combo := range cfg.Combo {
		_, timeframes, _ := common.NormalizeStrategyIdentity("", combo.Timeframes, "")
		if len(timeframes) == 0 {
			continue
		}
		for _, timeframe := range timeframes {
			duration, ok := market.TimeframeDuration(timeframe)
			if !ok {
				return nil, fmt.Errorf("invalid config.strategy.combo[%d] timeframe %q", i, timeframe)
			}
			set[timeframe] = duration
		}
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("config.strategy.combo is empty")
	}
	out := make([]string, 0, len(set))
	for timeframe := range set {
		out = append(out, timeframe)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, _ := market.TimeframeDuration(out[i])
		right, _ := market.TimeframeDuration(out[j])
		if left != right {
			return left < right
		}
		return out[i] < out[j]
	})
	return out, nil
}

func (s *Server) loadTradingViewStrategyOptions(mode string) ([]tradingViewStrategyOption, error) {
	if s.cfg.HistoryStore == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	value, found, err := s.cfg.HistoryStore.GetConfigValue("strategy")
	if err != nil {
		return nil, fmt.Errorf("get config.strategy: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("missing config.strategy")
	}
	var cfg tradingViewStrategyConfig
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return nil, fmt.Errorf("invalid config.strategy: %w", err)
	}
	names := make([]string, 0)
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "paper":
		names = append(names, cfg.Paper...)
	case "back-test":
		names = append(names, cfg.BackTest...)
	default:
		names = append(names, cfg.Live...)
	}
	normalizedNames := make([]string, 0, len(names))
	seenNames := make(map[string]struct{}, len(names))
	for _, item := range names {
		normalized := strings.ToLower(strings.TrimSpace(item))
		if normalized == "" {
			continue
		}
		if _, exists := seenNames[normalized]; exists {
			continue
		}
		seenNames[normalized] = struct{}{}
		normalizedNames = append(normalizedNames, normalized)
	}
	tradeCombos := make([][]string, 0)
	seenCombos := make(map[string]struct{})
	for i, combo := range cfg.Combo {
		if !combo.TradeEnabled {
			continue
		}
		_, timeframes, comboKey := common.NormalizeStrategyIdentity("", combo.Timeframes, "")
		if len(timeframes) == 0 || comboKey == "" {
			return nil, fmt.Errorf("invalid config.strategy.combo[%d]: empty timeframes", i)
		}
		for _, timeframe := range timeframes {
			if _, ok := market.TimeframeDuration(timeframe); !ok {
				return nil, fmt.Errorf("invalid config.strategy.combo[%d] timeframe %q", i, timeframe)
			}
		}
		if _, exists := seenCombos[comboKey]; exists {
			continue
		}
		seenCombos[comboKey] = struct{}{}
		tradeCombos = append(tradeCombos, append([]string(nil), timeframes...))
	}
	optionCap := len(normalizedNames)
	if len(tradeCombos) > 0 {
		optionCap *= len(tradeCombos)
	}
	if optionCap <= 0 {
		optionCap = len(normalizedNames)
	}
	out := make([]tradingViewStrategyOption, 0, optionCap)
	for _, name := range normalizedNames {
		if len(tradeCombos) == 0 {
			out = append(out, tradingViewStrategyOption{
				StrategyName: name,
				DisplayLabel: name,
			})
			continue
		}
		for _, combo := range tradeCombos {
			comboKey := strings.Join(combo, "/")
			out = append(out, tradingViewStrategyOption{
				StrategyName:    name,
				TradeTimeframes: append([]string(nil), combo...),
				ComboKey:        comboKey,
				DisplayLabel:    fmt.Sprintf("%s %s", name, comboKey),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].StrategyName != out[j].StrategyName {
			return out[i].StrategyName < out[j].StrategyName
		}
		return out[i].ComboKey < out[j].ComboKey
	})
	return out, nil
}

func queryTradingViewSymbolStats(db *sql.DB, exchange string, symbols []string, timeframe string, startMS, endMS int64) (map[string]tradingViewSymbolStats, error) {
	out := make(map[string]tradingViewSymbolStats)
	if db == nil || len(symbols) == 0 || strings.TrimSpace(timeframe) == "" {
		return out, nil
	}
	placeholders := make([]string, 0, len(symbols))
	args := make([]any, 0, 4+len(symbols))
	args = append(args, exchange, timeframe, startMS, endMS)
	for _, symbol := range symbols {
		placeholders = append(placeholders, "?")
		args = append(args, symbol)
	}
	query := fmt.Sprintf(`
WITH filtered AS (
	SELECT
		s.symbol AS symbol,
		o.ts AS ts,
		o.open AS open,
		o.high AS high,
		o.low AS low,
		o.close AS close,
		o.volume AS volume,
		ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY o.ts ASC) AS rn_asc,
		ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY o.ts DESC) AS rn_desc
	FROM ohlcv o
	JOIN exchanges e ON o.exchange_id = e.id
	JOIN symbols s ON o.symbol_id = s.id
	WHERE e.name = ?
	  AND o.timeframe = ?
	  AND o.ts BETWEEN ? AND ?
	  AND s.symbol IN (%s)
)
SELECT
	symbol,
	COALESCE(MAX(CASE WHEN rn_desc = 1 THEN close END), 0),
	COALESCE(MAX(CASE WHEN rn_asc = 1 THEN open END), 0),
	COALESCE(MAX(high), 0),
	COALESCE(MIN(low), 0),
	COALESCE(SUM(close * volume), 0),
	COALESCE(MAX(CASE WHEN rn_desc = 1 THEN ts END), 0)
FROM filtered
GROUP BY symbol
`, strings.Join(placeholders, ","))
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tradingview symbol stats: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var item tradingViewSymbolStats
		if err := rows.Scan(
			&item.Symbol,
			&item.LastPrice,
			&item.Open24h,
			&item.High24h,
			&item.Low24h,
			&item.Turnover24h,
			&item.LastTS,
		); err != nil {
			return nil, fmt.Errorf("scan tradingview symbol stats: %w", err)
		}
		out[item.Symbol] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tradingview symbol stats: %w", err)
	}
	return out, nil
}

func tradingViewBuildSymbolItems(
	runtimeSymbols []market.RuntimeSymbolSnapshot,
	metaBySymbol map[string]tradingViewSymbolMeta,
	stats map[string]tradingViewSymbolStats,
	openPositions []models.Position,
) []tradingViewSymbolItem {
	positionBySymbol := make(map[string]models.Position)
	for _, position := range openPositions {
		if strings.TrimSpace(position.Symbol) == "" {
			continue
		}
		if existing, ok := positionBySymbol[position.Symbol]; ok {
			if tradingViewPreferPosition(position, existing) {
				positionBySymbol[position.Symbol] = position
			}
			continue
		}
		positionBySymbol[position.Symbol] = position
	}

	seen := make(map[string]struct{})
	out := make([]tradingViewSymbolItem, 0, len(runtimeSymbols)+len(positionBySymbol))
	for _, item := range runtimeSymbols {
		key := strings.TrimSpace(item.Symbol)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, tradingViewBuildSymbolItem(item.Exchange, key, item.Active, item.Dynamic, item.WSSubscribed, item.LastWSAtMS, metaBySymbol[key], stats[key], positionBySymbol[key]))
	}
	for symbol, position := range positionBySymbol {
		if _, ok := seen[symbol]; ok {
			continue
		}
		seen[symbol] = struct{}{}
		meta := metaBySymbol[symbol]
		if meta.Exchange == "" {
			meta.Exchange = position.Exchange
			meta.Symbol = symbol
		}
		out = append(out, tradingViewBuildSymbolItem(position.Exchange, symbol, false, false, false, 0, meta, stats[symbol], position))
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].IsHeld != out[j].IsHeld {
			return out[i].IsHeld
		}
		if out[i].IsHeld && out[j].IsHeld {
			left := absFloat(out[i].UnrealizedProfitAmount)
			right := absFloat(out[j].UnrealizedProfitAmount)
			if left != right {
				return left > right
			}
		}
		if out[i].Turnover24h != out[j].Turnover24h {
			return out[i].Turnover24h > out[j].Turnover24h
		}
		return out[i].DisplaySymbol < out[j].DisplaySymbol
	})
	return out
}

func tradingViewBuildSymbolItem(
	exchange string,
	symbol string,
	activeSync bool,
	dynamic bool,
	wsSubscribed bool,
	lastWSAtMS int64,
	meta tradingViewSymbolMeta,
	stats tradingViewSymbolStats,
	position models.Position,
) tradingViewSymbolItem {
	item := tradingViewSymbolItem{
		Exchange:      strings.TrimSpace(firstNonEmpty(exchange, meta.Exchange)),
		Symbol:        symbol,
		DisplaySymbol: tradingViewDisplaySymbol(symbol, meta.Type),
		Base:          meta.Base,
		Quote:         meta.Quote,
		MarketType:    strings.TrimSpace(meta.Type),
		ActiveSync:    activeSync,
		Dynamic:       dynamic,
		WSSubscribed:  wsSubscribed,
		LastWSAtMS:    lastWSAtMS,
		LastPrice:     stats.LastPrice,
		High24h:       stats.High24h,
		Low24h:        stats.Low24h,
		Turnover24h:   stats.Turnover24h,
	}
	if stats.Open24h > 0 && stats.LastPrice > 0 {
		item.Change24hPct = (stats.LastPrice - stats.Open24h) / stats.Open24h * 100
	}
	if strings.TrimSpace(position.Symbol) != "" {
		item.IsHeld = true
		item.PositionSide = strings.TrimSpace(position.PositionSide)
		item.LeverageMultiplier = position.LeverageMultiplier
		item.MarginAmount = position.MarginAmount
		item.UnrealizedProfitAmount = position.UnrealizedProfitAmount
		item.UnrealizedProfitRate = position.UnrealizedProfitRate * 100
		if item.LastPrice <= 0 && position.CurrentPrice > 0 {
			item.LastPrice = position.CurrentPrice
		}
	}
	return item
}

func tradingViewBuildFundsSummary(exchange string, funds models.RiskAccountFunds, positions []models.Position) tradingViewFundsSummary {
	summary := tradingViewFundsSummary{
		Exchange:           strings.TrimSpace(exchange),
		Currency:           strings.TrimSpace(funds.Currency),
		TotalEquityUSDT:    funds.TotalUSDT,
		FundingUSDT:        funds.FundingUSDT,
		TradingUSDT:        funds.TradingUSDT,
		PerTradeUSDT:       funds.PerTradeUSDT,
		DailyProfitUSDT:    funds.DailyProfitUSDT,
		ClosedProfitRate:   funds.ClosedProfitRate,
		FloatingProfitRate: funds.FloatingProfitRate,
		TotalProfitRate:    funds.TotalProfitRate,
		UpdatedAtMS:        funds.UpdatedAtMS,
	}
	for _, position := range positions {
		summary.FloatingProfitUSDT += position.UnrealizedProfitAmount
		summary.MarginInUseUSDT += position.MarginAmount
	}
	return summary
}

func tradingViewRuntimeLiteRequested(r *http.Request) bool {
	if r == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("lite"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (s *Server) overlayTradingViewRuntimeSymbolStats(
	exchange string,
	timeframe string,
	base map[string]tradingViewSymbolStats,
) map[string]tradingViewSymbolStats {
	provider, ok := s.cfg.TradingViewRuntime.(tradingViewRuntimeOHLCVListProvider)
	if !ok || provider == nil {
		return base
	}
	items := provider.ListRuntimeOHLCV(exchange, timeframe)
	if len(items) == 0 {
		return base
	}
	out := make(map[string]tradingViewSymbolStats, len(base))
	for key, value := range base {
		out[key] = value
	}
	for _, item := range items {
		if item.Closed || item.OHLCV.TS <= 0 {
			continue
		}
		stats := out[item.Symbol]
		stats.Symbol = item.Symbol
		if item.OHLCV.Close > 0 {
			stats.LastPrice = item.OHLCV.Close
		}
		if item.OHLCV.High > 0 && item.OHLCV.High > stats.High24h {
			stats.High24h = item.OHLCV.High
		}
		if item.OHLCV.Low > 0 && (stats.Low24h <= 0 || item.OHLCV.Low < stats.Low24h) {
			stats.Low24h = item.OHLCV.Low
		}
		if item.OHLCV.Close > 0 && item.OHLCV.Volume > 0 {
			stats.Turnover24h += item.OHLCV.Close * item.OHLCV.Volume
		}
		if item.OHLCV.TS > stats.LastTS {
			stats.LastTS = item.OHLCV.TS
		}
		out[item.Symbol] = stats
	}
	return out
}

func tradingViewUnionSymbols(runtimeSymbols []market.RuntimeSymbolSnapshot, openPositions []models.Position) []string {
	set := make(map[string]struct{})
	out := make([]string, 0, len(runtimeSymbols)+len(openPositions))
	for _, item := range runtimeSymbols {
		symbol := strings.TrimSpace(item.Symbol)
		if symbol == "" {
			continue
		}
		if _, ok := set[symbol]; ok {
			continue
		}
		set[symbol] = struct{}{}
		out = append(out, symbol)
	}
	for _, position := range openPositions {
		symbol := strings.TrimSpace(position.Symbol)
		if symbol == "" {
			continue
		}
		if _, ok := set[symbol]; ok {
			continue
		}
		set[symbol] = struct{}{}
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

func tradingViewFilterPositionsByExchange(positions []models.Position, exchange string) []models.Position {
	if exchange == "" {
		return append([]models.Position(nil), positions...)
	}
	out := make([]models.Position, 0, len(positions))
	for _, position := range positions {
		if strings.EqualFold(strings.TrimSpace(position.Exchange), exchange) {
			out = append(out, position)
		}
	}
	return out
}

func tradingViewBuildCandles(items []models.OHLCV) []tradingViewCandle {
	out := make([]tradingViewCandle, 0, len(items))
	for _, item := range items {
		out = append(out, tradingViewCandle{
			TS:     item.TS,
			Open:   item.Open,
			High:   item.High,
			Low:    item.Low,
			Close:  item.Close,
			Volume: item.Volume,
		})
	}
	return out
}

func (s *Server) buildTradingViewWSTail(exchange, symbol, timeframe string, limit int) (tradingViewCandlesResponse, error) {
	items, err := s.loadWSCandles(exchange, symbol, timeframe, limit)
	if err != nil {
		return tradingViewCandlesResponse{}, err
	}
	items = s.mergeTradingViewRuntimeOHLCV(items, exchange, symbol, timeframe, time.Time{}, time.Now().UTC(), limit)
	metaBySymbol, err := s.loadTradingViewSymbolMeta(exchange)
	if err != nil {
		return tradingViewCandlesResponse{}, err
	}
	meta := metaBySymbol[symbol]
	return tradingViewCandlesResponse{
		Exchange:      exchange,
		Symbol:        symbol,
		DisplaySymbol: tradingViewDisplaySymbol(symbol, meta.Type),
		MarketType:    strings.TrimSpace(meta.Type),
		Timeframe:     timeframe,
		Candles:       tradingViewBuildCandles(items),
		Indicators:    tradingViewBuildEMALines(items),
	}, nil
}

func (s *Server) mergeTradingViewRuntimeOHLCV(
	items []models.OHLCV,
	exchange string,
	symbol string,
	timeframe string,
	start time.Time,
	end time.Time,
	limit int,
) []models.OHLCV {
	provider, ok := s.cfg.TradingViewRuntime.(tradingViewRuntimeCandleProvider)
	if !ok || provider == nil {
		return items
	}
	snapshot, ok := provider.LookupRuntimeOHLCV(exchange, symbol, timeframe)
	if !ok || snapshot.OHLCV.TS <= 0 {
		return items
	}
	if tradingViewSkipRuntimeOHLCV(snapshot, timeframe, end) {
		return items
	}
	if !start.IsZero() && snapshot.OHLCV.TS < start.UnixMilli() {
		return items
	}
	if !end.IsZero() && snapshot.OHLCV.TS > end.UnixMilli() {
		return items
	}
	return tradingViewMergeOHLCV(items, snapshot.OHLCV, limit)
}

func tradingViewSkipRuntimeOHLCV(snapshot market.RuntimeOHLCVSnapshot, timeframe string, end time.Time) bool {
	if snapshot.Closed {
		return false
	}
	duration, ok := market.TimeframeDuration(timeframe)
	if !ok || duration <= 0 || end.IsZero() {
		return false
	}
	return snapshot.OHLCV.TS+duration.Milliseconds() <= end.UnixMilli()
}

func tradingViewMergeOHLCV(items []models.OHLCV, runtime models.OHLCV, limit int) []models.OHLCV {
	if runtime.TS <= 0 {
		return items
	}
	merged := make(map[int64]models.OHLCV, len(items)+1)
	for _, item := range items {
		if item.TS <= 0 {
			continue
		}
		merged[item.TS] = item
	}
	merged[runtime.TS] = runtime
	keys := make([]int64, 0, len(merged))
	for ts := range merged {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	if limit > 0 && len(keys) > limit {
		keys = keys[len(keys)-limit:]
	}
	out := make([]models.OHLCV, 0, len(keys))
	for _, ts := range keys {
		out = append(out, merged[ts])
	}
	return out
}

func tradingViewParseWSLimit(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return tradingViewWSTailLimitDefault
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return tradingViewWSTailLimitDefault
	}
	if value > tradingViewWSTailLimitMax {
		return tradingViewWSTailLimitMax
	}
	return value
}

func tradingViewBuildEMALines(items []models.OHLCV) []tradingViewIndicatorLine {
	if len(items) == 0 {
		return []tradingViewIndicatorLine{}
	}
	closeSeries := make([]float64, 0, len(items))
	for _, item := range items {
		closeSeries = append(closeSeries, item.Close)
	}
	periods := []struct {
		period      int
		color       string
		legendColor string
	}{
		{period: 5, color: "rgba(255,255,255,0.5)", legendColor: "#ffffff"},
		{period: 20, color: "rgba(248,215,0,0.5)", legendColor: "#f8d700"},
		{period: 60, color: "rgba(0,46,253,0.5)", legendColor: "#002efd"},
		{period: 120, color: "rgba(191,0,225,0.5)", legendColor: "#bf00e1"},
	}
	out := make([]tradingViewIndicatorLine, 0, len(periods))
	for _, period := range periods {
		points := make([]tradingViewIndicatorBar, 0)
		if len(closeSeries) >= period.period {
			values := talib.Ema(closeSeries, period.period)
			points = make([]tradingViewIndicatorBar, 0, len(values))
			for i, value := range values {
				if i >= len(items) {
					break
				}
				if value <= 0 {
					continue
				}
				points = append(points, tradingViewIndicatorBar{
					TS:    items[i].TS,
					Value: value,
				})
			}
		}
		out = append(out, tradingViewIndicatorLine{
			ID:          fmt.Sprintf("ema-%d", period.period),
			Label:       fmt.Sprintf("EMA %d", period.period),
			Color:       period.color,
			LegendColor: period.legendColor,
			Points:      points,
		})
	}
	return out
}

func tradingViewDisplaySymbol(symbol, marketType string) string {
	symbol = strings.TrimSpace(symbol)
	marketType = strings.ToLower(strings.TrimSpace(marketType))
	if symbol == "" {
		return ""
	}
	switch marketType {
	case "swap", "perpetual":
		return symbol + ".P"
	default:
		return symbol
	}
}

func tradingViewDefaultSymbol(items []tradingViewSymbolItem) string {
	if len(items) == 0 {
		return ""
	}
	return items[0].Symbol
}

func tradingViewDefaultTimeframe(timeframes []string) string {
	for _, timeframe := range timeframes {
		if timeframe == "1h" {
			return timeframe
		}
	}
	if len(timeframes) == 0 {
		return ""
	}
	return timeframes[0]
}

func tradingViewStatsTimeframe(timeframes []string) string {
	if len(timeframes) == 0 {
		return "1h"
	}
	return timeframes[0]
}

func tradingViewParseOptionalMS(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid millisecond timestamp %q", raw)
	}
	return value, nil
}

func tradingViewSelectExchange(requested string, items []tradingViewExchangeItem) string {
	requested = strings.TrimSpace(requested)
	if requested != "" {
		for _, item := range items {
			if strings.EqualFold(item.Name, requested) {
				return item.Name
			}
		}
	}
	for _, item := range items {
		if item.ActiveSymbolCount > 0 {
			return item.Name
		}
	}
	if len(items) == 0 {
		return ""
	}
	return items[0].Name
}

func tradingViewPositionTimeMS(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		"2006-01-02T15:04:05",
	}
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func tradingViewTrimHistoryPositionItems(items []historyPositionItem) []historyPositionItem {
	if len(items) <= 100 {
		return items
	}
	return items[:100]
}

func buildTradingViewHistoryRecordFromSnapshot(
	row models.RiskHistoryPosition,
	orderBuckets map[string][]tradingViewHistoryCloseOrder,
) (tradingViewHistoryPositionRecord, bool) {
	exchange := strings.ToLower(strings.TrimSpace(row.Exchange))
	symbol := normalizeVisualHistorySymbol(row.Symbol, row.InstID)
	positionSide := tradingViewNormalizePositionSide(row.PosSide)
	marginMode := tradingViewNormalizeMarginMode(row.MgnMode)
	entryMS := row.OpenTimeMS
	closeMS := row.CloseTimeMS
	if exchange == "" || symbol == "" || positionSide == "" || marginMode == "" || entryMS <= 0 || closeMS <= 0 {
		return tradingViewHistoryPositionRecord{}, false
	}

	entryQty := absFloat(parseNumericText(row.Pos))
	entryPrice := parseNumericText(row.AvgPx)
	entryValue := parseNumericText(row.NotionalUSD)
	if entryValue <= 0 && entryQty > 0 && entryPrice > 0 {
		entryValue = entryQty * entryPrice
	}
	marginAmount := parseNumericText(row.Margin)
	profitAmount := parseNumericText(row.RealizedPnl)
	profitRate := parseNumericText(row.PnlRatio)
	if profitRate == 0 && marginAmount > 0 {
		profitRate = profitAmount / marginAmount
	}
	metrics := summarizeTradingViewHistoryCloseMetrics(
		orderBuckets,
		exchange,
		symbol,
		positionSide,
		marginMode,
		entryMS,
		closeMS,
		entryPrice,
		entryQty,
		marginAmount,
	)
	sortAnchorMS := closeMS
	if metrics.FirstCloseMS > 0 {
		sortAnchorMS = metrics.FirstCloseMS
	}
	exitQty := entryQty
	if metrics.ClosedQty > 0 {
		exitQty = metrics.ClosedQty
	}
	exitPrice := parseNumericText(row.CloseAvgPx)
	if metrics.AverageClosePrice > 0 {
		exitPrice = metrics.AverageClosePrice
	}
	exitValue := 0.0
	if exitPrice > 0 && exitQty > 0 {
		exitValue = exitPrice * exitQty
	}
	meta := models.ExtractStrategyContextMeta(row.OpenRowJSON)
	item := historyPositionItem{
		SingletonID:             row.SingletonID,
		Exchange:                exchange,
		Symbol:                  symbol,
		Timeframe:               tradingViewPrimaryTimeframe(meta),
		PositionSide:            positionSide,
		GroupID:                 strings.TrimSpace(meta.GroupID),
		MarginMode:              marginMode,
		LeverageMultiplier:      tradingViewFirstPositiveFloat(parseNumericText(row.Lever), metrics.LeverageMultiplier),
		MarginAmount:            marginAmount,
		EntryPrice:              entryPrice,
		EntryQuantity:           entryQty,
		EntryValue:              entryValue,
		EntryTime:               formatLocalTimeMS(entryMS),
		TakeProfitPrice:         parseNumericText(row.TPTriggerPx),
		StopLossPrice:           parseNumericText(row.SLTriggerPx),
		CurrentPrice:            parseNumericText(row.MarkPx),
		ExitPrice:               exitPrice,
		ExitQuantity:            exitQty,
		ExitValue:               exitValue,
		ExitTime:                formatLocalTimeMS(closeMS),
		FeeAmount:               parseNumericText(row.Fee) + parseNumericText(row.FundingFee),
		ProfitAmount:            profitAmount,
		ProfitRate:              profitRate,
		MaxFloatingProfitAmount: row.MaxFloatingProfitAmount,
		MaxFloatingProfitRate:   calculateFloatingRate(row.MaxFloatingProfitAmount, marginAmount),
		MaxFloatingLossAmount:   row.MaxFloatingLossAmount,
		MaxFloatingLossRate:     calculateFloatingRate(row.MaxFloatingLossAmount, marginAmount),
		CloseStatus:             tradeActionFullClose,
		Status:                  models.PositionStatusClosed,
		StrategyName:            strings.TrimSpace(meta.StrategyName),
		StrategyVersion:         strings.TrimSpace(meta.StrategyVersion),
		UpdatedTime:             formatLocalTimeMS(maxVisualHistoryInt64(row.UpdatedAtMS, closeMS)),
	}
	return tradingViewHistoryPositionRecord{
		Key:          tradingViewHistoryPositionKey(exchange, symbol, positionSide, marginMode, entryMS),
		SortAnchorMS: sortAnchorMS,
		EntryMS:      entryMS,
		Item:         item,
	}, true
}

func buildTradingViewHistoryRecordFromOpenPosition(
	pos models.Position,
	orderBuckets map[string][]tradingViewHistoryCloseOrder,
) (tradingViewHistoryPositionRecord, bool) {
	exchange := strings.ToLower(strings.TrimSpace(pos.Exchange))
	symbol := strings.TrimSpace(pos.Symbol)
	positionSide := tradingViewNormalizePositionSide(pos.PositionSide)
	marginMode := tradingViewNormalizeMarginMode(pos.MarginMode)
	entryMS := tradingViewPositionTimeMS(pos.EntryTime)
	if exchange == "" || symbol == "" || positionSide == "" || marginMode == "" || entryMS <= 0 {
		return tradingViewHistoryPositionRecord{}, false
	}
	metrics := summarizeTradingViewHistoryCloseMetrics(
		orderBuckets,
		exchange,
		symbol,
		positionSide,
		marginMode,
		entryMS,
		0,
		pos.EntryPrice,
		pos.EntryQuantity,
		pos.MarginAmount,
	)
	if metrics.FirstCloseMS <= 0 {
		return tradingViewHistoryPositionRecord{}, false
	}
	exitValue := 0.0
	if metrics.AverageClosePrice > 0 && metrics.ClosedQty > 0 {
		exitValue = metrics.AverageClosePrice * metrics.ClosedQty
	}
	profitRate := 0.0
	if pos.EntryQuantity > 0 && pos.MarginAmount > 0 && metrics.ClosedQty > 0 {
		marginReleased := pos.MarginAmount * (metrics.ClosedQty / pos.EntryQuantity)
		profitRate = calculateFloatingRate(metrics.RealizedProfit, marginReleased)
	}
	item := historyPositionItem{
		PositionID:              pos.PositionID,
		SingletonID:             pos.SingletonID,
		Exchange:                exchange,
		Symbol:                  symbol,
		Timeframe:               strings.TrimSpace(pos.Timeframe),
		PositionSide:            positionSide,
		GroupID:                 strings.TrimSpace(pos.GroupID),
		MarginMode:              marginMode,
		LeverageMultiplier:      tradingViewFirstPositiveFloat(pos.LeverageMultiplier, metrics.LeverageMultiplier),
		MarginAmount:            pos.MarginAmount,
		EntryPrice:              pos.EntryPrice,
		EntryQuantity:           pos.EntryQuantity,
		EntryValue:              pos.EntryValue,
		EntryTime:               pos.EntryTime,
		TakeProfitPrice:         pos.TakeProfitPrice,
		StopLossPrice:           pos.StopLossPrice,
		CurrentPrice:            pos.CurrentPrice,
		UnrealizedProfitAmount:  pos.UnrealizedProfitAmount,
		UnrealizedProfitRate:    pos.UnrealizedProfitRate,
		ExitPrice:               metrics.AverageClosePrice,
		ExitQuantity:            metrics.ClosedQty,
		ExitValue:               exitValue,
		ExitTime:                formatLocalTimeMS(metrics.LastCloseMS),
		ProfitAmount:            metrics.RealizedProfit,
		ProfitRate:              profitRate,
		MaxFloatingProfitAmount: pos.MaxFloatingProfitAmount,
		MaxFloatingProfitRate:   calculateFloatingRate(pos.MaxFloatingProfitAmount, pos.MarginAmount),
		MaxFloatingLossAmount:   pos.MaxFloatingLossAmount,
		MaxFloatingLossRate:     calculateFloatingRate(pos.MaxFloatingLossAmount, pos.MarginAmount),
		CloseStatus:             tradeActionPartial,
		Status:                  models.PositionStatusOpen,
		StrategyName:            strings.TrimSpace(pos.StrategyName),
		StrategyVersion:         strings.TrimSpace(pos.StrategyVersion),
		UpdatedTime:             formatLocalTimeMS(maxVisualHistoryInt64(metrics.LastCloseMS, tradingViewPositionTimeMS(pos.UpdatedTime))),
	}
	return tradingViewHistoryPositionRecord{
		Key:          tradingViewHistoryPositionKey(exchange, symbol, positionSide, marginMode, entryMS),
		SortAnchorMS: metrics.FirstCloseMS,
		EntryMS:      entryMS,
		Item:         item,
	}, true
}

func buildTradingViewHistoryOrderBuckets(orders []tradingViewHistoryCloseOrder) map[string][]tradingViewHistoryCloseOrder {
	if len(orders) == 0 {
		return nil
	}
	out := make(map[string][]tradingViewHistoryCloseOrder, len(orders))
	for _, item := range orders {
		symbol := normalizeVisualHistorySymbol(item.Symbol, item.InstID)
		positionSide := tradingViewNormalizePositionSide(item.PositionSide)
		if symbol == "" || positionSide == "" {
			continue
		}
		key := tradingViewHistoryOrderBucketKey(item.Exchange, symbol, positionSide)
		out[key] = append(out[key], item)
	}
	return out
}

func summarizeTradingViewHistoryCloseMetrics(
	orderBuckets map[string][]tradingViewHistoryCloseOrder,
	exchange string,
	symbol string,
	positionSide string,
	marginMode string,
	entryMS int64,
	exitMS int64,
	entryPrice float64,
	entryQty float64,
	marginAmount float64,
) tradingViewHistoryCloseMetrics {
	key := tradingViewHistoryOrderBucketKey(exchange, symbol, positionSide)
	orders := orderBuckets[key]
	if len(orders) == 0 {
		return tradingViewHistoryCloseMetrics{}
	}
	marginMode = tradingViewNormalizeMarginMode(marginMode)
	out := tradingViewHistoryCloseMetrics{}
	closeValue := 0.0
	for _, item := range orders {
		eventMS := tradingViewHistoryOrderEventMS(item)
		if eventMS <= 0 || eventMS < entryMS {
			continue
		}
		if exitMS > 0 && eventMS > exitMS {
			continue
		}
		itemMarginMode := tradingViewNormalizeMarginMode(item.MarginMode)
		if itemMarginMode != "" && marginMode != "" && itemMarginMode != marginMode {
			continue
		}
		qty := item.Size
		if qty < 0 {
			qty = -qty
		}
		if out.FirstCloseMS <= 0 || eventMS < out.FirstCloseMS {
			out.FirstCloseMS = eventMS
		}
		if eventMS > out.LastCloseMS {
			out.LastCloseMS = eventMS
		}
		if qty > 0 {
			out.ClosedQty += qty
			if item.Price > 0 {
				closeValue += item.Price * qty
				out.RealizedProfit += tradingViewComputeCloseProfit(positionSide, entryPrice, item.Price, qty)
			}
		}
		out.LeverageMultiplier = tradingViewFirstPositiveFloat(out.LeverageMultiplier, item.LeverageMultiplier)
	}
	if out.ClosedQty > 0 && closeValue > 0 {
		out.AverageClosePrice = closeValue / out.ClosedQty
	}
	if out.RealizedProfit == 0 && out.AverageClosePrice > 0 && entryPrice > 0 && out.ClosedQty > 0 {
		out.RealizedProfit = tradingViewComputeCloseProfit(positionSide, entryPrice, out.AverageClosePrice, out.ClosedQty)
	}
	if out.LeverageMultiplier <= 0 && marginAmount > 0 && entryQty > 0 && entryPrice > 0 {
		notional := entryQty * entryPrice
		if notional > 0 {
			out.LeverageMultiplier = notional / marginAmount
		}
	}
	return out
}

func tradingViewHistoryOrderBucketKey(exchange string, symbol string, positionSide string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + "|" + strings.TrimSpace(symbol) + "|" + tradingViewNormalizePositionSide(positionSide)
}

func tradingViewHistoryPositionKey(exchange string, symbol string, positionSide string, marginMode string, entryMS int64) string {
	return fmt.Sprintf(
		"%s|%s|%s|%s|%d",
		strings.ToLower(strings.TrimSpace(exchange)),
		strings.TrimSpace(symbol),
		tradingViewNormalizePositionSide(positionSide),
		tradingViewNormalizeMarginMode(marginMode),
		entryMS,
	)
}

func tradingViewHistoryOrderEventMS(item tradingViewHistoryCloseOrder) int64 {
	if item.EventTS > 0 {
		return item.EventTS
	}
	if item.StartedAtMS > 0 {
		return item.StartedAtMS
	}
	return item.FinishedAtMS
}

func tradingViewPrimaryTimeframe(meta models.StrategyContextMeta) string {
	meta = models.NormalizeStrategyContextMeta(meta)
	if len(meta.StrategyTimeframes) == 0 {
		return ""
	}
	return meta.StrategyTimeframes[len(meta.StrategyTimeframes)-1]
}

func tradingViewNormalizePositionSide(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "buy", "long":
		return "long"
	case "sell", "short":
		return "short"
	default:
		return strings.ToLower(strings.TrimSpace(side))
	}
}

func tradingViewNormalizeMarginMode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return strings.ToLower(models.MarginModeIsolated)
	}
	return value
}

func tradingViewComputeCloseProfit(positionSide string, entryPrice float64, closePrice float64, quantity float64) float64 {
	if entryPrice <= 0 || closePrice <= 0 || quantity <= 0 {
		return 0
	}
	switch tradingViewNormalizePositionSide(positionSide) {
	case "short":
		return (entryPrice - closePrice) * quantity
	default:
		return (closePrice - entryPrice) * quantity
	}
}

func tradingViewFirstPositiveFloat(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func tradingViewPreferPosition(left, right models.Position) bool {
	leftUpdated := tradingViewPositionTimeMS(left.UpdatedTime)
	rightUpdated := tradingViewPositionTimeMS(right.UpdatedTime)
	if leftUpdated != rightUpdated {
		return leftUpdated > rightUpdated
	}
	return absFloat(left.UnrealizedProfitAmount) > absFloat(right.UnrealizedProfitAmount)
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

type tradingViewExchangeList []tradingViewExchangeItem

func (items tradingViewExchangeList) withSelected(selected string) []tradingViewExchangeItem {
	out := make([]tradingViewExchangeItem, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}
