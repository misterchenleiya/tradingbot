package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/exporter/bubbles"
	historyui "github.com/misterchenleiya/tradingbot/exporter/history"
	tradingviewui "github.com/misterchenleiya/tradingbot/exporter/tradingview"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"github.com/misterchenleiya/tradingbot/storage"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

const (
	defaultUpdateInterval       = time.Second
	defaultShutdownTimeout      = 5 * time.Second
	defaultSingletonRunningTTL  = 2 * time.Second
	defaultSingletonClosedTTL   = 5 * time.Minute
	defaultSingletonNotFoundTTL = 5 * time.Second
	wsWriteTimeout              = 5 * time.Second
	wsMaxMessageBytes           = 1 << 20
	signalOHLCVWindowSize       = 10
	wsStreamSignals             = "signals"
	wsStreamGroups              = "groups"
	wsStreamAccount             = "account"
	wsStreamPosition            = "position"
	wsStreamHistory             = "history"
	wsStreamSymbols             = "symbols"
	wsStreamCandles             = "candles"
	wsFetchTargetStatus         = "status"
	wsFetchTargetHistory        = "history"
	wsFetchTargetCandles        = "candles"
	wsHistoryRangeToday         = "today"
	wsHistoryRange24h           = "24h"
	wsHistoryRange3d            = "3d"
	wsHistoryRange7d            = "7d"
	wsCandlesMaxRequests        = 64
	wsCandlesMaxTimeframes      = 64
	wsCandlesDefaultLimit       = 160
	wsCandlesMaxLimit           = 2048
)

type SignalProvider interface {
	ListSignals() map[string]map[string]models.Signal
}

type signalOHLCVProvider interface {
	ListRecentClosedOHLCV(exchange, symbol, timeframe string, limit int) ([]models.OHLCV, error)
}

type signalGroupedProvider interface {
	LookupSignalGrouped(signal models.Signal) (models.SignalGroupedInfo, bool)
}

type SymbolProvider interface {
	ListSymbols() ([]models.Symbol, error)
	ListExchanges() ([]models.Exchange, error)
}

type AccountPositionProvider interface {
	GetAccountFunds(exchange string) (models.RiskAccountFunds, error)
	ListAllOpenPositions() ([]models.Position, error)
	ListOpenPositions(exchange, symbol, timeframe string) ([]models.Position, error)
	ListHistoryPositions(exchange, symbol string) ([]models.Position, error)
}

type Config struct {
	Address                    string
	Version                    string
	Mode                       string
	Provider                   SignalProvider
	SymbolProvider             SymbolProvider
	TradingViewRuntime         tradingViewRuntimeProvider
	AccountProvider            AccountPositionProvider
	HistoryStore               *storage.SQLite
	HistoryRequester           iface.HistoryRequester
	TradeEvaluator             iface.Evaluator
	TradeExecutor              iface.Executor
	Status                     *common.Status
	StatusProviders            []iface.StatusProvider
	SingletonUUID              string
	BacktestRequestController  *market.RequestController
	BacktestMarketClients      map[string]iface.ExchangeMarketDataSource
	BacktestRequireMarketPlane map[string]bool
	BacktestSingletonTTL       time.Duration
	Logger                     *zap.Logger
	UpdateInterval             time.Duration
	ShutdownTimeout            time.Duration
	WSOriginPatterns           []string
}

type Server struct {
	cfg Config

	logger   *zap.Logger
	started  atomic.Bool
	server   *http.Server
	baseURL  string
	basePath string

	singletonCacheMu     sync.RWMutex
	singletonCache       map[singletonQueryKey]singletonCacheEntry
	singletonRunningTTL  time.Duration
	singletonClosedTTL   time.Duration
	singletonNotFoundTTL time.Duration

	tradingViewBacktestRunner  *tradingViewBacktestRunner
	tradingViewManualOrderStop context.CancelFunc
}

type singletonQueryKey struct {
	ID   int64
	UUID string
}

type singletonCacheEntry struct {
	Record    models.SingletonRecord
	Found     bool
	ExpiresAt time.Time
}

func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = glog.Nop()
	}
	cfg.Mode = strings.TrimSpace(cfg.Mode)
	if cfg.Mode == "" {
		cfg.Mode = "live"
	}
	if cfg.UpdateInterval <= 0 {
		cfg.UpdateInterval = defaultUpdateInterval
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	server := &Server{
		cfg:                  cfg,
		logger:               cfg.Logger,
		singletonCache:       make(map[singletonQueryKey]singletonCacheEntry),
		singletonRunningTTL:  defaultSingletonRunningTTL,
		singletonClosedTTL:   defaultSingletonClosedTTL,
		singletonNotFoundTTL: defaultSingletonNotFoundTTL,
	}
	server.tradingViewBacktestRunner = newTradingViewBacktestRunner(tradingViewBacktestRunnerConfig{
		Logger:             cfg.Logger,
		Store:              cfg.HistoryStore,
		Version:            cfg.Version,
		SingletonTTL:       cfg.BacktestSingletonTTL,
		RequestController:  cfg.BacktestRequestController,
		MarketClients:      cfg.BacktestMarketClients,
		RequireMarketPlane: cfg.BacktestRequireMarketPlane,
	})
	return server
}

func (s *Server) Start(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("nil exporter")
	}
	logger := s.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("exporter start",
		zap.String("address", s.cfg.Address),
		zap.Int("status_provider_count", len(s.cfg.StatusProviders)),
		zap.Duration("update_interval", s.cfg.UpdateInterval),
		zap.Duration("shutdown_timeout", s.cfg.ShutdownTimeout),
	)
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("exporter already started")
	}
	if s.cfg.Provider == nil {
		s.started.Store(false)
		return fmt.Errorf("nil signal provider")
	}
	addrInfo, err := parseAddress(s.cfg.Address)
	if err != nil {
		s.started.Store(false)
		return err
	}
	s.baseURL = addrInfo.baseURL
	s.basePath = addrInfo.basePath

	signalsPath := joinPath(s.basePath, "/signals")
	signalsEventsPath := joinPath(signalsPath, "/events")
	groupsPath := joinPath(s.basePath, "/groups")
	accountPath := joinPath(s.basePath, "/account")
	positionsPath := joinPath(s.basePath, "/positions")
	positionPath := joinPath(s.basePath, "/position")
	historyPath := joinPath(s.basePath, "/history")
	executionOrdersPath := joinPath(s.basePath, "/execution-orders")
	riskDecisionsPath := joinPath(s.basePath, "/risk-decisions")
	positionsEventsPath := joinPath(positionsPath, "/events")
	positionEventsPath := joinPath(positionPath, "/events")
	historyEventsPath := joinPath(historyPath, "/events")
	singletonPath := joinPath(s.basePath, "/singleton")
	tradePath := joinPath(s.basePath, "/trade")
	wsPath := joinPath(s.basePath, "/ws/stream")
	statusPath := joinPath(s.basePath, "/status")
	ohlcvStatusPath := joinPath(s.basePath, "/ohlcv-status")
	tradingViewRuntimePath := joinPath(s.basePath, tradingViewAPIPrefix+"/runtime")
	tradingViewCandlesPath := joinPath(s.basePath, tradingViewAPIPrefix+"/candles")
	tradingViewWSPath := joinPath(s.basePath, tradingViewAPIPrefix+"/ws")
	tradingViewTradePath := joinPath(s.basePath, tradingViewAPIPrefix+"/trade")
	tradingViewPositionDelegatePath := joinPath(s.basePath, tradingViewAPIPrefix+"/position-delegate")
	tradingViewBacktestsPath := joinPath(s.basePath, tradingViewAPIPrefix+"/backtests")

	mux := http.NewServeMux()
	mux.HandleFunc(signalsPath, s.handleSignals)
	mux.HandleFunc(signalsEventsPath, s.handleSignalEvents)
	mux.HandleFunc(groupsPath, s.handleGroups)
	mux.HandleFunc(accountPath, s.handleAccount)
	mux.HandleFunc(positionsPath, s.handlePosition)
	mux.HandleFunc(positionPath, s.handlePosition)
	mux.HandleFunc(positionsEventsPath, s.handlePositionEvents)
	mux.HandleFunc(positionEventsPath, s.handlePositionEvents)
	mux.HandleFunc(historyPath, s.handleHistory)
	mux.HandleFunc(executionOrdersPath, s.handleExecutionOrders)
	mux.HandleFunc(riskDecisionsPath, s.handleRiskDecisions)
	mux.HandleFunc(historyEventsPath, s.handleHistoryEvents)
	mux.HandleFunc(singletonPath, s.handleSingleton)
	mux.HandleFunc(tradePath, s.handleTrade)
	mux.HandleFunc(wsPath, s.handleWS)
	mux.HandleFunc(statusPath, s.handleStatus)
	mux.HandleFunc(ohlcvStatusPath, s.handleOHLCVStatus)
	mux.HandleFunc(tradingViewRuntimePath, s.handleTradingViewRuntime)
	mux.HandleFunc(tradingViewCandlesPath, s.handleTradingViewCandles)
	mux.HandleFunc(tradingViewWSPath, s.handleTradingViewWS)
	mux.HandleFunc(tradingViewTradePath, s.handleTradingViewTrade)
	mux.HandleFunc(tradingViewPositionDelegatePath, s.handleTradingViewPositionDelegate)
	mux.HandleFunc(tradingViewBacktestsPath, s.handleTradingViewBacktests)
	mux.HandleFunc(tradingViewBacktestsPath+"/", s.handleTradingViewBacktestSubRoutes)

	// /bubbles/ 前缀的 API 别名，供内嵌前端通过相对路径访问。
	mux.HandleFunc("/bubbles/signals", s.handleSignals)
	mux.HandleFunc("/bubbles/signals/events", s.handleSignalEvents)
	mux.HandleFunc("/bubbles/groups", s.handleGroups)
	mux.HandleFunc("/bubbles/account", s.handleAccount)
	mux.HandleFunc("/bubbles/positions", s.handlePosition)
	mux.HandleFunc("/bubbles/position", s.handlePosition)
	mux.HandleFunc("/bubbles/positions/events", s.handlePositionEvents)
	mux.HandleFunc("/bubbles/position/events", s.handlePositionEvents)
	mux.HandleFunc("/bubbles/history", s.handleHistory)
	mux.HandleFunc("/bubbles/execution-orders", s.handleExecutionOrders)
	mux.HandleFunc("/bubbles/risk-decisions", s.handleRiskDecisions)
	mux.HandleFunc("/bubbles/history/events", s.handleHistoryEvents)
	mux.HandleFunc("/bubbles/singleton", s.handleSingleton)
	mux.HandleFunc("/bubbles/trade", s.handleTrade)
	mux.HandleFunc("/bubbles/ws/stream", s.handleWS)
	mux.HandleFunc("/bubbles/status", s.handleStatus)
	mux.HandleFunc("/bubbles/ohlcv-status", s.handleOHLCVStatus)

	// /tradingview/ 前缀下的状态与通用 WS 别名，供嵌入前端在带前缀部署下访问。
	mux.HandleFunc("/tradingview/positions/events", s.handlePositionEvents)
	mux.HandleFunc("/tradingview/position/events", s.handlePositionEvents)
	mux.HandleFunc("/tradingview/ws/stream", s.handleWS)
	mux.HandleFunc("/tradingview/status", s.handleStatus)

	// visual-history API，供 /visual-history/ 前端使用。
	mux.HandleFunc("/visual-history/api/v1/positions", s.handleVisualHistoryPositions)
	mux.HandleFunc("/visual-history/api/v1/positions/", s.handleVisualHistoryPositionSubRoutes)

	// 内嵌前端 SPA 静态文件服务。
	distFS, err := fs.Sub(bubbles.DistFS, "dist")
	if err != nil {
		s.started.Store(false)
		return fmt.Errorf("open embedded dist: %w", err)
	}
	mux.Handle("/bubbles/", http.StripPrefix("/bubbles/", newSPAHandler(distFS)))
	historyDistFS, err := fs.Sub(historyui.DistFS, "dist")
	if err != nil {
		s.started.Store(false)
		return fmt.Errorf("open embedded history dist: %w", err)
	}
	mux.HandleFunc("/visual-history", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/visual-history/", http.StatusMovedPermanently)
	})
	mux.Handle("/visual-history/", http.StripPrefix("/visual-history/", newSPAHandler(historyDistFS)))
	tradingViewDistFS, err := fs.Sub(tradingviewui.DistFS, "dist")
	if err != nil {
		s.started.Store(false)
		return fmt.Errorf("open embedded tradingview dist: %w", err)
	}
	mux.HandleFunc("/tradingview", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/tradingview/", http.StatusMovedPermanently)
	})
	mux.Handle("/tradingview/", http.StripPrefix("/tradingview/", newSPAHandler(tradingViewDistFS)))

	s.server = &http.Server{
		Addr:    addrInfo.listenAddr,
		Handler: mux,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("exporter listen failed", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()

	if strings.TrimSpace(s.cfg.Mode) == "live" || strings.TrimSpace(s.cfg.Mode) == "paper" {
		manualCtx, manualCancel := context.WithCancel(context.Background())
		s.tradingViewManualOrderStop = manualCancel
		go s.runTradingViewManualOrderLoop(manualCtx)
	}

	logger.Info("exporter started",
		zap.String("address", s.baseURL),
		zap.String("signals", s.baseURL+signalsPath),
		zap.String("account", s.baseURL+accountPath),
		zap.String("position", s.baseURL+positionsPath),
		zap.String("history", s.baseURL+historyPath),
		zap.String("singleton", s.baseURL+singletonPath),
		zap.String("trade", s.baseURL+tradePath),
		zap.String("ws", s.baseURL+wsPath),
		zap.String("status", s.baseURL+statusPath),
		zap.String("ohlcv_status", s.baseURL+ohlcvStatusPath),
		zap.String("bubbles", s.baseURL+"/bubbles/"),
		zap.String("visual_history", s.baseURL+"/visual-history/"),
		zap.Duration("update_interval", s.cfg.UpdateInterval),
	)
	return nil
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	logger := s.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("exporter close")
	defer logger.Info("exporter closed")
	if !s.started.CompareAndSwap(true, false) {
		return nil
	}
	if s.tradingViewBacktestRunner != nil {
		s.tradingViewBacktestRunner.Close(s.cfg.ShutdownTimeout)
	}
	if s.tradingViewManualOrderStop != nil {
		s.tradingViewManualOrderStop()
		s.tradingViewManualOrderStop = nil
	}
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err != nil {
		logger.Error("exporter shutdown failed", zap.Error(err))
	}
	return err
}

func (s *Server) Stop() error {
	return s.Close()
}

func (s *Server) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	s.cfg.Logger = logger
	s.logger = logger
}

type addressInfo struct {
	listenAddr string
	baseURL    string
	basePath   string
}

func parseAddress(raw string) (addressInfo, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return addressInfo{}, fmt.Errorf("empty exporter address")
	}
	if !strings.Contains(raw, "://") {
		if strings.Contains(raw, "/") {
			return addressInfo{}, fmt.Errorf("invalid exporter address: %s", raw)
		}
		return addressInfo{
			listenAddr: raw,
			baseURL:    "http://" + raw,
			basePath:   "",
		}, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return addressInfo{}, fmt.Errorf("parse exporter address: %w", err)
	}
	if parsed.Host == "" {
		return addressInfo{}, fmt.Errorf("invalid exporter address: %s", raw)
	}
	scheme := parsed.Scheme
	if scheme == "" {
		scheme = "http"
	}
	basePath := normalizeBasePath(parsed.Path)
	baseURL := scheme + "://" + parsed.Host + basePath
	return addressInfo{
		listenAddr: parsed.Host,
		baseURL:    baseURL,
		basePath:   basePath,
	}, nil
}

func normalizeBasePath(raw string) string {
	path := strings.TrimSpace(raw)
	if path == "" || path == "/" {
		return ""
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(path, "/")
}

func joinPath(basePath, suffix string) string {
	if basePath == "" {
		return suffix
	}
	return basePath + suffix
}

type SignalFilter struct {
	Exchange        string `json:"exchange,omitempty"`
	Symbol          string `json:"symbol,omitempty"`
	Timeframe       string `json:"timeframe,omitempty"`
	ComboKey        string `json:"combo_key,omitempty"`
	GroupID         string `json:"group_id,omitempty"`
	Strategy        string `json:"strategy,omitempty"`
	StrategyVersion string `json:"strategy_version,omitempty"`
}

func normalizeFilter(filter SignalFilter) SignalFilter {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	filter.Timeframe = strings.TrimSpace(filter.Timeframe)
	_, _, filter.ComboKey = common.NormalizeStrategyIdentity("", nil, filter.ComboKey)
	filter.GroupID = strings.TrimSpace(filter.GroupID)
	filter.Strategy = strings.TrimSpace(filter.Strategy)
	filter.StrategyVersion = strings.TrimSpace(filter.StrategyVersion)
	return filter
}

func (f SignalFilter) match(signal models.Signal) bool {
	if f.Exchange != "" && !strings.EqualFold(signal.Exchange, f.Exchange) {
		return false
	}
	if f.Symbol != "" && !strings.EqualFold(signal.Symbol, f.Symbol) {
		return false
	}
	if f.Timeframe != "" && !strings.EqualFold(signal.Timeframe, f.Timeframe) {
		return false
	}
	if f.ComboKey != "" && !strings.EqualFold(strategyComboKeyFromSignal(signal), f.ComboKey) {
		return false
	}
	if f.GroupID != "" && !strings.EqualFold(strings.TrimSpace(signal.GroupID), f.GroupID) {
		return false
	}
	if f.Strategy != "" && !strings.EqualFold(signal.Strategy, f.Strategy) {
		return false
	}
	if f.StrategyVersion != "" && !strings.EqualFold(signal.StrategyVersion, f.StrategyVersion) {
		return false
	}
	return true
}

func signalFilterFromQuery(r *http.Request) SignalFilter {
	if r == nil {
		return SignalFilter{}
	}
	q := r.URL.Query()
	return normalizeFilter(SignalFilter{
		Exchange:        q.Get("exchange"),
		Symbol:          q.Get("symbol"),
		Timeframe:       q.Get("timeframe"),
		ComboKey:        q.Get("combo_key"),
		GroupID:         q.Get("group_id"),
		Strategy:        q.Get("strategy"),
		StrategyVersion: q.Get("strategy_version"),
	})
}

type GroupsFilter struct {
	GroupID          string
	Strategy         string
	PrimaryTimeframe string
	Side             string
	Symbol           string
}

func normalizeGroupsFilter(filter GroupsFilter) GroupsFilter {
	filter.GroupID = strings.TrimSpace(filter.GroupID)
	filter.Strategy = strings.TrimSpace(filter.Strategy)
	filter.PrimaryTimeframe = strings.TrimSpace(filter.PrimaryTimeframe)
	filter.Side = strings.TrimSpace(filter.Side)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	return filter
}

func groupsFilterFromQuery(r *http.Request) GroupsFilter {
	if r == nil {
		return GroupsFilter{}
	}
	q := r.URL.Query()
	return normalizeGroupsFilter(GroupsFilter{
		GroupID:          q.Get("group_id"),
		Strategy:         q.Get("strategy"),
		PrimaryTimeframe: q.Get("primary_timeframe"),
		Side:             q.Get("side"),
		Symbol:           q.Get("symbol"),
	})
}

type PositionFilter struct {
	Exchange        string `json:"exchange,omitempty"`
	Symbol          string `json:"symbol,omitempty"`
	Timeframe       string `json:"timeframe,omitempty"`
	PositionSide    string `json:"position_side,omitempty"`
	StrategyName    string `json:"strategy_name,omitempty"`
	StrategyVersion string `json:"strategy_version,omitempty"`
}

func normalizePositionFilter(filter PositionFilter) PositionFilter {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	filter.Timeframe = strings.TrimSpace(filter.Timeframe)
	filter.PositionSide = strings.TrimSpace(filter.PositionSide)
	filter.StrategyName = strings.TrimSpace(filter.StrategyName)
	filter.StrategyVersion = strings.TrimSpace(filter.StrategyVersion)
	return filter
}

func positionFilterFromQuery(r *http.Request) PositionFilter {
	if r == nil {
		return PositionFilter{}
	}
	q := r.URL.Query()
	return normalizePositionFilter(PositionFilter{
		Exchange:        q.Get("exchange"),
		Symbol:          q.Get("symbol"),
		Timeframe:       q.Get("timeframe"),
		PositionSide:    q.Get("position_side"),
		StrategyName:    q.Get("strategy_name"),
		StrategyVersion: q.Get("strategy_version"),
	})
}

func (f PositionFilter) match(pos models.Position) bool {
	if f.Exchange != "" && !strings.EqualFold(pos.Exchange, f.Exchange) {
		return false
	}
	if f.Symbol != "" && !strings.EqualFold(pos.Symbol, f.Symbol) {
		return false
	}
	if f.Timeframe != "" && !strings.EqualFold(pos.Timeframe, f.Timeframe) {
		return false
	}
	if f.PositionSide != "" && !strings.EqualFold(pos.PositionSide, f.PositionSide) {
		return false
	}
	if f.StrategyName != "" && !strings.EqualFold(pos.StrategyName, f.StrategyName) {
		return false
	}
	if f.StrategyVersion != "" && !strings.EqualFold(pos.StrategyVersion, f.StrategyVersion) {
		return false
	}
	return true
}

type HistoryFilter struct {
	Exchange        string `json:"exchange,omitempty"`
	Symbol          string `json:"symbol,omitempty"`
	Timeframe       string `json:"timeframe,omitempty"`
	PositionSide    string `json:"position_side,omitempty"`
	StrategyName    string `json:"strategy_name,omitempty"`
	StrategyVersion string `json:"strategy_version,omitempty"`
	StartTime       string `json:"start_time,omitempty"`
	EndTime         string `json:"end_time,omitempty"`
	Range           string `json:"range,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

func normalizeHistoryRange(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return wsHistoryRangeToday, nil
	}
	switch value {
	case wsHistoryRangeToday, wsHistoryRange24h, wsHistoryRange3d, wsHistoryRange7d:
		return value, nil
	default:
		return "", fmt.Errorf("unsupported history range: %s", raw)
	}
}

func normalizeHistoryFilter(filter HistoryFilter) (HistoryFilter, error) {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	filter.Timeframe = strings.TrimSpace(filter.Timeframe)
	filter.PositionSide = strings.TrimSpace(filter.PositionSide)
	filter.StrategyName = strings.TrimSpace(filter.StrategyName)
	filter.StrategyVersion = strings.TrimSpace(filter.StrategyVersion)
	filter.StartTime = strings.TrimSpace(filter.StartTime)
	filter.EndTime = strings.TrimSpace(filter.EndTime)
	if (filter.StartTime == "") != (filter.EndTime == "") {
		return HistoryFilter{}, fmt.Errorf("start_time and end_time must be provided together")
	}
	if filter.StartTime == "" && filter.EndTime == "" {
		rng, err := normalizeHistoryRange(filter.Range)
		if err != nil {
			return HistoryFilter{}, err
		}
		filter.Range = rng
		return filter, nil
	}
	filter.Range = strings.TrimSpace(filter.Range)
	if filter.Limit < 0 {
		return HistoryFilter{}, fmt.Errorf("limit must be a positive integer")
	}
	return filter, nil
}

func historyFilterFromQuery(r *http.Request) (HistoryFilter, error) {
	if r == nil {
		return normalizeHistoryFilter(HistoryFilter{})
	}
	q := r.URL.Query()
	limit := 0
	limitRaw := strings.TrimSpace(q.Get("limit"))
	if limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err != nil || parsed <= 0 {
			return HistoryFilter{}, fmt.Errorf("invalid limit")
		}
		limit = parsed
	}
	return normalizeHistoryFilter(HistoryFilter{
		Exchange:        q.Get("exchange"),
		Symbol:          q.Get("symbol"),
		Timeframe:       q.Get("timeframe"),
		PositionSide:    q.Get("position_side"),
		StrategyName:    q.Get("strategy_name"),
		StrategyVersion: q.Get("strategy_version"),
		StartTime:       q.Get("start_time"),
		EndTime:         q.Get("end_time"),
		Range:           q.Get("range"),
		Limit:           limit,
	})
}

func normalizeExecutionOrderFilter(filter executionOrderFilter) (executionOrderFilter, error) {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	filter.Action = strings.TrimSpace(filter.Action)
	filter.ResultStatus = strings.TrimSpace(filter.ResultStatus)
	filter.StartTime = strings.TrimSpace(filter.StartTime)
	filter.EndTime = strings.TrimSpace(filter.EndTime)
	if (filter.StartTime == "") != (filter.EndTime == "") {
		return executionOrderFilter{}, fmt.Errorf("start_time and end_time must be provided together")
	}
	if filter.StartTime == "" && filter.EndTime == "" {
		rng, err := normalizeHistoryRange(filter.Range)
		if err != nil {
			return executionOrderFilter{}, err
		}
		filter.Range = rng
		return filter, nil
	}
	filter.Range = strings.TrimSpace(filter.Range)
	if filter.Limit < 0 {
		return executionOrderFilter{}, fmt.Errorf("limit must be a positive integer")
	}
	return filter, nil
}

func normalizeRiskDecisionFilter(filter riskDecisionFilter) (riskDecisionFilter, error) {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	filter.Strategy = strings.TrimSpace(filter.Strategy)
	filter.GroupID = strings.TrimSpace(filter.GroupID)
	filter.DecisionAction = strings.TrimSpace(filter.DecisionAction)
	filter.ResultStatus = strings.TrimSpace(filter.ResultStatus)
	filter.StartTime = strings.TrimSpace(filter.StartTime)
	filter.EndTime = strings.TrimSpace(filter.EndTime)
	if (filter.StartTime == "") != (filter.EndTime == "") {
		return riskDecisionFilter{}, fmt.Errorf("start_time and end_time must be provided together")
	}
	if filter.StartTime == "" && filter.EndTime == "" {
		rng, err := normalizeHistoryRange(filter.Range)
		if err != nil {
			return riskDecisionFilter{}, err
		}
		filter.Range = rng
		return filter, nil
	}
	filter.Range = strings.TrimSpace(filter.Range)
	if filter.Limit < 0 {
		return riskDecisionFilter{}, fmt.Errorf("limit must be a positive integer")
	}
	return filter, nil
}

func executionOrderFilterFromQuery(r *http.Request) (executionOrderFilter, error) {
	if r == nil {
		return normalizeExecutionOrderFilter(executionOrderFilter{})
	}
	q := r.URL.Query()
	limit := 0
	limitRaw := strings.TrimSpace(q.Get("limit"))
	if limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err != nil || parsed <= 0 {
			return executionOrderFilter{}, fmt.Errorf("invalid limit")
		}
		limit = parsed
	}
	return normalizeExecutionOrderFilter(executionOrderFilter{
		Exchange:     q.Get("exchange"),
		Symbol:       q.Get("symbol"),
		Action:       q.Get("action"),
		ResultStatus: q.Get("result_status"),
		StartTime:    q.Get("start_time"),
		EndTime:      q.Get("end_time"),
		Range:        q.Get("range"),
		Limit:        limit,
	})
}

func riskDecisionFilterFromQuery(r *http.Request) (riskDecisionFilter, error) {
	if r == nil {
		return normalizeRiskDecisionFilter(riskDecisionFilter{})
	}
	q := r.URL.Query()
	limit := 0
	limitRaw := strings.TrimSpace(q.Get("limit"))
	if limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err != nil || parsed <= 0 {
			return riskDecisionFilter{}, fmt.Errorf("invalid limit")
		}
		limit = parsed
	}
	signalAction := 0
	signalActionRaw := strings.TrimSpace(q.Get("signal_action"))
	if signalActionRaw != "" {
		parsed, err := strconv.Atoi(signalActionRaw)
		if err != nil || parsed < 0 {
			return riskDecisionFilter{}, fmt.Errorf("invalid signal_action")
		}
		signalAction = parsed
	}
	return normalizeRiskDecisionFilter(riskDecisionFilter{
		Exchange:       q.Get("exchange"),
		Symbol:         q.Get("symbol"),
		Strategy:       q.Get("strategy"),
		GroupID:        q.Get("group_id"),
		SignalAction:   signalAction,
		DecisionAction: q.Get("decision_action"),
		ResultStatus:   q.Get("result_status"),
		StartTime:      q.Get("start_time"),
		EndTime:        q.Get("end_time"),
		Range:          q.Get("range"),
		Limit:          limit,
	})
}

func filterGrouped(grouped map[string]map[string]models.Signal, filter SignalFilter) map[string]map[string]models.Signal {
	out := make(map[string]map[string]models.Signal)
	if len(grouped) == 0 {
		return out
	}
	filter = normalizeFilter(filter)
	for _, inner := range grouped {
		for _, signal := range inner {
			if !filter.match(signal) {
				continue
			}
			outerKey, innerKey := groupedKeys(signal)
			bucket := out[outerKey]
			if bucket == nil {
				bucket = make(map[string]models.Signal)
				out[outerKey] = bucket
			}
			bucket[innerKey] = signal
		}
	}
	return out
}

func strategyComboKeyFromSignal(signal models.Signal) string {
	_, _, comboKey := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
	return comboKey
}

func groupedKeys(signal models.Signal) (outer, inner string) {
	comboKey := strategyComboKeyFromSignal(signal)
	outer = signal.Exchange + "|" + signal.Symbol
	inner = signal.Strategy + "|" + comboKey
	return outer, inner
}

func flatKey(signal models.Signal) string {
	comboKey := strategyComboKeyFromSignal(signal)
	return signal.Exchange + "|" + signal.Symbol + "|" + signal.Strategy + "|" + comboKey
}

func flattenGrouped(grouped map[string]map[string]models.Signal) map[string]models.Signal {
	out := make(map[string]models.Signal)
	for _, inner := range grouped {
		for _, signal := range inner {
			out[flatKey(signal)] = signal
		}
	}
	return out
}

func diffSignals(prev, curr map[string]models.Signal) (added, updated map[string]models.Signal, removed []string) {
	added = make(map[string]models.Signal)
	updated = make(map[string]models.Signal)
	for key, signal := range curr {
		old, ok := prev[key]
		if !ok {
			added[key] = signal
			continue
		}
		if !reflect.DeepEqual(old, signal) {
			updated[key] = signal
		}
	}
	for key := range prev {
		if _, ok := curr[key]; !ok {
			removed = append(removed, key)
		}
	}
	if len(removed) > 1 {
		sort.Strings(removed)
	}
	if len(added) == 0 {
		added = nil
	}
	if len(updated) == 0 {
		updated = nil
	}
	return added, updated, removed
}

func groupedFromFlat(items map[string]models.Signal) map[string]map[string]models.Signal {
	out := make(map[string]map[string]models.Signal)
	for _, signal := range items {
		outer, inner := groupedKeys(signal)
		bucket := out[outer]
		if bucket == nil {
			bucket = make(map[string]models.Signal)
			out[outer] = bucket
		}
		bucket[inner] = signal
	}
	return out
}

func (s *Server) snapshot(filter SignalFilter) (map[string]map[string]models.Signal, map[string]models.Signal) {
	grouped := s.enrichGroupedSignalsWithGroupID(s.cfg.Provider.ListSignals())
	grouped = filterGrouped(grouped, filter)
	return grouped, flattenGrouped(grouped)
}

func groupCandidateSymbol(candidateKey string) string {
	parts := strings.SplitN(strings.TrimSpace(candidateKey), "|", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[1])
	}
	return strings.TrimSpace(candidateKey)
}

func (f GroupsFilter) match(group groupItem) bool {
	if f.GroupID != "" && !strings.EqualFold(group.GroupID, f.GroupID) {
		return false
	}
	if f.Strategy != "" && !strings.EqualFold(group.Strategy, f.Strategy) {
		return false
	}
	if f.PrimaryTimeframe != "" && !strings.EqualFold(group.PrimaryTimeframe, f.PrimaryTimeframe) {
		return false
	}
	if f.Side != "" && !strings.EqualFold(group.Side, f.Side) {
		return false
	}
	if f.Symbol != "" {
		for _, candidate := range group.Candidates {
			if strings.EqualFold(groupCandidateSymbol(candidate.CandidateKey), f.Symbol) {
				return true
			}
		}
		return false
	}
	return true
}

func filterGroupsResponse(resp groupsResponse, filter GroupsFilter) groupsResponse {
	filter = normalizeGroupsFilter(filter)
	if len(resp.Groups) == 0 {
		resp.Groups = []groupItem{}
		resp.GroupsActive = 0
		resp.GroupsTotal = 0
		return resp
	}
	filtered := make([]groupItem, 0, len(resp.Groups))
	for _, group := range resp.Groups {
		if !filter.match(group) {
			continue
		}
		filtered = append(filtered, group)
	}
	resp.Groups = filtered
	resp.GroupsActive = len(filtered)
	resp.GroupsTotal = len(filtered)
	return resp
}

func filterPositions(positions []models.Position, filter PositionFilter) []models.Position {
	filter = normalizePositionFilter(filter)
	if len(positions) == 0 {
		return nil
	}
	out := make([]models.Position, 0, len(positions))
	for _, pos := range positions {
		if !filter.match(pos) {
			continue
		}
		out = append(out, pos)
	}
	return out
}

func parseHistoryFilterTime(raw string) (time.Time, error) {
	return time.ParseInLocation("2006/01/02 15:04", strings.TrimSpace(raw), time.Local)
}

func buildHistoryTimeRange(filter HistoryFilter, now time.Time) (int64, int64, error) {
	filter, err := normalizeHistoryFilter(filter)
	if err != nil {
		return 0, 0, err
	}
	if filter.StartTime != "" && filter.EndTime != "" {
		start, err := parseHistoryFilterTime(filter.StartTime)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid start_time")
		}
		end, err := parseHistoryFilterTime(filter.EndTime)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid end_time")
		}
		if end.Before(start) {
			return 0, 0, fmt.Errorf("end_time must be greater than or equal to start_time")
		}
		return start.UnixMilli(), end.UnixMilli(), nil
	}
	startMS := buildWSHistoryRangeStart(now, filter.Range)
	return startMS, now.UnixMilli(), nil
}

func buildExecutionOrderTimeRange(filter executionOrderFilter, now time.Time) (int64, int64, error) {
	return buildHistoryTimeRange(HistoryFilter{
		StartTime: filter.StartTime,
		EndTime:   filter.EndTime,
		Range:     filter.Range,
		Limit:     filter.Limit,
	}, now)
}

func buildRiskDecisionTimeRange(filter riskDecisionFilter, now time.Time) (int64, int64, error) {
	return buildHistoryTimeRange(HistoryFilter{
		StartTime: filter.StartTime,
		EndTime:   filter.EndTime,
		Range:     filter.Range,
		Limit:     filter.Limit,
	}, now)
}

func historyPositionExitTimeMS(pos models.Position) int64 {
	return parseWSTimestampMS(pos.ExitTime)
}

func sortHistoryPositions(items []models.Position) {
	sort.Slice(items, func(i, j int) bool {
		leftExit := historyPositionExitTimeMS(items[i])
		rightExit := historyPositionExitTimeMS(items[j])
		if leftExit != rightExit {
			return leftExit > rightExit
		}
		leftEntry := parseWSTimestampMS(items[i].EntryTime)
		rightEntry := parseWSTimestampMS(items[j].EntryTime)
		if leftEntry != rightEntry {
			return leftEntry > rightEntry
		}
		if items[i].Exchange != items[j].Exchange {
			return items[i].Exchange < items[j].Exchange
		}
		if items[i].Symbol != items[j].Symbol {
			return items[i].Symbol < items[j].Symbol
		}
		return items[i].PositionSide < items[j].PositionSide
	})
}

func filterHistoryPositions(positions []models.Position, filter HistoryFilter, now time.Time) ([]models.Position, error) {
	filter, err := normalizeHistoryFilter(filter)
	if err != nil {
		return nil, err
	}
	startMS, endMS, err := buildHistoryTimeRange(filter, now)
	if err != nil {
		return nil, err
	}
	out := make([]models.Position, 0, len(positions))
	baseFilter := normalizePositionFilter(PositionFilter{
		Exchange:        filter.Exchange,
		Symbol:          filter.Symbol,
		Timeframe:       filter.Timeframe,
		PositionSide:    filter.PositionSide,
		StrategyName:    filter.StrategyName,
		StrategyVersion: filter.StrategyVersion,
	})
	for _, pos := range positions {
		if !baseFilter.match(pos) {
			continue
		}
		exitMS := historyPositionExitTimeMS(pos)
		if exitMS <= 0 || exitMS < startMS || exitMS > endMS {
			continue
		}
		out = append(out, pos)
	}
	sortHistoryPositions(out)
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

type signalOHLCVCacheKey struct {
	Exchange  string
	Symbol    string
	Timeframe string
}

type signalGroupIDCacheEntry struct {
	GroupID string
	OK      bool
}

func normalizeSignalOHLCVWindow(items []models.OHLCV) []models.OHLCV {
	window := make([]models.OHLCV, signalOHLCVWindowSize)
	if len(items) == 0 {
		return window
	}
	limit := len(items)
	if limit > signalOHLCVWindowSize {
		limit = signalOHLCVWindowSize
	}
	copy(window, items[:limit])
	return window
}

func (s *Server) loadSignalGroupID(
	cache map[string]signalGroupIDCacheEntry,
	signal models.Signal,
) string {
	key := flatKey(signal)
	if entry, ok := cache[key]; ok {
		return entry.GroupID
	}
	source, ok := s.cfg.AccountProvider.(signalGroupedProvider)
	if !ok || source == nil {
		cache[key] = signalGroupIDCacheEntry{}
		return ""
	}
	info, found := source.LookupSignalGrouped(signal)
	entry := signalGroupIDCacheEntry{OK: found}
	if found {
		entry.GroupID = strings.TrimSpace(info.GroupID)
	}
	cache[key] = entry
	return entry.GroupID
}

func (s *Server) enrichFlatSignalsWithGroupID(items map[string]models.Signal) map[string]models.Signal {
	if len(items) == 0 {
		return items
	}
	out := make(map[string]models.Signal, len(items))
	cache := make(map[string]signalGroupIDCacheEntry)
	for key, signal := range items {
		next := signal
		next.GroupID = s.loadSignalGroupID(cache, next)
		out[key] = next
	}
	return out
}

func (s *Server) enrichGroupedSignalsWithGroupID(grouped map[string]map[string]models.Signal) map[string]map[string]models.Signal {
	if len(grouped) == 0 {
		return grouped
	}
	out := make(map[string]map[string]models.Signal, len(grouped))
	cache := make(map[string]signalGroupIDCacheEntry)
	for outerKey, inner := range grouped {
		bucket := make(map[string]models.Signal, len(inner))
		for innerKey, signal := range inner {
			next := signal
			next.GroupID = s.loadSignalGroupID(cache, next)
			bucket[innerKey] = next
		}
		out[outerKey] = bucket
	}
	return out
}

func (s *Server) loadSignalOHLCV(
	cache map[signalOHLCVCacheKey][]models.OHLCV,
	exchange, symbol, timeframe string,
) []models.OHLCV {
	key := signalOHLCVCacheKey{
		Exchange:  strings.TrimSpace(exchange),
		Symbol:    strings.TrimSpace(symbol),
		Timeframe: strings.TrimSpace(timeframe),
	}
	if cached, ok := cache[key]; ok {
		return cached
	}
	fallback := normalizeSignalOHLCVWindow(nil)
	if key.Exchange == "" || key.Symbol == "" || key.Timeframe == "" {
		cache[key] = fallback
		return fallback
	}
	source, ok := s.cfg.Provider.(signalOHLCVProvider)
	if !ok || source == nil {
		cache[key] = fallback
		return fallback
	}
	items, err := source.ListRecentClosedOHLCV(key.Exchange, key.Symbol, key.Timeframe, signalOHLCVWindowSize)
	if err != nil {
		s.logger.Warn("load signal ohlcv from cache failed",
			zap.String("exchange", key.Exchange),
			zap.String("symbol", key.Symbol),
			zap.String("timeframe", key.Timeframe),
			zap.Error(err),
		)
		cache[key] = fallback
		return fallback
	}
	normalized := normalizeSignalOHLCVWindow(items)
	cache[key] = normalized
	return normalized
}

func (s *Server) enrichGroupedSignalsWithOHLCV(grouped map[string]map[string]models.Signal) map[string]map[string]models.Signal {
	if len(grouped) == 0 {
		return grouped
	}
	out := make(map[string]map[string]models.Signal, len(grouped))
	cache := make(map[signalOHLCVCacheKey][]models.OHLCV)
	for outerKey, inner := range grouped {
		bucket := make(map[string]models.Signal, len(inner))
		for innerKey, signal := range inner {
			next := signal
			next.OHLCV = s.loadSignalOHLCV(cache, next.Exchange, next.Symbol, next.Timeframe)
			bucket[innerKey] = next
		}
		out[outerKey] = bucket
	}
	return out
}

func (s *Server) enrichGroupedSignalsForOutput(grouped map[string]map[string]models.Signal) map[string]map[string]models.Signal {
	grouped = s.enrichGroupedSignalsWithGroupID(grouped)
	return s.enrichGroupedSignalsWithOHLCV(grouped)
}

func (s *Server) handleSignals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	filter := signalFilterFromQuery(r)
	grouped, _ := s.snapshot(filter)
	grouped = s.enrichGroupedSignalsWithOHLCV(grouped)
	writeJSON(w, http.StatusOK, grouped)
}

type errorResponse struct {
	Error string `json:"error"`
}

type groupsResponse struct {
	Enabled      bool        `json:"enabled"`
	Mode         string      `json:"mode"`
	GroupsTotal  int         `json:"groups_total"`
	GroupsActive int         `json:"groups_active"`
	Groups       []groupItem `json:"groups"`
}

type groupItem struct {
	GroupID                   string                          `json:"group_id"`
	Strategy                  string                          `json:"strategy"`
	PrimaryTimeframe          string                          `json:"primary_timeframe"`
	Side                      string                          `json:"side"`
	AnchorTrendingTimestampMS int64                           `json:"anchor_trending_timestamp_ms"`
	State                     string                          `json:"state"`
	LockStage                 string                          `json:"lock_stage,omitempty"`
	SelectedCandidateKey      string                          `json:"selected_candidate_key,omitempty"`
	EntryCount                int                             `json:"entry_count"`
	Candidates                []models.SignalGroupedCandidate `json:"candidates,omitempty"`
}

type accountResponse struct {
	Exchange           string  `json:"exchange"`
	Currency           string  `json:"currency"`
	FundingUSDT        float64 `json:"funding_usdt"`
	TradingUSDT        float64 `json:"trading_usdt"`
	TotalUSDT          float64 `json:"total_usdt"`
	PerTradeUSDT       float64 `json:"per_trade_usdt"`
	DailyProfitUSDT    float64 `json:"daily_profit_usdt"`
	ClosedProfitRate   float64 `json:"closed_profit_rate"`
	FloatingProfitRate float64 `json:"floating_profit_rate"`
	TotalProfitRate    float64 `json:"total_profit_rate"`
	UpdatedAtMS        int64   `json:"updated_at_ms"`
}

type positionResponse struct {
	Count     int            `json:"count"`
	Positions []positionItem `json:"positions"`
}

type historyResponse struct {
	Count     int                   `json:"count"`
	Positions []historyPositionItem `json:"positions"`
	Groups    []groupItem           `json:"groups"`
}

type executionOrderFilter struct {
	Exchange     string `json:"exchange,omitempty"`
	Symbol       string `json:"symbol,omitempty"`
	Action       string `json:"action,omitempty"`
	ResultStatus string `json:"result_status,omitempty"`
	StartTime    string `json:"start_time,omitempty"`
	EndTime      string `json:"end_time,omitempty"`
	Range        string `json:"range,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

type executionOrdersResponse struct {
	Count  int                  `json:"count"`
	Orders []executionOrderItem `json:"orders"`
}

type riskDecisionFilter struct {
	Exchange       string `json:"exchange,omitempty"`
	Symbol         string `json:"symbol,omitempty"`
	Strategy       string `json:"strategy,omitempty"`
	GroupID        string `json:"group_id,omitempty"`
	SignalAction   int    `json:"signal_action,omitempty"`
	DecisionAction string `json:"decision_action,omitempty"`
	ResultStatus   string `json:"result_status,omitempty"`
	StartTime      string `json:"start_time,omitempty"`
	EndTime        string `json:"end_time,omitempty"`
	Range          string `json:"range,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

type riskDecisionsResponse struct {
	Count     int                `json:"count"`
	Decisions []riskDecisionItem `json:"decisions"`
}

type riskDecisionItem struct {
	ID                  int64  `json:"id"`
	SingletonID         int64  `json:"singleton_id"`
	SingletonUUID       string `json:"singleton_uuid,omitempty"`
	Mode                string `json:"mode,omitempty"`
	Exchange            string `json:"exchange"`
	Symbol              string `json:"symbol"`
	Timeframe           string `json:"timeframe,omitempty"`
	Strategy            string `json:"strategy,omitempty"`
	ComboKey            string `json:"combo_key,omitempty"`
	GroupID             string `json:"group_id,omitempty"`
	SignalAction        int    `json:"signal_action"`
	HighSide            int    `json:"high_side"`
	DecisionAction      string `json:"decision_action,omitempty"`
	ResultStatus        string `json:"result_status,omitempty"`
	RejectReason        string `json:"reject_reason,omitempty"`
	EventAtMS           int64  `json:"event_at_ms"`
	EventTime           string `json:"event_time,omitempty"`
	TriggerTimestampMS  int64  `json:"trigger_timestamp_ms,omitempty"`
	TriggerTime         string `json:"trigger_time,omitempty"`
	TrendingTimestampMS int64  `json:"trending_timestamp_ms,omitempty"`
	TrendingTime        string `json:"trending_time,omitempty"`
}

type executionOrderItem struct {
	SourceID           int64   `json:"source_id"`
	AttemptID          string  `json:"attempt_id,omitempty"`
	SingletonUUID      string  `json:"singleton_uuid,omitempty"`
	Mode               string  `json:"mode,omitempty"`
	Source             string  `json:"source,omitempty"`
	Exchange           string  `json:"exchange"`
	Symbol             string  `json:"symbol"`
	Action             string  `json:"action,omitempty"`
	OrderType          string  `json:"order_type,omitempty"`
	PositionSide       string  `json:"position_side,omitempty"`
	Size               float64 `json:"size,omitempty"`
	LeverageMultiplier float64 `json:"leverage_multiplier,omitempty"`
	Price              float64 `json:"price,omitempty"`
	TakeProfitPrice    float64 `json:"take_profit_price,omitempty"`
	StopLossPrice      float64 `json:"stop_loss_price,omitempty"`
	Strategy           string  `json:"strategy,omitempty"`
	ResultStatus       string  `json:"result_status,omitempty"`
	FailSource         string  `json:"fail_source,omitempty"`
	FailStage          string  `json:"fail_stage,omitempty"`
	FailReason         string  `json:"fail_reason,omitempty"`
	ExchangeCode       string  `json:"exchange_code,omitempty"`
	ExchangeMessage    string  `json:"exchange_message,omitempty"`
	ExchangeOrderID    string  `json:"exchange_order_id,omitempty"`
	HasSideEffect      bool    `json:"has_side_effect"`
	StartedAtMS        int64   `json:"started_at_ms"`
	StartedTime        string  `json:"started_time,omitempty"`
	FinishedAtMS       int64   `json:"finished_at_ms,omitempty"`
	FinishedTime       string  `json:"finished_time,omitempty"`
	DurationMS         int64   `json:"duration_ms,omitempty"`
}

type positionEventsResponse struct {
	PositionID  int64                     `json:"position_id,omitempty"`
	PositionKey string                    `json:"position_key,omitempty"`
	IsOpen      bool                      `json:"is_open,omitempty"`
	Count       int                       `json:"count"`
	Total       int                       `json:"total,omitempty"`
	Truncated   bool                      `json:"truncated,omitempty"`
	Events      []visualHistoryEventEntry `json:"events"`
}

type signalEventsResponse struct {
	Count     int                       `json:"count"`
	Total     int                       `json:"total,omitempty"`
	Truncated bool                      `json:"truncated,omitempty"`
	Events    []visualHistoryEventEntry `json:"events"`
}

type positionItem struct {
	PositionID              int64    `json:"position_id"`
	SingletonID             int64    `json:"singleton_id"`
	ExchangeID              int64    `json:"exchange_id"`
	SymbolID                int64    `json:"symbol_id"`
	Exchange                string   `json:"exchange"`
	Symbol                  string   `json:"symbol"`
	Timeframe               string   `json:"timeframe"`
	PositionSide            string   `json:"position_side"`
	GroupID                 string   `json:"group_id,omitempty"`
	MarginMode              string   `json:"margin_mode"`
	LeverageMultiplier      float64  `json:"leverage_multiplier"`
	MarginAmount            float64  `json:"margin_amount"`
	EntryPrice              float64  `json:"entry_price"`
	EntryQuantity           float64  `json:"entry_quantity"`
	EntryValue              float64  `json:"entry_value"`
	EntryTime               string   `json:"entry_time"`
	TakeProfitPrice         float64  `json:"take_profit_price"`
	StopLossPrice           float64  `json:"stop_loss_price"`
	CurrentPrice            float64  `json:"current_price"`
	UnrealizedProfitAmount  float64  `json:"unrealized_profit_amount"`
	UnrealizedProfitRate    float64  `json:"unrealized_profit_rate"`
	HoldingDurationMS       int64    `json:"holding_duration_ms,omitempty"`
	ExitPrice               float64  `json:"exit_price"`
	ExitQuantity            float64  `json:"exit_quantity"`
	ExitValue               float64  `json:"exit_value"`
	ExitTime                string   `json:"exit_time"`
	FeeAmount               float64  `json:"fee_amount"`
	ProfitAmount            float64  `json:"profit_amount"`
	ProfitRate              float64  `json:"profit_rate"`
	MaxFloatingProfitAmount float64  `json:"max_floating_profit_amount"`
	MaxFloatingProfitRate   float64  `json:"max_floating_profit_rate"`
	MaxFloatingLossAmount   float64  `json:"max_floating_loss_amount"`
	MaxFloatingLossRate     float64  `json:"max_floating_loss_rate"`
	Status                  string   `json:"status"`
	StrategyName            string   `json:"strategy_name"`
	StrategyVersion         string   `json:"strategy_version"`
	StrategyTimeframes      []string `json:"strategy_timeframes,omitempty"`
	ComboKey                string   `json:"combo_key,omitempty"`
	UpdatedTime             string   `json:"updated_time"`
}

type historyPositionItem struct {
	PositionID              int64    `json:"position_id,omitempty"`
	SingletonID             int64    `json:"singleton_id"`
	Exchange                string   `json:"exchange"`
	Symbol                  string   `json:"symbol"`
	Timeframe               string   `json:"timeframe"`
	PositionSide            string   `json:"position_side"`
	GroupID                 string   `json:"group_id,omitempty"`
	MarginMode              string   `json:"margin_mode"`
	LeverageMultiplier      float64  `json:"leverage_multiplier"`
	MarginAmount            float64  `json:"margin_amount"`
	EntryPrice              float64  `json:"entry_price"`
	EntryQuantity           float64  `json:"entry_quantity"`
	EntryValue              float64  `json:"entry_value"`
	EntryTime               string   `json:"entry_time"`
	TakeProfitPrice         float64  `json:"take_profit_price"`
	StopLossPrice           float64  `json:"stop_loss_price"`
	CurrentPrice            float64  `json:"current_price"`
	UnrealizedProfitAmount  float64  `json:"unrealized_profit_amount"`
	UnrealizedProfitRate    float64  `json:"unrealized_profit_rate"`
	ExitPrice               float64  `json:"exit_price"`
	ExitQuantity            float64  `json:"exit_quantity"`
	ExitValue               float64  `json:"exit_value"`
	ExitTime                string   `json:"exit_time"`
	FeeAmount               float64  `json:"fee_amount"`
	ProfitAmount            float64  `json:"profit_amount"`
	ProfitRate              float64  `json:"profit_rate"`
	MaxFloatingProfitAmount float64  `json:"max_floating_profit_amount"`
	MaxFloatingProfitRate   float64  `json:"max_floating_profit_rate"`
	MaxFloatingLossAmount   float64  `json:"max_floating_loss_amount"`
	MaxFloatingLossRate     float64  `json:"max_floating_loss_rate"`
	CloseStatus             string   `json:"close_status,omitempty"`
	Status                  string   `json:"status"`
	StrategyName            string   `json:"strategy_name"`
	StrategyVersion         string   `json:"strategy_version"`
	StrategyTimeframes      []string `json:"strategy_timeframes,omitempty"`
	ComboKey                string   `json:"combo_key,omitempty"`
	UpdatedTime             string   `json:"updated_time"`
}

const (
	tradeActionOpen         = "open"
	tradeActionMoveTPSL     = "move_tpsl"
	tradeActionPartial      = "partial_close"
	tradeActionFullClose    = "full_close"
	tradeStrategyDefault    = "exporter.trade"
	tradeSideLongCanonical  = "long"
	tradeSideShortCanonical = "short"
)

type tradeRequest struct {
	Action    string  `json:"action"`
	Exchange  string  `json:"exchange"`
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	OrderType string  `json:"order_type,omitempty"`
	Amount    float64 `json:"amount"`
	Entry     float64 `json:"entry"`
	TP        float64 `json:"tp"`
	SL        float64 `json:"sl"`
	Strategy  string  `json:"strategy,omitempty"`
}

type tradeResponse struct {
	PositionFound  bool             `json:"position_found"`
	Position       *positionItem    `json:"position,omitempty"`
	Decision       *models.Decision `json:"decision,omitempty"`
	Executed       bool             `json:"executed"`
	RiskError      string           `json:"risk_error,omitempty"`
	ExecutionError string           `json:"execution_error,omitempty"`
}

type tradeExecutionNotifier interface {
	NotifyExecutionResult(decision models.Decision, execErr error)
}

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.AccountProvider == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "account provider unavailable"})
		return
	}
	exchange := strings.TrimSpace(r.URL.Query().Get("exchange"))
	funds, err := s.cfg.AccountProvider.GetAccountFunds(exchange)
	if err != nil {
		s.logger.Warn("get account funds failed",
			zap.String("exchange", exchange),
			zap.Error(err),
		)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, accountResponseFromFunds(funds))
}

func (s *Server) handlePosition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.AccountProvider == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "position provider unavailable"})
		return
	}
	filter := positionFilterFromQuery(r)
	positions, err := s.loadOpenPositions(filter)
	if err != nil {
		s.logger.Warn("list open positions failed",
			zap.String("exchange", filter.Exchange),
			zap.String("symbol", filter.Symbol),
			zap.String("timeframe", filter.Timeframe),
			zap.Error(err),
		)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	items := buildPositionItems(filterPositions(positions, filter))
	writeJSON(w, http.StatusOK, positionResponse{
		Count:     len(items),
		Positions: items,
	})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.AccountProvider == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history provider unavailable"})
		return
	}
	filter, err := historyFilterFromQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	positions, err := s.listHistoryPositions(filter.Exchange, filter.Symbol)
	if err != nil {
		s.logger.Warn("list history positions failed",
			zap.String("exchange", filter.Exchange),
			zap.String("symbol", filter.Symbol),
			zap.Error(err),
		)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	filtered, err := filterHistoryPositions(positions, filter, time.Now())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	items := buildPositionItems(filtered)
	groups := s.buildHistoryGroups(filtered)
	writeJSON(w, http.StatusOK, historyResponse{
		Count:     len(items),
		Positions: buildHistoryPositionItems(items),
		Groups:    groups,
	})
}

func (s *Server) handleExecutionOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	filter, err := executionOrderFilterFromQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	startMS, endMS, err := buildExecutionOrderTimeRange(filter, time.Now())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	items, err := queryExecutionOrderItems(s.cfg.HistoryStore.DB, filter, startMS, endMS)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, executionOrdersResponse{
		Count:  len(items),
		Orders: items,
	})
}

func (s *Server) handleRiskDecisions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	filter, err := riskDecisionFilterFromQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	startMS, endMS, err := buildRiskDecisionTimeRange(filter, time.Now())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	items, err := queryRiskDecisionItems(s.cfg.HistoryStore.DB, filter, startMS, endMS)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, riskDecisionsResponse{
		Count:     len(items),
		Decisions: items,
	})
}

func (s *Server) handlePositionEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	eventLimit := parseVisualHistoryEventLimit(query.Get("event_limit"), 200)
	row, err := s.resolvePositionEventsRow(query.Get("position_id"), query.Get("position_key"), query.Get("exchange"), query.Get("symbol"), query.Get("position_side"), query.Get("margin_mode"), query.Get("entry_time"), query.Get("strategy"), query.Get("version"))
	if err != nil {
		status := http.StatusNotFound
		if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, errorResponse{Error: err.Error()})
		return
	}

	events, err := s.buildVisualHistoryEvents(row)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	total := len(events)
	events, truncated := trimVisualHistoryEvents(events, eventLimit)
	writeJSON(w, http.StatusOK, buildPositionEventsResponse(row, events, total, truncated))
}

func (s *Server) handleSignalEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	req := bubbleSignalEventsRequest{
		Exchange:          strings.TrimSpace(query.Get("exchange")),
		Symbol:            strings.TrimSpace(query.Get("symbol")),
		Timeframe:         strings.TrimSpace(query.Get("timeframe")),
		Strategy:          strings.TrimSpace(query.Get("strategy")),
		StrategyVersion:   strings.TrimSpace(query.Get("version")),
		ComboKey:          strings.TrimSpace(query.Get("combo_key")),
		GroupID:           strings.TrimSpace(query.Get("group_id")),
		TriggerTimestamp:  parseWSTimestampMS(query.Get("trigger_timestamp")),
		TrendingTimestamp: parseWSTimestampMS(query.Get("trending_timestamp")),
	}
	eventLimit := parseVisualHistoryEventLimit(query.Get("event_limit"), 200)
	events, err := s.buildBubbleSignalEvents(req)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, errorResponse{Error: err.Error()})
		return
	}
	total := len(events)
	events, truncated := trimVisualHistoryEvents(events, eventLimit)
	writeJSON(w, http.StatusOK, signalEventsResponse{
		Count:     len(events),
		Total:     total,
		Truncated: truncated,
		Events:    events,
	})
}

func (s *Server) handleHistoryEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	eventLimit := parseVisualHistoryEventLimit(query.Get("event_limit"), 200)
	row, err := s.resolveHistoryEventsRow(query.Get("position_id"), query.Get("position_key"), query.Get("exchange"), query.Get("symbol"), query.Get("position_side"), query.Get("margin_mode"), query.Get("entry_time"), query.Get("exit_time"), query.Get("updated_time"), query.Get("strategy"), query.Get("version"))
	if err != nil {
		status := http.StatusNotFound
		if strings.Contains(strings.ToLower(err.Error()), "unavailable") {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, errorResponse{Error: err.Error()})
		return
	}

	events, err := s.buildVisualHistoryEvents(row)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	total := len(events)
	events, truncated := trimVisualHistoryEvents(events, eventLimit)
	writeJSON(w, http.StatusOK, buildPositionEventsResponse(row, events, total, truncated))
}

func (s *Server) handleSingleton(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "singleton store unavailable"})
		return
	}

	query := r.URL.Query()
	rawID := strings.TrimSpace(query.Get("id"))
	uuid := strings.TrimSpace(query.Get("uuid"))
	if rawID == "" && uuid == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "singleton query requires id or uuid"})
		return
	}

	var id int64
	if rawID != "" {
		parsed, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || parsed <= 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid singleton id"})
			return
		}
		id = parsed
	}

	record, found, err := s.loadSingletonRecord(id, uuid)
	if err != nil {
		s.logger.Warn("get singleton failed",
			zap.Int64("id", id),
			zap.String("uuid", uuid),
			zap.Error(err),
		)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "singleton not found"})
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) listHistoryPositions(exchange, symbol string) ([]models.Position, error) {
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	if exchange != "" {
		return s.cfg.AccountProvider.ListHistoryPositions(exchange, symbol)
	}
	if s.cfg.SymbolProvider == nil {
		return nil, fmt.Errorf("symbol provider unavailable")
	}
	exchanges, err := s.cfg.SymbolProvider.ListExchanges()
	if err != nil {
		return nil, fmt.Errorf("list exchanges: %w", err)
	}
	enabled := make([]string, 0, len(exchanges))
	seen := make(map[string]struct{}, len(exchanges))
	for _, ex := range exchanges {
		if !ex.Active {
			continue
		}
		name := strings.TrimSpace(ex.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		enabled = append(enabled, name)
	}
	sort.Strings(enabled)
	out := make([]models.Position, 0)
	for _, name := range enabled {
		positions, listErr := s.cfg.AccountProvider.ListHistoryPositions(name, symbol)
		if listErr != nil {
			return nil, fmt.Errorf("list history positions for %s: %w", name, listErr)
		}
		out = append(out, positions...)
	}
	return out, nil
}

func (s *Server) loadOpenPositions(filter PositionFilter) ([]models.Position, error) {
	filter = normalizePositionFilter(filter)
	if s.cfg.AccountProvider == nil {
		return nil, fmt.Errorf("position provider unavailable")
	}
	if filter.Exchange != "" && filter.PositionSide == "" && filter.StrategyName == "" && filter.StrategyVersion == "" {
		return s.cfg.AccountProvider.ListOpenPositions(filter.Exchange, filter.Symbol, filter.Timeframe)
	}
	return s.cfg.AccountProvider.ListAllOpenPositions()
}

func (s *Server) loadSingletonRecord(id int64, uuid string) (models.SingletonRecord, bool, error) {
	key := singletonQueryKey{ID: id, UUID: strings.TrimSpace(uuid)}
	now := time.Now()

	s.singletonCacheMu.RLock()
	entry, ok := s.singletonCache[key]
	s.singletonCacheMu.RUnlock()
	if ok && now.Before(entry.ExpiresAt) {
		return entry.Record, entry.Found, nil
	}

	record, found, err := s.cfg.HistoryStore.GetSingleton(id, uuid)
	if err != nil {
		return models.SingletonRecord{}, false, err
	}

	ttl := s.singletonNotFoundTTL
	if found {
		if record.Closed == nil {
			ttl = s.singletonRunningTTL
		} else {
			ttl = s.singletonClosedTTL
		}
	}

	s.singletonCacheMu.Lock()
	s.singletonCache[key] = singletonCacheEntry{
		Record:    record,
		Found:     found,
		ExpiresAt: now.Add(ttl),
	}
	s.singletonCacheMu.Unlock()
	return record, found, nil
}

func historyGroupPublicID(group models.RiskTrendGroup) string {
	strategy := strings.TrimSpace(group.Strategy)
	primaryTimeframe := strings.TrimSpace(group.PrimaryTimeframe)
	side := strings.TrimSpace(group.Side)
	if strategy == "" || primaryTimeframe == "" || side == "" || group.AnchorTrendingTimestampMS <= 0 {
		return ""
	}
	return strategy + "|" + primaryTimeframe + "|" + side + "|" + strconv.FormatInt(group.AnchorTrendingTimestampMS, 10)
}

func buildHistoryGroupCandidateItems(candidates []models.RiskTrendGroupCandidate) []models.SignalGroupedCandidate {
	if len(candidates) == 0 {
		return []models.SignalGroupedCandidate{}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CandidateKey < candidates[j].CandidateKey
	})
	out := make([]models.SignalGroupedCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, models.SignalGroupedCandidate{
			CandidateKey:    candidate.CandidateKey,
			CandidateState:  candidate.CandidateState,
			IsSelected:      candidate.IsSelected,
			PriorityScore:   candidate.PriorityScore,
			HasOpenPosition: candidate.HasOpenPosition,
		})
	}
	return out
}

func (s *Server) buildHistoryGroups(positions []models.Position) []groupItem {
	if len(positions) == 0 || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return []groupItem{}
	}
	mode := strings.TrimSpace(s.cfg.Mode)
	if mode == "" {
		mode = "live"
	}
	latestExitByGroup := make(map[string]int64)
	for _, pos := range positions {
		groupID := strings.TrimSpace(pos.GroupID)
		if groupID == "" {
			continue
		}
		exitMS := historyPositionExitTimeMS(pos)
		if exitMS > latestExitByGroup[groupID] {
			latestExitByGroup[groupID] = exitMS
		}
	}
	if len(latestExitByGroup) == 0 {
		return []groupItem{}
	}

	groups, err := s.cfg.HistoryStore.ListRiskTrendGroups(mode)
	if err != nil {
		s.logger.Warn("list risk trend groups failed", zap.Error(err))
		return []groupItem{}
	}
	groupsByNumericID := make(map[int64]models.RiskTrendGroup)
	groupIDsByNumericID := make(map[int64]string)
	for _, group := range groups {
		publicID := historyGroupPublicID(group)
		if publicID == "" {
			continue
		}
		if _, ok := latestExitByGroup[publicID]; !ok {
			continue
		}
		groupsByNumericID[group.ID] = group
		groupIDsByNumericID[group.ID] = publicID
	}
	if len(groupsByNumericID) == 0 {
		return []groupItem{}
	}

	candidates, err := s.cfg.HistoryStore.ListRiskTrendGroupCandidates(mode)
	if err != nil {
		s.logger.Warn("list risk trend group candidates failed", zap.Error(err))
		return []groupItem{}
	}
	candidatesByGroupID := make(map[int64][]models.RiskTrendGroupCandidate)
	for _, candidate := range candidates {
		if _, ok := groupsByNumericID[candidate.GroupID]; !ok {
			continue
		}
		candidatesByGroupID[candidate.GroupID] = append(candidatesByGroupID[candidate.GroupID], candidate)
	}

	type orderedHistoryGroup struct {
		Group      models.RiskTrendGroup
		PublicID   string
		LatestExit int64
	}
	ordered := make([]orderedHistoryGroup, 0, len(groupsByNumericID))
	for id, group := range groupsByNumericID {
		publicID := groupIDsByNumericID[id]
		ordered = append(ordered, orderedHistoryGroup{
			Group:      group,
			PublicID:   publicID,
			LatestExit: latestExitByGroup[publicID],
		})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].LatestExit != ordered[j].LatestExit {
			return ordered[i].LatestExit > ordered[j].LatestExit
		}
		return ordered[i].PublicID < ordered[j].PublicID
	})

	items := make([]groupItem, 0, len(ordered))
	for _, item := range ordered {
		group := item.Group
		items = append(items, groupItem{
			GroupID:                   item.PublicID,
			Strategy:                  group.Strategy,
			PrimaryTimeframe:          group.PrimaryTimeframe,
			Side:                      group.Side,
			AnchorTrendingTimestampMS: group.AnchorTrendingTimestampMS,
			State:                     group.State,
			LockStage:                 group.LockStage,
			SelectedCandidateKey:      group.SelectedCandidateKey,
			EntryCount:                group.EntryCount,
			Candidates:                buildHistoryGroupCandidateItems(candidatesByGroupID[group.ID]),
		})
	}
	return items
}

func (s *Server) handleTrade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.TradeEvaluator == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "trade evaluator unavailable"})
		return
	}
	if s.cfg.TradeExecutor == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "trade executor unavailable"})
		return
	}

	var req tradeRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	actionCode, _, err := parseTradeAction(req.Action)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	orderType, err := parseTradeOrderType(req.OrderType)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	if actionCode != 8 {
		orderType = ""
	}
	exchange := strings.TrimSpace(req.Exchange)
	symbol := strings.TrimSpace(req.Symbol)
	if exchange == "" || symbol == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "exchange and symbol are required"})
		return
	}
	side, highSide, err := parseTradeSide(req.Side)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	strategy := strings.TrimSpace(req.Strategy)
	if strategy == "" {
		strategy = tradeStrategyDefault
	}

	signal := models.Signal{
		Exchange:  exchange,
		Symbol:    symbol,
		OrderType: orderType,
		Amount:    req.Amount,
		Entry:     req.Entry,
		TP:        req.TP,
		SL:        req.SL,
		Action:    actionCode,
		HighSide:  highSide,
		Strategy:  strategy,
	}
	response := tradeResponse{}

	positions, err := s.cfg.TradeEvaluator.ListOpenPositions(exchange, symbol, "")
	if err != nil {
		s.logger.Warn("trade list open positions failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Error(err),
		)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	position, hasPosition := selectTradePosition(positions, side, "")
	response.PositionFound = hasPosition
	if hasPosition {
		itemList := buildPositionItems([]models.Position{position})
		if len(itemList) > 0 {
			response.Position = &itemList[0]
		}
	}

	marketData := models.MarketData{
		Exchange: exchange,
		Symbol:   symbol,
		OHLCV: models.OHLCV{
			Open:  req.Entry,
			Close: req.Entry,
		},
	}

	var decision models.Decision
	switch {
	case actionCode == 8 && !hasPosition:
		decision, err = s.cfg.TradeEvaluator.EvaluateOpenBatch([]models.Signal{signal}, marketData)
	case hasPosition:
		decision, err = s.cfg.TradeEvaluator.EvaluateUpdate(signal, position, marketData)
	default:
		response.RiskError = "no open position found for requested action"
		writeJSON(w, http.StatusOK, response)
		return
	}
	if err != nil {
		response.RiskError = err.Error()
		writeJSON(w, http.StatusOK, response)
		return
	}
	response.Decision = &decision
	if decision.Action == "" || decision.Action == models.DecisionActionIgnore {
		writeJSON(w, http.StatusOK, response)
		return
	}

	if err := s.cfg.TradeExecutor.Place(decision); err != nil {
		if notifier, ok := s.cfg.TradeEvaluator.(tradeExecutionNotifier); ok {
			notifier.NotifyExecutionResult(decision, err)
		}
		response.ExecutionError = err.Error()
		writeJSON(w, http.StatusOK, response)
		return
	}
	if notifier, ok := s.cfg.TradeEvaluator.(tradeExecutionNotifier); ok {
		notifier.NotifyExecutionResult(decision, nil)
	}
	response.Executed = true
	writeJSON(w, http.StatusOK, response)
}

func parseTradeAction(raw string) (int, string, error) {
	action := strings.ToLower(strings.TrimSpace(raw))
	switch action {
	case tradeActionOpen:
		return 8, tradeActionOpen, nil
	case "move", "move-tpsl", tradeActionMoveTPSL:
		return 16, tradeActionMoveTPSL, nil
	case "partial", "partial-close", tradeActionPartial:
		return 32, tradeActionPartial, nil
	case "close", "full-close", tradeActionFullClose:
		return 64, tradeActionFullClose, nil
	default:
		return 0, "", fmt.Errorf("unsupported trade action: %s", raw)
	}
}

func parseTradeSide(raw string) (string, int, error) {
	side := strings.ToLower(strings.TrimSpace(raw))
	switch side {
	case tradeSideLongCanonical, "buy":
		return tradeSideLongCanonical, 1, nil
	case tradeSideShortCanonical, "sell":
		return tradeSideShortCanonical, -1, nil
	default:
		return "", 0, fmt.Errorf("unsupported side: %s", raw)
	}
}

func parseTradeOrderType(raw string) (string, error) {
	orderType := strings.ToLower(strings.TrimSpace(raw))
	switch orderType {
	case "":
		return "", nil
	case models.OrderTypeMarket:
		return models.OrderTypeMarket, nil
	case models.OrderTypeLimit:
		return models.OrderTypeLimit, nil
	default:
		return "", fmt.Errorf("unsupported order_type: %s", raw)
	}
}

func selectTradePosition(positions []models.Position, side, timeframe string) (models.Position, bool) {
	if len(positions) == 0 {
		return models.Position{}, false
	}
	side = strings.ToLower(strings.TrimSpace(side))
	timeframe = strings.TrimSpace(timeframe)

	for _, pos := range positions {
		if side != "" && strings.ToLower(strings.TrimSpace(pos.PositionSide)) != side {
			continue
		}
		if timeframe != "" && strings.TrimSpace(pos.Timeframe) != timeframe {
			continue
		}
		return pos, true
	}
	for _, pos := range positions {
		if side != "" && strings.ToLower(strings.TrimSpace(pos.PositionSide)) != side {
			continue
		}
		return pos, true
	}
	if timeframe != "" {
		for _, pos := range positions {
			if strings.TrimSpace(pos.Timeframe) == timeframe {
				return pos, true
			}
		}
	}
	return positions[0], true
}

func accountResponseFromFunds(funds models.RiskAccountFunds) accountResponse {
	return accountResponse{
		Exchange:           funds.Exchange,
		Currency:           funds.Currency,
		FundingUSDT:        funds.FundingUSDT,
		TradingUSDT:        funds.TradingUSDT,
		TotalUSDT:          funds.TotalUSDT,
		PerTradeUSDT:       funds.PerTradeUSDT,
		DailyProfitUSDT:    funds.DailyProfitUSDT,
		ClosedProfitRate:   funds.ClosedProfitRate,
		FloatingProfitRate: funds.FloatingProfitRate,
		TotalProfitRate:    funds.TotalProfitRate,
		UpdatedAtMS:        funds.UpdatedAtMS,
	}
}

func buildPositionItems(positions []models.Position) []positionItem {
	items := make([]positionItem, 0, len(positions))
	for _, pos := range positions {
		maxFloatingProfitRate := calculateFloatingRate(pos.MaxFloatingProfitAmount, pos.MarginAmount)
		maxFloatingLossRate := calculateFloatingRate(pos.MaxFloatingLossAmount, pos.MarginAmount)
		items = append(items, positionItem{
			PositionID:              pos.PositionID,
			SingletonID:             pos.SingletonID,
			ExchangeID:              pos.ExchangeID,
			SymbolID:                pos.SymbolID,
			Exchange:                pos.Exchange,
			Symbol:                  pos.Symbol,
			Timeframe:               pos.Timeframe,
			PositionSide:            pos.PositionSide,
			GroupID:                 strings.TrimSpace(pos.GroupID),
			MarginMode:              pos.MarginMode,
			LeverageMultiplier:      pos.LeverageMultiplier,
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
			HoldingDurationMS:       pos.HoldingDurationMS,
			ExitPrice:               pos.ExitPrice,
			ExitQuantity:            pos.ExitQuantity,
			ExitValue:               pos.ExitValue,
			ExitTime:                pos.ExitTime,
			FeeAmount:               pos.FeeAmount,
			ProfitAmount:            pos.ProfitAmount,
			ProfitRate:              pos.ProfitRate,
			MaxFloatingProfitAmount: pos.MaxFloatingProfitAmount,
			MaxFloatingProfitRate:   maxFloatingProfitRate,
			MaxFloatingLossAmount:   pos.MaxFloatingLossAmount,
			MaxFloatingLossRate:     maxFloatingLossRate,
			Status:                  pos.Status,
			StrategyName:            pos.StrategyName,
			StrategyVersion:         pos.StrategyVersion,
			StrategyTimeframes:      append([]string(nil), pos.StrategyTimeframes...),
			ComboKey:                strings.TrimSpace(pos.ComboKey),
			UpdatedTime:             pos.UpdatedTime,
		})
	}
	return items
}

func buildHistoryPositionItems(items []positionItem) []historyPositionItem {
	out := make([]historyPositionItem, 0, len(items))
	for _, item := range items {
		out = append(out, historyPositionItem{
			PositionID:              item.PositionID,
			SingletonID:             item.SingletonID,
			Exchange:                item.Exchange,
			Symbol:                  item.Symbol,
			Timeframe:               item.Timeframe,
			PositionSide:            item.PositionSide,
			GroupID:                 item.GroupID,
			MarginMode:              item.MarginMode,
			LeverageMultiplier:      item.LeverageMultiplier,
			MarginAmount:            item.MarginAmount,
			EntryPrice:              item.EntryPrice,
			EntryQuantity:           item.EntryQuantity,
			EntryValue:              item.EntryValue,
			EntryTime:               item.EntryTime,
			TakeProfitPrice:         item.TakeProfitPrice,
			StopLossPrice:           item.StopLossPrice,
			CurrentPrice:            item.CurrentPrice,
			UnrealizedProfitAmount:  item.UnrealizedProfitAmount,
			UnrealizedProfitRate:    item.UnrealizedProfitRate,
			ExitPrice:               item.ExitPrice,
			ExitQuantity:            item.ExitQuantity,
			ExitValue:               item.ExitValue,
			ExitTime:                item.ExitTime,
			FeeAmount:               item.FeeAmount,
			ProfitAmount:            item.ProfitAmount,
			ProfitRate:              item.ProfitRate,
			MaxFloatingProfitAmount: item.MaxFloatingProfitAmount,
			MaxFloatingProfitRate:   item.MaxFloatingProfitRate,
			MaxFloatingLossAmount:   item.MaxFloatingLossAmount,
			MaxFloatingLossRate:     item.MaxFloatingLossRate,
			CloseStatus:             tradeActionFullClose,
			Status:                  item.Status,
			StrategyName:            item.StrategyName,
			StrategyVersion:         item.StrategyVersion,
			StrategyTimeframes:      append([]string(nil), item.StrategyTimeframes...),
			ComboKey:                strings.TrimSpace(item.ComboKey),
			UpdatedTime:             item.UpdatedTime,
		})
	}
	return out
}

func buildPositionEventsResponse(row visualHistoryPositionRow, events []visualHistoryEventEntry, total int, truncated bool) positionEventsResponse {
	return positionEventsResponse{
		PositionID:  row.ID,
		PositionKey: strings.TrimSpace(row.PositionKey),
		IsOpen:      row.IsOpen,
		Count:       len(events),
		Total:       total,
		Truncated:   truncated,
		Events:      events,
	}
}

func copyPositionItems(items []positionItem) []positionItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]positionItem, len(items))
	for i, item := range items {
		out[i] = item
		if len(item.StrategyTimeframes) > 0 {
			out[i].StrategyTimeframes = append([]string(nil), item.StrategyTimeframes...)
		}
	}
	return out
}

func equalPositionItems(left, right []positionItem) bool {
	return reflect.DeepEqual(left, right)
}

func calculateFloatingRate(amount, margin float64) float64 {
	if !floatcmp.GT(margin, 0) {
		return 0
	}
	return amount / margin
}

func copyHistoryPositionItems(items []historyPositionItem) []historyPositionItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]historyPositionItem, len(items))
	for i, item := range items {
		out[i] = item
		if len(item.StrategyTimeframes) > 0 {
			out[i].StrategyTimeframes = append([]string(nil), item.StrategyTimeframes...)
		}
	}
	return out
}

func copyGroupItems(items []groupItem) []groupItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]groupItem, len(items))
	for i, item := range items {
		out[i] = item
		if len(item.Candidates) > 0 {
			out[i].Candidates = make([]models.SignalGroupedCandidate, len(item.Candidates))
			copy(out[i].Candidates, item.Candidates)
		}
	}
	return out
}

func copyHistoryResponse(resp historyResponse) historyResponse {
	out := resp
	out.Positions = copyHistoryPositionItems(resp.Positions)
	out.Groups = copyGroupItems(resp.Groups)
	return out
}

func equalHistoryResponse(left, right historyResponse) bool {
	return reflect.DeepEqual(left, right)
}

func buildWSHistoryRangeStart(now time.Time, rng string) int64 {
	switch rng {
	case wsHistoryRange24h:
		return now.Add(-24 * time.Hour).UnixMilli()
	case wsHistoryRange3d:
		return now.Add(-3 * 24 * time.Hour).UnixMilli()
	case wsHistoryRange7d:
		return now.Add(-7 * 24 * time.Hour).UnixMilli()
	case wsHistoryRangeToday:
		fallthrough
	default:
		localNow := now.In(time.Local)
		start := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, localNow.Location())
		return start.UnixMilli()
	}
}

func parseWSTimestampMS(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if v > 0 && v < 1_000_000_000_000 {
			return v * 1000
		}
		return v
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
	}
	for _, layout := range layouts {
		t, err := time.ParseInLocation(layout, raw, time.Local)
		if err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func formatLocalTimeMS(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).In(time.Local).Format("2006-01-02 15:04:05")
}

func bubblesHistoryEventQueryWindow(entryMS, closeMS int64) (int64, int64) {
	const padding = 7 * 24 * int64(time.Hour/time.Millisecond)
	startMS := int64(0)
	endMS := int64(0)

	appendPoint := func(ts int64) {
		if ts <= 0 {
			return
		}
		windowStart := ts - padding
		windowEnd := ts + padding
		if startMS == 0 || windowStart < startMS {
			startMS = windowStart
		}
		if endMS == 0 || windowEnd > endMS {
			endMS = windowEnd
		}
	}

	appendPoint(entryMS)
	appendPoint(closeMS)
	if startMS == 0 || endMS == 0 {
		nowMS := time.Now().UnixMilli()
		return nowMS - 30*24*int64(time.Hour/time.Millisecond), nowMS + int64(time.Hour/time.Millisecond)
	}
	if startMS < 0 {
		startMS = 0
	}
	if endMS <= startMS {
		endMS = startMS + int64(time.Hour/time.Millisecond)
	}
	return startMS, endMS
}

func (s *Server) resolvePositionEventsRow(
	positionIDRaw, positionKey, exchange, symbol, positionSide, marginMode, entryTime, strategyName, strategyVersion string,
) (visualHistoryPositionRow, error) {
	req := wsCandlesFetchRequestItem{
		Exchange: strings.TrimSpace(exchange),
		Symbol:   strings.TrimSpace(symbol),
		Position: &wsCandlesPositionContext{
			PositionKey:     strings.TrimSpace(positionKey),
			PositionSide:    strings.TrimSpace(positionSide),
			MarginMode:      strings.TrimSpace(marginMode),
			EntryTime:       strings.TrimSpace(entryTime),
			StrategyName:    strings.TrimSpace(strategyName),
			StrategyVersion: strings.TrimSpace(strategyVersion),
		},
	}
	if positionID, err := parseQueryInt64(positionIDRaw); err != nil {
		return visualHistoryPositionRow{}, fmt.Errorf("invalid position_id")
	} else {
		req.Position.PositionID = positionID
	}
	rows, err := s.queryWSCandlesOpenPositionRows(req)
	if err != nil {
		return visualHistoryPositionRow{}, err
	}
	if row, ok := chooseWSCandlesBestPositionRow(rows, req); ok {
		return row, nil
	}
	return visualHistoryPositionRow{}, fmt.Errorf("position not found for exchange=%s symbol=%s", strings.TrimSpace(exchange), strings.TrimSpace(symbol))
}

func (s *Server) resolveHistoryEventsRow(
	positionIDRaw, positionKey, exchange, symbol, positionSide, marginMode, entryTime, exitTime, updatedTime, strategyName, strategyVersion string,
) (visualHistoryPositionRow, error) {
	positionID, err := parseQueryInt64(positionIDRaw)
	if err != nil {
		return visualHistoryPositionRow{}, fmt.Errorf("invalid position_id")
	}
	if row, loadErr := s.loadVisualHistoryPosition(positionID, strings.TrimSpace(positionKey)); loadErr == nil {
		return row, nil
	}

	entryMS := parseWSCandlesEntryTimeMS(entryTime)
	closeMS := parseWSTimestampMS(exitTime)
	if closeMS <= 0 {
		closeMS = parseWSTimestampMS(updatedTime)
	}
	startMS, endMS := bubblesHistoryEventQueryWindow(entryMS, closeMS)
	rows, err := s.queryVisualHistoryClosedPositionRows(startMS, endMS, strings.TrimSpace(exchange), strings.TrimSpace(symbol))
	if err != nil {
		return visualHistoryPositionRow{}, err
	}
	req := wsCandlesFetchRequestItem{
		Exchange: strings.TrimSpace(exchange),
		Symbol:   strings.TrimSpace(symbol),
		Position: &wsCandlesPositionContext{
			PositionSide:    strings.TrimSpace(positionSide),
			MarginMode:      strings.TrimSpace(marginMode),
			EntryTime:       strings.TrimSpace(entryTime),
			StrategyName:    strings.TrimSpace(strategyName),
			StrategyVersion: strings.TrimSpace(strategyVersion),
		},
	}
	if row, ok := chooseWSCandlesBestPositionRow(rows, req); ok {
		return row, nil
	}
	return visualHistoryPositionRow{}, fmt.Errorf("history position not found for exchange=%s symbol=%s", strings.TrimSpace(exchange), strings.TrimSpace(symbol))
}

func (s *Server) buildWSHistoryResponse(filter HistoryFilter) (historyResponse, historyResponse, error) {
	if s.cfg.AccountProvider == nil {
		return historyResponse{}, historyResponse{}, fmt.Errorf("history provider unavailable")
	}
	positions, err := s.listHistoryPositions(filter.Exchange, filter.Symbol)
	if err != nil {
		return historyResponse{}, historyResponse{}, fmt.Errorf("list history positions: %w", err)
	}
	filtered, err := filterHistoryPositions(positions, filter, time.Now())
	if err != nil {
		return historyResponse{}, historyResponse{}, err
	}
	items := buildHistoryPositionItems(buildPositionItems(filtered))
	resp := historyResponse{
		Count:     len(items),
		Positions: items,
		Groups:    s.buildHistoryGroups(filtered),
	}
	return resp, copyHistoryResponse(resp), nil
}

type statusVersion struct {
	Tag       string `json:"tag"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

type statusRuntime struct {
	Seconds int64  `json:"seconds"`
	Human   string `json:"human"`
}

type statusSingleton struct {
	UUID string `json:"uuid"`
}

type statusMeta struct {
	Runtime   statusRuntime                 `json:"runtime"`
	Singleton statusSingleton               `json:"singleton"`
	Modules   map[string]iface.ModuleStatus `json:"modules,omitempty"`
}

type statusResponse struct {
	Version   statusVersion                 `json:"version"`
	Runtime   statusRuntime                 `json:"runtime"`
	Singleton statusSingleton               `json:"singleton"`
	Modules   map[string]iface.ModuleStatus `json:"modules,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := s.buildStatusResponse()
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := filterGroupsResponse(s.buildGroupsResponse(), groupsFilterFromQuery(r))
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) buildStatusResponse() statusResponse {
	version := statusVersion{
		Tag:       common.Tag,
		Commit:    common.Commit,
		BuildTime: common.BuildTime,
	}
	meta := s.buildStatusMeta()
	return statusResponse{
		Version:   version,
		Runtime:   meta.Runtime,
		Singleton: meta.Singleton,
		Modules:   meta.Modules,
	}
}

func (s *Server) buildStatusMeta() statusMeta {
	runtime := statusRuntime{Seconds: 0, Human: "0s"}
	if s.cfg.Status != nil {
		dur := s.cfg.Status.Runtime()
		seconds := int64(dur.Truncate(time.Second).Seconds())
		if seconds < 0 {
			seconds = 0
		}
		runtime = statusRuntime{
			Seconds: seconds,
			Human:   s.cfg.Status.RuntimeString(),
		}
	}
	modules := sanitizeStatusModules(buildStatusModules(s.cfg.StatusProviders))
	return statusMeta{
		Runtime:   runtime,
		Singleton: statusSingleton{UUID: s.cfg.SingletonUUID},
		Modules:   modules,
	}
}

func (s *Server) buildGroupsResponse() groupsResponse {
	modules := buildStatusModules(s.cfg.StatusProviders)
	if len(modules) == 0 {
		return groupsResponse{Groups: []groupItem{}}
	}
	riskModule, ok := modules["risk"]
	if !ok || riskModule.Details == nil {
		return groupsResponse{Groups: []groupItem{}}
	}
	details, ok := riskModule.Details.(map[string]any)
	if !ok {
		return groupsResponse{Groups: []groupItem{}}
	}
	raw, ok := details["trend_guard"]
	if !ok || raw == nil {
		return groupsResponse{Groups: []groupItem{}}
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		s.logger.Warn("marshal trend_guard groups failed", zap.Error(err))
		return groupsResponse{Groups: []groupItem{}}
	}
	var resp groupsResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		s.logger.Warn("unmarshal trend_guard groups failed", zap.Error(err))
		return groupsResponse{Groups: []groupItem{}}
	}
	if resp.Groups == nil {
		resp.Groups = []groupItem{}
	}
	return resp
}

func buildStatusModules(providers []iface.StatusProvider) map[string]iface.ModuleStatus {
	if len(providers) == 0 {
		return nil
	}
	modules := make(map[string]iface.ModuleStatus, len(providers))
	for _, provider := range providers {
		if provider == nil {
			continue
		}
		status := provider.Status()
		name := strings.TrimSpace(status.Name)
		if name == "" {
			continue
		}
		modules[name] = status
	}
	if len(modules) == 0 {
		return nil
	}
	return modules
}

func sanitizeStatusModules(modules map[string]iface.ModuleStatus) map[string]iface.ModuleStatus {
	if len(modules) == 0 {
		return modules
	}
	out := make(map[string]iface.ModuleStatus, len(modules))
	for key, module := range modules {
		out[key] = module
	}
	riskModule, ok := out["risk"]
	if !ok || riskModule.Details == nil {
		return out
	}
	details, ok := normalizeStatusDetails(riskModule.Details)
	if !ok {
		return out
	}
	raw, ok := details["trend_guard"]
	if !ok || raw == nil {
		return out
	}
	payload, err := json.Marshal(raw)
	if err != nil {
		return out
	}
	var trendGuard map[string]any
	if err := json.Unmarshal(payload, &trendGuard); err != nil {
		return out
	}
	delete(trendGuard, "groups")
	details["trend_guard"] = trendGuard
	riskModule.Details = details
	out["risk"] = riskModule
	return out
}

func normalizeStatusDetails(details any) (map[string]any, bool) {
	if details == nil {
		return nil, false
	}
	if typed, ok := details.(map[string]any); ok {
		return typed, true
	}
	payload, err := json.Marshal(details)
	if err != nil {
		return nil, false
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, false
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

type wsAccountFilter struct {
	Exchange string `json:"exchange,omitempty"`
}

type wsSymbolsFilter struct {
	Exchange string `json:"exchange,omitempty"`
}

type wsCandlesFilter struct {
	Exchange  string `json:"exchange,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
	Timeframe string `json:"timeframe,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type wsSubscription struct {
	Streams        []string        `json:"streams,omitempty"`
	SignalsFilter  SignalFilter    `json:"signals_filter,omitempty"`
	AccountFilter  wsAccountFilter `json:"account_filter,omitempty"`
	PositionFilter PositionFilter  `json:"position_filter,omitempty"`
	HistoryFilter  HistoryFilter   `json:"history_filter,omitempty"`
	SymbolsFilter  wsSymbolsFilter `json:"symbols_filter,omitempty"`
	CandlesFilter  wsCandlesFilter `json:"candles_filter,omitempty"`
}

type wsRequest struct {
	Type         string                `json:"type"`
	RequestID    string                `json:"request_id,omitempty"`
	Target       string                `json:"target,omitempty"`        // fetch target: ""/snapshot/status/history/candles
	HistoryRange string                `json:"history_range,omitempty"` // fetch/subscribe history range override: today/24h/7d
	Subscription *wsSubscription       `json:"subscription,omitempty"`
	Filter       *SignalFilter         `json:"filter,omitempty"` // legacy signals filter
	CandlesFetch *wsCandlesFetchFilter `json:"candles_fetch,omitempty"`
}

type wsSnapshot struct {
	Type         string                              `json:"type"`
	RequestID    string                              `json:"request_id,omitempty"`
	Subscription wsSubscription                      `json:"subscription,omitempty"`
	Signals      map[string]map[string]models.Signal `json:"signals,omitempty"`
	Groups       *groupsResponse                     `json:"groups,omitempty"`
	Account      *accountResponse                    `json:"account,omitempty"`
	Position     *positionResponse                   `json:"position,omitempty"`
	History      *historyResponse                    `json:"history,omitempty"`
	Symbols      *wsSymbolsResponse                  `json:"symbols,omitempty"`
	Candles      *tradingViewCandlesResponse         `json:"candles,omitempty"`
	Status       *statusResponse                     `json:"status,omitempty"`
	TS           int64                               `json:"ts"`
}

type wsSignalDiff struct {
	Added   map[string]map[string]models.Signal `json:"added,omitempty"`
	Updated map[string]map[string]models.Signal `json:"updated,omitempty"`
	Removed []string                            `json:"removed,omitempty"`
}

type wsDiff struct {
	Type         string                      `json:"type"`
	Subscription wsSubscription              `json:"subscription,omitempty"`
	Signals      *wsSignalDiff               `json:"signals,omitempty"`
	Groups       *groupsResponse             `json:"groups,omitempty"`
	Account      *accountResponse            `json:"account,omitempty"`
	Position     *positionResponse           `json:"position,omitempty"`
	History      *historyResponse            `json:"history,omitempty"`
	Symbols      *wsSymbolsResponse          `json:"symbols,omitempty"`
	Candles      *tradingViewCandlesResponse `json:"candles,omitempty"`
	Status       *statusResponse             `json:"status,omitempty"`
	TS           int64                       `json:"ts"`
}

type wsSymbolsResponse struct {
	Exchange string                  `json:"exchange"`
	Symbols  []tradingViewSymbolItem `json:"symbols"`
}

type wsPong struct {
	Type      string     `json:"type"`
	RequestID string     `json:"request_id,omitempty"`
	Data      statusMeta `json:"data"`
	TS        int64      `json:"ts"`
}

type wsStatus struct {
	Type      string         `json:"type"`
	RequestID string         `json:"request_id,omitempty"`
	Data      statusResponse `json:"data"`
	TS        int64          `json:"ts"`
}

type wsError struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id,omitempty"`
	Message   string `json:"message"`
	TS        int64  `json:"ts"`
}

type wsCandlesFetchFilter struct {
	Requests      []wsCandlesFetchRequestItem `json:"requests,omitempty"`
	ClosedOnly    bool                        `json:"closed_only,omitempty"`
	IncludeEvents bool                        `json:"include_events,omitempty"`
	EventLimit    int                         `json:"event_limit,omitempty"`
}

type wsCandlesFetchRequestItem struct {
	Exchange   string                    `json:"exchange,omitempty"`
	Symbol     string                    `json:"symbol,omitempty"`
	Timeframes []string                  `json:"timeframes,omitempty"`
	Limit      int                       `json:"limit,omitempty"`
	Position   *wsCandlesPositionContext `json:"position,omitempty"`
}

type wsCandlesPositionContext struct {
	PositionID      int64  `json:"position_id,omitempty"`
	PositionKey     string `json:"position_key,omitempty"`
	PositionSide    string `json:"position_side,omitempty"`
	MarginMode      string `json:"margin_mode,omitempty"`
	EntryTime       string `json:"entry_time,omitempty"`
	StrategyName    string `json:"strategy_name,omitempty"`
	StrategyVersion string `json:"strategy_version,omitempty"`
}

type wsCandlesResponse struct {
	Type      string                  `json:"type"`
	RequestID string                  `json:"request_id,omitempty"`
	Data      []wsCandlesResponseItem `json:"data,omitempty"`
	Warnings  []string                `json:"warnings,omitempty"`
	TS        int64                   `json:"ts"`
}

type wsCandlesResponseItem struct {
	Exchange        string                             `json:"exchange"`
	Symbol          string                             `json:"symbol"`
	Timeframes      map[string]wsCandlesResponseSeries `json:"timeframes"`
	Events          []visualHistoryEventEntry          `json:"events,omitempty"`
	EventsTotal     int                                `json:"events_total,omitempty"`
	EventsTruncated bool                               `json:"events_truncated,omitempty"`
	Position        *wsCandlesResponsePosition         `json:"position,omitempty"`
}

type wsCandlesResponseSeries struct {
	Requested int            `json:"requested"`
	Returned  int            `json:"returned"`
	Bars      []models.OHLCV `json:"bars"`
}

type wsCandlesResponsePosition struct {
	PositionKey  string  `json:"position_key,omitempty"`
	IsOpen       bool    `json:"is_open"`
	PositionSide string  `json:"position_side,omitempty"`
	EntryPrice   float64 `json:"entry_price,omitempty"`
	ExitPrice    float64 `json:"exit_price,omitempty"`
}

type wsState struct {
	subscription    wsSubscription
	signalBaseline  map[string]models.Signal
	groupBase       groupsResponse
	accountBaseline accountResponse
	accountReady    bool
	positionBase    []positionItem
	historyBase     historyResponse
	symbolsBase     wsSymbolsResponse
	symbolsReady    bool
	candlesBase     tradingViewCandlesResponse
	candlesReady    bool
	version         int64
}

type wsSnapshotState struct {
	snapshot   wsSnapshot
	signalFlat map[string]models.Signal
	groups     groupsResponse
	account    accountResponse
	hasAccount bool
	positions  []positionItem
	history    historyResponse
	symbols    wsSymbolsResponse
	hasSymbols bool
	candles    tradingViewCandlesResponse
	hasCandles bool
}

var wsDefaultStreams = []string{wsStreamSignals, wsStreamGroups, wsStreamAccount, wsStreamPosition, wsStreamHistory}
var wsAllStreams = []string{wsStreamSignals, wsStreamGroups, wsStreamAccount, wsStreamPosition, wsStreamHistory, wsStreamSymbols, wsStreamCandles}

func normalizeWSAccountFilter(filter wsAccountFilter) wsAccountFilter {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	return filter
}

func normalizeWSSymbolsFilter(filter wsSymbolsFilter) wsSymbolsFilter {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	return filter
}

func normalizeWSCandlesFilter(filter wsCandlesFilter) (wsCandlesFilter, error) {
	filter.Exchange = strings.TrimSpace(filter.Exchange)
	filter.Symbol = strings.TrimSpace(filter.Symbol)
	filter.Timeframe = strings.ToLower(strings.TrimSpace(filter.Timeframe))
	if filter.Limit <= 0 {
		filter.Limit = tradingViewWSTailLimitDefault
	}
	if filter.Limit > tradingViewWSTailLimitMax {
		filter.Limit = tradingViewWSTailLimitMax
	}
	if filter.Timeframe != "" {
		if _, ok := market.TimeframeDuration(filter.Timeframe); !ok {
			return wsCandlesFilter{}, fmt.Errorf("invalid candles timeframe: %s", filter.Timeframe)
		}
	}
	return filter, nil
}

func normalizeWSPositionFilter(filter PositionFilter) PositionFilter {
	return normalizePositionFilter(filter)
}

func normalizeWSHistoryRange(raw string) (string, error) {
	return normalizeHistoryRange(raw)
}

func normalizeWSHistoryFilter(filter HistoryFilter) (HistoryFilter, error) {
	return normalizeHistoryFilter(filter)
}

func normalizeWSStreams(streams []string) ([]string, error) {
	if len(streams) == 0 {
		return append([]string(nil), wsDefaultStreams...), nil
	}
	enabled := make(map[string]struct{}, len(wsAllStreams))
	for _, raw := range streams {
		for _, part := range strings.Split(raw, ",") {
			stream := strings.ToLower(strings.TrimSpace(part))
			if stream == "" {
				continue
			}
			switch stream {
			case wsStreamSignals, wsStreamGroups, wsStreamAccount, wsStreamPosition, wsStreamHistory, wsStreamSymbols, wsStreamCandles:
				enabled[stream] = struct{}{}
			default:
				return nil, fmt.Errorf("unsupported stream: %s", stream)
			}
		}
	}
	if len(enabled) == 0 {
		return append([]string(nil), wsDefaultStreams...), nil
	}
	out := make([]string, 0, len(enabled))
	for _, stream := range wsAllStreams {
		if _, ok := enabled[stream]; ok {
			out = append(out, stream)
		}
	}
	return out, nil
}

func normalizeWSSubscription(sub wsSubscription) (wsSubscription, error) {
	streams, err := normalizeWSStreams(sub.Streams)
	if err != nil {
		return wsSubscription{}, err
	}
	historyFilter, err := normalizeWSHistoryFilter(sub.HistoryFilter)
	if err != nil {
		return wsSubscription{}, err
	}
	candlesFilter, err := normalizeWSCandlesFilter(sub.CandlesFilter)
	if err != nil {
		return wsSubscription{}, err
	}
	symbolsFilter := normalizeWSSymbolsFilter(sub.SymbolsFilter)
	if streamEnabledInList(streams, wsStreamSymbols) && symbolsFilter.Exchange == "" {
		return wsSubscription{}, fmt.Errorf("symbols_filter.exchange is required")
	}
	if streamEnabledInList(streams, wsStreamCandles) {
		if candlesFilter.Exchange == "" || candlesFilter.Symbol == "" || candlesFilter.Timeframe == "" {
			return wsSubscription{}, fmt.Errorf("candles_filter.exchange, symbol and timeframe are required")
		}
	}
	return wsSubscription{
		Streams:        streams,
		SignalsFilter:  normalizeFilter(sub.SignalsFilter),
		AccountFilter:  normalizeWSAccountFilter(sub.AccountFilter),
		PositionFilter: normalizeWSPositionFilter(sub.PositionFilter),
		HistoryFilter:  historyFilter,
		SymbolsFilter:  symbolsFilter,
		CandlesFilter:  candlesFilter,
	}, nil
}

func wsSubscriptionFromQuery(r *http.Request) (wsSubscription, error) {
	if r == nil {
		return normalizeWSSubscription(wsSubscription{})
	}
	q := r.URL.Query()
	streamsRaw := strings.TrimSpace(q.Get("streams"))
	streams := []string(nil)
	if streamsRaw != "" {
		streams = []string{streamsRaw}
	}
	sub := wsSubscription{
		Streams:       streams,
		SignalsFilter: signalFilterFromQuery(r),
		AccountFilter: wsAccountFilter{Exchange: strings.TrimSpace(q.Get("account_exchange"))},
		PositionFilter: PositionFilter{
			Exchange:        strings.TrimSpace(q.Get("position_exchange")),
			Symbol:          strings.TrimSpace(q.Get("position_symbol")),
			Timeframe:       strings.TrimSpace(q.Get("position_timeframe")),
			PositionSide:    strings.TrimSpace(q.Get("position_side")),
			StrategyName:    strings.TrimSpace(q.Get("position_strategy_name")),
			StrategyVersion: strings.TrimSpace(q.Get("position_strategy_version")),
		},
		HistoryFilter: HistoryFilter{
			Exchange:        strings.TrimSpace(q.Get("history_exchange")),
			Symbol:          strings.TrimSpace(q.Get("history_symbol")),
			Timeframe:       strings.TrimSpace(q.Get("history_timeframe")),
			PositionSide:    strings.TrimSpace(q.Get("history_position_side")),
			StrategyName:    strings.TrimSpace(q.Get("history_strategy_name")),
			StrategyVersion: strings.TrimSpace(q.Get("history_strategy_version")),
			StartTime:       strings.TrimSpace(q.Get("history_start_time")),
			EndTime:         strings.TrimSpace(q.Get("history_end_time")),
			Range:           strings.TrimSpace(q.Get("history_range")),
		},
		SymbolsFilter: wsSymbolsFilter{
			Exchange: strings.TrimSpace(q.Get("symbols_exchange")),
		},
		CandlesFilter: wsCandlesFilter{
			Exchange:  strings.TrimSpace(q.Get("candles_exchange")),
			Symbol:    strings.TrimSpace(q.Get("candles_symbol")),
			Timeframe: strings.TrimSpace(q.Get("candles_timeframe")),
		},
	}
	if limitRaw := strings.TrimSpace(q.Get("history_limit")); limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err != nil || parsed <= 0 {
			return wsSubscription{}, fmt.Errorf("invalid history_limit")
		}
		sub.HistoryFilter.Limit = parsed
	}
	if limitRaw := strings.TrimSpace(q.Get("candles_limit")); limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err != nil || parsed <= 0 {
			return wsSubscription{}, fmt.Errorf("invalid candles_limit")
		}
		sub.CandlesFilter.Limit = parsed
	}
	return normalizeWSSubscription(sub)
}

func applyWSHistoryRange(sub wsSubscription, raw string) (wsSubscription, error) {
	if strings.TrimSpace(raw) == "" {
		return sub, nil
	}
	next := sub
	next.HistoryFilter.Range = raw
	return normalizeWSSubscription(next)
}

func normalizeWSCandlesFetchFilter(raw *wsCandlesFetchFilter) (wsCandlesFetchFilter, error) {
	if raw == nil {
		return wsCandlesFetchFilter{}, fmt.Errorf("candles_fetch is required")
	}
	requests := make([]wsCandlesFetchRequestItem, 0, len(raw.Requests))
	for _, item := range raw.Requests {
		exchange := strings.ToLower(strings.TrimSpace(item.Exchange))
		symbol := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if exchange == "" || symbol == "" {
			return wsCandlesFetchFilter{}, fmt.Errorf("candles request missing exchange/symbol")
		}
		limit := item.Limit
		if limit <= 0 {
			limit = wsCandlesDefaultLimit
		}
		if limit > wsCandlesMaxLimit {
			limit = wsCandlesMaxLimit
		}
		timeframes := make([]string, 0, len(item.Timeframes))
		seen := make(map[string]struct{}, len(item.Timeframes))
		for _, timeframeRaw := range item.Timeframes {
			timeframe := strings.ToLower(strings.TrimSpace(timeframeRaw))
			if timeframe == "" {
				continue
			}
			if _, ok := market.TimeframeDuration(timeframe); !ok {
				return wsCandlesFetchFilter{}, fmt.Errorf("invalid candles timeframe: %s", timeframeRaw)
			}
			if _, exists := seen[timeframe]; exists {
				continue
			}
			seen[timeframe] = struct{}{}
			timeframes = append(timeframes, timeframe)
		}
		if len(timeframes) == 0 {
			return wsCandlesFetchFilter{}, fmt.Errorf("candles request missing timeframes")
		}
		if len(timeframes) > wsCandlesMaxTimeframes {
			timeframes = timeframes[:wsCandlesMaxTimeframes]
		}
		requests = append(requests, wsCandlesFetchRequestItem{
			Exchange:   exchange,
			Symbol:     symbol,
			Timeframes: timeframes,
			Limit:      limit,
			Position:   normalizeWSCandlesPositionContext(item.Position),
		})
	}
	if len(requests) == 0 {
		return wsCandlesFetchFilter{}, fmt.Errorf("candles request list is empty")
	}
	if len(requests) > wsCandlesMaxRequests {
		requests = requests[:wsCandlesMaxRequests]
	}
	eventLimit := parseVisualHistoryEventLimit(strconv.Itoa(raw.EventLimit), visualHistoryDefaultEventLimit)
	includeEvents := raw.IncludeEvents
	return wsCandlesFetchFilter{
		Requests:      requests,
		ClosedOnly:    true,
		IncludeEvents: includeEvents,
		EventLimit:    eventLimit,
	}, nil
}

func normalizeWSCandlesPositionContext(raw *wsCandlesPositionContext) *wsCandlesPositionContext {
	if raw == nil {
		return nil
	}
	next := &wsCandlesPositionContext{
		PositionID:      raw.PositionID,
		PositionKey:     strings.TrimSpace(raw.PositionKey),
		PositionSide:    strings.ToLower(strings.TrimSpace(raw.PositionSide)),
		MarginMode:      strings.ToLower(strings.TrimSpace(raw.MarginMode)),
		EntryTime:       strings.TrimSpace(raw.EntryTime),
		StrategyName:    strings.TrimSpace(raw.StrategyName),
		StrategyVersion: strings.TrimSpace(raw.StrategyVersion),
	}
	if next.PositionID <= 0 &&
		next.PositionKey == "" &&
		next.PositionSide == "" &&
		next.MarginMode == "" &&
		next.EntryTime == "" &&
		next.StrategyName == "" &&
		next.StrategyVersion == "" {
		return nil
	}
	return next
}

func normalizeCandlesFromCache(items []models.OHLCV, limit int) []models.OHLCV {
	if len(items) == 0 || limit <= 0 {
		return nil
	}
	selected := make([]models.OHLCV, 0, limit)
	seen := make(map[int64]struct{}, len(items))
	for _, item := range items {
		if item.TS <= 0 {
			continue
		}
		if _, exists := seen[item.TS]; exists {
			continue
		}
		seen[item.TS] = struct{}{}
		selected = append(selected, item)
		if len(selected) >= limit {
			break
		}
	}
	return selected
}

func isClosedOHLCV(ts int64, timeframe string, now time.Time) bool {
	if ts <= 0 {
		return false
	}
	dur, ok := market.TimeframeDuration(timeframe)
	if !ok || dur <= 0 {
		return false
	}
	closeAt := ts + dur.Milliseconds()
	return closeAt <= now.UTC().UnixMilli()
}

func normalizeCandlesFromStore(items []models.OHLCV, timeframe string, limit int, now time.Time) []models.OHLCV {
	if len(items) == 0 || limit <= 0 {
		return nil
	}
	selected := make([]models.OHLCV, 0, limit)
	seen := make(map[int64]struct{}, len(items))
	for i := len(items) - 1; i >= 0 && len(selected) < limit; i-- {
		item := items[i]
		if item.TS <= 0 {
			continue
		}
		if !isClosedOHLCV(item.TS, timeframe, now) {
			continue
		}
		if _, exists := seen[item.TS]; exists {
			continue
		}
		seen[item.TS] = struct{}{}
		selected = append(selected, item)
	}
	return selected
}

func mergeRecentCandles(cacheItems, storeItems []models.OHLCV, limit int) []models.OHLCV {
	if limit <= 0 {
		return nil
	}
	if len(cacheItems) == 0 && len(storeItems) == 0 {
		return nil
	}
	merged := make(map[int64]models.OHLCV, len(cacheItems)+len(storeItems))
	for _, item := range storeItems {
		if item.TS <= 0 {
			continue
		}
		merged[item.TS] = item
	}
	for _, item := range cacheItems {
		if item.TS <= 0 {
			continue
		}
		merged[item.TS] = item // 内存优先覆盖
	}
	keys := make([]int64, 0, len(merged))
	for ts := range merged {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] }) // recent -> old
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]models.OHLCV, 0, len(keys))
	for _, ts := range keys {
		out = append(out, merged[ts])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TS < out[j].TS }) // chart: old -> recent
	return out
}

func (s *Server) loadWSCandles(exchange, symbol, timeframe string, limit int) ([]models.OHLCV, error) {
	if limit <= 0 {
		limit = wsCandlesDefaultLimit
	}
	cacheItems := []models.OHLCV(nil)
	if source, ok := s.cfg.Provider.(signalOHLCVProvider); ok && source != nil {
		items, err := source.ListRecentClosedOHLCV(exchange, symbol, timeframe, limit)
		if err != nil {
			return nil, fmt.Errorf("load candles from cache: %w", err)
		}
		cacheItems = normalizeCandlesFromCache(items, limit)
	}
	if len(cacheItems) >= limit {
		return mergeRecentCandles(cacheItems, nil, limit), nil
	}
	storeItems := []models.OHLCV(nil)
	if s.cfg.HistoryStore != nil {
		fallbackLimit := limit * 2
		if fallbackLimit < limit+16 {
			fallbackLimit = limit + 16
		}
		items, err := s.cfg.HistoryStore.ListRecentOHLCV(exchange, symbol, timeframe, fallbackLimit)
		if err != nil {
			return mergeRecentCandles(cacheItems, nil, limit), nil
		}
		storeItems = normalizeCandlesFromStore(items, timeframe, limit, time.Now().UTC())
	}
	return mergeRecentCandles(cacheItems, storeItems, limit), nil
}

func parseWSCandlesEntryTimeMS(raw string) int64 {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		if parsed > 0 && parsed < 1_000_000_000_000 {
			return parsed * 1000
		}
		if parsed > 0 {
			return parsed
		}
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		t, err := time.ParseInLocation(layout, value, time.UTC)
		if err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func normalizeWSCandlesMode(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", "real-time":
		return "live"
	default:
		return mode
	}
}

func normalizeWSCandlesSymbolKey(value string) string {
	upper := strings.ToUpper(strings.TrimSpace(value))
	return strings.NewReplacer("/", "", "-", "", "_", "", " ", "").Replace(upper)
}

func wsCandlesSymbolLikelyMatch(left, right string) bool {
	leftKey := normalizeWSCandlesSymbolKey(left)
	rightKey := normalizeWSCandlesSymbolKey(right)
	if leftKey == "" || rightKey == "" {
		return false
	}
	if leftKey == rightKey {
		return true
	}
	return strings.Contains(leftKey, rightKey) || strings.Contains(rightKey, leftKey)
}

func normalizeWSCandlesPosSide(raw string) string {
	side := strings.ToLower(strings.TrimSpace(raw))
	switch side {
	case "buy":
		return "long"
	case "sell":
		return "short"
	case "long", "short":
		return side
	default:
		return side
	}
}

func matchWSCandlesPositionRow(row visualHistoryPositionRow, req wsCandlesFetchRequestItem) bool {
	if strings.TrimSpace(req.Exchange) != "" && !strings.EqualFold(strings.TrimSpace(row.Exchange), strings.TrimSpace(req.Exchange)) {
		return false
	}
	if strings.TrimSpace(req.Symbol) != "" {
		rowSymbol := normalizeVisualHistorySymbol(row.Symbol, row.InstID)
		if !wsCandlesSymbolLikelyMatch(rowSymbol, req.Symbol) {
			return false
		}
	}
	pos := req.Position
	if pos == nil {
		return true
	}
	if pos.PositionSide != "" && normalizeWSCandlesPosSide(row.PosSide) != normalizeWSCandlesPosSide(pos.PositionSide) {
		return false
	}
	if pos.MarginMode != "" && strings.ToLower(strings.TrimSpace(row.MgnMode)) != strings.ToLower(strings.TrimSpace(pos.MarginMode)) {
		return false
	}
	meta := visualHistoryStrategyMetaFromRow(row)
	if pos.StrategyName != "" && !strings.EqualFold(strings.TrimSpace(meta.StrategyName), pos.StrategyName) {
		return false
	}
	if pos.StrategyVersion != "" && !strings.EqualFold(strings.TrimSpace(meta.StrategyVersion), pos.StrategyVersion) {
		return false
	}
	return true
}

func positionRowTimeDistanceMS(row visualHistoryPositionRow, targetMS int64) int64 {
	if targetMS <= 0 {
		return 0
	}
	diff := row.OpenTimeMS - targetMS
	if diff < 0 {
		diff = -diff
	}
	return diff
}

func chooseWSCandlesBestPositionRow(candidates []visualHistoryPositionRow, req wsCandlesFetchRequestItem) (visualHistoryPositionRow, bool) {
	if len(candidates) == 0 {
		return visualHistoryPositionRow{}, false
	}
	entryMS := int64(0)
	if req.Position != nil {
		entryMS = parseWSCandlesEntryTimeMS(req.Position.EntryTime)
	}
	bestIdx := -1
	bestScore := int64(-1)
	for idx, item := range candidates {
		if !matchWSCandlesPositionRow(item, req) {
			continue
		}
		// 更高分优先：先按开仓时间距离，再按更新时间兜底。
		distance := positionRowTimeDistanceMS(item, entryMS)
		score := int64(0)
		if entryMS > 0 {
			// 距离越小分越高，最多扣到 0。
			score = 1_000_000_000_000 - distance
		}
		score += maxVisualHistoryInt64(item.UpdatedAtMS, item.RevisionMS, item.OpenTimeMS)
		if bestIdx < 0 || score > bestScore {
			bestIdx = idx
			bestScore = score
		}
	}
	if bestIdx < 0 {
		return visualHistoryPositionRow{}, false
	}
	return candidates[bestIdx], true
}

func buildWSCandlesOpenPositionRow(item models.RiskOpenPosition) visualHistoryPositionRow {
	exchange := strings.ToLower(strings.TrimSpace(item.Exchange))
	instID := strings.ToUpper(strings.TrimSpace(item.InstID))
	posSide := strings.ToLower(strings.TrimSpace(item.PosSide))
	marginMode := strings.ToLower(strings.TrimSpace(item.MgnMode))
	symbol := normalizeVisualHistorySymbol(item.Symbol, instID)
	if strings.TrimSpace(symbol) == "" {
		symbol = visualHistorySymbolFromInstID(instID)
	}
	revisionMS := maxVisualHistoryInt64(item.UpdatedAtMS, item.UpdateTimeMS, item.OpenTimeMS)
	if revisionMS <= 0 {
		revisionMS = item.OpenTimeMS
	}
	return visualHistoryPositionRow{
		PositionKey:             buildVisualHistoryPositionKey(exchange, instID, posSide, marginMode, item.OpenTimeMS),
		ID:                      0,
		IsOpen:                  true,
		Exchange:                exchange,
		Symbol:                  symbol,
		InstID:                  instID,
		Pos:                     item.Pos,
		PosSide:                 posSide,
		MgnMode:                 marginMode,
		Margin:                  item.Margin,
		Lever:                   item.Lever,
		AvgPx:                   item.AvgPx,
		NotionalUSD:             item.NotionalUSD,
		MarkPx:                  item.MarkPx,
		LiqPx:                   item.LiqPx,
		TPTriggerPx:             item.TPTriggerPx,
		SLTriggerPx:             item.SLTriggerPx,
		OpenTimeMS:              item.OpenTimeMS,
		OpenUpdateTimeMS:        item.UpdateTimeMS,
		MaxFloatingLossAmount:   item.MaxFloatingLossAmount,
		MaxFloatingProfitAmount: item.MaxFloatingProfitAmount,
		OpenRowJSON:             item.RowJSON,
		State:                   models.PositionStatusOpen,
		RevisionMS:              revisionMS,
		CreatedAtMS:             item.OpenTimeMS,
		UpdatedAtMS:             revisionMS,
	}
}

func (s *Server) queryWSCandlesOpenPositionRows(req wsCandlesFetchRequestItem) ([]visualHistoryPositionRow, error) {
	if s == nil || s.cfg.HistoryStore == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	mode := normalizeWSCandlesMode(s.cfg.Mode)
	items, err := s.cfg.HistoryStore.ListRiskOpenPositions(mode, strings.TrimSpace(req.Exchange))
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	rows := make([]visualHistoryPositionRow, 0, len(items))
	for _, item := range items {
		rows = append(rows, buildWSCandlesOpenPositionRow(item))
	}
	return rows, nil
}

func (s *Server) queryVisualHistoryPositionRowsByOpenTimeRange(
	startMS, endMS int64,
	exchange, symbol string,
	limit int,
) ([]visualHistoryPositionRow, error) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	if startMS < 0 {
		startMS = 0
	}
	if endMS <= startMS {
		endMS = startMS + 1
	}
	if limit <= 0 {
		limit = 64
	}

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(`SELECT id, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
	        notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
	        open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
	        open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
	        close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
	   FROM history_positions
	  WHERE COALESCE(state, '') <> ?
	    AND open_time_ms >= ?
	    AND open_time_ms < ?`)
	args := []any{historyStateSyncPending, startMS, endMS}
	if strings.TrimSpace(exchange) != "" {
		queryBuilder.WriteString(` AND exchange = ?`)
		args = append(args, strings.TrimSpace(exchange))
	}
	if strings.TrimSpace(symbol) != "" {
		queryBuilder.WriteString(` AND (UPPER(symbol) = UPPER(?) OR REPLACE(UPPER(symbol), '/', '') = REPLACE(UPPER(?), '/', '') OR UPPER(inst_id) = REPLACE(UPPER(?), '/', '-'))`)
		args = append(args, symbol, symbol, symbol)
	}
	queryBuilder.WriteString(` ORDER BY open_time_ms DESC, id DESC LIMIT ?`)
	args = append(args, limit)

	rows, err := s.cfg.HistoryStore.DB.Query(queryBuilder.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]visualHistoryPositionRow, 0, limit)
	for rows.Next() {
		var item visualHistoryPositionRow
		if scanErr := rows.Scan(
			&item.ID,
			&item.Exchange,
			&item.Symbol,
			&item.InstID,
			&item.Pos,
			&item.PosSide,
			&item.MgnMode,
			&item.Margin,
			&item.Lever,
			&item.AvgPx,
			&item.NotionalUSD,
			&item.MarkPx,
			&item.LiqPx,
			&item.TPTriggerPx,
			&item.SLTriggerPx,
			&item.OpenTimeMS,
			&item.OpenUpdateTimeMS,
			&item.MaxFloatingLossAmount,
			&item.MaxFloatingProfitAmount,
			&item.OpenRowJSON,
			&item.CloseAvgPx,
			&item.RealizedPnl,
			&item.PnlRatio,
			&item.Fee,
			&item.FundingFee,
			&item.CloseTimeMS,
			&item.State,
			&item.CloseRowJSON,
			&item.CreatedAtMS,
			&item.UpdatedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		item.PositionKey = buildVisualHistoryPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS)
		item.Symbol = normalizeVisualHistorySymbol(item.Symbol, item.InstID)
		item.IsOpen = item.CloseTimeMS <= 0
		item.RevisionMS = maxVisualHistoryInt64(item.UpdatedAtMS, item.OpenUpdateTimeMS, item.OpenTimeMS)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) resolveWSCandlesPositionRow(req wsCandlesFetchRequestItem) (visualHistoryPositionRow, error) {
	if req.Position != nil {
		if strings.TrimSpace(req.Position.PositionKey) != "" {
			row, err := s.loadVisualHistoryPosition(0, strings.TrimSpace(req.Position.PositionKey))
			if err == nil {
				return row, nil
			}
		}
		if req.Position.PositionID > 0 {
			row, err := s.loadVisualHistoryPosition(req.Position.PositionID, "")
			if err == nil {
				return row, nil
			}
		}
	}
	var fallbackErr error
	openRows, err := s.queryWSCandlesOpenPositionRows(req)
	if err != nil {
		fallbackErr = err
	} else if row, ok := chooseWSCandlesBestPositionRow(openRows, req); ok {
		return row, nil
	}
	if req.Position != nil {
		entryMS := parseWSCandlesEntryTimeMS(req.Position.EntryTime)
		if entryMS > 0 {
			const windowMS = int64((7 * 24 * time.Hour) / time.Millisecond)
			rows, err := s.queryVisualHistoryPositionRowsByOpenTimeRange(entryMS-windowMS, entryMS+windowMS+1, req.Exchange, req.Symbol, 96)
			if err != nil {
				fallbackErr = err
			} else if row, ok := chooseWSCandlesBestPositionRow(rows, req); ok {
				return row, nil
			}
		}
	}

	if req.Position == nil {
		return visualHistoryPositionRow{}, fmt.Errorf("position context is required for events")
	}
	if fallbackErr != nil {
		return visualHistoryPositionRow{}, fallbackErr
	}
	return visualHistoryPositionRow{}, fmt.Errorf("position not found for exchange=%s symbol=%s", req.Exchange, req.Symbol)
}

func buildWSCandlesResponsePosition(row visualHistoryPositionRow) *wsCandlesResponsePosition {
	return &wsCandlesResponsePosition{
		PositionKey:  strings.TrimSpace(row.PositionKey),
		IsOpen:       row.IsOpen,
		PositionSide: strings.TrimSpace(row.PosSide),
		EntryPrice:   parseNumericText(row.AvgPx),
		ExitPrice:    parseNumericText(row.CloseAvgPx),
	}
}

func (s *Server) loadWSCandlesEvents(
	req wsCandlesFetchRequestItem,
	eventLimit int,
) ([]visualHistoryEventEntry, int, bool, *wsCandlesResponsePosition, error) {
	row, err := s.resolveWSCandlesPositionRow(req)
	if err != nil {
		return nil, 0, false, nil, err
	}
	events, err := s.buildVisualHistoryEvents(row)
	if err != nil {
		return nil, 0, false, buildWSCandlesResponsePosition(row), err
	}
	total := len(events)
	trimmed, truncated := trimVisualHistoryEvents(events, eventLimit)
	return trimmed, total, truncated, buildWSCandlesResponsePosition(row), nil
}

func (s *Server) buildWSCandlesResponse(requestID string, filter wsCandlesFetchFilter) wsCandlesResponse {
	resp := wsCandlesResponse{
		Type:      wsFetchTargetCandles,
		RequestID: requestID,
		Data:      make([]wsCandlesResponseItem, 0, len(filter.Requests)),
		Warnings:  []string(nil),
		TS:        time.Now().UTC().UnixMilli(),
	}
	for _, req := range filter.Requests {
		item := wsCandlesResponseItem{
			Exchange:   req.Exchange,
			Symbol:     req.Symbol,
			Timeframes: make(map[string]wsCandlesResponseSeries, len(req.Timeframes)),
		}
		for _, timeframe := range req.Timeframes {
			bars, err := s.loadWSCandles(req.Exchange, req.Symbol, timeframe, req.Limit)
			if err != nil {
				resp.Warnings = append(resp.Warnings, fmt.Sprintf("%s %s %s: %v", req.Exchange, req.Symbol, timeframe, err))
				continue
			}
			item.Timeframes[timeframe] = wsCandlesResponseSeries{
				Requested: req.Limit,
				Returned:  len(bars),
				Bars:      bars,
			}
		}
		if filter.IncludeEvents {
			events, total, truncated, position, err := s.loadWSCandlesEvents(req, filter.EventLimit)
			if err != nil {
				resp.Warnings = append(resp.Warnings, fmt.Sprintf("%s %s events: %v", req.Exchange, req.Symbol, err))
			} else {
				item.Events = events
				item.EventsTotal = total
				item.EventsTruncated = truncated
				item.Position = position
			}
		}
		resp.Data = append(resp.Data, item)
	}
	return resp
}

func wsRequestSubscription(req wsRequest, current wsSubscription) (wsSubscription, error) {
	if req.Subscription == nil && req.Filter == nil {
		return current, nil
	}
	if req.Subscription == nil && req.Filter != nil {
		legacy := current
		legacy.Streams = []string{wsStreamSignals}
		legacy.SignalsFilter = normalizeFilter(*req.Filter)
		return normalizeWSSubscription(legacy)
	}
	sub := *req.Subscription
	if req.Filter != nil {
		sub.SignalsFilter = normalizeFilter(*req.Filter)
	}
	return normalizeWSSubscription(sub)
}

func wsStreamEnabled(sub wsSubscription, target string) bool {
	return streamEnabledInList(sub.Streams, target)
}

func streamEnabledInList(streams []string, target string) bool {
	for _, stream := range streams {
		if stream == target {
			return true
		}
	}
	return false
}

func (s *Server) buildWSSnapshotStateLenient(requestID string, sub wsSubscription) (wsSnapshotState, []string) {
	statusPayload := s.buildStatusResponse()
	out := wsSnapshotState{
		snapshot: wsSnapshot{
			Type:         "snapshot",
			RequestID:    requestID,
			Subscription: sub,
			Status:       &statusPayload,
			TS:           time.Now().UTC().UnixMilli(),
		},
	}
	errors := make([]string, 0, 3)
	if wsStreamEnabled(sub, wsStreamSignals) {
		grouped, _ := s.snapshot(sub.SignalsFilter)
		out.snapshot.Signals = s.enrichGroupedSignalsWithOHLCV(grouped)
		out.signalFlat = flattenGrouped(grouped)
	}
	if wsStreamEnabled(sub, wsStreamGroups) {
		resp := s.buildGroupsResponse()
		out.snapshot.Groups = &resp
		out.groups = resp
	}
	if wsStreamEnabled(sub, wsStreamAccount) {
		if s.cfg.AccountProvider == nil {
			errors = append(errors, "account provider unavailable")
		} else {
			funds, err := s.cfg.AccountProvider.GetAccountFunds(sub.AccountFilter.Exchange)
			if err != nil {
				errors = append(errors, fmt.Sprintf("get account funds: %v", err))
			} else {
				acc := accountResponseFromFunds(funds)
				out.snapshot.Account = &acc
				out.account = acc
				out.hasAccount = true
			}
		}
	}
	if wsStreamEnabled(sub, wsStreamPosition) {
		if s.cfg.AccountProvider == nil {
			errors = append(errors, "position provider unavailable")
		} else {
			positions, err := s.loadOpenPositions(sub.PositionFilter)
			if err != nil {
				errors = append(errors, fmt.Sprintf("list open positions: %v", err))
			} else {
				items := buildPositionItems(filterPositions(positions, sub.PositionFilter))
				out.snapshot.Position = &positionResponse{
					Count:     len(items),
					Positions: items,
				}
				out.positions = copyPositionItems(items)
			}
		}
	}
	if wsStreamEnabled(sub, wsStreamHistory) {
		resp, base, err := s.buildWSHistoryResponse(sub.HistoryFilter)
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			out.snapshot.History = &resp
			out.history = base
		}
	}
	if wsStreamEnabled(sub, wsStreamSymbols) {
		resp, err := s.buildWSSymbolsResponse(sub.SymbolsFilter.Exchange)
		if err != nil {
			errors = append(errors, fmt.Sprintf("build symbols snapshot: %v", err))
		} else {
			out.snapshot.Symbols = &resp
			out.symbols = resp
			out.hasSymbols = true
		}
	}
	if wsStreamEnabled(sub, wsStreamCandles) {
		resp, err := s.buildWSCandlesStreamSnapshot(sub.CandlesFilter)
		if err != nil {
			errors = append(errors, fmt.Sprintf("build candles snapshot: %v", err))
		} else {
			out.snapshot.Candles = &resp
			out.candles = resp
			out.hasCandles = true
		}
	}
	return out, errors
}

func (s *Server) buildWSDiff(
	sub wsSubscription,
	prevSignals map[string]models.Signal,
	prevGroups groupsResponse,
	prevAccount accountResponse,
	prevAccountReady bool,
	prevPositions []positionItem,
	prevHistory historyResponse,
	prevSymbols wsSymbolsResponse,
	prevSymbolsReady bool,
	prevCandles tradingViewCandlesResponse,
	prevCandlesReady bool,
) (wsDiff, map[string]models.Signal, groupsResponse, accountResponse, bool, []positionItem, historyResponse, wsSymbolsResponse, bool, tradingViewCandlesResponse, bool, bool, []string) {
	diff := wsDiff{
		Type:         "diff",
		Subscription: sub,
		TS:           time.Now().UTC().UnixMilli(),
	}
	nextSignals := prevSignals
	nextGroups := prevGroups
	nextAccount := prevAccount
	nextAccountReady := prevAccountReady
	nextPositions := copyPositionItems(prevPositions)
	nextHistory := copyHistoryResponse(prevHistory)
	nextSymbols := prevSymbols
	nextSymbolsReady := prevSymbolsReady
	nextCandles := prevCandles
	nextCandlesReady := prevCandlesReady
	changed := false
	diffErrors := make([]string, 0, 3)

	if wsStreamEnabled(sub, wsStreamSignals) {
		_, currSignals := s.snapshot(sub.SignalsFilter)
		added, updated, removed := diffSignals(prevSignals, currSignals)
		nextSignals = currSignals
		if len(added) > 0 || len(updated) > 0 || len(removed) > 0 {
			signalDiff := wsSignalDiff{Removed: removed}
			if len(added) > 0 {
				signalDiff.Added = s.enrichGroupedSignalsWithOHLCV(groupedFromFlat(added))
			}
			if len(updated) > 0 {
				signalDiff.Updated = s.enrichGroupedSignalsWithOHLCV(groupedFromFlat(updated))
			}
			diff.Signals = &signalDiff
			changed = true
		}
	}

	if wsStreamEnabled(sub, wsStreamGroups) {
		resp := s.buildGroupsResponse()
		nextGroups = resp
		if !reflect.DeepEqual(prevGroups, resp) {
			diff.Groups = &resp
			changed = true
		}
	}

	if wsStreamEnabled(sub, wsStreamAccount) {
		if s.cfg.AccountProvider == nil {
			diffErrors = append(diffErrors, "account provider unavailable")
		} else {
			funds, err := s.cfg.AccountProvider.GetAccountFunds(sub.AccountFilter.Exchange)
			if err != nil {
				diffErrors = append(diffErrors, fmt.Sprintf("get account funds: %v", err))
			} else {
				acc := accountResponseFromFunds(funds)
				nextAccount = acc
				nextAccountReady = true
				if !prevAccountReady || prevAccount != acc {
					diff.Account = &acc
					changed = true
				}
			}
		}
	}

	if wsStreamEnabled(sub, wsStreamPosition) {
		if s.cfg.AccountProvider == nil {
			diffErrors = append(diffErrors, "position provider unavailable")
		} else {
			positions, err := s.loadOpenPositions(sub.PositionFilter)
			if err != nil {
				diffErrors = append(diffErrors, fmt.Sprintf("list open positions: %v", err))
			} else {
				items := buildPositionItems(filterPositions(positions, sub.PositionFilter))
				nextPositions = copyPositionItems(items)
				if !equalPositionItems(prevPositions, items) {
					diff.Position = &positionResponse{
						Count:     len(items),
						Positions: items,
					}
					changed = true
				}
			}
		}
	}

	if wsStreamEnabled(sub, wsStreamHistory) {
		resp, base, err := s.buildWSHistoryResponse(sub.HistoryFilter)
		if err != nil {
			diffErrors = append(diffErrors, err.Error())
		} else {
			nextHistory = base
			if !equalHistoryResponse(prevHistory, base) {
				diff.History = &resp
				changed = true
			}
		}
	}
	if wsStreamEnabled(sub, wsStreamSymbols) {
		resp, err := s.buildWSSymbolsResponse(sub.SymbolsFilter.Exchange)
		if err != nil {
			diffErrors = append(diffErrors, fmt.Sprintf("build symbols snapshot: %v", err))
		} else {
			nextSymbols = resp
			nextSymbolsReady = true
			if !prevSymbolsReady || !reflect.DeepEqual(prevSymbols, resp) {
				diff.Symbols = &resp
				changed = true
			}
		}
	}
	if wsStreamEnabled(sub, wsStreamCandles) {
		resp, err := s.buildWSCandlesStreamSnapshot(sub.CandlesFilter)
		if err != nil {
			diffErrors = append(diffErrors, fmt.Sprintf("build candles snapshot: %v", err))
		} else {
			nextCandles = resp
			nextCandlesReady = true
			if !prevCandlesReady || !reflect.DeepEqual(prevCandles, resp) {
				diff.Candles = &resp
				changed = true
			}
		}
	}

	if changed {
		statusPayload := s.buildStatusResponse()
		diff.Status = &statusPayload
	}

	return diff, nextSignals, nextGroups, nextAccount, nextAccountReady, nextPositions, nextHistory, nextSymbols, nextSymbolsReady, nextCandles, nextCandlesReady, changed, diffErrors
}

func (s *Server) writeWSError(ctx context.Context, conn *websocket.Conn, mu *sync.Mutex, requestID, message string) error {
	return s.writeWS(ctx, conn, mu, wsError{
		Type:      "error",
		RequestID: requestID,
		Message:   message,
		TS:        time.Now().UTC().UnixMilli(),
	})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	initialSub, err := wsSubscriptionFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// 显式关闭 WS 压缩，降低部分浏览器/代理链路上的兼容性抖动风险。
		CompressionMode: websocket.CompressionDisabled,
		OriginPatterns:  s.cfg.WSOriginPatterns,
	})
	if err != nil {
		s.logger.Warn("ws accept failed", zap.Error(err))
		return
	}
	conn.SetReadLimit(wsMaxMessageBytes)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer func() {
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			s.logger.Debug("ws close failed", zap.Error(err))
		}
	}()

	var (
		sendMu  sync.Mutex
		state   wsState
		stateMu sync.RWMutex
	)

	initialSnapshot, initialErrors := s.buildWSSnapshotStateLenient("", initialSub)
	state = wsState{
		subscription:    initialSnapshot.snapshot.Subscription,
		signalBaseline:  initialSnapshot.signalFlat,
		groupBase:       initialSnapshot.groups,
		accountBaseline: initialSnapshot.account,
		accountReady:    initialSnapshot.hasAccount,
		positionBase:    copyPositionItems(initialSnapshot.positions),
		historyBase:     copyHistoryResponse(initialSnapshot.history),
		symbolsBase:     initialSnapshot.symbols,
		symbolsReady:    initialSnapshot.hasSymbols,
		candlesBase:     initialSnapshot.candles,
		candlesReady:    initialSnapshot.hasCandles,
		version:         1,
	}

	if err := s.writeWS(ctx, conn, &sendMu, initialSnapshot.snapshot); err != nil {
		return
	}
	for _, message := range initialErrors {
		s.logger.Warn("ws initial snapshot degraded", zap.String("error", message))
		if err := s.writeWSError(ctx, conn, &sendMu, "", message); err != nil {
			return
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.cfg.UpdateInterval)
		defer ticker.Stop()
		lastDiffErrorMessage := ""
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stateMu.RLock()
				currentSub := state.subscription
				prevSignals := state.signalBaseline
				prevGroups := state.groupBase
				prevAccount := state.accountBaseline
				prevAccountReady := state.accountReady
				prevPositions := state.positionBase
				prevHistory := state.historyBase
				prevSymbols := state.symbolsBase
				prevSymbolsReady := state.symbolsReady
				prevCandles := state.candlesBase
				prevCandlesReady := state.candlesReady
				version := state.version
				stateMu.RUnlock()

				diff, nextSignals, nextGroups, nextAccount, nextAccountReady, nextPositions, nextHistory, nextSymbols, nextSymbolsReady, nextCandles, nextCandlesReady, changed, diffErrors := s.buildWSDiff(
					currentSub,
					prevSignals,
					prevGroups,
					prevAccount,
					prevAccountReady,
					prevPositions,
					prevHistory,
					prevSymbols,
					prevSymbolsReady,
					prevCandles,
					prevCandlesReady,
				)
				diffErrorMessage := strings.Join(diffErrors, "; ")
				if diffErrorMessage != "" {
					if diffErrorMessage != lastDiffErrorMessage {
						if err := s.writeWSError(ctx, conn, &sendMu, "", diffErrorMessage); err != nil {
							s.logger.Warn("ws diff error push failed", zap.Error(err))
							continue
						}
						lastDiffErrorMessage = diffErrorMessage
					}
				} else {
					lastDiffErrorMessage = ""
				}
				if !changed {
					continue
				}

				stateMu.RLock()
				if state.version != version {
					stateMu.RUnlock()
					continue
				}
				stateMu.RUnlock()

				if err := s.writeWS(ctx, conn, &sendMu, diff); err != nil {
					s.logger.Warn("ws diff push failed", zap.Error(err))
					continue
				}

				stateMu.Lock()
				if state.version == version {
					state.signalBaseline = nextSignals
					state.groupBase = nextGroups
					state.accountBaseline = nextAccount
					state.accountReady = nextAccountReady
					state.positionBase = copyPositionItems(nextPositions)
					state.historyBase = copyHistoryResponse(nextHistory)
					state.symbolsBase = nextSymbols
					state.symbolsReady = nextSymbolsReady
					state.candlesBase = nextCandles
					state.candlesReady = nextCandlesReady
				}
				stateMu.Unlock()
			}
		}
	}()

readLoop:
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		var req wsRequest
		if err := json.Unmarshal(data, &req); err != nil {
			if err := s.writeWSError(ctx, conn, &sendMu, "", "invalid request"); err != nil {
				break readLoop
			}
			continue
		}
		reqType := strings.ToLower(strings.TrimSpace(req.Type))
		switch reqType {
		case "fetch":
			fetchTarget := strings.ToLower(strings.TrimSpace(req.Target))
			switch fetchTarget {
			case "", "snapshot":
			case wsFetchTargetStatus:
				statusPayload := s.buildStatusResponse()
				if err := s.writeWS(ctx, conn, &sendMu, wsStatus{
					Type:      "status",
					RequestID: req.RequestID,
					Data:      statusPayload,
					TS:        time.Now().UTC().UnixMilli(),
				}); err != nil {
					break readLoop
				}
				continue
			case wsFetchTargetHistory:
			case wsFetchTargetCandles:
				filter, err := normalizeWSCandlesFetchFilter(req.CandlesFetch)
				if err != nil {
					if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, err.Error()); err != nil {
						break readLoop
					}
					continue
				}
				response := s.buildWSCandlesResponse(req.RequestID, filter)
				if err := s.writeWS(ctx, conn, &sendMu, response); err != nil {
					break readLoop
				}
				continue
			default:
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, "unsupported fetch target"); err != nil {
					break readLoop
				}
				continue
			}
			stateMu.RLock()
			currentSub := state.subscription
			stateMu.RUnlock()
			requestSub, err := wsRequestSubscription(req, currentSub)
			if err != nil {
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, err.Error()); err != nil {
					break readLoop
				}
				continue
			}
			requestSub, err = applyWSHistoryRange(requestSub, req.HistoryRange)
			if err != nil {
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, err.Error()); err != nil {
					break readLoop
				}
				continue
			}
			if fetchTarget == wsFetchTargetHistory {
				requestSub.Streams = []string{wsStreamHistory}
				requestSub, err = normalizeWSSubscription(requestSub)
				if err != nil {
					if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, err.Error()); err != nil {
						break readLoop
					}
					continue
				}
			}
			snapshotState, snapshotErrors := s.buildWSSnapshotStateLenient(req.RequestID, requestSub)
			if err := s.writeWS(ctx, conn, &sendMu, snapshotState.snapshot); err != nil {
				break readLoop
			}
			for _, message := range snapshotErrors {
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, message); err != nil {
					break readLoop
				}
			}
		case "subscribe":
			stateMu.RLock()
			currentSub := state.subscription
			stateMu.RUnlock()
			requestSub, err := wsRequestSubscription(req, currentSub)
			if err != nil {
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, err.Error()); err != nil {
					break readLoop
				}
				continue
			}
			requestSub, err = applyWSHistoryRange(requestSub, req.HistoryRange)
			if err != nil {
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, err.Error()); err != nil {
					break readLoop
				}
				continue
			}
			snapshotState, snapshotErrors := s.buildWSSnapshotStateLenient(req.RequestID, requestSub)
			stateMu.Lock()
			state.subscription = snapshotState.snapshot.Subscription
			state.signalBaseline = snapshotState.signalFlat
			state.groupBase = snapshotState.groups
			state.accountBaseline = snapshotState.account
			state.accountReady = snapshotState.hasAccount
			state.positionBase = copyPositionItems(snapshotState.positions)
			state.historyBase = copyHistoryResponse(snapshotState.history)
			state.symbolsBase = snapshotState.symbols
			state.symbolsReady = snapshotState.hasSymbols
			state.candlesBase = snapshotState.candles
			state.candlesReady = snapshotState.hasCandles
			state.version++
			stateMu.Unlock()
			if err := s.writeWS(ctx, conn, &sendMu, snapshotState.snapshot); err != nil {
				break readLoop
			}
			for _, message := range snapshotErrors {
				if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, message); err != nil {
					break readLoop
				}
			}
		case "ping":
			meta := s.buildStatusMeta()
			if err := s.writeWS(ctx, conn, &sendMu, wsPong{
				Type:      "pong",
				RequestID: req.RequestID,
				Data:      meta,
				TS:        time.Now().UTC().UnixMilli(),
			}); err != nil {
				break readLoop
			}
		default:
			if err := s.writeWSError(ctx, conn, &sendMu, req.RequestID, "unsupported request type"); err != nil {
				break readLoop
			}
		}
	}

	cancel()
	<-done
}

func (s *Server) writeWS(ctx context.Context, conn *websocket.Conn, mu *sync.Mutex, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		s.logger.Warn("ws marshal failed", zap.Error(err))
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	mu.Lock()
	err = conn.Write(writeCtx, websocket.MessageText, data)
	mu.Unlock()
	if err != nil {
		s.logger.Debug("ws write failed", zap.Error(err))
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, private")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	if err := enc.Encode(payload); err != nil {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"error":%q}`, err.Error())))
	}
}

// spaHandler 提供嵌入式 SPA 静态文件服务，对不存在的路径回退到 index.html。
type spaHandler struct {
	root    fs.FS
	handler http.Handler
}

func newSPAHandler(root fs.FS) *spaHandler {
	return &spaHandler{
		root:    root,
		handler: http.FileServer(http.FS(root)),
	}
}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" {
		p = "index.html"
	}
	if _, err := fs.Stat(h.root, p); err != nil {
		// 文件不存在，SPA 回退到 index.html。
		r.URL.Path = "/"
	}
	h.handler.ServeHTTP(w, r)
}
