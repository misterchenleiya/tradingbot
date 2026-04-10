package exporter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"github.com/misterchenleiya/tradingbot/storage"
)

const ohlcvStatusTimeLayout = "2006-01-02 15:04"

type ohlcvStatusResponse struct {
	Timezone   string            `json:"timezone"`
	TimeFormat string            `json:"time_format"`
	Count      int               `json:"count"`
	Items      []ohlcvStatusItem `json:"items"`
}

type ohlcvStatusItem struct {
	Exchange             string                      `json:"exchange"`
	Symbol               string                      `json:"symbol"`
	ConfiguredTimeframes []string                    `json:"configured_timeframes,omitempty"`
	AvailableTimeframes  []ohlcvStatusTimeframeRange `json:"available_timeframes,omitempty"`
	OHLCVBounds          ohlcvStatusBound            `json:"ohlcv_bounds"`
}

type ohlcvStatusTimeframeRange struct {
	Timeframe string `json:"timeframe"`
	Bars      int64  `json:"bars"`
	StartTSMS int64  `json:"start_ts_ms"`
	StartTime string `json:"start_time"`
	EndTSMS   int64  `json:"end_ts_ms"`
	EndTime   string `json:"end_time"`
}

type ohlcvStatusBound struct {
	Exists                bool   `json:"exists"`
	EarliestAvailableTSMS int64  `json:"earliest_available_ts_ms,omitempty"`
	EarliestAvailableTime string `json:"earliest_available_time,omitempty"`
}

type ohlcvStatusAggregate struct {
	item ohlcvStatusItem
}

func (s *Server) handleOHLCVStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp, err := s.buildOHLCVStatusResponse()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) buildOHLCVStatusResponse() (ohlcvStatusResponse, error) {
	if s.cfg.SymbolProvider == nil {
		return ohlcvStatusResponse{}, fmt.Errorf("symbol provider unavailable")
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return ohlcvStatusResponse{}, fmt.Errorf("history store unavailable")
	}
	exchanges, err := s.cfg.SymbolProvider.ListExchanges()
	if err != nil {
		return ohlcvStatusResponse{}, err
	}
	symbols, err := s.cfg.SymbolProvider.ListSymbols()
	if err != nil {
		return ohlcvStatusResponse{}, err
	}
	ranges, err := s.cfg.HistoryStore.ListOHLCVTimeframeRanges()
	if err != nil {
		return ohlcvStatusResponse{}, err
	}
	bounds, err := s.cfg.HistoryStore.ListOHLCVBounds()
	if err != nil {
		return ohlcvStatusResponse{}, err
	}
	items := buildOHLCVStatusItems(exchanges, symbols, ranges, bounds)
	return ohlcvStatusResponse{
		Timezone:   time.Now().In(time.Local).Location().String(),
		TimeFormat: "YYYY-MM-DD HH:MM",
		Count:      len(items),
		Items:      items,
	}, nil
}

func buildOHLCVStatusItems(
	exchanges []models.Exchange,
	symbols []models.Symbol,
	ranges []storage.OHLCVTimeframeRange,
	bounds []storage.OHLCVBoundRecord,
) []ohlcvStatusItem {
	exchangeDefaults := make(map[string][]string, len(exchanges))
	for _, exchange := range exchanges {
		exchangeDefaults[ohlcvStatusPairKey(exchange.Name, "")] = parseOHLCVStatusTimeframes(exchange.Timeframes, nil)
	}

	aggregates := make(map[string]*ohlcvStatusAggregate, len(symbols))
	order := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		key := ohlcvStatusPairKey(symbol.Exchange, symbol.Symbol)
		if _, exists := aggregates[key]; exists {
			continue
		}
		configured := parseOHLCVStatusTimeframes(
			symbol.Timeframes,
			exchangeDefaults[ohlcvStatusPairKey(symbol.Exchange, "")],
		)
		aggregates[key] = &ohlcvStatusAggregate{
			item: ohlcvStatusItem{
				Exchange:             strings.TrimSpace(symbol.Exchange),
				Symbol:               strings.TrimSpace(symbol.Symbol),
				ConfiguredTimeframes: configured,
				AvailableTimeframes:  []ohlcvStatusTimeframeRange{},
				OHLCVBounds:          ohlcvStatusBound{Exists: false},
			},
		}
		order = append(order, key)
	}

	for _, item := range ranges {
		key := ohlcvStatusPairKey(item.Exchange, item.Symbol)
		agg, ok := aggregates[key]
		if !ok {
			agg = &ohlcvStatusAggregate{
				item: ohlcvStatusItem{
					Exchange:            strings.TrimSpace(item.Exchange),
					Symbol:              strings.TrimSpace(item.Symbol),
					AvailableTimeframes: []ohlcvStatusTimeframeRange{},
					OHLCVBounds:         ohlcvStatusBound{Exists: false},
				},
			}
			aggregates[key] = agg
			order = append(order, key)
		}
		agg.item.AvailableTimeframes = append(agg.item.AvailableTimeframes, ohlcvStatusTimeframeRange{
			Timeframe: strings.TrimSpace(item.Timeframe),
			Bars:      item.Bars,
			StartTSMS: item.StartTS,
			StartTime: formatOHLCVStatusTime(item.StartTS),
			EndTSMS:   item.EndTS,
			EndTime:   formatOHLCVStatusTime(item.EndTS),
		})
	}

	for _, item := range bounds {
		key := ohlcvStatusPairKey(item.Exchange, item.Symbol)
		agg, ok := aggregates[key]
		if !ok {
			agg = &ohlcvStatusAggregate{
				item: ohlcvStatusItem{
					Exchange:            strings.TrimSpace(item.Exchange),
					Symbol:              strings.TrimSpace(item.Symbol),
					AvailableTimeframes: []ohlcvStatusTimeframeRange{},
				},
			}
			aggregates[key] = agg
			order = append(order, key)
		}
		agg.item.OHLCVBounds = ohlcvStatusBound{
			Exists:                item.EarliestAvailableTS > 0,
			EarliestAvailableTSMS: item.EarliestAvailableTS,
			EarliestAvailableTime: formatOHLCVStatusTime(item.EarliestAvailableTS),
		}
	}

	items := make([]ohlcvStatusItem, 0, len(order))
	for _, key := range order {
		agg := aggregates[key]
		sort.Slice(agg.item.AvailableTimeframes, func(i, j int) bool {
			left := agg.item.AvailableTimeframes[i].Timeframe
			right := agg.item.AvailableTimeframes[j].Timeframe
			return ohlcvStatusTimeframeLess(left, right)
		})
		items = append(items, agg.item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		leftExchange := strings.ToLower(strings.TrimSpace(items[i].Exchange))
		rightExchange := strings.ToLower(strings.TrimSpace(items[j].Exchange))
		if leftExchange != rightExchange {
			return leftExchange < rightExchange
		}
		return strings.TrimSpace(items[i].Symbol) < strings.TrimSpace(items[j].Symbol)
	})
	return items
}

func parseOHLCVStatusTimeframes(raw string, fallback []string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return cloneOHLCVStatusTimeframes(fallback)
	}
	var items []string
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return cloneOHLCVStatusTimeframes(fallback)
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		timeframe := strings.TrimSpace(item)
		if timeframe == "" {
			continue
		}
		if _, ok := market.TimeframeDuration(timeframe); !ok {
			continue
		}
		if _, ok := seen[timeframe]; ok {
			continue
		}
		seen[timeframe] = struct{}{}
		out = append(out, timeframe)
	}
	if len(out) == 0 {
		return cloneOHLCVStatusTimeframes(fallback)
	}
	sort.Slice(out, func(i, j int) bool {
		return ohlcvStatusTimeframeLess(out[i], out[j])
	})
	return out
}

func cloneOHLCVStatusTimeframes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func ohlcvStatusTimeframeLess(left, right string) bool {
	leftDur, leftOK := market.TimeframeDuration(strings.TrimSpace(left))
	rightDur, rightOK := market.TimeframeDuration(strings.TrimSpace(right))
	switch {
	case leftOK && rightOK:
		if leftDur == rightDur {
			return left < right
		}
		return leftDur < rightDur
	case leftOK:
		return true
	case rightOK:
		return false
	default:
		return left < right
	}
}

func formatOHLCVStatusTime(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).In(time.Local).Format(ohlcvStatusTimeLayout)
}

func ohlcvStatusPairKey(exchange, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + "|" + strings.TrimSpace(symbol)
}
