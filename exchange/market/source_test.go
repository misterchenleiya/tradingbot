package market

import (
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func withTestLocalLocation(t *testing.T, location *time.Location) {
	t.Helper()
	original := time.Local
	time.Local = location
	t.Cleanup(func() {
		time.Local = original
	})
}

func parsePointInLocation(t *testing.T, raw string, location *time.Location) time.Time {
	t.Helper()
	value, _, err := parseTimePointWithLocation(raw, location)
	if err != nil {
		t.Fatalf("parseTimePointWithLocation failed: %v", err)
	}
	return value
}

func TestParseExchangeTimeRangeStartOnly(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	start, end, autoEnd, rangeUsesUTC, err := parseExchangeTimeRange("20260101_1200", now)
	if err != nil {
		t.Fatalf("parseExchangeTimeRange returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if !start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", start, expectedStart)
	}
	if !end.Equal(now) {
		t.Fatalf("unexpected end: got %s want %s", end, now)
	}
	if !autoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if rangeUsesUTC {
		t.Fatalf("expected rangeUsesUTC=false")
	}
}

func TestParseExchangeTimeRangeStartOnlyDateDefaultsToMidnight(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	start, end, autoEnd, rangeUsesUTC, err := parseExchangeTimeRange("20260101", now)
	if err != nil {
		t.Fatalf("parseExchangeTimeRange returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", start, expectedStart)
	}
	if !end.Equal(now) {
		t.Fatalf("unexpected end: got %s want %s", end, now)
	}
	if !autoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if rangeUsesUTC {
		t.Fatalf("expected rangeUsesUTC=false")
	}
}

func TestParseExchangeTimeRangeWithExplicitEnd(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	start, end, autoEnd, rangeUsesUTC, err := parseExchangeTimeRange("20260101_1200-20260101_1600", now)
	if err != nil {
		t.Fatalf("parseExchangeTimeRange returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 1, 1, 16, 0, 0, 0, time.UTC)
	if !start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", start, expectedStart)
	}
	if !end.Equal(expectedEnd) {
		t.Fatalf("unexpected end: got %s want %s", end, expectedEnd)
	}
	if autoEnd {
		t.Fatalf("expected autoEnd=false")
	}
	if rangeUsesUTC {
		t.Fatalf("expected rangeUsesUTC=false")
	}
}

func TestParseExchangeTimeRangeWithMixedDateFormats(t *testing.T) {
	local := time.FixedZone("UTC+8", 8*3600)
	withTestLocalLocation(t, local)

	nowLocal := time.Date(2026, 1, 18, 10, 30, 0, 0, local)
	start, end, autoEnd, rangeUsesUTC, err := parseExchangeTimeRange("20260101-20260118_1200", nowLocal)
	if err != nil {
		t.Fatalf("parseExchangeTimeRange returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 0, 0, 0, 0, local).UTC()
	expectedEnd := time.Date(2026, 1, 18, 12, 0, 0, 0, local).UTC()
	if !start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", start, expectedStart)
	}
	if !end.Equal(expectedEnd) {
		t.Fatalf("unexpected end: got %s want %s", end, expectedEnd)
	}
	if autoEnd {
		t.Fatalf("expected autoEnd=false")
	}
	if rangeUsesUTC {
		t.Fatalf("expected rangeUsesUTC=false")
	}
}

func TestParseExchangeTimeRangeStartOnlyRequiresNowAfterStart(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	now := time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC)
	_, _, _, _, err := parseExchangeTimeRange("20260101_1200", now)
	if err == nil {
		t.Fatalf("expected error for start-only range when now <= start")
	}
}

func TestParseExchangeTimeRangeUsesSystemLocalTimezone(t *testing.T) {
	local := time.FixedZone("UTC+8", 8*3600)
	withTestLocalLocation(t, local)

	nowLocal := time.Date(2026, 1, 2, 10, 30, 0, 0, local)
	start, end, autoEnd, rangeUsesUTC, err := parseExchangeTimeRange("20260101_1200", nowLocal)
	if err != nil {
		t.Fatalf("parseExchangeTimeRange returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 12, 0, 0, 0, local).UTC()
	if !start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", start, expectedStart)
	}
	if !end.Equal(nowLocal.UTC()) {
		t.Fatalf("unexpected end: got %s want %s", end, nowLocal.UTC())
	}
	if !autoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if rangeUsesUTC {
		t.Fatalf("expected rangeUsesUTC=false")
	}
}

func TestParseExchangeTimeRangeExplicitUTCMarker(t *testing.T) {
	local := time.FixedZone("UTC+8", 8*3600)
	withTestLocalLocation(t, local)

	nowLocal := time.Date(2026, 1, 2, 10, 30, 0, 0, local)
	start, end, autoEnd, rangeUsesUTC, err := parseExchangeTimeRange("20260101_1200Z", nowLocal)
	if err != nil {
		t.Fatalf("parseExchangeTimeRange returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if !start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", start, expectedStart)
	}
	if !end.Equal(nowLocal.UTC()) {
		t.Fatalf("unexpected end: got %s want %s", end, nowLocal.UTC())
	}
	if !autoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if !rangeUsesUTC {
		t.Fatalf("expected rangeUsesUTC=true")
	}
}

func TestParseExchangeTimeRangeRejectsMixedTimezoneMarkers(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	_, _, _, _, err := parseExchangeTimeRange("20260101_1200-20260101_1600Z", time.Now())
	if err == nil {
		t.Fatalf("expected timezone marker mismatch error")
	}
}

func TestParseExchangeSourceStartOnly(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	source, err := parseExchangeSource("exchange:okx:btcusdtp:15m/1h/4h:20260101_1200")
	if err != nil {
		t.Fatalf("parseExchangeSource returned error: %v", err)
	}
	if source.Type != sourceTypeExchange {
		t.Fatalf("unexpected source type: %s", source.Type)
	}
	if source.Exchange != "okx" {
		t.Fatalf("unexpected exchange: %s", source.Exchange)
	}
	if source.Symbol != "BTC/USDT" {
		t.Fatalf("unexpected symbol: %s", source.Symbol)
	}
	if source.TimeRange != "20260101_1200" {
		t.Fatalf("unexpected time range: %s", source.TimeRange)
	}
	if !source.AutoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if !source.End.After(source.Start) {
		t.Fatalf("expected end after start, got start=%s end=%s", source.Start, source.End)
	}
	if got, want := len(source.Timeframes), 3; got != want {
		t.Fatalf("unexpected timeframe count: got %d want %d", got, want)
	}
	if source.Timeframes[0] != "15m" || source.Timeframes[1] != "1h" || source.Timeframes[2] != "4h" {
		t.Fatalf("unexpected timeframes: %#v", source.Timeframes)
	}
}

func TestParseDBSourceStartOnly(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	source, err := parseDBSource("db:okx:btcusdtp:15m/1h/4h:20260101_1200")
	if err != nil {
		t.Fatalf("parseDBSource returned error: %v", err)
	}
	if source.Type != sourceTypeDB {
		t.Fatalf("unexpected source type: %s", source.Type)
	}
	if source.Exchange != "okx" {
		t.Fatalf("unexpected exchange: %s", source.Exchange)
	}
	if source.Symbol != "BTC/USDT" {
		t.Fatalf("unexpected symbol: %s", source.Symbol)
	}
	if source.TimeRange != "20260101_1200" {
		t.Fatalf("unexpected time range: %s", source.TimeRange)
	}
	if !source.AutoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if !source.End.After(source.Start) {
		t.Fatalf("expected end after start, got start=%s end=%s", source.Start, source.End)
	}
}

func TestParseCSVSourceStartOnlyRangeOverride(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "csv:/tmp/okx_btcusdtp_1h_20260101_1200-20260131_2300.csv:20260110_1200"
	source, err := parseCSVSource(raw, len(sourceTypeCSV)+1)
	if err != nil {
		t.Fatalf("parseCSVSource returned error: %v", err)
	}
	if source.Type != sourceTypeCSV {
		t.Fatalf("unexpected source type: %s", source.Type)
	}
	if source.TimeRange != "20260110_1200" {
		t.Fatalf("unexpected time range: %s", source.TimeRange)
	}
	expectedStart := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.AutoEnd {
		t.Fatalf("expected autoEnd=true")
	}
	if !source.End.After(source.Start) {
		t.Fatalf("expected end after start, got start=%s end=%s", source.Start, source.End)
	}
	if got, want := len(source.Files), 1; got != want {
		t.Fatalf("unexpected file count: got %d want %d", got, want)
	}
	if source.Files[0].TimeRange != "20260110_1200" {
		t.Fatalf("unexpected file time range: %s", source.Files[0].TimeRange)
	}
}

func TestParseCSVSourceExplicitRangeOverride(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "csv:/tmp/okx_btcusdtp_1h_20260101_1200-20260131_2300.csv:20260110_1200-20260115_1600"
	source, err := parseCSVSource(raw, len(sourceTypeCSV)+1)
	if err != nil {
		t.Fatalf("parseCSVSource returned error: %v", err)
	}
	if source.TimeRange != "20260110_1200-20260115_1600" {
		t.Fatalf("unexpected time range: %s", source.TimeRange)
	}
	expectedStart := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 1, 15, 16, 0, 0, 0, time.UTC)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.End.Equal(expectedEnd) {
		t.Fatalf("unexpected end: got %s want %s", source.End, expectedEnd)
	}
	if source.AutoEnd {
		t.Fatalf("expected autoEnd=false")
	}
}

func TestParseBackTestSourceWithReplayStart(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "exchange:okx:solusdtp:15m/1h:20251230_0000-20260108_1000@20260102_0600"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}
	if !source.HasReplayTS {
		t.Fatalf("expected replay start enabled")
	}
	expectedReplayStart := time.Date(2026, 1, 2, 6, 0, 0, 0, time.UTC)
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
	if source.TimeRange != "20251230_0000-20260108_1000" {
		t.Fatalf("unexpected time range: %s", source.TimeRange)
	}
}

func TestParseBackTestSourceWithDateOnlyRangeAndReplayStart(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "exchange:okx:solusdtp:3m/15m/1h:20260101-20260118@20260102"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}
	if !source.HasReplayTS {
		t.Fatalf("expected replay start enabled")
	}
	expectedStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 1, 18, 0, 0, 0, 0, time.UTC)
	expectedReplayStart := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.End.Equal(expectedEnd) {
		t.Fatalf("unexpected end: got %s want %s", source.End, expectedEnd)
	}
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestParseBackTestSourceWithMixedDateAndDateTime(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "exchange:okx:solusdtp:3m/15m/1h:20260101-20260118_1200@20260102"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}
	expectedEnd := time.Date(2026, 1, 18, 12, 0, 0, 0, time.UTC)
	expectedReplayStart := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if !source.End.Equal(expectedEnd) {
		t.Fatalf("unexpected end: got %s want %s", source.End, expectedEnd)
	}
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestParseBackTestSourceWithReplayStartOutOfRange(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "exchange:okx:solusdtp:15m/1h:20251230_0000-20260108_1000@20260109_0000"
	_, err := parseBackTestSource(raw)
	if err == nil {
		t.Fatalf("expected replay range error")
	}
}

func TestParseBackTestSourceCSVWithReplayStart(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "csv:/tmp/okx_btcusdtp_1h_20260101_1200-20260131_2300.csv:20260110_1200-20260115_1600@20260112_0600"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}
	if source.Type != sourceTypeCSV {
		t.Fatalf("unexpected source type: %s", source.Type)
	}
	if !source.HasReplayTS {
		t.Fatalf("expected replay start enabled")
	}
	expectedReplayStart := time.Date(2026, 1, 12, 6, 0, 0, 0, time.UTC)
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestParseBackTestSourceWithReplayStartUsesLocalTimezone(t *testing.T) {
	local := time.FixedZone("UTC+8", 8*3600)
	withTestLocalLocation(t, local)

	raw := "exchange:okx:solusdtp:15m/1h:20260101_1200-20260103_1200@20260102_0600"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}

	expectedStart := parsePointInLocation(t, "20260101_1200", local)
	expectedReplayStart := parsePointInLocation(t, "20260102_0600", local)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestParseBackTestSourceWithUTCInputs(t *testing.T) {
	local := time.FixedZone("UTC+8", 8*3600)
	withTestLocalLocation(t, local)

	raw := "exchange:okx:solusdtp:15m/1h:20260101_1200Z-20260103_1200Z@20260102_0600Z"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	expectedReplayStart := time.Date(2026, 1, 2, 6, 0, 0, 0, time.UTC)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestParseBackTestSourceWithDateOnlyUTCInputs(t *testing.T) {
	local := time.FixedZone("UTC+8", 8*3600)
	withTestLocalLocation(t, local)

	raw := "exchange:okx:solusdtp:15m/1h:20260101Z-20260103Z@20260102Z"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}

	expectedStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expectedReplayStart := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestParseBackTestSourceRejectsReplayTimezoneMismatch(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "exchange:okx:solusdtp:15m/1h:20260101_1200-20260103_1200@20260102_0600Z"
	_, err := parseBackTestSource(raw)
	if err == nil {
		t.Fatalf("expected timezone marker mismatch error")
	}
}

func TestParseCSVSourceDateOnlyRangeOverride(t *testing.T) {
	withTestLocalLocation(t, time.UTC)
	raw := "csv:/tmp/okx_btcusdtp_1h_20260101_1200-20260131_2300.csv:20260110-20260115@20260112"
	source, err := parseBackTestSource(raw)
	if err != nil {
		t.Fatalf("parseBackTestSource returned error: %v", err)
	}
	expectedStart := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	expectedReplayStart := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
	if !source.Start.Equal(expectedStart) {
		t.Fatalf("unexpected start: got %s want %s", source.Start, expectedStart)
	}
	if !source.End.Equal(expectedEnd) {
		t.Fatalf("unexpected end: got %s want %s", source.End, expectedEnd)
	}
	if !source.ReplayStart.Equal(expectedReplayStart) {
		t.Fatalf("unexpected replay start: got %s want %s", source.ReplayStart, expectedReplayStart)
	}
}

func TestOhlcvCoversRequestedRangeComplete(t *testing.T) {
	start := time.Date(2026, 2, 7, 12, 15, 0, 0, time.UTC)
	end := time.Date(2026, 2, 7, 16, 30, 0, 0, time.UTC)
	data := make([]models.OHLCV, 0, 17)
	for ts := start.UnixMilli(); ts < end.UnixMilli(); ts += int64(15 * time.Minute / time.Millisecond) {
		data = append(data, models.OHLCV{TS: ts})
	}
	if !ohlcvCoversRequestedRange(data, "15m", start, end) {
		t.Fatalf("expected complete range coverage")
	}
}

func TestOhlcvCoversRequestedRangeWithGap(t *testing.T) {
	start := time.Date(2026, 2, 7, 12, 15, 0, 0, time.UTC)
	end := time.Date(2026, 2, 7, 13, 15, 0, 0, time.UTC)
	data := []models.OHLCV{
		{TS: start.UnixMilli()},
		{TS: start.Add(2 * 15 * time.Minute).UnixMilli()},
	}
	if ohlcvCoversRequestedRange(data, "15m", start, end) {
		t.Fatalf("expected incomplete range coverage")
	}
}

func TestOhlcvCoversRequestedRangeAlignForward(t *testing.T) {
	start := time.Date(2026, 2, 7, 12, 16, 0, 0, time.UTC)
	end := time.Date(2026, 2, 7, 12, 46, 0, 0, time.UTC)
	step := int64(15 * time.Minute / time.Millisecond)
	data := []models.OHLCV{
		{TS: alignTimestampForward(start.UnixMilli(), step)},
		{TS: alignTimestampForward(start.UnixMilli(), step) + step},
	}
	if !ohlcvCoversRequestedRange(data, "15m", start, end) {
		t.Fatalf("expected aligned range coverage")
	}
}
