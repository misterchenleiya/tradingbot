package market

import "time"

type BackTestSummary struct {
	Source        string
	Seed          int64
	SourceType    string
	Exchange      string
	Symbol        string
	Timeframes    []string
	TimeRange     string
	HistoryBars   int
	RangeStart    time.Time
	RangeEnd      time.Time
	ReplayStart   time.Time
	CSVFiles      []CSVFileSpec
	StartedAtUTC  time.Time
	EndedAtUTC    time.Time
	SeriesCounts  map[string]int
	PreloadCounts map[string]int
	ExportFiles   []string
}

func (s BackTestSummary) Duration() time.Duration {
	if s.StartedAtUTC.IsZero() || s.EndedAtUTC.IsZero() {
		return 0
	}
	return s.EndedAtUTC.Sub(s.StartedAtUTC)
}
