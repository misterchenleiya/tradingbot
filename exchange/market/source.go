package market

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	sourceTypeExchange = "exchange"
	sourceTypeDB       = "db"
	sourceTypeCSV      = "csv"
)

var knownQuotes = []string{"usdt", "usdc", "usd", "btc", "eth"}
var csvTimeRangePattern = regexp.MustCompile(`\d{8}_\d{4}-\d{8}_\d{4}$`)
var sourceTimePointPattern = regexp.MustCompile(`^\d{8}(_\d{4})?([Zz])?$`)
var sourceTimeRangePattern = regexp.MustCompile(`^\d{8}(_\d{4})?([Zz])?-\d{8}(_\d{4})?([Zz])?$`)

type BackTestSource struct {
	Type        string
	Exchange    string
	Symbol      string
	SymbolToken string
	Timeframes  []string
	TimeRange   string
	Start       time.Time
	End         time.Time
	AutoEnd     bool
	ReplayStart time.Time
	HasReplayTS bool
	Files       []CSVFileSpec
}

type CSVFileSpec struct {
	Path        string
	DisplayPath string
	DisplayDir  string
	DisplayFile string
	Exchange    string
	Symbol      string
	SymbolToken string
	Timeframe   string
	TimeRange   string
	Start       time.Time
	End         time.Time
}

func parseBackTestSource(raw string) (BackTestSource, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return BackTestSource{}, fmt.Errorf("empty source")
	}
	base, replayRaw, hasReplayTS, err := splitBackTestReplayStart(raw)
	if err != nil {
		return BackTestSource{}, err
	}
	lower := strings.ToLower(base)
	var source BackTestSource
	switch {
	case strings.HasPrefix(lower, sourceTypeExchange+":"):
		source, err = parseExchangeSource(base)
	case strings.HasPrefix(lower, sourceTypeDB+":"):
		source, err = parseDBSource(base)
	case strings.HasPrefix(lower, sourceTypeCSV+":"):
		source, err = parseCSVSource(base, len(sourceTypeCSV)+1)
	default:
		return BackTestSource{}, fmt.Errorf("unsupported source: %s", raw)
	}
	if err != nil {
		return BackTestSource{}, err
	}
	if hasReplayTS {
		replayStart, replayUsesUTC, err := parseTimePointWithLocation(replayRaw, sourceParseLocation())
		if err != nil {
			return BackTestSource{}, fmt.Errorf("invalid replay start: %s", replayRaw)
		}
		rangeUsesUTC, err := detectTimeRangeUsesUTC(source.TimeRange)
		if err != nil {
			return BackTestSource{}, err
		}
		if replayUsesUTC != rangeUsesUTC {
			return BackTestSource{}, fmt.Errorf("time zone marker mismatch between time range and replay start: range=%s replay_start=%s", source.TimeRange, replayRaw)
		}
		source.ReplayStart = replayStart
		source.HasReplayTS = true
		if source.ReplayStart.Before(source.Start) || source.ReplayStart.After(source.End) {
			return BackTestSource{}, fmt.Errorf("replay start must be within time range: replay_start=%s range=%s",
				source.ReplayStart.UTC().Format("20060102_1504"),
				source.TimeRange,
			)
		}
	}
	return source, nil
}

func splitBackTestReplayStart(raw string) (string, string, bool, error) {
	idx := strings.LastIndex(raw, "@")
	if idx < 0 {
		return raw, "", false, nil
	}
	prefix := strings.TrimSpace(raw[:idx])
	suffix := strings.TrimSpace(raw[idx+1:])
	if prefix == "" || suffix == "" {
		return raw, "", false, nil
	}
	if !sourceTimePointPattern.MatchString(suffix) {
		return raw, "", false, nil
	}
	return prefix, suffix, true, nil
}

func parseExchangeSource(raw string) (BackTestSource, error) {
	return parseInstrumentSource(raw, sourceTypeExchange)
}

func parseDBSource(raw string) (BackTestSource, error) {
	return parseInstrumentSource(raw, sourceTypeDB)
}

func parseInstrumentSource(raw, sourceType string) (BackTestSource, error) {
	parts := strings.SplitN(raw, ":", 5)
	if len(parts) != 5 {
		return BackTestSource{}, fmt.Errorf("invalid %s source: %s", sourceType, raw)
	}
	exchange := strings.TrimSpace(parts[1])
	if exchange == "" {
		return BackTestSource{}, fmt.Errorf("empty exchange")
	}
	symbolRaw := strings.TrimSpace(parts[2])
	if symbolRaw == "" {
		return BackTestSource{}, fmt.Errorf("empty symbol")
	}
	timeframeRaw := strings.TrimSpace(parts[3])
	if timeframeRaw == "" {
		return BackTestSource{}, fmt.Errorf("empty timeframe")
	}
	timeRange := strings.TrimSpace(parts[4])
	if timeRange == "" {
		return BackTestSource{}, fmt.Errorf("empty time range")
	}

	timeframes, err := parseTimeframes(timeframeRaw)
	if err != nil {
		return BackTestSource{}, err
	}
	start, end, autoEnd, _, err := parseExchangeTimeRange(timeRange, time.Now())
	if err != nil {
		return BackTestSource{}, err
	}
	symbol, symbolToken, err := normalizeSymbol(symbolRaw)
	if err != nil {
		return BackTestSource{}, err
	}

	return BackTestSource{
		Type:        sourceType,
		Exchange:    strings.ToLower(exchange),
		Symbol:      symbol,
		SymbolToken: symbolToken,
		Timeframes:  timeframes,
		TimeRange:   timeRange,
		Start:       start,
		End:         end,
		AutoEnd:     autoEnd,
	}, nil
}

func parseCSVSource(raw string, prefixLen int) (BackTestSource, error) {
	value := strings.TrimSpace(raw[prefixLen:])
	if value == "" {
		return BackTestSource{}, fmt.Errorf("empty csv source")
	}
	overrideRange := ""
	if trimmed, override, ok := splitCSVSourceTimeRange(value); ok {
		value = trimmed
		overrideRange = override
	}
	parts := strings.Split(value, ",")
	if len(parts) == 0 {
		return BackTestSource{}, fmt.Errorf("empty csv source")
	}
	first := strings.TrimSpace(parts[0])
	if first == "" {
		return BackTestSource{}, fmt.Errorf("empty csv path")
	}
	baseDir := filepath.Dir(first)

	var files []CSVFileSpec
	for idx, item := range parts {
		rawItem := strings.TrimSpace(item)
		if rawItem == "" {
			return BackTestSource{}, fmt.Errorf("empty csv path at index %d", idx)
		}
		item = rawItem
		if idx > 0 && !filepath.IsAbs(item) {
			item = filepath.Join(baseDir, item)
		}
		spec, err := parseCSVFileSpec(item)
		if err != nil {
			return BackTestSource{}, err
		}
		spec.DisplayPath = rawItem
		spec.DisplayDir, spec.DisplayFile = splitCSVDisplayPath(rawItem)
		if spec.DisplayFile == "" {
			spec.DisplayFile = filepath.Base(item)
		}
		files = append(files, spec)
	}
	if len(files) == 0 {
		return BackTestSource{}, fmt.Errorf("no csv files")
	}

	firstSpec := files[0]
	for _, spec := range files[1:] {
		if !strings.EqualFold(spec.Exchange, firstSpec.Exchange) {
			return BackTestSource{}, fmt.Errorf("csv exchange mismatch: %s vs %s", spec.Exchange, firstSpec.Exchange)
		}
		if spec.Symbol != firstSpec.Symbol {
			return BackTestSource{}, fmt.Errorf("csv symbol mismatch: %s vs %s", spec.Symbol, firstSpec.Symbol)
		}
		if spec.TimeRange != firstSpec.TimeRange {
			return BackTestSource{}, fmt.Errorf("csv time range mismatch: %s vs %s", spec.TimeRange, firstSpec.TimeRange)
		}
	}

	timeframes := make([]string, 0, len(files))
	seen := make(map[string]bool)
	for _, spec := range files {
		if !seen[spec.Timeframe] {
			seen[spec.Timeframe] = true
			timeframes = append(timeframes, spec.Timeframe)
		}
	}
	sourceRange := firstSpec.TimeRange
	sourceStart := firstSpec.Start
	sourceEnd := firstSpec.End
	sourceAutoEnd := false
	if overrideRange != "" {
		start, end, autoEnd, _, err := parseExchangeTimeRange(overrideRange, time.Now())
		if err != nil {
			return BackTestSource{}, err
		}
		sourceRange = overrideRange
		sourceStart = start
		sourceEnd = end
		sourceAutoEnd = autoEnd
		for i := range files {
			files[i].TimeRange = sourceRange
			files[i].Start = sourceStart
			files[i].End = sourceEnd
		}
	}

	return BackTestSource{
		Type:        sourceTypeCSV,
		Exchange:    firstSpec.Exchange,
		Symbol:      firstSpec.Symbol,
		SymbolToken: firstSpec.SymbolToken,
		Timeframes:  timeframes,
		TimeRange:   sourceRange,
		Start:       sourceStart,
		End:         sourceEnd,
		AutoEnd:     sourceAutoEnd,
		Files:       files,
	}, nil
}

func splitCSVSourceTimeRange(raw string) (string, string, bool) {
	idx := strings.LastIndex(raw, ":")
	if idx < 0 {
		return raw, "", false
	}
	prefix := strings.TrimSpace(raw[:idx])
	suffix := strings.TrimSpace(raw[idx+1:])
	if prefix == "" || suffix == "" {
		return raw, "", false
	}
	if !sourceTimePointPattern.MatchString(suffix) && !sourceTimeRangePattern.MatchString(suffix) {
		return raw, "", false
	}
	return prefix, suffix, true
}

func parseCSVFileSpec(path string) (CSVFileSpec, error) {
	base := filepath.Base(path)
	ext := strings.ToLower(filepath.Ext(base))
	if ext != ".csv" {
		return CSVFileSpec{}, fmt.Errorf("invalid csv extension: %s", base)
	}
	name := strings.TrimSuffix(base, ext)

	loc := csvTimeRangePattern.FindStringIndex(name)
	if loc == nil || loc[1] != len(name) {
		return CSVFileSpec{}, fmt.Errorf("invalid csv file name: %s", base)
	}
	timeRange := name[loc[0]:loc[1]]
	prefix := strings.TrimSuffix(name[:loc[0]], "_")
	if prefix == "" {
		return CSVFileSpec{}, fmt.Errorf("invalid csv file name: %s", base)
	}

	lastUnderscore := strings.LastIndex(prefix, "_")
	if lastUnderscore < 0 {
		return CSVFileSpec{}, fmt.Errorf("invalid csv file name: %s", base)
	}
	timeframe := strings.TrimSpace(prefix[lastUnderscore+1:])
	if timeframe == "" {
		return CSVFileSpec{}, fmt.Errorf("empty timeframe in csv file: %s", base)
	}
	if _, err := timeframeMinutes(timeframe); err != nil {
		return CSVFileSpec{}, err
	}
	exchangeSymbol := strings.TrimSpace(prefix[:lastUnderscore])
	if exchangeSymbol == "" {
		return CSVFileSpec{}, fmt.Errorf("invalid csv file name: %s", base)
	}
	parts := strings.Split(exchangeSymbol, "_")
	if len(parts) < 2 {
		return CSVFileSpec{}, fmt.Errorf("invalid csv file name: %s", base)
	}
	exchange := strings.TrimSpace(parts[0])
	if exchange == "" {
		return CSVFileSpec{}, fmt.Errorf("empty exchange in csv file: %s", base)
	}
	symbolToken := strings.TrimSpace(strings.Join(parts[1:], "_"))
	if symbolToken == "" {
		return CSVFileSpec{}, fmt.Errorf("empty symbol in csv file: %s", base)
	}
	start, end, err := parseTimeRange(timeRange)
	if err != nil {
		return CSVFileSpec{}, err
	}
	symbol, err := symbolFromToken(symbolToken)
	if err != nil {
		return CSVFileSpec{}, err
	}

	return CSVFileSpec{
		Path:        path,
		Exchange:    strings.ToLower(exchange),
		Symbol:      symbol,
		SymbolToken: strings.ToLower(symbolToken),
		Timeframe:   timeframe,
		TimeRange:   timeRange,
		Start:       start,
		End:         end,
	}, nil
}

func parseTimeframes(raw string) ([]string, error) {
	parts := strings.Split(raw, "/")
	out := make([]string, 0, len(parts))
	seen := make(map[string]bool)
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if seen[item] {
			continue
		}
		if _, err := timeframeMinutes(item); err != nil {
			return nil, err
		}
		seen[item] = true
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty timeframe list")
	}
	return out, nil
}

func parseTimeRange(raw string) (time.Time, time.Time, error) {
	start, end, _, err := parseTimeRangeWithLocation(raw, sourceParseLocation())
	return start, end, err
}

func parseTimeRangeWithLocation(raw string, location *time.Location) (time.Time, time.Time, bool, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, false, fmt.Errorf("invalid time range: %s", raw)
	}
	start, startUsesUTC, err := parseTimePointWithLocation(parts[0], location)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("invalid start time: %s", raw)
	}
	end, endUsesUTC, err := parseTimePointWithLocation(parts[1], location)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("invalid end time: %s", raw)
	}
	if startUsesUTC != endUsesUTC {
		return time.Time{}, time.Time{}, false, fmt.Errorf("time range timezone markers must be consistent: %s", raw)
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, false, fmt.Errorf("end time must be after start time: %s", raw)
	}
	return start.UTC(), end.UTC(), startUsesUTC, nil
}

func parseExchangeTimeRange(raw string, now time.Time) (time.Time, time.Time, bool, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, time.Time{}, false, false, fmt.Errorf("empty time range")
	}
	location := sourceParseLocation()
	if strings.Contains(raw, "-") {
		start, end, rangeUsesUTC, err := parseTimeRangeWithLocation(raw, location)
		if err != nil {
			return time.Time{}, time.Time{}, false, false, err
		}
		return start, end, false, rangeUsesUTC, nil
	}
	start, startUsesUTC, err := parseTimePointWithLocation(raw, location)
	if err != nil {
		return time.Time{}, time.Time{}, false, false, fmt.Errorf("invalid start time: %s", raw)
	}
	if now.IsZero() {
		now = time.Now()
	}
	end := now.In(location)
	if startUsesUTC {
		end = now.UTC()
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, false, false, fmt.Errorf("end time must be after start time: %s", raw)
	}
	return start.UTC(), end.UTC(), true, startUsesUTC, nil
}

func parseTimePoint(raw string) (time.Time, error) {
	point, _, err := parseTimePointWithLocation(raw, sourceParseLocation())
	return point, err
}

func parseTimePointWithLocation(raw string, location *time.Location) (time.Time, bool, error) {
	layouts := []string{"20060102_1504", "20060102"}
	location = normalizeParseLocation(location)
	value := strings.TrimSpace(raw)
	explicitUTC := strings.HasSuffix(value, "Z") || strings.HasSuffix(value, "z")
	if explicitUTC {
		value = strings.TrimSpace(value[:len(value)-1])
	}
	parseLocation := location
	if explicitUTC {
		parseLocation = time.UTC
	}
	var parseErr error
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, parseLocation)
		if err == nil {
			return parsed.UTC(), explicitUTC, nil
		}
		parseErr = err
	}
	return time.Time{}, false, parseErr
}

func detectTimeRangeUsesUTC(raw string) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, fmt.Errorf("empty time range")
	}
	if strings.Contains(raw, "-") {
		parts := strings.Split(raw, "-")
		if len(parts) != 2 {
			return false, fmt.Errorf("invalid time range: %s", raw)
		}
		_, startUsesUTC, err := parseTimePointWithLocation(parts[0], sourceParseLocation())
		if err != nil {
			return false, fmt.Errorf("invalid start time: %s", raw)
		}
		_, endUsesUTC, err := parseTimePointWithLocation(parts[1], sourceParseLocation())
		if err != nil {
			return false, fmt.Errorf("invalid end time: %s", raw)
		}
		if startUsesUTC != endUsesUTC {
			return false, fmt.Errorf("time range timezone markers must be consistent: %s", raw)
		}
		return startUsesUTC, nil
	}
	_, usesUTC, err := parseTimePointWithLocation(raw, sourceParseLocation())
	if err != nil {
		return false, fmt.Errorf("invalid start time: %s", raw)
	}
	return usesUTC, nil
}

func sourceParseLocation() *time.Location {
	return normalizeParseLocation(time.Local)
}

func normalizeParseLocation(location *time.Location) *time.Location {
	if location == nil {
		return time.UTC
	}
	return location
}

func normalizeSymbol(raw string) (string, string, error) {
	symbol, err := parseSymbol(raw)
	if err != nil {
		return "", "", err
	}
	token := normalizeSymbolToken(raw)
	return symbol, token, nil
}

func symbolFromToken(token string) (string, error) {
	return parseSymbol(token)
}

func parseSymbol(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty symbol")
	}
	lower := strings.ToLower(raw)
	var base, quote string
	switch {
	case strings.Contains(lower, "/"):
		parts := strings.Split(lower, "/")
		if len(parts) < 2 {
			return "", fmt.Errorf("invalid symbol: %s", raw)
		}
		base = parts[0]
		quote = parts[1]
	case strings.Contains(lower, "-"):
		parts := strings.Split(lower, "-")
		if len(parts) < 2 {
			return "", fmt.Errorf("invalid symbol: %s", raw)
		}
		base = parts[0]
		quote = parts[1]
	case strings.Contains(lower, "_"):
		parts := strings.Split(lower, "_")
		if len(parts) < 2 {
			return "", fmt.Errorf("invalid symbol: %s", raw)
		}
		base = parts[0]
		quote = parts[1]
	default:
		base, quote = splitByQuoteSuffix(lower)
		if base == "" || quote == "" {
			return "", fmt.Errorf("unsupported symbol: %s", raw)
		}
	}

	base = strings.TrimSpace(base)
	quote = normalizeQuote(strings.TrimSpace(quote))
	if base == "" || quote == "" {
		return "", fmt.Errorf("invalid symbol: %s", raw)
	}
	return strings.ToUpper(base) + "/" + strings.ToUpper(quote), nil
}

func normalizeQuote(quote string) string {
	for _, item := range knownQuotes {
		if quote == item {
			return quote
		}
		if strings.HasSuffix(quote, "p") && strings.TrimSuffix(quote, "p") == item {
			return item
		}
	}
	return quote
}

func splitCSVDisplayPath(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	file := filepath.Base(raw)
	if file == "." {
		file = ""
	}
	sepIndex := strings.LastIndexAny(raw, "/\\")
	if sepIndex < 0 {
		return "", file
	}
	dir := raw[:sepIndex]
	if sepIndex == 0 {
		dir = raw[:1]
	}
	if sepIndex == 1 && strings.HasPrefix(raw, "./") {
		dir = "./"
	}
	if sepIndex == 2 && strings.HasPrefix(raw, "../") {
		dir = "../"
	}
	return dir, file
}

func splitByQuoteSuffix(raw string) (string, string) {
	for _, quote := range knownQuotes {
		if strings.HasSuffix(raw, quote+"p") {
			base := strings.TrimSuffix(raw, quote+"p")
			return base, quote
		}
		if strings.HasSuffix(raw, quote) {
			base := strings.TrimSuffix(raw, quote)
			return base, quote
		}
	}
	return "", ""
}

func normalizeSymbolToken(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.ToLower(raw)
	raw = strings.ReplaceAll(raw, "/", "_")
	raw = strings.ReplaceAll(raw, "-", "_")
	return raw
}
