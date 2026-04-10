package common

import (
	"sort"
	"strings"
	"time"
)

const QuoteFamilyUSDStable = "USD_STABLE"

var stableQuoteFamilies = map[string]string{
	"USDT": QuoteFamilyUSDStable,
	"USDC": QuoteFamilyUSDStable,
	"BUSD": QuoteFamilyUSDStable,
	"USD":  QuoteFamilyUSDStable,
}

var canonicalQuotes = []string{"USDT", "USDC", "BUSD", "USD", "BTC", "ETH"}

// CanonicalSymbol normalizes common market symbol forms like BTCUSDT, BTC-USDT-SWAP,
// BTC/USDT.P into a stable BASE/QUOTE shape.
func CanonicalSymbol(symbol string) string {
	text := strings.ToUpper(strings.TrimSpace(symbol))
	text = strings.TrimSuffix(text, ".P")
	if idx := strings.Index(text, ":"); idx > 0 {
		text = text[:idx]
	}
	if strings.HasSuffix(text, "-SWAP") {
		parts := strings.Split(text, "-")
		if len(parts) >= 3 {
			text = parts[0] + "/" + parts[1]
		}
	}
	if compact, ok := splitCompactSymbol(text); ok {
		return compact
	}
	return text
}

func SymbolBaseQuote(symbol string) (string, string, bool) {
	text := CanonicalSymbol(symbol)
	if text == "" {
		return "", "", false
	}
	parts := strings.Split(text, "/")
	if len(parts) >= 2 {
		base := strings.TrimSpace(parts[0])
		quote := strings.TrimSpace(parts[1])
		if base != "" && quote != "" {
			return base, quote, true
		}
	}
	if compact, ok := splitCompactSymbol(text); ok {
		parts = strings.Split(compact, "/")
		if len(parts) == 2 {
			return parts[0], parts[1], true
		}
	}
	return "", "", false
}

func QuoteFamily(quote string) string {
	quote = strings.ToUpper(strings.TrimSpace(quote))
	if quote == "" {
		return ""
	}
	if family, ok := stableQuoteFamilies[quote]; ok {
		return family
	}
	return quote
}

func ExposureKey(symbol string) string {
	base, quote, ok := SymbolBaseQuote(symbol)
	if !ok {
		return CanonicalSymbol(symbol)
	}
	family := QuoteFamily(quote)
	if family == "" {
		return base
	}
	return base + "|" + family
}

func NormalizeStrategyIdentity(primary string, timeframes []string, comboKey string) (string, []string, string) {
	out := make([]string, 0, len(timeframes)+4)
	appendTimeframe := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return
		}
		out = append(out, value)
	}
	appendTimeframe(primary)
	for _, item := range timeframes {
		appendTimeframe(item)
	}
	for _, item := range strings.Split(strings.TrimSpace(comboKey), "/") {
		appendTimeframe(item)
	}
	if len(out) == 0 {
		return "", nil, ""
	}
	seen := make(map[string]struct{}, len(out))
	normalized := make([]string, 0, len(out))
	for _, item := range out {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	sort.SliceStable(normalized, func(i, j int) bool {
		leftDur, leftOK := timeframeDuration(normalized[i])
		rightDur, rightOK := timeframeDuration(normalized[j])
		switch {
		case leftOK && rightOK:
			if leftDur != rightDur {
				return leftDur < rightDur
			}
		case leftOK:
			return true
		case rightOK:
			return false
		}
		return normalized[i] < normalized[j]
	})
	resolvedPrimary := normalized[len(normalized)-1]
	return resolvedPrimary, normalized, strings.Join(normalized, "/")
}

func splitCompactSymbol(raw string) (string, bool) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	if raw == "" || strings.Contains(raw, "/") || strings.Contains(raw, "-") {
		return "", false
	}
	for _, quote := range canonicalQuotes {
		if !strings.HasSuffix(raw, quote) || len(raw) <= len(quote) {
			continue
		}
		base := strings.TrimSpace(raw[:len(raw)-len(quote)])
		if base == "" {
			continue
		}
		return base + "/" + quote, true
	}
	return "", false
}

func timeframeDuration(timeframe string) (time.Duration, bool) {
	switch strings.ToLower(strings.TrimSpace(timeframe)) {
	case "1m":
		return time.Minute, true
	case "3m":
		return 3 * time.Minute, true
	case "5m":
		return 5 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "1h":
		return time.Hour, true
	case "2h":
		return 2 * time.Hour, true
	case "4h":
		return 4 * time.Hour, true
	case "6h":
		return 6 * time.Hour, true
	case "8h":
		return 8 * time.Hour, true
	case "12h":
		return 12 * time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}
