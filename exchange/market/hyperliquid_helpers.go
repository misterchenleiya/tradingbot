package market

import (
	"errors"
	"fmt"
	"strings"
)

func hyperliquidNormalizeCoin(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", errors.New("symbol is required")
	}
	if strings.Contains(text, "/") {
		parts := strings.SplitN(text, "/", 2)
		text = parts[0]
	}
	text = strings.TrimSuffix(text, ".P")
	text = strings.TrimSuffix(text, ".p")
	text = strings.TrimSuffix(text, "-SWAP")
	text = strings.TrimSuffix(text, "-swap")
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("invalid symbol: %s", raw)
	}
	if strings.Contains(text, ":") {
		parts := strings.SplitN(text, ":", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return "", fmt.Errorf("invalid symbol format: %s", raw)
		}
		return fmt.Sprintf("%s:%s",
			strings.ToLower(strings.TrimSpace(parts[0])),
			strings.ToUpper(strings.TrimSpace(parts[1])),
		), nil
	}
	return strings.ToUpper(text), nil
}

func hyperliquidNormalizeSymbol(raw string) (string, string, string, bool, error) {
	base, quote := hyperliquidSplitSymbol(raw)
	if strings.TrimSpace(base) == "" {
		return "", "", "", false, fmt.Errorf("empty symbol")
	}
	coin, err := hyperliquidNormalizeCoin(base)
	if err != nil {
		return "", "", "", false, err
	}
	quote = hyperliquidNormalizeQuote(quote)
	if quote == "" {
		quote = "USDC"
	}
	replaced := strings.EqualFold(quote, "USDT")
	if replaced {
		quote = "USDC"
	}
	return coin + "/" + strings.ToUpper(quote), coin, strings.ToUpper(quote), replaced, nil
}

func hyperliquidCoinFromSymbol(raw string) (string, error) {
	base, _ := hyperliquidSplitSymbol(raw)
	if strings.TrimSpace(base) == "" {
		return "", fmt.Errorf("empty symbol")
	}
	return hyperliquidNormalizeCoin(base)
}

func hyperliquidSymbolFromCoin(raw string) string {
	coin, err := hyperliquidNormalizeCoin(raw)
	if err != nil {
		return raw
	}
	return coin + "/USDC"
}

func hyperliquidNormalizeQuote(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}
	text = strings.TrimSuffix(text, ".P")
	text = strings.TrimSuffix(text, ".p")
	text = strings.TrimSuffix(text, "-SWAP")
	text = strings.TrimSuffix(text, "-swap")
	lower := strings.ToLower(text)
	lower = normalizeQuote(lower)
	if lower == "" {
		return ""
	}
	return strings.ToUpper(lower)
}

func hyperliquidSplitSymbol(raw string) (string, string) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", ""
	}
	clean := strings.TrimSuffix(strings.TrimSuffix(text, "-SWAP"), "-swap")
	clean = strings.TrimSuffix(strings.TrimSuffix(clean, ".P"), ".p")
	if strings.Contains(clean, "/") {
		parts := strings.SplitN(clean, "/", 2)
		return parts[0], parts[1]
	}
	if strings.Contains(clean, "-") {
		parts := strings.Split(clean, "-")
		if len(parts) >= 2 {
			return parts[0], parts[1]
		}
	}
	if strings.Contains(clean, "_") {
		parts := strings.Split(clean, "_")
		if len(parts) >= 2 {
			return parts[0], parts[1]
		}
	}
	lower := strings.ToLower(clean)
	base, quote := splitByQuoteSuffix(lower)
	if base != "" && quote != "" {
		return base, quote
	}
	return clean, ""
}

func hyperliquidInterval(tf string) (string, error) {
	switch tf {
	case "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d":
		return tf, nil
	default:
		return "", fmt.Errorf("unsupported timeframe for hyperliquid: %s", tf)
	}
}

func hyperliquidStreamKey(coin, interval string) string {
	return strings.ToLower(coin) + "|" + strings.ToLower(interval)
}
