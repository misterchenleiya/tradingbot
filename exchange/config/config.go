package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/misterchenleiya/tradingbot/exchange/core"
)

type Config struct {
	Exchanges map[string]ExchangeConfig
}

type ExchangeConfig struct {
	Name        string
	APIKey      string
	SecretKey   string
	Passphrase  string
	RateLimitMS int
	Leverage    int
	MarginMode  string
	PosMode     string
	Simulated   bool
	Proxy       string
}

func LoadConfig(path string) (cfg Config, err error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	cfg = Config{Exchanges: make(map[string]ExchangeConfig)}
	scanner := bufio.NewScanner(file)
	section := ""
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := stripComment(scanner.Text())
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		key, rawValue, err := parseKeyValue(line)
		if err != nil {
			return Config{}, fmt.Errorf("config line %d: %w", lineNo, err)
		}
		value, err := parseValue(rawValue)
		if err != nil {
			return Config{}, fmt.Errorf("config line %d: %w", lineNo, err)
		}
		exchangeName, ok := exchangeSection(section)
		if !ok {
			continue
		}
		item := cfg.Exchanges[exchangeName]
		if item.Name == "" {
			item.Name = exchangeName
		}
		if err := applyExchangeField(&item, key, value); err != nil {
			return Config{}, fmt.Errorf("config line %d: %w", lineNo, err)
		}
		cfg.Exchanges[exchangeName] = item
	}
	if err := scanner.Err(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (cfg ExchangeConfig) Validate() error {
	name := strings.ToLower(strings.TrimSpace(cfg.Name))
	switch name {
	case "okx":
		if cfg.APIKey == "" {
			return fmt.Errorf("api_key is required")
		}
		if cfg.SecretKey == "" {
			return fmt.Errorf("secret_key is required")
		}
		if cfg.Passphrase == "" {
			return fmt.Errorf("passphrase is required")
		}
		return nil
	default:
		if name == "" {
			return fmt.Errorf("exchange name is required")
		}
		return nil
	}
}

func stripComment(line string) string {
	inSingle := false
	inDouble := false
	escaped := false
	for i, r := range line {
		switch r {
		case '\\':
			escaped = !escaped
		case '\'':
			if !escaped && !inDouble {
				inSingle = !inSingle
			}
			escaped = false
		case '"':
			if !escaped && !inSingle {
				inDouble = !inDouble
			}
			escaped = false
		case '#', ';':
			if !inSingle && !inDouble {
				return strings.TrimSpace(line[:i])
			}
			escaped = false
		default:
			escaped = false
		}
	}
	return strings.TrimSpace(line)
}

func parseKeyValue(line string) (string, string, error) {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid key/value: %s", line)
	}
	key := strings.TrimSpace(parts[0])
	if key == "" {
		return "", "", fmt.Errorf("empty key")
	}
	value := strings.TrimSpace(parts[1])
	if value == "" {
		return "", "", fmt.Errorf("empty value for key %s", key)
	}
	return key, value, nil
}

func parseValue(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty value")
	}
	if strings.HasPrefix(raw, "\"") && strings.HasSuffix(raw, "\"") && len(raw) >= 2 {
		return strconv.Unquote(raw)
	}
	if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") && len(raw) >= 2 {
		return raw[1 : len(raw)-1], nil
	}
	return raw, nil
}

func exchangeSection(section string) (string, bool) {
	section = strings.TrimSpace(section)
	if section == "" {
		return "", false
	}
	section = strings.ToLower(section)
	parts := strings.Split(section, ".")
	if len(parts) != 2 {
		return "", false
	}
	if parts[0] != "exchanges" {
		return "", false
	}
	name := strings.TrimSpace(parts[1])
	if name == "" {
		return "", false
	}
	return name, true
}

func normalizeKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	key = strings.ReplaceAll(key, "_", "")
	key = strings.ReplaceAll(key, "-", "")
	return key
}

func applyExchangeField(item *ExchangeConfig, key, value string) error {
	normalized := normalizeKey(key)
	switch normalized {
	case "name":
		item.Name = value
	case "apikey":
		item.APIKey = value
	case "secretkey":
		item.SecretKey = value
	case "passphrase":
		item.Passphrase = value
	case "ratelimit", "ratelimitms":
		rateLimit, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("invalid rate_limit: %s", value)
		}
		if rateLimit < 0 {
			return fmt.Errorf("invalid rate_limit: %s", value)
		}
		item.RateLimitMS = rateLimit
	case "leverage":
		lever, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid leverage: %s", value)
		}
		item.Leverage = core.NormalizeLeverage(lever)
	case "marginmode":
		item.MarginMode = strings.ToLower(strings.TrimSpace(value))
	case "posmode", "positionmode", "tsmode":
		item.PosMode = strings.ToLower(strings.TrimSpace(value))
	case "simulated", "simulatedtrading", "demo", "demotrading":
		enabled, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("invalid simulated: %s", value)
		}
		item.Simulated = enabled
	case "proxy", "socks5proxy":
		item.Proxy = strings.TrimSpace(value)
	default:
		return fmt.Errorf("unsupported key: %s", key)
	}
	return nil
}
