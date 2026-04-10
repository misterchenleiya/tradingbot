package market

import (
	"strings"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

func normalizeHyperliquidSymbols(logger *zap.Logger, store iface.SymbolStore, symbols []models.Symbol) []models.Symbol {
	if len(symbols) == 0 {
		return symbols
	}
	if logger == nil {
		logger = glog.Nop()
	}
	existing := make(map[string]bool, len(symbols))
	for _, sym := range symbols {
		key := strings.ToLower(sym.Exchange + "|" + sym.Symbol)
		existing[key] = true
	}

	out := make([]models.Symbol, 0, len(symbols))
	for _, sym := range symbols {
		if !strings.EqualFold(sym.Exchange, "hyperliquid") {
			out = append(out, sym)
			continue
		}
		normalized, coin, quote, replaced, err := hyperliquidNormalizeSymbol(sym.Symbol)
		if err != nil {
			logger.Warn("hyperliquid symbol normalize failed",
				zap.String("symbol", sym.Symbol),
				zap.Error(err),
			)
			out = append(out, sym)
			continue
		}
		oldSymbol := sym.Symbol
		sym.Symbol = normalized
		if coin != "" {
			sym.Base = coin
		}
		if quote != "" {
			sym.Quote = quote
		}
		if replaced {
			logger.Warn("hyperliquid symbol uses USDT, replaced with USDC",
				zap.String("symbol", oldSymbol),
				zap.String("normalized", normalized),
			)
		}
		if store != nil && !strings.EqualFold(oldSymbol, normalized) {
			if err := store.UpsertSymbol(sym); err != nil {
				logger.Warn("hyperliquid symbol upsert failed",
					zap.String("symbol", normalized),
					zap.Error(err),
				)
			} else if strings.TrimSpace(oldSymbol) != "" {
				if err := store.UpdateSymbolActive(sym.Exchange, oldSymbol, false); err != nil {
					logger.Warn("hyperliquid symbol deactivate failed",
						zap.String("symbol", oldSymbol),
						zap.Error(err),
					)
				}
			}
		}
		key := strings.ToLower(sym.Exchange + "|" + sym.Symbol)
		if oldSymbol != sym.Symbol && existing[key] {
			continue
		}
		out = append(out, sym)
	}
	return out
}
