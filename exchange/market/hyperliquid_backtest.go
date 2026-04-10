package market

import (
	"strings"

	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

func normalizeHyperliquidBackTestSource(logger *zap.Logger, source *BackTestSource) {
	if source == nil || !strings.EqualFold(source.Exchange, "hyperliquid") {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	normalized, _, _, replaced, err := hyperliquidNormalizeSymbol(source.Symbol)
	if err != nil {
		logger.Warn("hyperliquid back-test symbol normalize failed",
			zap.String("symbol", source.Symbol),
			zap.Error(err),
		)
		return
	}
	if replaced {
		logger.Warn("hyperliquid back-test symbol uses USDT, replaced with USDC",
			zap.String("symbol", source.Symbol),
			zap.String("normalized", normalized),
		)
	}
	if normalized != source.Symbol {
		source.Symbol = normalized
		source.SymbolToken = normalizeSymbolToken(normalized)
	}
	for i := range source.Files {
		if !strings.EqualFold(source.Files[i].Exchange, "hyperliquid") {
			continue
		}
		source.Files[i].Symbol = normalized
		source.Files[i].SymbolToken = normalizeSymbolToken(normalized)
	}
}
