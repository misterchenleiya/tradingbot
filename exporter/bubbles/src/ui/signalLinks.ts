import type { BubbleDatum } from "../app/types";

const PREFERRED_EXCHANGES = ["binance", "okx", "bitget"] as const;
const KNOWN_QUOTES = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "GBP", "JPY"] as const;

type ParsedSymbol = {
  raw: string;
  base: string;
  quote: string;
};

type TimeframeParts = {
  amount: number;
  unit: "m" | "h" | "d" | "w";
};

export type SignalLinkTone = "tradingview" | "binance" | "okx" | "bitget" | "generic";

export type SignalLinkItem = {
  key: string;
  label: string;
  url: string;
  iconText: string;
  iconTone: SignalLinkTone;
};

export type VisualHistoryLinkParams = {
  enabled?: boolean;
  date?: string;
  exchange?: string;
  symbol?: string;
  strategy?: string;
  version?: string;
};

type BuildSignalLinkOptions = {
  visualHistory?: VisualHistoryLinkParams;
};

function normalizeExchange(exchange: string): string {
  return exchange.trim().toLowerCase();
}

function splitExchangeTokens(exchange?: string): string[] {
  const normalized = normalizeExchange(exchange || "");
  if (!normalized) return [];
  return normalized
    .split("/")
    .map((item) => item.trim().toLowerCase())
    .filter((item) => item.length > 0);
}

export function sortExchanges(exchanges: string[]): string[] {
  const normalized = exchanges
    .map((item) => normalizeExchange(item))
    .filter((item) => item.length > 0);
  const unique = Array.from(new Set(normalized));
  const preferred = PREFERRED_EXCHANGES.filter((item) => unique.includes(item));
  const rest = unique
    .filter((item) => !PREFERRED_EXCHANGES.includes(item as (typeof PREFERRED_EXCHANGES)[number]))
    .sort((a, b) => a.localeCompare(b));
  return [...preferred, ...rest];
}

function parseSymbol(rawSymbol: string): ParsedSymbol {
  const raw = rawSymbol.trim().toUpperCase();
  if (!raw) {
    return { raw: "", base: "", quote: "USDT" };
  }

  const slashParts = raw.split("/");
  if (slashParts.length >= 2) {
    return {
      raw,
      base: slashParts[0].trim(),
      quote: slashParts[1].trim()
    };
  }

  const dashParts = raw.split("-");
  if (dashParts.length >= 2) {
    return {
      raw,
      base: dashParts[0].trim(),
      quote: dashParts[1].trim()
    };
  }

  const matchedQuote = KNOWN_QUOTES.find((quote) => raw.endsWith(quote) && raw.length > quote.length);
  if (matchedQuote) {
    return {
      raw,
      base: raw.slice(0, raw.length - matchedQuote.length),
      quote: matchedQuote
    };
  }

  return {
    raw,
    base: raw,
    quote: "USDT"
  };
}

function parseTimeframe(value?: string): TimeframeParts | null {
  if (!value) return null;
  const trimmed = value.trim();
  const match = /^(\d+)([mhdw])$/i.exec(trimmed);
  if (!match) return null;
  const amount = Number(match[1]);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return {
    amount,
    unit: match[2].toLowerCase() as TimeframeParts["unit"]
  };
}

function toTradingViewInterval(timeframe?: string): string | undefined {
  const parsed = parseTimeframe(timeframe);
  if (!parsed) return undefined;
  if (parsed.unit === "m") return String(parsed.amount);
  if (parsed.unit === "h") return String(parsed.amount * 60);
  if (parsed.unit === "d") return `${parsed.amount}D`;
  if (parsed.unit === "w") return `${parsed.amount}W`;
  return undefined;
}

function toOkxInterval(timeframe?: string): string | undefined {
  const parsed = parseTimeframe(timeframe);
  if (!parsed) return undefined;
  if (parsed.unit === "m") return `${parsed.amount}m`;
  if (parsed.unit === "h") return `${parsed.amount}H`;
  if (parsed.unit === "d") return `${parsed.amount}D`;
  if (parsed.unit === "w") return `${parsed.amount}W`;
  return undefined;
}

function toTradingViewExchange(exchange: string | undefined): string {
  const value = normalizeExchange(exchange || "");
  if (value === "okx") return "OKX";
  if (value === "bitget") return "BITGET";
  if (value === "binance") return "BINANCE";
  return "BINANCE";
}

function buildTradingViewUrl(exchange: string | undefined, symbol: ParsedSymbol, timeframe?: string): string {
  const tvExchange = toTradingViewExchange(exchange);
  const ticker = `${symbol.base}${symbol.quote}`;
  const tvSymbol = `${tvExchange}:${ticker}.P`;
  const url = new URL("https://www.tradingview.com/chart/");
  url.searchParams.set("symbol", tvSymbol);
  const interval = toTradingViewInterval(timeframe);
  if (interval) {
    url.searchParams.set("interval", interval);
  }
  return url.toString();
}

function buildExchangeChartUrl(exchange: string, symbol: ParsedSymbol, timeframe?: string): string {
  const key = normalizeExchange(exchange);
  if (key === "binance") {
    const url = new URL(`https://www.binance.com/en/futures/${encodeURIComponent(`${symbol.base}${symbol.quote}`)}`);
    url.searchParams.set("type", "perpetual");
    return url.toString();
  }
  if (key === "okx") {
    const url = new URL(`https://www.okx.com/trade-swap/${encodeURIComponent(`${symbol.base}-${symbol.quote}-swap`)}`);
    const interval = toOkxInterval(timeframe);
    if (interval) {
      url.searchParams.set("t", interval);
      url.searchParams.set("interval", interval);
      url.searchParams.set("period", interval);
    }
    return url.toString();
  }
  if (key === "bitget") {
    return `https://www.bitget.com/en/futures/usdt/${encodeURIComponent(`${symbol.base}${symbol.quote}`)}`;
  }
  if (key === "bybit") {
    return `https://www.bybit.com/trade/usdt/${encodeURIComponent(`${symbol.base}${symbol.quote}`)}`;
  }
  if (key === "mexc") {
    return `https://futures.mexc.com/exchange/${encodeURIComponent(`${symbol.base}_${symbol.quote}`)}?type=linear_swap`;
  }
  if (key === "kucoin") {
    return `https://www.kucoin.com/futures/trade/${encodeURIComponent(`${symbol.base}-${symbol.quote}`)}`;
  }
  return `https://www.google.com/search?q=${encodeURIComponent(`${exchange} ${symbol.raw} kline`)}`;
}

function iconToneByExchange(exchange: string): SignalLinkTone {
  const key = normalizeExchange(exchange);
  if (key === "binance") return "binance";
  if (key === "okx") return "okx";
  if (key === "bitget") return "bitget";
  return "generic";
}

function iconTextByExchange(exchange: string): string {
  const key = normalizeExchange(exchange);
  if (key === "binance") return "BN";
  if (key === "okx") return "OKX";
  if (key === "bitget") return "BG";
  return exchange.trim().slice(0, 2).toUpperCase() || "EX";
}

function parsePositionTimeMs(value?: string): number | undefined {
  if (!value) return undefined;
  const raw = value.trim();
  if (!raw) return undefined;

  if (/^\d{10,13}$/.test(raw)) {
    const ts = Number(raw);
    if (Number.isFinite(ts)) {
      return raw.length <= 10 ? ts * 1000 : ts;
    }
  }

  const utcMatch = raw.match(
    /^(\d{4})[/-](\d{2})[/-](\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (utcMatch) {
    const [, yyyy, mm, dd, hh = "0", mi = "0", ss = "0"] = utcMatch;
    const ts = Date.UTC(
      Number(yyyy),
      Number(mm) - 1,
      Number(dd),
      Number(hh),
      Number(mi),
      Number(ss)
    );
    if (Number.isFinite(ts)) return ts;
  }

  const direct = Date.parse(raw);
  if (Number.isFinite(direct)) return direct;

  const normalized = raw.replace(" ", "T");
  const parsed = Date.parse(normalized);
  if (Number.isFinite(parsed)) return parsed;

  return undefined;
}

function normalizeDateInput(value?: string): string | undefined {
  const raw = (value || "").trim();
  if (!raw) return undefined;
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : undefined;
}

function toDateInputByLocalTime(ms: number): string {
  const date = new Date(ms);
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

export function resolveVisualHistoryDateFromEntryTime(entryTime?: string): string | undefined {
  const entryMs = parsePositionTimeMs(entryTime);
  if (typeof entryMs !== "number" || !Number.isFinite(entryMs)) return undefined;
  return toDateInputByLocalTime(entryMs);
}

export function buildVisualHistoryUrl(params?: VisualHistoryLinkParams): string {
  const search = new URLSearchParams();
  const date = normalizeDateInput(params?.date);
  const exchange = (params?.exchange || "").trim();
  const symbol = (params?.symbol || "").trim();
  const strategy = (params?.strategy || "").trim();
  const version = (params?.version || "").trim();

  if (date) search.set("date", date);
  if (exchange) search.set("exchange", exchange);
  if (symbol) search.set("symbol", symbol);
  if (strategy) search.set("strategy", strategy);
  if (version) search.set("version", version);

  const query = search.toString();
  if (!query) return "/visual-history/";
  return `/visual-history/?${query}`;
}

export function resolveSymbolExchanges(selected: Pick<BubbleDatum, "symbol" | "exchange">, allDataList: BubbleDatum[]): string[] {
  const exchanges = allDataList
    .filter((item) => item.symbol === selected.symbol)
    .flatMap((item) => splitExchangeTokens(item.exchange));
  exchanges.push(...splitExchangeTokens(selected.exchange));
  return sortExchanges(exchanges);
}

export function buildSignalLinkItems(
  selected: Pick<BubbleDatum, "symbol" | "exchange" | "timeframe">,
  allDataList: BubbleDatum[],
  options?: BuildSignalLinkOptions
): SignalLinkItem[] {
  const exchanges = resolveSymbolExchanges(selected, allDataList);
  const parsedSymbol = parseSymbol(selected.symbol);
  const timeframe = selected.timeframe;
  const visualHistory = options?.visualHistory;

  const tradingView: SignalLinkItem = {
    key: "tradingview",
    label: "TradingView",
    url: buildTradingViewUrl(exchanges[0], parsedSymbol, timeframe),
    iconText: "TV",
    iconTone: "tradingview"
  };
  const exchangeLinks = exchanges.map((exchange) => ({
    key: exchange,
    label: exchange,
    url: buildExchangeChartUrl(exchange, parsedSymbol, timeframe),
    iconText: iconTextByExchange(exchange),
    iconTone: iconToneByExchange(exchange)
  }));
  const links: SignalLinkItem[] = [];
  if (visualHistory?.enabled) {
    links.push({
      key: "visual-history",
      label: "VisualHistory",
      url: buildVisualHistoryUrl(visualHistory),
      iconText: "VH",
      iconTone: "generic"
    });
  }

  links.push(tradingView, ...exchangeLinks);
  return links;
}
