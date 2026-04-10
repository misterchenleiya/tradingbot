import type {
  AccountSnapshot,
  BackendStatus,
  BubbleCandleBar,
  BubbleCandleEvent,
  BubbleCandlePosition,
  BubbleCandlesSnapshot,
  BubbleDatum,
  BubbleUpdate,
  PositionItem,
  PositionSnapshot,
  SignalOHLCVBar,
  TrendGroupCandidate,
  TrendGroupItem,
  TrendGroupsSnapshot,
  WsPongStatus
} from "../../app/types";
import { actionWeight, sideToTrendType } from "../../app/trend";

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function pickNumber(source: UnknownRecord, keys: string[], fallback = 0): number {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "number" && !Number.isNaN(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim() !== "") {
      const parsed = Number(value);
      if (!Number.isNaN(parsed)) return parsed;
    }
  }
  return fallback;
}

function pickString(source: UnknownRecord, keys: string[], fallback = ""): string {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string") {
      return value;
    }
  }
  return fallback;
}

function pickOptionalNumber(source: UnknownRecord, keys: string[]): number | undefined {
  const value = pickNumber(source, keys, NaN);
  if (Number.isNaN(value)) {
    return undefined;
  }
  return value;
}

function pickOptionalString(source: UnknownRecord, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) return trimmed;
    }
  }
  return undefined;
}

function hasAnyValue(source: UnknownRecord, keys: string[]): boolean {
  return keys.some((key) => source[key] !== undefined && source[key] !== null && source[key] !== "");
}

function unwrapRecordCandidates(payload: UnknownRecord): UnknownRecord[] {
  const out: UnknownRecord[] = [payload];
  for (const key of ["data", "result", "payload"]) {
    const value = payload[key];
    if (isRecord(value)) {
      out.push(value);
    }
  }
  return out;
}

function looksLikePositionRecord(source: UnknownRecord): boolean {
  const symbol = pickString(source, ["symbol", "instId", "inst_id", "instrument", "pair", "code"], "").trim();
  if (!symbol) return false;
  return hasAnyValue(source, [
    "position_side",
    "positionSide",
    "posSide",
    "side",
    "direction",
    "entry_quantity",
    "entryQuantity",
    "position_size",
    "positionSize",
    "size",
    "qty",
    "current_price",
    "currentPrice",
    "mark_price",
    "markPrice",
    "status",
    "position_status",
    "positionStatus",
    "strategy_name",
    "strategyName"
  ]);
}

function extractPositionRows(source: UnknownRecord): unknown[] | null {
  for (const key of ["positions", "items", "list", "rows", "data", "position_list", "positionList", "position"]) {
    const value = source[key];
    if (Array.isArray(value)) return value;
    if (isRecord(value) && looksLikePositionRecord(value)) return [value];
    if (isRecord(value)) {
      const values = Object.values(value).filter((item) => isRecord(item) && looksLikePositionRecord(item));
      if (values.length > 0) return values;
    }
  }
  if (looksLikePositionRecord(source)) return [source];
  return null;
}

function buildPositionSnapshot(positionsRaw: unknown[] | null, countRaw?: number): PositionSnapshot | null {
  if (!positionsRaw && typeof countRaw !== "number") return null;
  const positions = (positionsRaw || [])
    .map((item) => normalizePositionItem(item))
    .filter((item): item is PositionItem => item !== null);
  const count = typeof countRaw === "number" ? Math.max(0, Math.trunc(countRaw)) : positions.length;
  return {
    count,
    positions,
    fetchedAt: Date.now()
  };
}

function normalizeModules(modulesPayload: unknown): BackendStatus["modules"] {
  if (!isRecord(modulesPayload)) return [];
  const modules: BackendStatus["modules"] = [];
  for (const [name, value] of Object.entries(modulesPayload)) {
    if (!isRecord(value)) continue;
    const state = pickString(value, ["state"], "");
    if (!state) continue;
    const updatedAt = pickString(value, ["updated_at", "updatedAt"], "");
    modules.push({
      name: pickString(value, ["name"], name),
      state,
      updatedAt: updatedAt || undefined
    });
  }
  return modules;
}

function normalizeItem(raw: UnknownRecord, rankFallback: number): BubbleDatum | null {
  if (looksLikeSignal(raw)) {
    return normalizeSignalItem(raw, rankFallback);
  }
  return normalizeMarketItem(raw, rankFallback);
}

function normalizeMarketItem(raw: UnknownRecord, rankFallback: number): BubbleDatum | null {
  const id = pickString(raw, ["id", "symbol", "slug", "code"], "");
  const symbol = pickString(raw, ["symbol", "ticker", "code"], "").toUpperCase();
  const name = pickString(raw, ["name", "fullName", "title"], symbol || id);
  if (!id || !symbol) {
    return null;
  }

  const marketCap = pickNumber(raw, ["marketCap", "market_cap", "market_cap_usd", "cap"], 0);
  const volume24h = pickNumber(raw, ["volume24h", "total_volume", "volume_24h"], 0);
  const price = pickNumber(raw, ["price", "current_price", "price_usd"], 0);
  const change24h = pickNumber(raw, ["change24h", "price_change_percentage_24h", "percent_change_24h"], 0);
  const change7d = pickNumber(raw, ["change7d", "price_change_percentage_7d", "percent_change_7d"], 0);
  const rank = pickNumber(raw, ["rank", "market_cap_rank"], rankFallback);
  const logoUrl = pickString(raw, ["logo", "logoUrl", "image", "icon"], "");

  return {
    id,
    symbol,
    name,
    marketCap,
    volume24h,
    price,
    change24h,
    change7d,
    rank,
    logoUrl: logoUrl || undefined
  };
}

function looksLikeSignal(raw: UnknownRecord): boolean {
  const hasSignalKey =
    typeof raw.side === "number" ||
    typeof raw.high_side === "number" ||
    typeof raw.mid_side === "number" ||
    typeof raw.action === "number" ||
    typeof raw.trending_timestamp === "number" ||
    typeof raw.trigger_timestamp === "number";
  const hasMeta =
    typeof raw.exchange === "string" || typeof raw.timeframe === "string" || typeof raw.strategy === "string";
  return hasSignalKey || hasMeta;
}

function isSignalSide(value: number): boolean {
  return (
    value === 0 ||
    value === 1 ||
    value === 8 ||
    value === -1 ||
    value === -8 ||
    value === 255 ||
    value === -255
  );
}

function normalizeSignalSide(raw: UnknownRecord): number {
  const side = Math.trunc(pickNumber(raw, ["side"], NaN));
  if (!Number.isNaN(side) && isSignalSide(side)) {
    return side;
  }

  const highSide = Math.trunc(pickNumber(raw, ["high_side", "highSide"], NaN));
  const midSide = Math.trunc(pickNumber(raw, ["mid_side", "midSide"], NaN));
  const hasHighSide = !Number.isNaN(highSide) && isSignalSide(highSide);
  const hasMidSide = !Number.isNaN(midSide) && isSignalSide(midSide);

  // 优先使用 mid_side 捕捉 8/-8 回调态；若 mid_side 为 0，则回退到 high_side。
  if (hasMidSide && midSide !== 0) {
    return midSide;
  }
  if (hasHighSide) {
    return highSide;
  }
  if (hasMidSide) {
    return midSide;
  }

  return 0;
}

function buildSignalName(exchange: string, strategy: string): string {
  if (exchange && strategy) return `${exchange.toUpperCase()} · ${strategy}`;
  return exchange ? exchange.toUpperCase() : strategy || "Signal";
}

export function buildSignalId(exchange: string, symbol: string, timeframe: string, strategy: string): string {
  const exchangePart = exchange.trim().toLowerCase();
  const symbolPart = symbol.trim().toUpperCase();
  const strategyPart = strategy.trim().toLowerCase();
  const comboPart = normalizeSignalComboKeyValue(timeframe, undefined, undefined);
  return `${exchangePart}|${symbolPart}|${strategyPart}|${comboPart}`;
}

function buildSignalIdWithCombo(
  exchange: string,
  symbol: string,
  strategy: string,
  comboKey?: string,
  timeframe?: string,
  strategyTimeframes?: string[]
): string {
  const exchangePart = exchange.trim().toLowerCase();
  const symbolPart = symbol.trim().toUpperCase();
  const strategyPart = strategy.trim().toLowerCase();
  const comboPart = normalizeSignalComboKeyValue(timeframe, strategyTimeframes, comboKey);
  return `${exchangePart}|${symbolPart}|${strategyPart}|${comboPart}`;
}

function normalizeSignalOHLCVBar(payload: unknown): SignalOHLCVBar | null {
  if (!isRecord(payload)) return null;

  const open = pickNumber(payload, ["open", "Open", "o"], NaN);
  const high = pickNumber(payload, ["high", "High", "h"], NaN);
  const low = pickNumber(payload, ["low", "Low", "l"], NaN);
  const close = pickNumber(payload, ["close", "Close", "c"], NaN);
  const volume = pickNumber(payload, ["volume", "Volume", "v"], 0);
  if (
    Number.isNaN(open) ||
    Number.isNaN(high) ||
    Number.isNaN(low) ||
    Number.isNaN(close)
  ) {
    return null;
  }

  const ts = pickNumber(payload, ["ts", "TS", "timestamp", "time"], 0);
  return {
    ts: Number.isFinite(ts) ? Math.trunc(ts) : 0,
    open,
    high,
    low,
    close,
    volume: Number.isFinite(volume) ? volume : 0
  };
}

function normalizeSignalOHLCV(raw: UnknownRecord): SignalOHLCVBar[] | undefined {
  const source = raw.ohlcv ?? raw.OHLCV;
  if (!Array.isArray(source)) return undefined;

  const out: SignalOHLCVBar[] = [];
  for (const item of source) {
    const bar = normalizeSignalOHLCVBar(item);
    if (bar) {
      out.push(bar);
    }
  }
  return out.length > 0 ? out : undefined;
}

function normalizeSignalStrategyTimeframes(raw: UnknownRecord): string[] | undefined {
  const source = raw.strategy_timeframes ?? raw.strategyTimeframes;
  let values: unknown[] = [];

  if (Array.isArray(source)) {
    values = source;
  } else if (typeof source === "string") {
    const trimmed = source.trim();
    if (!trimmed) return undefined;
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          values = parsed;
        }
      } catch {
        // Ignore JSON parse errors and fallback to delimiter split below.
      }
    }
    if (values.length === 0) {
      values = trimmed.split(/[,\s]+/);
    }
  }

  if (values.length === 0) return undefined;
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of values) {
    if (typeof item !== "string") continue;
    const normalized = item.trim().toLowerCase();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out.length > 0 ? out : undefined;
}

function normalizeSignalComboKeyValue(
  timeframe: string | undefined,
  strategyTimeframes: string[] | undefined,
  rawComboKey: string | undefined
): string {
  const values: string[] = [];
  const pushValue = (value: string | undefined) => {
    if (!value) return;
    const normalized = value.trim().toLowerCase();
    if (!normalized) return;
    values.push(normalized);
  };
  pushValue(timeframe);
  if (strategyTimeframes) {
    for (const item of strategyTimeframes) {
      pushValue(item);
    }
  }
  if (rawComboKey) {
    for (const item of rawComboKey.split("/")) {
      pushValue(item);
    }
  }
  if (values.length === 0) return "";
  const unique = Array.from(new Set(values));
  unique.sort((left, right) => timeframeSortWeight(left) - timeframeSortWeight(right) || left.localeCompare(right));
  return unique.join("/");
}

function timeframeSortWeight(value: string): number {
  switch (value.trim().toLowerCase()) {
    case "1m":
      return 1;
    case "3m":
      return 3;
    case "5m":
      return 5;
    case "15m":
      return 15;
    case "30m":
      return 30;
    case "1h":
      return 60;
    case "2h":
      return 120;
    case "4h":
      return 240;
    case "6h":
      return 360;
    case "8h":
      return 480;
    case "12h":
      return 720;
    case "1d":
      return 1440;
    default:
      return Number.MAX_SAFE_INTEGER;
  }
}

function sideScore(side: number): number {
  switch (side) {
    case 1:
      return 18;
    case 8:
      return 9;
    case -1:
      return -18;
    case -8:
      return -9;
    case 255:
    case -255:
      return 0;
    default:
      return 0;
  }
}

function actionScore(action: number): number {
  const weight = actionWeight(action);
  if (weight === 0) return 0;
  if ((action & 64) !== 0) return -5 - weight;
  return 5 + weight;
}

function normalizeSignalItem(raw: UnknownRecord, rankFallback: number): BubbleDatum | null {
  const exchange = pickString(raw, ["exchange"], "");
  const symbolRaw = pickString(raw, ["symbol", "ticker", "code"], "");
  const timeframe = pickString(raw, ["timeframe"], "");
  const strategy = pickString(raw, ["strategy"], "default");
  if (!symbolRaw || !timeframe) return null;
  const strategyTimeframes = normalizeSignalStrategyTimeframes(raw);
  const comboKey = normalizeSignalComboKeyValue(
    timeframe,
    strategyTimeframes,
    pickOptionalString(raw, ["combo_key", "comboKey"])
  );

  const id =
    pickString(raw, ["id"], "") ||
    buildSignalIdWithCombo(exchange || "unknown", symbolRaw, strategy || "default", comboKey, timeframe, strategyTimeframes);
  const symbol = symbolRaw.toUpperCase();
  const side = normalizeSignalSide(raw);
  const highSide = pickOptionalNumber(raw, ["high_side", "highSide"]);
  const midSide = pickOptionalNumber(raw, ["mid_side", "midSide"]);
  const action = Math.trunc(pickNumber(raw, ["action"], 0));
  const entry = pickOptionalNumber(raw, ["entry"]);
  const exit = pickOptionalNumber(raw, ["exit"]);
  const sl = pickOptionalNumber(raw, ["sl"]);
  const tp = pickOptionalNumber(raw, ["tp"]);
  const trendingTimestamp = Math.trunc(pickNumber(raw, ["trending_timestamp"], 0));
  const triggerTimestamp = Math.trunc(pickNumber(raw, ["trigger_timestamp"], 0));
  const strategyVersion = pickString(raw, ["strategy_version", "strategyVersion"], "");
  const groupId = pickString(raw, ["group_id", "groupId"], "");
  const ohlcv = normalizeSignalOHLCV(raw);
  const trendType = sideToTrendType(side);
  const trendMagnitude = trendType === "range" ? 1.5 : trendType === "none" ? 0.8 : Math.abs(side);
  const actionMagnitude = actionWeight(action);
  const recentTs = triggerTimestamp || trendingTimestamp;

  return {
    id,
    symbol,
    name: buildSignalName(exchange, strategy),
    marketCap: 100 + trendMagnitude * 80 + actionMagnitude * 30,
    volume24h: 1 + actionMagnitude * 10,
    price: recentTs > 0 ? recentTs : rankFallback,
    change24h: sideScore(side),
    change7d: actionScore(action),
    rank: rankFallback,
    exchange: exchange || undefined,
    timeframe,
    comboKey: comboKey || undefined,
    groupId: groupId || undefined,
    strategyTimeframes,
    strategy: strategy || undefined,
    strategyVersion: strategyVersion || undefined,
    entry,
    exit,
    sl,
    tp,
    action,
    highSide: typeof highSide === "number" ? Math.trunc(highSide) : undefined,
    midSide: typeof midSide === "number" ? Math.trunc(midSide) : undefined,
    side,
    trendType,
    trendingTimestamp: trendingTimestamp || undefined,
    triggerTimestamp: triggerTimestamp || undefined,
    ohlcv
  };
}

function flattenRecords(source: unknown): UnknownRecord[] {
  const out: UnknownRecord[] = [];
  const visit = (value: unknown) => {
    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item);
      }
      return;
    }
    if (!isRecord(value)) return;
    if (looksLikeSignal(value) || looksLikeMarketData(value)) {
      out.push(value);
      return;
    }
    for (const item of Object.values(value)) {
      visit(item);
    }
  };
  visit(source);
  return out;
}

function looksLikeMarketData(raw: UnknownRecord): boolean {
  const hasSymbol = typeof raw.symbol === "string" || typeof raw.ticker === "string" || typeof raw.code === "string";
  if (!hasSymbol) return false;
  return (
    typeof raw.marketCap === "number" ||
    typeof raw.market_cap === "number" ||
    typeof raw.current_price === "number" ||
    typeof raw.price_change_percentage_24h === "number"
  );
}

function extractRecords(payload: unknown): UnknownRecord[] {
  if (!payload) return [];
  if (Array.isArray(payload)) {
    return payload.filter(isRecord);
  }
  if (!isRecord(payload)) return [];
  if (Array.isArray(payload.data)) {
    return payload.data.filter(isRecord);
  }
  if (isRecord(payload.data)) {
    return flattenRecords(payload.data);
  }
  if (isRecord(payload.cache) && isRecord(payload.cache.signals)) {
    return flattenRecords(payload.cache.signals);
  }
  return flattenRecords(payload);
}

export function normalizeSnapshot(payload: unknown): BubbleDatum[] {
  const list = extractRecords(payload);
  if (list.length === 0) return [];

  const out: BubbleDatum[] = [];
  const seen = new Set<string>();
  for (let i = 0; i < list.length; i += 1) {
    const item = normalizeItem(list[i], i + 1);
    if (item && !seen.has(item.id)) {
      seen.add(item.id);
      out.push(item);
    }
  }
  return out;
}

export function normalizeUpdates(payload: unknown): BubbleUpdate[] {
  if (!payload) return [];
  if (Array.isArray(payload)) {
    return payload
      .map((item) => normalizeUpdateItem(item as UnknownRecord))
      .filter((item): item is BubbleUpdate => item !== null);
  }
  const raw = payload as UnknownRecord;
  if (Array.isArray(raw.data)) {
    return raw.data
      .map((item) => normalizeUpdateItem(item as UnknownRecord))
      .filter((item): item is BubbleUpdate => item !== null);
  }
  if (isRecord(raw.added) || isRecord(raw.updated)) {
    return [
      ...normalizeUpdates(raw.added),
      ...normalizeUpdates(raw.updated)
    ];
  }
  const single = normalizeUpdateItem(raw);
  return single ? [single] : [];
}

function normalizeUpdateItem(raw: UnknownRecord): BubbleUpdate | null {
  if (looksLikeSignal(raw)) {
    const exchange = pickString(raw, ["exchange"], "unknown");
    const symbolRaw = pickString(raw, ["symbol", "ticker", "code"], "");
    const timeframe = pickString(raw, ["timeframe"], "");
    const strategy = pickString(raw, ["strategy"], "default");
    if (!symbolRaw || !timeframe) return null;
    const strategyTimeframes = normalizeSignalStrategyTimeframes(raw);
    const comboKey = normalizeSignalComboKeyValue(
      timeframe,
      strategyTimeframes,
      pickOptionalString(raw, ["combo_key", "comboKey"])
    );

    const side = normalizeSignalSide(raw);
    const highSide = pickOptionalNumber(raw, ["high_side", "highSide"]);
    const midSide = pickOptionalNumber(raw, ["mid_side", "midSide"]);
    const action = Math.trunc(pickNumber(raw, ["action"], 0));
    const entry = pickOptionalNumber(raw, ["entry"]);
    const exit = pickOptionalNumber(raw, ["exit"]);
    const sl = pickOptionalNumber(raw, ["sl"]);
    const tp = pickOptionalNumber(raw, ["tp"]);
    const triggerTimestamp = Math.trunc(pickNumber(raw, ["trigger_timestamp"], 0));
    const trendingTimestamp = Math.trunc(pickNumber(raw, ["trending_timestamp"], 0));
    const ohlcv = normalizeSignalOHLCV(raw);
    const update: BubbleUpdate = {
      id:
        pickString(raw, ["id"], "") ||
        buildSignalIdWithCombo(exchange, symbolRaw, strategy, comboKey, timeframe, strategyTimeframes),
      symbol: symbolRaw.toUpperCase(),
      name: buildSignalName(exchange, strategy),
      exchange,
      timeframe,
      comboKey,
      groupId: pickString(raw, ["group_id", "groupId"], "") || undefined,
      strategy,
      strategyVersion: pickString(raw, ["strategy_version", "strategyVersion"], ""),
      strategyTimeframes,
      side,
      action,
      trendType: sideToTrendType(side),
      trendingTimestamp: trendingTimestamp || undefined,
      triggerTimestamp: triggerTimestamp || undefined,
      marketCap: 100 + Math.abs(side) * 80 + actionWeight(action) * 30,
      volume24h: 1 + actionWeight(action) * 10,
      price: triggerTimestamp || trendingTimestamp || 0,
      change24h: sideScore(side),
      change7d: actionScore(action)
    };

    if (hasAnyValue(raw, ["entry"]) && typeof entry === "number") update.entry = entry;
    if (hasAnyValue(raw, ["exit"]) && typeof exit === "number") update.exit = exit;
    if (hasAnyValue(raw, ["sl"]) && typeof sl === "number") update.sl = sl;
    if (hasAnyValue(raw, ["tp"]) && typeof tp === "number") update.tp = tp;
    if (hasAnyValue(raw, ["high_side", "highSide"]) && typeof highSide === "number") {
      update.highSide = Math.trunc(highSide);
    }
    if (hasAnyValue(raw, ["mid_side", "midSide"]) && typeof midSide === "number") {
      update.midSide = Math.trunc(midSide);
    }
    if (Array.isArray(raw.ohlcv) || Array.isArray(raw.OHLCV)) {
      update.ohlcv = ohlcv || [];
    }
    return update;
  }

  const id = pickString(raw, ["id", "symbol", "slug", "code"], "");
  if (!id) return null;
  const update: BubbleUpdate = { id };

  const price = pickNumber(raw, ["price", "current_price", "price_usd"], NaN);
  if (!Number.isNaN(price)) update.price = price;

  const marketCap = pickNumber(raw, ["marketCap", "market_cap", "market_cap_usd", "cap"], NaN);
  if (!Number.isNaN(marketCap)) update.marketCap = marketCap;

  const volume24h = pickNumber(raw, ["volume24h", "total_volume", "volume_24h"], NaN);
  if (!Number.isNaN(volume24h)) update.volume24h = volume24h;

  const change24h = pickNumber(raw, ["change24h", "price_change_percentage_24h", "percent_change_24h"], NaN);
  if (!Number.isNaN(change24h)) update.change24h = change24h;

  const change7d = pickNumber(raw, ["change7d", "price_change_percentage_7d", "percent_change_7d"], NaN);
  if (!Number.isNaN(change7d)) update.change7d = change7d;

  const rank = pickNumber(raw, ["rank", "market_cap_rank"], NaN);
  if (!Number.isNaN(rank)) update.rank = rank;

  const symbol = pickString(raw, ["symbol", "ticker", "code"], "");
  if (symbol) update.symbol = symbol.toUpperCase();

  const name = pickString(raw, ["name", "fullName", "title"], "");
  if (name) update.name = name;

  const logoUrl = pickString(raw, ["logo", "logoUrl", "image", "icon"], "");
  if (logoUrl) update.logoUrl = logoUrl;

  return update;
}

export function normalizeRemovedIds(payload: unknown): string[] {
  if (!Array.isArray(payload)) return [];
  const out: string[] = [];
  for (const item of payload) {
    if (typeof item !== "string") continue;
    const parts = item.split("|");
    if (parts.length >= 4) {
      if (looksLikeTimeframe(parts[2])) {
        out.push(buildSignalId(parts[0], parts[1], parts[2], parts.slice(3).join("|")));
        continue;
      }
      out.push(buildSignalIdWithCombo(parts[0], parts[1], parts[2], parts.slice(3).join("|")));
      continue;
    }
    out.push(item);
  }
  return out;
}

function looksLikeTimeframe(value: string): boolean {
  switch (value.trim().toLowerCase()) {
    case "1m":
    case "3m":
    case "5m":
    case "15m":
    case "30m":
    case "1h":
    case "2h":
    case "4h":
    case "6h":
    case "8h":
    case "12h":
    case "1d":
      return true;
    default:
      return false;
  }
}

export function normalizeBackendStatus(payload: unknown): BackendStatus | null {
  if (!isRecord(payload)) return null;
  const cache = isRecord(payload.cache) ? payload.cache : {};
  const runtime = isRecord(payload.runtime) ? payload.runtime : {};
  const version = isRecord(payload.version) ? payload.version : {};
  const singleton = isRecord(payload.singleton) ? payload.singleton : {};
  const modules = normalizeModules(payload.modules);

  return {
    fetchedAt: Date.now(),
    runtimeSeconds: pickNumber(runtime, ["seconds"], 0),
    runtimeHuman: pickString(runtime, ["human"], ""),
    singletonUuid: pickString(singleton, ["uuid"], ""),
    versionTag: pickString(version, ["tag"], ""),
    versionCommit: pickString(version, ["commit"], ""),
    buildTime: pickString(version, ["build_time", "buildTime"], ""),
    cache: {
      exchangeCount: pickNumber(cache, ["exchange_count", "exchangeCount"], 0),
      symbolCount: pickNumber(cache, ["symbol_count", "symbolCount"], 0),
      timeframeCount: pickNumber(cache, ["timeframe_count", "timeframeCount"], 0),
      signalCount: pickNumber(cache, ["signal_count", "signalCount"], 0)
    },
    modules
  };
}

export function normalizeAccount(payload: unknown): AccountSnapshot | null {
  if (!isRecord(payload)) return null;

  const source = unwrapRecordCandidates(payload).find((candidate) => {
    return (
      hasAnyValue(candidate, ["exchange", "currency"]) ||
      hasAnyValue(candidate, [
        "funding_usdt",
        "fundingUsdt",
        "trading_usdt",
        "tradingUsdt",
        "total_usdt",
        "totalUsdt",
        "per_trade_usdt",
        "perTradeUsdt",
        "daily_profit_usdt",
        "dailyProfitUsdt"
      ])
    );
  });
  if (!source) return null;

  const exchange = pickString(source, ["exchange"], "");
  const currency = pickString(source, ["currency"], "");
  const hasBalanceField = hasAnyValue(source, [
    "funding_usdt",
    "fundingUsdt",
    "trading_usdt",
    "tradingUsdt",
    "total_usdt",
    "totalUsdt",
    "per_trade_usdt",
    "perTradeUsdt",
    "daily_profit_usdt",
    "dailyProfitUsdt"
  ]);
  if (!exchange && !currency && !hasBalanceField) {
    return null;
  }

  const updatedAtMs = pickOptionalNumber(source, ["updated_at_ms", "updatedAtMs"]);
  return {
    exchange,
    currency,
    fundingUsdt: pickNumber(source, ["funding_usdt", "fundingUsdt"], 0),
    tradingUsdt: pickNumber(source, ["trading_usdt", "tradingUsdt"], 0),
    totalUsdt: pickNumber(source, ["total_usdt", "totalUsdt"], 0),
    perTradeUsdt: pickOptionalNumber(source, ["per_trade_usdt", "perTradeUsdt"]),
    dailyProfitUsdt: pickOptionalNumber(source, ["daily_profit_usdt", "dailyProfitUsdt"]),
    updatedAtMs: typeof updatedAtMs === "number" ? Math.trunc(updatedAtMs) : undefined,
    fetchedAt: Date.now()
  };
}

function normalizePositionItem(payload: unknown): PositionItem | null {
  if (!isRecord(payload)) return null;

  const exchange = pickString(payload, ["exchange", "exchange_name", "exchangeName", "venue", "market", "ex"], "").trim();
  const symbol = pickString(payload, ["symbol", "instId", "inst_id", "instrument", "pair", "code"], "").trim();
  const timeframe = pickString(payload, ["timeframe", "tf", "interval", "period", "bar", "bar_size"], "").trim();
  if (!symbol) return null;

  return {
    positionId: pickOptionalNumber(payload, ["position_id", "positionId", "id"]),
    exchange: exchange || "--",
    symbol: symbol.toUpperCase(),
    timeframe: timeframe || "--",
    groupId: pickOptionalString(payload, ["group_id", "groupId"]),
    positionSide: pickOptionalString(payload, ["position_side", "positionSide", "posSide", "side", "direction", "position_type"]),
    marginMode: pickOptionalString(payload, ["margin_mode", "marginMode"]),
    leverageMultiplier: pickOptionalNumber(payload, ["leverage_multiplier", "leverageMultiplier", "leverage"]),
    marginAmount: pickOptionalNumber(payload, ["margin_amount", "marginAmount"]),
    entryPrice: pickOptionalNumber(payload, ["entry_price", "entryPrice"]),
    exitPrice: pickOptionalNumber(payload, ["exit_price", "exitPrice", "close_price", "closePrice"]),
    entryQuantity: pickOptionalNumber(payload, ["entry_quantity", "entryQuantity", "position_size", "positionSize", "size", "qty"]),
    entryTime: pickOptionalString(payload, ["entry_time", "entryTime", "created_at", "createdAt"]),
    exitTime: pickOptionalString(payload, ["exit_time", "exitTime", "close_time", "closeTime", "closed_at", "closedAt"]),
    takeProfitPrice: pickOptionalNumber(payload, ["take_profit_price", "takeProfitPrice", "tp"]),
    stopLossPrice: pickOptionalNumber(payload, ["stop_loss_price", "stopLossPrice", "sl"]),
    currentPrice: pickOptionalNumber(payload, ["current_price", "currentPrice", "mark_price", "markPrice"]),
    unrealizedProfitAmount: pickOptionalNumber(payload, [
      "unrealized_profit_amount",
      "unrealizedProfitAmount",
      "upnl",
      "unrealizedPnl",
      "profit_amount",
      "profitAmount",
      "pnl"
    ]),
    unrealizedProfitRate: pickOptionalNumber(payload, [
      "unrealized_profit_rate",
      "unrealizedProfitRate",
      "upnl_ratio",
      "upnlRatio",
      "profit_rate",
      "profitRate",
      "pnl_ratio",
      "pnlRate"
    ]),
    profitAmount: pickOptionalNumber(payload, ["profit_amount", "profitAmount", "pnl"]),
    profitRate: pickOptionalNumber(payload, ["profit_rate", "profitRate", "pnl_ratio", "pnlRate"]),
    maxFloatingProfitAmount: pickOptionalNumber(payload, ["max_floating_profit_amount", "maxFloatingProfitAmount"]),
    maxFloatingProfitRate: pickOptionalNumber(payload, ["max_floating_profit_rate", "maxFloatingProfitRate"]),
    maxFloatingLossAmount: pickOptionalNumber(payload, ["max_floating_loss_amount", "maxFloatingLossAmount"]),
    maxFloatingLossRate: pickOptionalNumber(payload, ["max_floating_loss_rate", "maxFloatingLossRate"]),
    holdingTime: pickOptionalString(payload, ["holding_time", "holdingTime"]),
    status: pickOptionalString(payload, ["status", "position_status", "positionStatus", "state"]),
    strategyName: pickOptionalString(payload, ["strategy_name", "strategyName", "strategy"]),
    strategyVersion: pickOptionalString(payload, ["strategy_version", "strategyVersion"]),
    updatedTime: pickOptionalString(payload, ["updated_time", "updatedTime", "updated_at", "updatedAt", "update_time", "updateTime"])
  };
}

const KNOWN_GROUP_CANDIDATE_KEYS = new Map<string, keyof TrendGroupCandidate>([
  ["candidate_key", "candidateKey"],
  ["candidateKey", "candidateKey"],
  ["candidate_state", "candidateState"],
  ["candidateState", "candidateState"],
  ["is_selected", "isSelected"],
  ["isSelected", "isSelected"],
  ["priority_score", "priorityScore"],
  ["priorityScore", "priorityScore"],
  ["has_open_position", "hasOpenPosition"],
  ["hasOpenPosition", "hasOpenPosition"]
]);

function normalizeTrendGroupCandidate(payload: unknown): TrendGroupCandidate | null {
  if (!isRecord(payload)) return null;
  const candidate: TrendGroupCandidate = {
    candidateKey: pickOptionalString(payload, ["candidate_key", "candidateKey"]),
    candidateState: pickOptionalString(payload, ["candidate_state", "candidateState"]),
    isSelected:
      typeof payload.is_selected === "boolean"
        ? payload.is_selected
        : typeof payload.isSelected === "boolean"
        ? payload.isSelected
        : undefined,
    priorityScore: pickOptionalNumber(payload, ["priority_score", "priorityScore"]),
    hasOpenPosition:
      typeof payload.has_open_position === "boolean"
        ? payload.has_open_position
        : typeof payload.hasOpenPosition === "boolean"
        ? payload.hasOpenPosition
        : undefined
  };

  for (const [key, value] of Object.entries(payload)) {
    const mappedKey = KNOWN_GROUP_CANDIDATE_KEYS.get(key);
    if (mappedKey) continue;
    candidate[key] = value;
  }

  return candidate;
}

function normalizeTrendGroupItem(payload: unknown): TrendGroupItem | null {
  if (!isRecord(payload)) return null;
  const groupId = pickString(payload, ["group_id", "groupId"], "").trim();
  if (!groupId) return null;
  const candidatesRaw = Array.isArray(payload.candidates) ? payload.candidates : [];
  const candidates = candidatesRaw
    .map((item) => normalizeTrendGroupCandidate(item))
    .filter((item): item is TrendGroupCandidate => item !== null);

  return {
    groupId,
    strategy: pickOptionalString(payload, ["strategy"]),
    primaryTimeframe: pickOptionalString(payload, ["primary_timeframe", "primaryTimeframe"]),
    side: pickOptionalString(payload, ["side"]),
    anchorTrendingTimestampMs: pickOptionalNumber(payload, [
      "anchor_trending_timestamp_ms",
      "anchorTrendingTimestampMs"
    ]),
    state: pickOptionalString(payload, ["state"]),
    lockStage: pickOptionalString(payload, ["lock_stage", "lockStage"]),
    selectedCandidateKey: pickOptionalString(payload, [
      "selected_candidate_key",
      "selectedCandidateKey"
    ]),
    entryCount: pickOptionalNumber(payload, ["entry_count", "entryCount"]),
    candidates
  };
}

export function normalizeGroups(payload: unknown): TrendGroupsSnapshot | null {
  if (!isRecord(payload)) return null;

  const candidates: UnknownRecord[] = [payload];
  if (isRecord(payload.data)) candidates.push(payload.data);
  if (isRecord(payload.result)) candidates.push(payload.result);
  if (isRecord(payload.payload)) candidates.push(payload.payload);
  const modules = isRecord(payload.modules) ? payload.modules : undefined;
  const risk = modules && isRecord(modules.risk) ? modules.risk : undefined;
  const details = risk && isRecord(risk.details) ? risk.details : undefined;
  const trendGuard = details && isRecord(details.trend_guard) ? details.trend_guard : undefined;
  if (trendGuard) {
    candidates.push(trendGuard);
  }

  for (const candidate of candidates) {
    const groupsRaw = Array.isArray(candidate.groups) ? candidate.groups : null;
    const hasMeta =
      typeof candidate.enabled === "boolean" ||
      typeof candidate.mode === "string" ||
      typeof candidate.groups_total === "number" ||
      typeof candidate.groups_active === "number" ||
      Array.isArray(candidate.groups);
    if (!groupsRaw && !hasMeta) continue;

    const groups = (groupsRaw || [])
      .map((item) => normalizeTrendGroupItem(item))
      .filter((item): item is TrendGroupItem => item !== null);

    return {
      enabled: typeof candidate.enabled === "boolean" ? candidate.enabled : undefined,
      mode: pickOptionalString(candidate, ["mode"]),
      groupsTotal:
        typeof candidate.groups_total === "number"
          ? Math.max(0, Math.trunc(candidate.groups_total))
          : groups.length,
      groupsActive:
        typeof candidate.groups_active === "number"
          ? Math.max(0, Math.trunc(candidate.groups_active))
          : groups.length,
      groups,
      fetchedAt: Date.now()
    };
  }

  return null;
}

export function normalizePosition(payload: unknown): PositionSnapshot | null {
  if (Array.isArray(payload)) {
    return buildPositionSnapshot(payload);
  }
  if (!isRecord(payload)) return null;

  const candidates: unknown[] = [payload, payload.data, payload.result, payload.payload];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      const snapshot = buildPositionSnapshot(candidate);
      if (snapshot) return snapshot;
      continue;
    }
    if (!isRecord(candidate)) continue;
    const positionsRaw = extractPositionRows(candidate);
    const countRaw = pickOptionalNumber(candidate, ["count", "position_count", "positionCount", "total", "size"]);
    const snapshot = buildPositionSnapshot(positionsRaw, countRaw);
    if (snapshot) return snapshot;
  }
  return null;
}

export function normalizePong(payload: unknown): WsPongStatus | null {
  if (!isRecord(payload)) return null;
  const data = isRecord(payload.data) ? payload.data : {};
  const runtime = isRecord(data.runtime) ? data.runtime : {};
  const singleton = isRecord(data.singleton) ? data.singleton : {};
  const requestId = pickString(payload, ["request_id", "requestId"], "");
  const serverTs = pickNumber(payload, ["ts"], NaN);
  const runtimeSeconds = pickNumber(runtime, ["seconds"], NaN);
  const runtimeHuman = pickString(runtime, ["human"], "");
  const singletonUuid = pickString(singleton, ["uuid"], "");

  return {
    requestId: requestId || undefined,
    serverTs: Number.isNaN(serverTs) ? undefined : Math.trunc(serverTs),
    receivedAt: Date.now(),
    rttMs: undefined,
    runtimeSeconds: Number.isNaN(runtimeSeconds) ? undefined : runtimeSeconds,
    runtimeHuman: runtimeHuman || undefined,
    singletonUuid: singletonUuid || undefined,
    modules: normalizeModules(data.modules)
  };
}

function normalizeCandleBar(payload: unknown): BubbleCandleBar | null {
  if (!isRecord(payload)) return null;
  const open = pickNumber(payload, ["open", "Open", "o"], NaN);
  const high = pickNumber(payload, ["high", "High", "h"], NaN);
  const low = pickNumber(payload, ["low", "Low", "l"], NaN);
  const close = pickNumber(payload, ["close", "Close", "c"], NaN);
  const volume = pickNumber(payload, ["volume", "Volume", "v"], 0);
  const ts = pickNumber(payload, ["ts", "TS", "timestamp", "time"], NaN);
  if (
    Number.isNaN(open) ||
    Number.isNaN(high) ||
    Number.isNaN(low) ||
    Number.isNaN(close) ||
    Number.isNaN(ts)
  ) {
    return null;
  }
  return {
    ts: Math.trunc(ts),
    open,
    high,
    low,
    close,
    volume: Number.isFinite(volume) ? volume : 0
  };
}

function normalizeCandleBars(payload: unknown): BubbleCandleBar[] {
  if (!Array.isArray(payload)) return [];
  const dedup = new Map<number, BubbleCandleBar>();
  for (const item of payload) {
    const bar = normalizeCandleBar(item);
    if (!bar || bar.ts <= 0) continue;
    dedup.set(bar.ts, bar);
  }
  return Array.from(dedup.values()).sort((left, right) => left.ts - right.ts);
}

function normalizeEventLevel(value: string): BubbleCandleEvent["level"] {
  const normalized = value.trim().toLowerCase();
  if (normalized === "warning") return "warning";
  if (normalized === "success") return "success";
  if (normalized === "error") return "error";
  return "info";
}

function normalizeCandleEvents(payload: unknown): BubbleCandleEvent[] {
  if (!Array.isArray(payload)) return [];
  const out: BubbleCandleEvent[] = [];
  for (const item of payload) {
    if (!isRecord(item)) continue;
    const id = pickString(item, ["id"], "").trim();
    const eventAtRaw = pickNumber(item, ["event_at_ms", "eventAtMs", "event_ts", "ts"], NaN);
    if (!id || !Number.isFinite(eventAtRaw)) continue;
    const detail = isRecord(item.detail) ? item.detail : undefined;
    out.push({
      id,
      source: pickString(item, ["source"], "").trim(),
      type: pickString(item, ["type"], "").trim(),
      level: normalizeEventLevel(pickString(item, ["level"], "info")),
      eventAtMs: Math.trunc(eventAtRaw),
      title: pickString(item, ["title"], "").trim(),
      summary: pickString(item, ["summary"], "").trim(),
      detail
    });
  }
  return out.sort((left, right) => left.eventAtMs - right.eventAtMs);
}

function normalizeCandlePosition(payload: unknown): BubbleCandlePosition | undefined {
  if (!isRecord(payload)) return undefined;
  const positionKey = pickOptionalString(payload, ["position_key", "positionKey"]);
  const positionSide = pickOptionalString(payload, ["position_side", "positionSide"]);
  const entryPrice = pickOptionalNumber(payload, ["entry_price", "entryPrice"]);
  const exitPrice = pickOptionalNumber(payload, ["exit_price", "exitPrice"]);
  const isOpenRaw = payload.is_open;
  const isOpen =
    typeof isOpenRaw === "boolean"
      ? isOpenRaw
      : typeof isOpenRaw === "number"
      ? isOpenRaw > 0
      : typeof isOpenRaw === "string"
      ? ["1", "true", "yes", "open"].includes(isOpenRaw.trim().toLowerCase())
      : undefined;
  if (
    !positionKey &&
    !positionSide &&
    typeof entryPrice !== "number" &&
    typeof exitPrice !== "number" &&
    typeof isOpen !== "boolean"
  ) {
    return undefined;
  }
  return {
    positionKey: positionKey || undefined,
    isOpen,
    positionSide: positionSide || undefined,
    entryPrice,
    exitPrice
  };
}

export function normalizeCandlesSnapshot(payload: unknown): BubbleCandlesSnapshot | null {
  if (!isRecord(payload)) return null;
  const rowsRaw = Array.isArray(payload.data)
    ? payload.data
    : Array.isArray(payload.items)
    ? payload.items
    : [];
  if (rowsRaw.length === 0) return null;
  const items: BubbleCandlesSnapshot["items"] = [];

  for (const row of rowsRaw) {
    if (!isRecord(row)) continue;
    const exchange = pickString(row, ["exchange"], "").trim().toLowerCase();
    const symbol = pickString(row, ["symbol"], "").trim().toUpperCase();
    if (!exchange || !symbol) continue;

    const timeframesSource =
      (isRecord(row.timeframes) ? row.timeframes : isRecord(row.series) ? row.series : undefined) || {};
    const series: BubbleCandlesSnapshot["items"][number]["series"] = {};
    for (const [timeframeRaw, value] of Object.entries(timeframesSource)) {
      if (!isRecord(value)) continue;
      const timeframe = timeframeRaw.trim().toLowerCase();
      if (!timeframe) continue;
      const bars = normalizeCandleBars(value.bars ?? value.data ?? value.items);
      const requestedRaw = pickNumber(value, ["requested", "limit", "requested_limit"], bars.length);
      const returnedRaw = pickNumber(value, ["returned", "count"], bars.length);
      series[timeframe] = {
        timeframe,
        requested: Math.max(0, Math.trunc(Number.isFinite(requestedRaw) ? requestedRaw : bars.length)),
        returned: Math.max(0, Math.trunc(Number.isFinite(returnedRaw) ? returnedRaw : bars.length)),
        bars
      };
    }

    if (Object.keys(series).length === 0) continue;
    const events = normalizeCandleEvents(row.events);
    const eventsTotalRaw = pickOptionalNumber(row, ["events_total", "eventsTotal"]);
    const eventsTruncatedRaw = row.events_truncated;
    const eventsTruncated =
      typeof eventsTruncatedRaw === "boolean"
        ? eventsTruncatedRaw
        : typeof eventsTruncatedRaw === "number"
        ? eventsTruncatedRaw > 0
        : typeof eventsTruncatedRaw === "string"
        ? ["1", "true", "yes"].includes(eventsTruncatedRaw.trim().toLowerCase())
        : undefined;
    const position = normalizeCandlePosition(row.position);
    items.push({
      exchange,
      symbol,
      series,
      events: events.length > 0 ? events : undefined,
      eventsTotal: typeof eventsTotalRaw === "number" ? Math.max(0, Math.trunc(eventsTotalRaw)) : undefined,
      eventsTruncated: typeof eventsTruncated === "boolean" ? eventsTruncated : undefined,
      position
    });
  }

  if (items.length === 0) return null;
  const warnings = Array.isArray(payload.warnings)
    ? payload.warnings.filter((item): item is string => typeof item === "string" && item.trim().length > 0)
    : [];
  return {
    fetchedAt: Date.now(),
    items,
    warnings
  };
}
