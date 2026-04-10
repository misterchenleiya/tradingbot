import type { CSSProperties } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type PriceFormat,
  type Time
} from "lightweight-charts";
import type {
  BubbleCandlesFetchRequest,
  BubbleCandleEvent,
  BubbleCandlePosition,
  BubbleCandleSeries,
  BubbleDatum,
  PositionItem
} from "../app/types";
import { useAppStore } from "../app/store";
import {
  buildBubbleMarkerTooltipState,
  buildBubbleOverlayMarkers,
  formatBubbleMarkerPrice,
  resolveBubbleLatestMarkerPrice,
  type BubbleMarkerTooltipState
} from "./klineEventOverlay";

const PREFERRED_EXCHANGES = ["binance", "okx", "bitget"] as const;
const DEFAULT_CANDLE_LIMIT = 100;
const DEFAULT_VISIBLE_BARS = 60;
const DEFAULT_POLL_INTERVAL_MS = 60_000;
const INCREMENTAL_CANDLE_LIMIT = 4;
const FULL_RESYNC_EVERY_POLLS = 60;
const DEFAULT_EVENT_LIMIT = 1200;
const PRICE_AXIS_MIN_WIDTH = 52;
const PRICE_AXIS_MAX_WIDTH = 96;
const SCIENTIFIC_LEADING_ZERO_THRESHOLD = 5;
const PRICE_AXIS_FONT = "11px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
const PRICE_SCALE_MARGINS = { top: 0.12, bottom: 0.22 } as const;
const CROSSHAIR_AXIS_LABEL_HEIGHT = 30;
const CROSSHAIR_TIME_LABEL_WIDTH = 124;
const EMA_COLORS = {
  ema5: "rgba(255,255,255,0.5)",
  ema10: "rgba(250,250,0,0.5)",
  ema20: "rgba(248,215,0,0.5)",
  ema60: "rgba(0,46,253,0.5)",
  ema120: "rgba(191,0,225,0.5)"
} as const;

type BubbleKlinePanelProps = {
  selected: BubbleDatum;
  matchedPosition?: PositionItem;
  allDataList: BubbleDatum[];
};

type BubbleCandlesRequestPosition = NonNullable<BubbleCandlesFetchRequest["requests"][number]["position"]>;

type IndicatorState = {
  ema5: boolean;
  ema20: boolean;
  ema60: boolean;
};

type DynamicPriceDisplay = {
  key: string;
  axisWidth: number;
  priceFormat: PriceFormat;
};

type BubblePositionLevelKind = "ENTRY" | "TP" | "SL";

type BubblePositionLevelOverlay = {
  id: string;
  kind: BubblePositionLevelKind;
  y: number;
  lineLength: number;
  axisOffset: number;
  side: "long" | "short";
};

type CrosshairAxisLabelState = {
  top: number;
  priceText: string;
  percentText: string;
};

type CrosshairTimeLabelState = {
  left: number;
  text: string;
};

let priceAxisMeasureCtx: CanvasRenderingContext2D | null | undefined;

function clampNumber(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function resolvePriceAxisMeasureContext(): CanvasRenderingContext2D | null {
  if (priceAxisMeasureCtx !== undefined) return priceAxisMeasureCtx;
  if (typeof document === "undefined") {
    priceAxisMeasureCtx = null;
    return priceAxisMeasureCtx;
  }
  const canvas = document.createElement("canvas");
  priceAxisMeasureCtx = canvas.getContext("2d");
  return priceAxisMeasureCtx;
}

function measurePriceLabelWidth(text: string): number {
  const ctx = resolvePriceAxisMeasureContext();
  if (!ctx) return text.length * 7;
  ctx.font = PRICE_AXIS_FONT;
  return ctx.measureText(text).width;
}

function countFractionLeadingZeros(value: number): number {
  if (!Number.isFinite(value) || value <= 0 || value >= 1) return 0;
  const fixed = value.toFixed(18);
  const dot = fixed.indexOf(".");
  if (dot < 0) return 0;
  let count = 0;
  for (let i = dot + 1; i < fixed.length; i += 1) {
    if (fixed[i] !== "0") break;
    count += 1;
  }
  return count;
}

function formatScientificWithZeroInteger(value: number, digits: number): string {
  if (!Number.isFinite(value)) return "--";
  if (value === 0) return "0";
  const sign = value < 0 ? "-" : "";
  const abs = Math.abs(value);
  const exponent = Math.floor(Math.log10(abs));
  const displayExponent = exponent + 1;
  const mantissa = abs / Math.pow(10, displayExponent);
  const suffix = displayExponent >= 0 ? `+${displayExponent}` : `${displayExponent}`;
  return `${sign}${mantissa.toFixed(digits)}e${suffix}`;
}

function formatPriceByDisplay(value: number, display: DynamicPriceDisplay): string {
  if (!Number.isFinite(value)) return "--";
  if (display.priceFormat.type === "custom" && typeof display.priceFormat.formatter === "function") {
    return display.priceFormat.formatter(value);
  }
  const precision =
    display.priceFormat.type === "price" && typeof display.priceFormat.precision === "number"
      ? display.priceFormat.precision
      : 4;
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: precision
  });
}

function formatSignedPercent(value: number): string {
  if (!Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function buildDynamicPriceDisplay(bars: BubbleCandleSeries["bars"]): DynamicPriceDisplay {
  if (!Array.isArray(bars) || bars.length === 0) {
    return {
      key: "empty",
      axisWidth: PRICE_AXIS_MIN_WIDTH,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 }
    };
  }

  const values: number[] = [];
  let maxAbs = 0;
  let minPositiveAbs = Number.POSITIVE_INFINITY;
  for (const bar of bars) {
    values.push(bar.open, bar.high, bar.low, bar.close);
  }
  for (const value of values) {
    if (!Number.isFinite(value)) continue;
    const abs = Math.abs(value);
    if (abs > maxAbs) maxAbs = abs;
    if (abs > 0 && abs < minPositiveAbs) minPositiveAbs = abs;
  }
  if (!Number.isFinite(maxAbs) || maxAbs <= 0) {
    return {
      key: "zero",
      axisWidth: PRICE_AXIS_MIN_WIDTH,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 }
    };
  }

  const minPositive = Number.isFinite(minPositiveAbs) ? minPositiveAbs : maxAbs;
  const leadingZeros = countFractionLeadingZeros(minPositive);
  const useScientific = maxAbs < 1 && leadingZeros >= SCIENTIFIC_LEADING_ZERO_THRESHOLD;

  const plainPrecision =
    maxAbs >= 1000
      ? 2
      : maxAbs >= 1
      ? 4
      : clampNumber(leadingZeros + 3, 4, 10);
  const minMove = Number((1 / Math.pow(10, plainPrecision)).toPrecision(12));

  const scientificDigits = clampNumber(plainPrecision - leadingZeros + 1, 2, 5);
  const scientificFormatter = (value: number): string =>
    formatScientificWithZeroInteger(value, scientificDigits);
  const plainFormatter = (value: number): string =>
    value.toLocaleString("en-US", {
      minimumFractionDigits: 0,
      maximumFractionDigits: plainPrecision
    });
  const formatter = useScientific ? scientificFormatter : plainFormatter;

  let maxLabelWidth = measurePriceLabelWidth(formatter(0));
  for (const value of values) {
    if (!Number.isFinite(value)) continue;
    maxLabelWidth = Math.max(maxLabelWidth, measurePriceLabelWidth(formatter(value)));
  }
  const axisWidth = clampNumber(Math.ceil(maxLabelWidth + 14), PRICE_AXIS_MIN_WIDTH, PRICE_AXIS_MAX_WIDTH);

  if (useScientific) {
    return {
      key: `sci|${scientificDigits}|w:${axisWidth}`,
      axisWidth,
      priceFormat: {
        type: "custom",
        minMove,
        formatter: scientificFormatter
      }
    };
  }
  return {
    key: `plain|${plainPrecision}|w:${axisWidth}`,
    axisWidth,
    priceFormat: {
      type: "price",
      precision: plainPrecision,
      minMove
    }
  };
}

function normalizeSymbol(value?: string): string {
  return (value || "").trim().toUpperCase();
}

function normalizeSymbolKey(value?: string): string {
  return normalizeSymbol(value).replace(/[^A-Z0-9]/g, "");
}

function symbolsLikelyMatch(left?: string, right?: string): boolean {
  const l = normalizeSymbolKey(left);
  const r = normalizeSymbolKey(right);
  if (!l || !r) return false;
  if (l === r) return true;
  return l.includes(r) || r.includes(l);
}

function splitExchangeTokens(exchange?: string): string[] {
  return (exchange || "")
    .split("/")
    .map((item) => item.trim().toLowerCase())
    .filter((item) => item.length > 0);
}

function sortExchanges(exchanges: string[]): string[] {
  const unique = Array.from(new Set(exchanges.map((item) => item.trim().toLowerCase()).filter(Boolean)));
  const preferred = PREFERRED_EXCHANGES.filter((item) => unique.includes(item));
  const others = unique
    .filter((item) => !PREFERRED_EXCHANGES.includes(item as (typeof PREFERRED_EXCHANGES)[number]))
    .sort((left, right) => left.localeCompare(right));
  return [...preferred, ...others];
}

function timeframeSortValue(timeframe: string): number {
  const match = /^(\d+)([mhdw])$/i.exec(timeframe.trim());
  if (!match) return Number.MAX_SAFE_INTEGER;
  const num = Number(match[1]);
  const unit = match[2].toLowerCase();
  switch (unit) {
    case "m":
      return num;
    case "h":
      return num * 60;
    case "d":
      return num * 60 * 24;
    case "w":
      return num * 60 * 24 * 7;
    default:
      return Number.MAX_SAFE_INTEGER;
  }
}

function sortTimeframes(timeframes: string[]): string[] {
  return Array.from(new Set(timeframes.map((item) => item.trim().toLowerCase()).filter(Boolean))).sort(
    (left, right) => timeframeSortValue(left) - timeframeSortValue(right)
  );
}

function pickLargestTimeframe(timeframes: string[]): string {
  if (!Array.isArray(timeframes) || timeframes.length === 0) return "";
  return timeframes[timeframes.length - 1] || "";
}

function shouldIgnoreShortcut(event: KeyboardEvent): boolean {
  const target = event.target as HTMLElement | null;
  if (!target) return false;
  const tag = target.tagName.toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") return true;
  return target.isContentEditable;
}

function getDigitIndex(key: string): number | null {
  if (!/^[1-9]$/.test(key)) return null;
  return Number(key) - 1;
}

function appendTimeframes(out: string[], timeframes?: string[]): void {
  if (!Array.isArray(timeframes)) return;
  for (const timeframe of timeframes) {
    if (typeof timeframe !== "string") continue;
    const normalized = timeframe.trim().toLowerCase();
    if (!normalized) continue;
    out.push(normalized);
  }
}

function computeEMA(values: number[], period: number): number[] {
  if (!Array.isArray(values) || values.length === 0 || period <= 1) return [...values];
  const out: number[] = [];
  const alpha = 2 / (period + 1);
  let prev = values[0];
  for (let i = 0; i < values.length; i += 1) {
    const next = i === 0 ? values[0] : values[i] * alpha + prev * (1 - alpha);
    out.push(next);
    prev = next;
  }
  return out;
}

function dedupeAndSortBars(series?: BubbleCandleSeries): BubbleCandleSeries["bars"] {
  if (!series || !Array.isArray(series.bars) || series.bars.length === 0) return [];
  const dedup = new Map<number, BubbleCandleSeries["bars"][number]>();
  for (const bar of series.bars) {
    if (!bar || !Number.isFinite(bar.ts)) continue;
    dedup.set(Math.trunc(bar.ts), bar);
  }
  return Array.from(dedup.values()).sort((left, right) => left.ts - right.ts);
}

function normalizeSeriesMap(
  source: Record<string, BubbleCandleSeries> | undefined,
  maxBars: number
): Record<string, BubbleCandleSeries> {
  if (!source) return {};
  const out: Record<string, BubbleCandleSeries> = {};
  for (const [timeframeRaw, series] of Object.entries(source)) {
    const timeframe = timeframeRaw.trim().toLowerCase();
    if (!timeframe || !series) continue;
    const bars = dedupeAndSortBars(series).slice(-maxBars);
    out[timeframe] = {
      timeframe,
      requested: Math.max(0, Math.trunc(series.requested || bars.length)),
      returned: bars.length,
      bars
    };
  }
  return out;
}

function mergeSeriesBars(
  base: BubbleCandleSeries["bars"],
  incoming: BubbleCandleSeries["bars"],
  maxBars: number
): BubbleCandleSeries["bars"] {
  const merged = new Map<number, BubbleCandleSeries["bars"][number]>();
  for (const bar of base) {
    if (!bar || !Number.isFinite(bar.ts)) continue;
    merged.set(Math.trunc(bar.ts), bar);
  }
  for (const bar of incoming) {
    if (!bar || !Number.isFinite(bar.ts)) continue;
    merged.set(Math.trunc(bar.ts), bar);
  }
  return Array.from(merged.values())
    .sort((left, right) => left.ts - right.ts)
    .slice(-maxBars);
}

function mergeSeriesMaps(
  base: Record<string, BubbleCandleSeries>,
  incoming: Record<string, BubbleCandleSeries>,
  maxBars: number
): Record<string, BubbleCandleSeries> {
  const out: Record<string, BubbleCandleSeries> = {};
  const keys = new Set<string>([...Object.keys(base), ...Object.keys(incoming)]);
  for (const key of keys) {
    const prev = base[key];
    const next = incoming[key];
    if (!prev && !next) continue;
    if (!prev && next) {
      out[key] = next;
      continue;
    }
    if (prev && !next) {
      out[key] = prev;
      continue;
    }
    const prevBars = dedupeAndSortBars(prev).slice(-maxBars);
    const nextBars = dedupeAndSortBars(next).slice(-maxBars);
    const bars = mergeSeriesBars(prevBars, nextBars, maxBars);
    out[key] = {
      timeframe: key,
      requested: Math.max(prev?.requested || 0, next?.requested || 0),
      returned: bars.length,
      bars
    };
  }
  return out;
}

function resolvePreferredExchanges(selected: BubbleDatum, matchedPosition: PositionItem | undefined, allDataList: BubbleDatum[]): string[] {
  const out: string[] = [];
  out.push(...splitExchangeTokens(selected.exchange));
  out.push(...splitExchangeTokens(matchedPosition?.exchange));
  for (const item of allDataList) {
    if (!symbolsLikelyMatch(selected.symbol, item.symbol)) continue;
    out.push(...splitExchangeTokens(item.exchange));
  }
  return sortExchanges(out);
}

function resolveStrategyTimeframes(
  selected: BubbleDatum,
  matchedPosition: PositionItem | undefined,
  allDataList: BubbleDatum[]
): string[] {
  const strategy = (matchedPosition?.strategyName || selected.strategy || "").trim().toLowerCase();
  const strategyVersion = (matchedPosition?.strategyVersion || selected.strategyVersion || "").trim().toLowerCase();
  const out: string[] = [];

  for (const item of allDataList) {
    if (!symbolsLikelyMatch(selected.symbol, item.symbol)) continue;
    const itemStrategy = (item.strategy || "").trim().toLowerCase();
    const itemVersion = (item.strategyVersion || "").trim().toLowerCase();
    if (strategy && itemStrategy !== strategy) continue;
    if (strategyVersion && itemVersion !== strategyVersion) continue;
    appendTimeframes(out, item.strategyTimeframes);
    if (item.timeframe) out.push(item.timeframe);
  }

  appendTimeframes(out, selected.strategyTimeframes);
  if (matchedPosition?.timeframe) {
    out.push(matchedPosition.timeframe);
  }
  if (selected.timeframe) {
    out.push(selected.timeframe);
  }
  const normalized = sortTimeframes(out);
  return normalized.length > 0 ? normalized : ["1h"];
}

function formatLocalDateTime(timestamp: number): string {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "--";
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "--";
  return formatDateToLocalMinute(date);
}

function formatDateToLocalMinute(date: Date): string {
  const yyyy = date.getFullYear();
  const mm = `${date.getMonth() + 1}`.padStart(2, "0");
  const dd = `${date.getDate()}`.padStart(2, "0");
  const hh = `${date.getHours()}`.padStart(2, "0");
  const mi = `${date.getMinutes()}`.padStart(2, "0");
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}`;
}

function formatChartTimeLocal(time: Time): string {
  if (typeof time === "number" && Number.isFinite(time)) {
    return formatLocalDateTime(time * 1000);
  }
  if (typeof time === "string") {
    const date = new Date(time);
    if (!Number.isNaN(date.getTime())) {
      return formatDateToLocalMinute(date);
    }
    return "--";
  }
  if (typeof time === "object" && time !== null && "year" in time && "month" in time && "day" in time) {
    const year = Number(time.year);
    const month = Number(time.month);
    const day = Number(time.day);
    if (Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)) {
      return formatDateToLocalMinute(new Date(year, month - 1, day));
    }
  }
  return "--";
}

function normalizePositionSide(value?: string): "long" | "short" | undefined {
  const side = (value || "").trim().toLowerCase();
  if (side === "buy") return "long";
  if (side === "sell") return "short";
  if (side === "long" || side === "short") return side;
  return undefined;
}

function firstPositiveNumber(...values: Array<number | null | undefined>): number | null {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value) && value > 0) {
      return value;
    }
  }
  return null;
}

function buildBubblePositionLevelOverlays(
  candleSeries: ISeriesApi<"Candlestick">,
  bars: BubbleCandleSeries["bars"],
  position: PositionItem | undefined,
  events: BubbleCandleEvent[],
  chartWidth: number,
  chartHeight: number,
  axisWidth: number
): BubblePositionLevelOverlay[] {
  if (!position || bars.length === 0 || chartWidth <= 0 || chartHeight <= 0) {
    return [];
  }
  const side = normalizePositionSide(position.positionSide);
  if (!side) return [];

  const entryPrice = firstPositiveNumber(position.entryPrice);
  if (entryPrice == null) return [];

  const tpPrice = firstPositiveNumber(position.takeProfitPrice, resolveBubbleLatestMarkerPrice(events, "TP"));
  const slPrice = firstPositiveNumber(position.stopLossPrice, resolveBubbleLatestMarkerPrice(events, "SL"));
  const visibleBars = Math.max(1, Math.min(DEFAULT_VISIBLE_BARS, bars.length));
  const plotWidth = Math.max(36, chartWidth - axisWidth - 6);
  const lineLength = clampNumber(
    Math.round((plotWidth * Math.min(4, visibleBars)) / visibleBars),
    18,
    Math.max(18, Math.round(plotWidth * 0.18))
  );
  const axisOffset = Math.max(2, axisWidth + 2);
  const clampTop = 8;
  const clampBottom = Math.max(clampTop, chartHeight - 8);
  const overlays: BubblePositionLevelOverlay[] = [];

  const appendLevel = (kind: BubblePositionLevelKind, price: number | null) => {
    if (price == null) return;
    const rawY = candleSeries.priceToCoordinate(price);
    if (!Number.isFinite(rawY)) return;
    overlays.push({
      id: `bubble-position-level-${kind}`,
      kind,
      y: clampNumber(Number(rawY), clampTop, clampBottom),
      lineLength,
      axisOffset,
      side
    });
  };

  appendLevel("ENTRY", entryPrice);
  appendLevel("TP", tpPrice);
  appendLevel("SL", slPrice);
  return overlays;
}

function buildCandlesRequest(
  exchange: string,
  symbol: string,
  timeframes: string[],
  limit: number,
  requestPosition?: BubbleCandlesRequestPosition
): BubbleCandlesFetchRequest {
  const includeEvents = Boolean(requestPosition);
  return {
    closedOnly: true,
    includeEvents,
    eventLimit: includeEvents ? DEFAULT_EVENT_LIMIT : undefined,
    requests: [
      {
        exchange,
        symbol,
        timeframes,
        limit,
        position: includeEvents ? requestPosition : undefined
      }
    ]
  };
}

export function BubbleKlinePanel(props: BubbleKlinePanelProps) {
  const { selected, matchedPosition, allDataList } = props;
  const requestBubbleCandles = useAppStore((s) => s.requestBubbleCandles);
  const dataSourceStatus = useAppStore((s) => s.dataSourceStatus);

  const chartBodyRef = useRef<HTMLDivElement | null>(null);
  const chartContainerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const ema5SeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const ema20SeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const ema60SeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const seriesMapRef = useRef<Record<string, BubbleCandleSeries>>({});
  const fetchCounterRef = useRef(0);
  const fetchRequestSeqRef = useRef(0);
  const priceDisplayKeyRef = useRef("");

  const [selectedExchange, setSelectedExchange] = useState("");
  const [activeTimeframe, setActiveTimeframe] = useState("");
  const [isForegroundLoading, setIsForegroundLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [warnings, setWarnings] = useState<string[]>([]);
  const [lastFetchedAt, setLastFetchedAt] = useState<number | undefined>(undefined);
  const [seriesMap, setSeriesMap] = useState<Record<string, BubbleCandleSeries>>({});
  const [events, setEvents] = useState<BubbleCandleEvent[]>([]);
  const [eventPosition, setEventPosition] = useState<BubbleCandlePosition | undefined>(undefined);
  const [overlayTick, setOverlayTick] = useState(0);
  const [hoveredMarker, setHoveredMarker] = useState<BubbleMarkerTooltipState | null>(null);
  const [crosshairAxisLabel, setCrosshairAxisLabel] = useState<CrosshairAxisLabelState | null>(null);
  const [crosshairTimeLabel, setCrosshairTimeLabel] = useState<CrosshairTimeLabelState | null>(null);
  const [indicators, setIndicators] = useState<IndicatorState>({
    ema5: true,
    ema20: true,
    ema60: true
  });

  const preferredExchanges = useMemo(
    () => resolvePreferredExchanges(selected, matchedPosition, allDataList),
    [allDataList, matchedPosition, selected]
  );
  const preferredTimeframes = useMemo(
    () => resolveStrategyTimeframes(selected, matchedPosition, allDataList),
    [allDataList, matchedPosition, selected]
  );
  const preferredExchangesKey = useMemo(() => preferredExchanges.join("|"), [preferredExchanges]);
  const preferredTimeframesKey = useMemo(() => preferredTimeframes.join("|"), [preferredTimeframes]);
  const stablePreferredExchanges = useMemo(
    () => (preferredExchangesKey ? preferredExchangesKey.split("|").filter(Boolean) : []),
    [preferredExchangesKey]
  );
  const stablePreferredTimeframes = useMemo(
    () => (preferredTimeframesKey ? preferredTimeframesKey.split("|").filter(Boolean) : []),
    [preferredTimeframesKey]
  );
  const targetSymbol = useMemo(
    () => normalizeSymbol(matchedPosition?.symbol || selected.symbol),
    [matchedPosition?.symbol, selected.symbol]
  );
  const hasMatchedPosition = Boolean(matchedPosition);
  const matchedPositionID = matchedPosition?.positionId;
  const matchedPositionSide = normalizePositionSide(matchedPosition?.positionSide);
  const matchedPositionMarginMode = (matchedPosition?.marginMode || "").trim().toLowerCase() || undefined;
  const matchedPositionEntryTime = (matchedPosition?.entryTime || "").trim() || undefined;
  const matchedPositionStrategyName = (matchedPosition?.strategyName || "").trim() || undefined;
  const matchedPositionStrategyVersion = (matchedPosition?.strategyVersion || "").trim() || undefined;
  const requestPositionContext = useMemo<BubbleCandlesRequestPosition | undefined>(() => {
    if (!hasMatchedPosition) return undefined;
    return {
      positionId: matchedPositionID,
      positionSide: matchedPositionSide,
      marginMode: matchedPositionMarginMode,
      entryTime: matchedPositionEntryTime,
      strategyName: matchedPositionStrategyName,
      strategyVersion: matchedPositionStrategyVersion
    };
  }, [
    hasMatchedPosition,
    matchedPositionID,
    matchedPositionEntryTime,
    matchedPositionMarginMode,
    matchedPositionSide,
    matchedPositionStrategyName,
    matchedPositionStrategyVersion
  ]);
  const requestPositionContextKey = useMemo(() => {
    if (!hasMatchedPosition) return "";
    return [
      matchedPositionID ?? 0,
      matchedPositionSide || "",
      matchedPositionMarginMode || "",
      matchedPositionEntryTime || "",
      matchedPositionStrategyName || "",
      matchedPositionStrategyVersion || ""
    ].join("|");
  }, [
    hasMatchedPosition,
    matchedPositionID,
    matchedPositionEntryTime,
    matchedPositionMarginMode,
    matchedPositionSide,
    matchedPositionStrategyName,
    matchedPositionStrategyVersion
  ]);

  useEffect(() => {
    const nextExchange = stablePreferredExchanges[0] || "";
    setSelectedExchange((prev) => (stablePreferredExchanges.includes(prev) ? prev : nextExchange));
  }, [stablePreferredExchanges]);

  useEffect(() => {
    const nextTimeframe = pickLargestTimeframe(stablePreferredTimeframes);
    setActiveTimeframe((prev) =>
      stablePreferredTimeframes.includes(prev) ? prev : nextTimeframe
    );
  }, [stablePreferredTimeframes]);

  useEffect(() => {
    seriesMapRef.current = seriesMap;
  }, [seriesMap]);

  useEffect(() => {
    // 切换交易标的或周期集合后，下一次轮询回到全量同步。
    fetchCounterRef.current = 0;
  }, [preferredTimeframesKey, selectedExchange, targetSymbol]);

  useEffect(() => {
    const container = chartContainerRef.current;
    if (!container) return;
    const rootStyle = getComputedStyle(document.documentElement);
    const panelStrong = rootStyle.getPropertyValue("--panel-strong").trim() || "#1c1c1e";
    const muted = rootStyle.getPropertyValue("--muted").trim() || "rgba(242,242,247,0.58)";

    const chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: panelStrong },
        textColor: muted,
        fontSize: 11
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.05)" },
        horzLines: { color: "rgba(255,255,255,0.05)" }
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.12)",
        scaleMargins: PRICE_SCALE_MARGINS
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.12)",
        timeVisible: true,
        secondsVisible: false,
        minBarSpacing: 0.5,
        maxBarSpacing: 50
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "rgba(232,241,255,0.55)", width: 1, labelVisible: false },
        horzLine: { color: "rgba(232,241,255,0.55)", width: 1, labelVisible: false }
      },
      handleScroll: false,
      handleScale: false
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#00c897",
      downColor: "#ff5b68",
      wickUpColor: "#00c897",
      wickDownColor: "#ff5b68",
      borderVisible: false,
      lastValueVisible: false,
      priceLineVisible: false
    });
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      lastValueVisible: false,
      priceFormat: { type: "volume" }
    });
    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 }
    });

    const ema5Series = chart.addSeries(LineSeries, {
      color: EMA_COLORS.ema5,
      lineWidth: 1.6,
      lastValueVisible: false,
      priceLineVisible: false
    });
    const ema20Series = chart.addSeries(LineSeries, {
      color: EMA_COLORS.ema20,
      lineWidth: 1.6,
      lastValueVisible: false,
      priceLineVisible: false
    });
    const ema60Series = chart.addSeries(LineSeries, {
      color: EMA_COLORS.ema60,
      lineWidth: 1.6,
      lastValueVisible: false,
      priceLineVisible: false
    });

    chart.applyOptions({
      localization: {
        locale: "en-US",
        timeFormatter: formatChartTimeLocal
      },
      timeScale: {
        tickMarkFormatter: (time) => formatChartTimeLocal(time)
      }
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;
    ema5SeriesRef.current = ema5Series;
    ema20SeriesRef.current = ema20Series;
    ema60SeriesRef.current = ema60Series;

    const repaint = () => setOverlayTick((value) => value + 1);
    const resize = () => {
      const width = container.clientWidth;
      const height = container.clientHeight;
      if (width <= 0 || height <= 0) return;
      chart.applyOptions({ width, height });
      repaint();
    };

    const observer = new ResizeObserver(resize);
    observer.observe(container);
    chart.timeScale().subscribeVisibleLogicalRangeChange(repaint);
    resize();

    return () => {
      observer.disconnect();
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(repaint);
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      ema5SeriesRef.current = null;
      ema20SeriesRef.current = null;
      ema60SeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!selectedExchange || !targetSymbol || stablePreferredTimeframes.length === 0) {
      setSeriesMap({});
      setEvents([]);
      setEventPosition(undefined);
      setWarnings([]);
      setErrorMessage("");
      setIsForegroundLoading(false);
      setHoveredMarker(null);
      return;
    }

    let canceled = false;
    const runFetch = async (mode: "foreground" | "background") => {
      const requestSeq = fetchRequestSeqRef.current + 1;
      fetchRequestSeqRef.current = requestSeq;
      const shouldFullSync =
        fetchCounterRef.current === 0 || fetchCounterRef.current >= FULL_RESYNC_EVERY_POLLS;
      const requestLimit = shouldFullSync ? DEFAULT_CANDLE_LIMIT : INCREMENTAL_CANDLE_LIMIT;
      if (mode === "foreground") {
        setIsForegroundLoading(true);
      }
      try {
        const snapshot = await requestBubbleCandles(
          buildCandlesRequest(
            selectedExchange,
            targetSymbol,
            stablePreferredTimeframes,
            requestLimit,
            requestPositionContext
          )
        );
        if (canceled || requestSeq !== fetchRequestSeqRef.current) return;
        if (!snapshot || snapshot.items.length === 0) {
          setSeriesMap({});
          setEvents([]);
          setEventPosition(undefined);
          setWarnings([]);
          setErrorMessage("K线数据暂不可用");
          fetchCounterRef.current = 0;
          return;
        }

        const selectedItem =
          snapshot.items.find(
            (item) =>
              item.exchange === selectedExchange &&
              symbolsLikelyMatch(item.symbol, targetSymbol)
          ) ||
          snapshot.items.find((item) => symbolsLikelyMatch(item.symbol, targetSymbol)) ||
          snapshot.items[0];

        if (!selectedItem) {
          setSeriesMap({});
          setEvents([]);
          setEventPosition(undefined);
          setWarnings([]);
          setErrorMessage("K线数据匹配失败");
          fetchCounterRef.current = 0;
          return;
        }

        const incomingSeriesMap = normalizeSeriesMap(selectedItem.series, DEFAULT_CANDLE_LIMIT);
        const nextSeriesMap = shouldFullSync
          ? incomingSeriesMap
          : mergeSeriesMaps(seriesMapRef.current, incomingSeriesMap, DEFAULT_CANDLE_LIMIT);
        seriesMapRef.current = nextSeriesMap;
        setSeriesMap(nextSeriesMap);
        setEvents(Array.isArray(selectedItem.events) ? selectedItem.events : []);
        setEventPosition(selectedItem.position || undefined);
        setWarnings(snapshot.warnings);
        setLastFetchedAt(snapshot.fetchedAt);
        setErrorMessage("");
        const nextTimeframes = sortTimeframes(Object.keys(nextSeriesMap));
        if (nextTimeframes.length > 0) {
          setActiveTimeframe((prev) =>
            nextTimeframes.includes(prev) ? prev : pickLargestTimeframe(nextTimeframes)
          );
        }
        if (shouldFullSync) {
          fetchCounterRef.current = 1;
        } else {
          fetchCounterRef.current += 1;
        }
      } catch (error) {
        if (canceled || requestSeq !== fetchRequestSeqRef.current) return;
        console.warn("[bubble-kline] fetch candles failed", error);
        setSeriesMap({});
        setEvents([]);
        setEventPosition(undefined);
        setWarnings([]);
        setErrorMessage("K线数据暂不可用");
        fetchCounterRef.current = 0;
      } finally {
        if (mode === "foreground" && !canceled && requestSeq === fetchRequestSeqRef.current) {
          setIsForegroundLoading(false);
        }
      }
    };

    void runFetch("foreground");
    const timer = window.setInterval(() => {
      void runFetch("background");
    }, DEFAULT_POLL_INTERVAL_MS);

    return () => {
      canceled = true;
      window.clearInterval(timer);
    };
  }, [
    requestBubbleCandles,
    requestPositionContext,
    requestPositionContextKey,
    selectedExchange,
    stablePreferredTimeframes,
    targetSymbol
  ]);

  const availableTimeframes = useMemo(
    () => sortTimeframes(Object.keys(seriesMap)),
    [seriesMap]
  );

  const activeSeries = useMemo(() => {
    if (!activeTimeframe) return undefined;
    return seriesMap[activeTimeframe];
  }, [activeTimeframe, seriesMap]);

  const activeBars = useMemo(() => dedupeAndSortBars(activeSeries), [activeSeries]);
  const activePriceDisplay = useMemo(() => buildDynamicPriceDisplay(activeBars), [activeBars]);

  const overlayMarkers = useMemo(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const chartBody = chartBodyRef.current;
    if (!chart || !candleSeries || !chartBody || activeBars.length === 0 || events.length === 0) {
      return [];
    }
    return buildBubbleOverlayMarkers(
      chart,
      candleSeries,
      activeBars,
      events,
      eventPosition,
      activeTimeframe,
      chartBody.clientHeight
    );
  }, [activeBars, activeTimeframe, eventPosition, events, overlayTick]);

  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const chartBody = chartBodyRef.current;
    if (!chart || !candleSeries || !chartBody || activeBars.length === 0) {
      setCrosshairAxisLabel(null);
      setCrosshairTimeLabel(null);
      return;
    }

    const latestClose = activeBars[activeBars.length - 1]?.close;
    if (!Number.isFinite(latestClose) || latestClose <= 0) {
      setCrosshairAxisLabel(null);
      setCrosshairTimeLabel(null);
      return;
    }

    const handleCrosshairMove = (param: { point?: { x: number; y: number }; time?: Time }) => {
      const point = param.point;
      if (!point || param.time == null) {
        setCrosshairAxisLabel(null);
        setCrosshairTimeLabel(null);
        return;
      }

      const chartWidth = chartBody.clientWidth;
      const chartHeight = chartBody.clientHeight;
      if (chartWidth <= 0 || chartHeight <= 0) {
        setCrosshairAxisLabel(null);
        setCrosshairTimeLabel(null);
        return;
      }

      const plotWidth = Math.max(0, chartWidth - activePriceDisplay.axisWidth);
      if (point.x < 0 || point.x > plotWidth || point.y < 0 || point.y > chartHeight) {
        setCrosshairAxisLabel(null);
        setCrosshairTimeLabel(null);
        return;
      }

      const priceTop = chartHeight * PRICE_SCALE_MARGINS.top;
      const priceBottom = chartHeight * (1 - PRICE_SCALE_MARGINS.bottom);
      if (point.y < priceTop || point.y > priceBottom) {
        setCrosshairAxisLabel(null);
        setCrosshairTimeLabel(null);
        return;
      }

      const price = candleSeries.coordinateToPrice(point.y);
      if (!Number.isFinite(price)) {
        setCrosshairAxisLabel(null);
        setCrosshairTimeLabel(null);
        return;
      }

      const pct = ((Number(price) - latestClose) / latestClose) * 100;
      const top = clampNumber(
        point.y - CROSSHAIR_AXIS_LABEL_HEIGHT * 0.5,
        4,
        Math.max(4, chartHeight - CROSSHAIR_AXIS_LABEL_HEIGHT - 4)
      );
      setCrosshairAxisLabel({
        top,
        priceText: formatPriceByDisplay(Number(price), activePriceDisplay),
        percentText: formatSignedPercent(pct)
      });
      const timeText = formatChartTimeLocal(param.time);
      const left = clampNumber(
        point.x - CROSSHAIR_TIME_LABEL_WIDTH * 0.5,
        4,
        Math.max(4, plotWidth - CROSSHAIR_TIME_LABEL_WIDTH - 4)
      );
      setCrosshairTimeLabel({
        left,
        text: timeText
      });
    };

    chart.subscribeCrosshairMove(handleCrosshairMove);
    return () => {
      chart.unsubscribeCrosshairMove(handleCrosshairMove);
    };
  }, [activeBars, activePriceDisplay]);

  const positionLevels = useMemo(() => {
    const candleSeries = candleSeriesRef.current;
    const chartBody = chartBodyRef.current;
    if (!candleSeries || !chartBody || activeBars.length === 0 || !matchedPosition) {
      return [];
    }
    return buildBubblePositionLevelOverlays(
      candleSeries,
      activeBars,
      matchedPosition,
      events,
      chartBody.clientWidth,
      chartBody.clientHeight,
      activePriceDisplay.axisWidth
    );
  }, [activeBars, activePriceDisplay.axisWidth, events, matchedPosition, overlayTick]);

  useEffect(() => {
    if (!hoveredMarker) return;
    if (!overlayMarkers.some((item) => item.id === hoveredMarker.markerID)) {
      setHoveredMarker(null);
    }
  }, [hoveredMarker, overlayMarkers]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (shouldIgnoreShortcut(event)) return;
      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
      const index = getDigitIndex(event.key);
      if (index == null) return;
      const next = availableTimeframes[index];
      if (!next) return;
      event.preventDefault();
      setActiveTimeframe(next);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [availableTimeframes]);

  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    const ema5Series = ema5SeriesRef.current;
    const ema20Series = ema20SeriesRef.current;
    const ema60Series = ema60SeriesRef.current;
    if (!chart || !candleSeries || !volumeSeries || !ema5Series || !ema20Series || !ema60Series) return;

    if (activeBars.length === 0) {
      candleSeries.setData([]);
      volumeSeries.setData([]);
      ema5Series.setData([]);
      ema20Series.setData([]);
      ema60Series.setData([]);
      setOverlayTick((value) => value + 1);
      return;
    }

    const candleData: CandlestickData<Time>[] = activeBars.map((bar) => ({
      time: Math.floor(bar.ts / 1000) as Time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close
    }));
    candleSeries.setData(candleData);

    if (priceDisplayKeyRef.current !== activePriceDisplay.key) {
      priceDisplayKeyRef.current = activePriceDisplay.key;
      chart.applyOptions({
        rightPriceScale: {
          minimumWidth: activePriceDisplay.axisWidth
        }
      });
      candleSeries.applyOptions({ priceFormat: activePriceDisplay.priceFormat });
      ema5Series.applyOptions({ priceFormat: activePriceDisplay.priceFormat });
      ema20Series.applyOptions({ priceFormat: activePriceDisplay.priceFormat });
      ema60Series.applyOptions({ priceFormat: activePriceDisplay.priceFormat });
    }

    const volumeData: HistogramData<Time>[] = activeBars.map((bar) => ({
      time: Math.floor(bar.ts / 1000) as Time,
      value: bar.volume,
      color: bar.close >= bar.open ? "rgba(0,200,151,0.25)" : "rgba(255,91,104,0.25)"
    }));
    volumeSeries.setData(volumeData);

    const closes = activeBars.map((bar) => bar.close);
    const ema5 = computeEMA(closes, 5);
    const ema20 = computeEMA(closes, 20);
    const ema60 = computeEMA(closes, 60);

    const toLineData = (values: number[]): LineData<Time>[] =>
      values.map((value, index) => ({
        time: Math.floor(activeBars[index].ts / 1000) as Time,
        value
      }));

    ema5Series.setData(indicators.ema5 ? toLineData(ema5) : []);
    ema20Series.setData(indicators.ema20 ? toLineData(ema20) : []);
    ema60Series.setData(indicators.ema60 ? toLineData(ema60) : []);

    const total = candleData.length;
    const from = Math.max(0, total - DEFAULT_VISIBLE_BARS);
    const to = Math.max(total - 1, 0);
    chart.timeScale().setVisibleLogicalRange({ from, to });
    setOverlayTick((value) => value + 1);
  }, [activeBars, activePriceDisplay, indicators]);

  const toggleIndicator = (key: keyof IndicatorState) => {
    setIndicators((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const requestedBars = activeSeries?.requested || DEFAULT_CANDLE_LIMIT;
  const returnedBars = activeSeries?.returned || activeBars.length;
  const wsUnavailable = dataSourceStatus.wsStatus !== "open" && dataSourceStatus.wsStatus !== "connecting";
  const disableExchangeSwitch = isForegroundLoading && availableTimeframes.length === 0;

  return (
    <section className="bubble-kline-panel bubble-kline-panel--full-bleed" aria-label="气泡K线窗口">
      <div className="bubble-kline-panel__header">
        <div className="bubble-kline-panel__group bubble-kline-panel__group--exchange">
          <div className="bubble-kline-panel__chips">
            {stablePreferredExchanges.map((exchange) => (
              <button
                key={exchange}
                type="button"
                className={`bubble-kline-panel__chip ${
                  exchange === selectedExchange ? "is-active" : ""
                }`}
                onClick={() => setSelectedExchange(exchange)}
                disabled={disableExchangeSwitch}
              >
                {exchange.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
        <div className="bubble-kline-panel__group bubble-kline-panel__group--timeframe">
          <div className="bubble-kline-panel__chips">
            {availableTimeframes.map((timeframe) => (
              <button
                key={timeframe}
                type="button"
                className={`bubble-kline-panel__chip ${
                  timeframe === activeTimeframe ? "is-active" : ""
                }`}
                onClick={() => setActiveTimeframe(timeframe)}
              >
                {timeframe}
              </button>
            ))}
          </div>
        </div>
        <div className="bubble-kline-panel__group bubble-kline-panel__group--indicators">
          <div className="bubble-kline-panel__chips bubble-kline-panel__chips--indicators">
            <button
              type="button"
              className={`bubble-kline-panel__toggle ${indicators.ema5 ? "is-on" : ""}`}
              onClick={() => toggleIndicator("ema5")}
            >
              EMA 5
            </button>
            <button
              type="button"
              className={`bubble-kline-panel__toggle ${indicators.ema20 ? "is-on" : ""}`}
              onClick={() => toggleIndicator("ema20")}
            >
              EMA 20
            </button>
            <button
              type="button"
              className={`bubble-kline-panel__toggle ${indicators.ema60 ? "is-on" : ""}`}
              onClick={() => toggleIndicator("ema60")}
            >
              EMA 60
            </button>
          </div>
        </div>
      </div>
      <div ref={chartBodyRef} className="bubble-kline-panel__chart">
        <div ref={chartContainerRef} className="bubble-kline-panel__chart-canvas" />
        <div className="bubble-kline-panel__crosshair-axis-overlay">
          {crosshairAxisLabel ? (
            <div
              className="bubble-kline-panel__crosshair-axis-label"
              style={{
                top: `${crosshairAxisLabel.top}px`,
                width: `${activePriceDisplay.axisWidth}px`
              }}
            >
              <span>{crosshairAxisLabel.priceText}</span>
              <span>{crosshairAxisLabel.percentText}</span>
            </div>
          ) : null}
        </div>
        <div className="bubble-kline-panel__crosshair-time-overlay">
          {crosshairTimeLabel ? (
            <div
              className="bubble-kline-panel__crosshair-time-label"
              style={{
                left: `${crosshairTimeLabel.left}px`,
                width: `${CROSSHAIR_TIME_LABEL_WIDTH}px`
              }}
            >
              {crosshairTimeLabel.text}
            </div>
          ) : null}
        </div>
        <div className="bubble-kline-panel__level-overlay">
          {positionLevels.map((level) => {
            const levelStyle: CSSProperties &
              Record<"--bubble-kline-level-line-length" | "--bubble-kline-level-axis-offset", string> = {
              top: `${level.y}px`,
              "--bubble-kline-level-line-length": `${level.lineLength}px`,
              "--bubble-kline-level-axis-offset": `${level.axisOffset}px`
            };
            const levelClassName =
              level.kind === "ENTRY"
                ? level.side === "short"
                  ? "bubble-kline-level is-entry is-short"
                  : "bubble-kline-level is-entry"
                : level.kind === "TP"
                  ? "bubble-kline-level is-tp"
                  : "bubble-kline-level is-sl";
            return <div key={level.id} className={levelClassName} style={levelStyle} />;
          })}
        </div>
        <div className="bubble-kline-panel__event-overlay">
          {overlayMarkers.map((marker) => {
            if (marker.kind === "ENTRY" || marker.kind === "EXIT") {
              const arrowStyle: CSSProperties & Record<"--bubble-kline-arrow-line-length", string> = {
                left: `${marker.x}px`,
                top: `${marker.y}px`,
                "--bubble-kline-arrow-line-length": `${marker.arrowLength}px`
              };
              return (
                <div
                  key={marker.id}
                  className={
                    marker.kind === "ENTRY"
                      ? "bubble-kline-chart-arrow is-entry"
                      : "bubble-kline-chart-arrow is-exit"
                  }
                  style={arrowStyle}
                >
                  <span className="bubble-kline-chart-arrow-label">{marker.kind}</span>
                  <span className="bubble-kline-chart-arrow-line" />
                  <span className="bubble-kline-chart-arrow-head" />
                </div>
              );
            }
            return (
              <div key={marker.id}>
                {marker.clampGuideHeight > 0 ? (
                  <div
                    className={`bubble-kline-chart-clamp-guide ${
                      marker.kind === "TP" ? "is-tp" : "is-sl"
                    }`}
                    style={{
                      left: `${marker.x}px`,
                      top: `${marker.clampGuideTop}px`,
                      height: `${marker.clampGuideHeight}px`
                    }}
                  />
                ) : null}
                <div
                  className={
                    marker.kind === "TP"
                      ? "bubble-kline-chart-badge is-tp"
                      : "bubble-kline-chart-badge is-sl"
                  }
                  style={{
                    left: `${marker.x}px`,
                    top: `${marker.y}px`,
                    width: `${marker.size}px`,
                    height: `${marker.size}px`,
                    fontSize: `${Math.max(9, Math.min(12, marker.size * 0.38))}px`
                  }}
                  onPointerEnter={(event) => {
                    setHoveredMarker(
                      buildBubbleMarkerTooltipState(
                        marker,
                        event.clientX,
                        event.clientY,
                        chartBodyRef.current
                      )
                    );
                  }}
                  onPointerMove={(event) => {
                    setHoveredMarker(
                      buildBubbleMarkerTooltipState(
                        marker,
                        event.clientX,
                        event.clientY,
                        chartBodyRef.current
                      )
                    );
                  }}
                  onPointerLeave={() => {
                    setHoveredMarker((current) => (current?.markerID === marker.id ? null : current));
                  }}
                >
                  {marker.kind === "TP" ? "T" : "S"}
                </div>
              </div>
            );
          })}
        </div>
        {hoveredMarker ? (
          <div
            className="bubble-kline-marker-tooltip"
            style={{
              left: `${hoveredMarker.left}px`,
              top: `${hoveredMarker.top}px`
            }}
          >
            <div className="bubble-kline-marker-tooltip__title">{hoveredMarker.kind}</div>
            <div className="bubble-kline-marker-tooltip__value">
              {formatBubbleMarkerPrice(hoveredMarker.price)}
            </div>
            {hoveredMarker.clampDirection ? (
              <div className="bubble-kline-marker-tooltip__note">
                {hoveredMarker.clampDirection === "top"
                  ? "超出当前显示范围（顶部）"
                  : "超出当前显示范围（底部）"}
              </div>
            ) : null}
          </div>
        ) : null}
        {isForegroundLoading ? <div className="bubble-kline-panel__overlay">K线加载中...</div> : null}
        {!isForegroundLoading && errorMessage ? <div className="bubble-kline-panel__overlay">{errorMessage}</div> : null}
        {!isForegroundLoading && !errorMessage && wsUnavailable ? (
          <div className="bubble-kline-panel__overlay bubble-kline-panel__overlay--subtle">
            WS 连接未就绪，K线可能延迟
          </div>
        ) : null}
      </div>
      <div className="bubble-kline-panel__meta bubble-kline-panel__meta--below">
        <div>Bars: {Math.min(DEFAULT_VISIBLE_BARS, returnedBars)}/{requestedBars}</div>
        <div>CrosshairMode: Normal</div>
        <div>Events: {events.length}</div>
        <div>Update: {lastFetchedAt ? formatLocalDateTime(lastFetchedAt) : "--"}</div>
      </div>
      {warnings.length > 0 ? (
        <div className="bubble-kline-panel__warnings">
          {warnings.map((item, index) => (
            <span key={`${item}-${index}`}>{item}</span>
          ))}
        </div>
      ) : null}
    </section>
  );
}
