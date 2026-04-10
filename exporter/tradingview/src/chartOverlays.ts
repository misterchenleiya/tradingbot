import type { IChartApi, ISeriesApi, Time } from "lightweight-charts";
import type { TradingViewCandle, TradingViewEventEntry } from "./types";

export interface ChartRangeSelection {
  rangeStartMS: number;
  rangeEndMS: number;
  priceLow: number;
  priceHigh: number;
  selectionDirection: "up" | "down";
}

export interface TradingViewEventMarker {
  id: string;
  eventID: string;
  label: string;
  kind: string;
  variant: "badge" | "arrow";
  colorClass: string;
  left: number;
  top: number;
  title: string;
  price: number;
  clampDirection: "top" | "bottom" | null;
  tooltipTitle: string;
  tooltipRows: TradingViewEventMarkerTooltipRow[];
  arrowLength?: number;
}

export interface TradingViewEventMarkerTooltipRow {
  label: string;
  value: string;
}

interface RawMarker {
  id: string;
  eventID: string;
  candleTS: number;
  eventAtMS: number;
  kind: string;
  label: string;
  variant: "badge" | "arrow";
  colorClass: string;
  x: number;
  anchorX: number;
  price: number;
  rawY: number;
  title: string;
  tooltipTitle: string;
  tooltipRows: TradingViewEventMarkerTooltipRow[];
  candleHalfWidth: number;
}

const MAX_TOTAL_MARKERS = 1200;
const MARKER_SIZE = 18;
const ENTRY_EXECUTION_MATCH_WINDOW_MS = 5 * 60 * 1000;

export function buildTradingViewEventMarkers(
  chart: IChartApi,
  candleSeries: ISeriesApi<"Candlestick">,
  candles: TradingViewCandle[],
  events: TradingViewEventEntry[],
  chartHeight: number,
  priceFormatter: (value: number) => string
): TradingViewEventMarker[] {
  if (candles.length === 0 || events.length === 0 || chartHeight <= 0) {
    return [];
  }
  const timeScale = chart.timeScale();
  const markerByKey = new Map<string, RawMarker>();
  const sortedEvents = [...events].sort((left, right) => left.event_at_ms - right.event_at_ms);
  const preferredEntryPriceByEventID = buildPreferredEntryPriceByEventID(sortedEvents);
  for (const event of sortedEvents) {
    const kind = resolveEventMarkerKind(event);
    if (!kind) {
      continue;
    }
    const nearestIndex = findNearestCandleIndex(event.event_at_ms, candles);
    if (nearestIndex < 0) {
      continue;
    }
    const nearest = candles[nearestIndex];
    const x = timeScale.timeToCoordinate(Math.floor(nearest.ts / 1000) as Time);
    if (!Number.isFinite(x)) {
      continue;
    }
    const candleHalfWidth = estimateCandleHalfWidth(timeScale, candles, nearestIndex);
    const price = resolveEventMarkerPrice(kind, event, nearest, preferredEntryPriceByEventID.get(event.id));
    const rawY = candleSeries.priceToCoordinate(price);
    if (!Number.isFinite(rawY)) {
      continue;
    }
    const key = `${kind.id}|${nearest.ts}`;
    const candidate: RawMarker = {
      id: `${event.id}-${kind.id}`,
      eventID: event.id,
      candleTS: nearest.ts,
      eventAtMS: event.event_at_ms,
      kind: kind.id,
      label: kind.label,
      variant: kind.id === "ENTRY" || kind.id === "EXIT" ? "arrow" : "badge",
      colorClass: kind.colorClass,
      x: Number(x),
      anchorX: kind.id === "ENTRY" ? Number(x) - candleHalfWidth : Number(x) + candleHalfWidth,
      price,
      rawY: Number(rawY),
      title: buildMarkerTitle(event, kind.label),
      tooltipTitle: markerTooltipTitle(kind.id),
      tooltipRows: buildMarkerTooltipRows(kind.id, event, priceFormatter),
      candleHalfWidth
    };
    const previous = markerByKey.get(key);
    if (previous && previous.eventAtMS > candidate.eventAtMS) {
      continue;
    }
    if (!previous && markerByKey.size >= MAX_TOTAL_MARKERS) {
      break;
    }
    markerByKey.set(key, candidate);
  }
  const rawMarkers = Array.from(markerByKey.values()).sort((left, right) => {
    if (left.candleTS !== right.candleTS) {
      return left.candleTS - right.candleTS;
    }
    return left.eventAtMS - right.eventAtMS;
  });
  const placed = new Map<number, number>();
  const out: TradingViewEventMarker[] = [];
  const clampTop = 10;
  const clampBottom = Math.max(10, chartHeight - 10);
  for (const marker of rawMarkers) {
    let top = clamp(marker.rawY - MARKER_SIZE * 0.5, 6, Math.max(6, chartHeight - MARKER_SIZE - 6));
    const bucket = Math.round(marker.x);
    const previousTop = placed.get(bucket);
    if (typeof previousTop === "number" && Math.abs(previousTop - top) < MARKER_SIZE + 2) {
      top = clamp(previousTop + MARKER_SIZE + 4, 6, Math.max(6, chartHeight - MARKER_SIZE - 6));
    }
    placed.set(bucket, top);
    const clampDirection = marker.rawY < clampTop ? "top" : marker.rawY > clampBottom ? "bottom" : null;
    if (marker.variant === "arrow") {
      out.push({
        id: marker.id,
        eventID: marker.eventID,
        label: marker.label,
        kind: marker.kind,
        variant: "arrow",
        colorClass: marker.colorClass,
        left: marker.anchorX,
        top: clamp(marker.rawY, clampTop, clampBottom),
        title: marker.title,
        price: marker.price,
        clampDirection,
        tooltipTitle: marker.tooltipTitle,
        tooltipRows: marker.tooltipRows,
        arrowLength: estimateEventArrowLength(marker.candleHalfWidth)
      });
      continue;
    }
    out.push({
      id: marker.id,
      eventID: marker.eventID,
      label: marker.label,
      kind: marker.kind,
      variant: "badge",
      colorClass: marker.colorClass,
      left: marker.x - MARKER_SIZE * 0.5,
      top,
      title: marker.title,
      price: marker.price,
      clampDirection,
      tooltipTitle: marker.tooltipTitle,
      tooltipRows: marker.tooltipRows
    });
  }
  return out;
}

function buildPreferredEntryPriceByEventID(events: TradingViewEventEntry[]): Map<string, number> {
  const openExecutions = events
    .filter((event) => normalizeEventToken(event.type) === "EXECUTION")
    .map((event) => {
      const action = normalizeEventToken(readFirstString(event.detail, ["action"]));
      const result = normalizeEventToken(readFirstString(event.detail, ["result_status"]));
      const price = readFirstNumber(event.detail, ["fill_price", "price", "entry_price", "avg_px"]);
      return {
        action,
        result,
        price,
        eventAtMS: event.event_at_ms
      };
    })
    .filter(
      (event) =>
        Number.isFinite(event.price) &&
        isExecutionOpenAction(event.action) &&
        isSuccessfulExecutionResult(event.result)
    );
  if (openExecutions.length === 0) {
    return new Map<string, number>();
  }
  const preferred = new Map<string, number>();
  for (const event of events) {
    if (normalizeEventToken(event.type) !== "ENTRY") {
      continue;
    }
    let candidate: { price: number; eventAtMS: number } | null = null;
    for (const execution of openExecutions) {
      if (Math.abs(execution.eventAtMS - event.event_at_ms) > ENTRY_EXECUTION_MATCH_WINDOW_MS) {
        continue;
      }
      if (!candidate || execution.eventAtMS < candidate.eventAtMS) {
        candidate = { price: execution.price as number, eventAtMS: execution.eventAtMS };
      }
    }
    if (candidate) {
      preferred.set(event.id, candidate.price);
    }
  }
  return preferred;
}

function resolveEventMarkerKind(
  event: TradingViewEventEntry
): { id: string; label: string; colorClass: string } | null {
  const type = normalizeEventToken(event.type);
  const marker = normalizeEventToken(readFirstString(event.detail, ["marker"]));
  switch (type) {
    case "ENTRY":
      return { id: "ENTRY", label: "ENTRY", colorClass: "is-entry" };
    case "EXIT":
      return { id: "EXIT", label: "EXIT", colorClass: "is-exit" };
    case "EXECUTION":
      return { id: "EXECUTION", label: "E", colorClass: "is-execution" };
    case "ARMED":
      return { id: "ARMED", label: "A", colorClass: "is-armed" };
    case "TREND_DETECTED":
      return { id: "TREND", label: "N", colorClass: "is-trend" };
    case "HIGH_SIDE_CHANGED":
      return { id: "HIGH", label: "H", colorClass: "is-high" };
    case "MID_SIDE_CHANGED":
      return { id: "MID", label: "M", colorClass: "is-mid" };
    case "R_PROTECT_2R":
      return { id: "R2", label: "2R", colorClass: "is-2r" };
    case "R_PROTECT_4R":
      return { id: "R4", label: "4R", colorClass: "is-4r" };
    case "TRAILING_TP":
    case "TP":
      return { id: "TP", label: "T", colorClass: "is-tp" };
    case "TRAILING_STOP":
    case "SL":
      return { id: "SL", label: "S", colorClass: "is-sl" };
    case "TRAILING_TP_SL":
      return { id: "TP", label: "T", colorClass: "is-tp" };
    default:
      if (marker === "TP") {
        return { id: "TP", label: "T", colorClass: "is-tp" };
      }
      if (marker === "SL") {
        return { id: "SL", label: "S", colorClass: "is-sl" };
      }
      return null;
  }
}

function resolveEventMarkerPrice(
  kind: { id: string },
  event: TradingViewEventEntry,
  candle: TradingViewCandle,
  preferredEntryPrice?: number
): number {
  const detail = event.detail;
  if (kind.id === "TP") {
    return readFirstNumber(detail, ["tp_price", "take_profit_price", "tp"]) ?? candle.high;
  }
  if (kind.id === "SL") {
    return readFirstNumber(detail, ["sl_price", "stop_loss_price", "sl"]) ?? candle.low;
  }
  if (kind.id === "ENTRY") {
    return preferredEntryPrice ?? readFirstNumber(detail, ["entry_price", "price", "fill_price", "avg_px"]) ?? candle.close;
  }
  if (kind.id === "EXIT") {
    return readFirstNumber(detail, ["exit_price", "price", "fill_price", "close_avg_px"]) ?? candle.close;
  }
  if (kind.id === "EXECUTION") {
    const action = normalizeEventToken(readFirstString(detail, ["action"]));
    if (isExecutionOpenAction(action)) {
      return readFirstNumber(detail, ["fill_price", "price", "entry_price", "avg_px"]) ?? candle.close;
    }
    if (isExecutionCloseAction(action)) {
      return readFirstNumber(detail, ["fill_price", "price", "exit_price", "close_avg_px"]) ?? candle.close;
    }
    return candle.close;
  }
  return candle.close;
}

function buildMarkerTitle(event: TradingViewEventEntry, fallback: string): string {
  const title = (event.title || "").trim() || fallback;
  const summary = (event.summary || "").trim();
  return summary ? `${title}\n${summary}` : title;
}

function markerTooltipTitle(kind: string): string {
  switch (kind) {
    case "ENTRY":
      return "开仓详情";
    case "EXIT":
      return "平仓详情";
    case "EXECUTION":
      return "执行详情";
    case "TP":
      return "止盈详情";
    case "SL":
      return "止损详情";
    case "ARMED":
      return "Armed 事件";
    case "TREND":
      return "趋势事件";
    case "HIGH":
      return "高周期状态";
    case "MID":
      return "中周期状态";
    case "R2":
      return "2R 保护";
    case "R4":
      return "4R 保护";
    default:
      return kind;
  }
}

function buildMarkerTooltipRows(
  kind: string,
  event: TradingViewEventEntry,
  priceFormatter: (value: number) => string
): TradingViewEventMarkerTooltipRow[] {
  const rows: TradingViewEventMarkerTooltipRow[] = [];
  switch (kind) {
    case "ENTRY":
      pushTooltipPriceRow(rows, "entry_price", readFirstNumber(event.detail, ["entry_price", "price", "fill_price", "avg_px"]), priceFormatter);
      pushTooltipNumberRow(rows, "quantity", readFirstNumber(event.detail, ["quantity", "size"]));
      pushTooltipStringRow(rows, "position_side", readFirstString(event.detail, ["position_side", "side"]));
      break;
    case "EXIT":
      pushTooltipPriceRow(rows, "exit_price", readFirstNumber(event.detail, ["exit_price", "price", "fill_price", "close_avg_px"]), priceFormatter);
      pushTooltipNumberRow(rows, "realized_pnl", readFirstNumber(event.detail, ["realized_pnl"]));
      pushTooltipRatioRow(rows, "pnl_ratio", readFirstNumber(event.detail, ["pnl_ratio"]));
      break;
    case "EXECUTION":
      pushTooltipStringRow(rows, "action", readFirstString(event.detail, ["action"]));
      pushTooltipStringRow(rows, "order_type", readFirstString(event.detail, ["order_type"]));
      pushTooltipStringRow(rows, "position_side", readFirstString(event.detail, ["position_side"]));
      pushTooltipStringRow(rows, "margin_mode", readFirstString(event.detail, ["margin_mode"]));
      pushTooltipPriceRow(rows, "price", readFirstNumber(event.detail, ["fill_price", "price", "avg_px"]), priceFormatter);
      pushTooltipPriceRow(rows, "tp_price", readFirstNumber(event.detail, ["tp_price", "take_profit_price", "tp"]), priceFormatter);
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]), priceFormatter);
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "result_status", readFirstString(event.detail, ["result_status"]));
      break;
    case "TP":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipStringRow(rows, "order_type", readFirstString(event.detail, ["order_type"]));
      pushTooltipIntegerRow(rows, "has_position", readFirstNumber(event.detail, ["has_position"]));
      pushTooltipPriceRow(rows, "tp_price", readFirstNumber(event.detail, ["tp_price", "take_profit_price", "tp"]), priceFormatter);
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]), priceFormatter);
      break;
    case "SL":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipStringRow(rows, "order_type", readFirstString(event.detail, ["order_type"]));
      pushTooltipIntegerRow(rows, "has_position", readFirstNumber(event.detail, ["has_position"]));
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]), priceFormatter);
      pushTooltipPriceRow(rows, "tp_price", readFirstNumber(event.detail, ["tp_price", "take_profit_price", "tp"]), priceFormatter);
      break;
    case "ARMED":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipTimestampRow(rows, "entry_watch_timestamp", readFirstNumber(event.detail, ["entry_watch_timestamp"]));
      break;
    case "TREND":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipTimestampRow(rows, "trending_timestamp", readFirstNumber(event.detail, ["trending_timestamp"]));
      pushTooltipIntegerRow(rows, "high_side", readFirstNumber(event.detail, ["high_side", "highside"]));
      pushTooltipIntegerRow(rows, "mid_side", readFirstNumber(event.detail, ["mid_side", "midside"]));
      break;
    case "HIGH":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipIntegerRow(rows, "high_side", readFirstNumber(event.detail, ["high_side", "highside"]));
      break;
    case "MID":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipIntegerRow(rows, "mid_side", readFirstNumber(event.detail, ["mid_side", "midside"]));
      break;
    case "R2":
    case "R4":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]), priceFormatter);
      pushTooltipPriceRow(rows, "entry_price", readFirstNumber(event.detail, ["entry_price", "entry", "price", "fill_price", "avg_px"]), priceFormatter);
      pushTooltipPriceRow(rows, "initial_sl", readFirstNumber(event.detail, ["initial_sl"]), priceFormatter);
      pushTooltipRatioRow(rows, "initial_risk_pct", readFirstNumber(event.detail, ["initial_risk_pct", "initialriskpct"]));
      pushTooltipRatioRow(rows, "max_favorable_profit_pct", readFirstNumber(event.detail, ["max_favorable_profit_pct", "maxfavorableprofitpct"]));
      pushTooltipRRow(rows, "mfer", readFirstNumber(event.detail, ["mfer"]));
      pushTooltipIntegerRow(rows, "profit_protect_stage", readFirstNumber(event.detail, ["profit_protect_stage", "profitprotectstage"]));
      break;
    default:
      break;
  }
  return rows;
}

function normalizeEventToken(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
}

function isExecutionOpenAction(action: string): boolean {
  return action === "OPEN" || action === "OPEN_LONG" || action === "OPEN_SHORT";
}

function isExecutionCloseAction(action: string): boolean {
  return action === "CLOSE" || action === "FULL_CLOSE" || action === "PARTIAL_CLOSE" || action === "CLOSE_LONG" || action === "CLOSE_SHORT";
}

function isSuccessfulExecutionResult(result: string): boolean {
  if (!result) {
    return true;
  }
  return result === "SUCCESS" || result === "SUCCEEDED" || result === "OK";
}

function pushTooltipStringRow(rows: TradingViewEventMarkerTooltipRow[], label: string, value: string): void {
  const normalized = value.trim();
  if (!normalized) {
    return;
  }
  rows.push({ label, value: normalized });
}

function pushTooltipIntegerRow(rows: TradingViewEventMarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value)) {
    return;
  }
  rows.push({ label, value: String(Math.round(value)) });
}

function pushTooltipNumberRow(rows: TradingViewEventMarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value)) {
    return;
  }
  rows.push({ label, value: formatPlainNumber(value) });
}

function pushTooltipPriceRow(
  rows: TradingViewEventMarkerTooltipRow[],
  label: string,
  value: number | null,
  priceFormatter: (value: number) => string
): void {
  if (value == null || !Number.isFinite(value)) {
    return;
  }
  rows.push({ label, value: priceFormatter(value) });
}

function pushTooltipRatioRow(rows: TradingViewEventMarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value)) {
    return;
  }
  rows.push({ label, value: formatSignedPercent(value * 100) });
}

function pushTooltipTimestampRow(rows: TradingViewEventMarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return;
  }
  rows.push({ label, value: formatLocalTimestamp(value) });
}

function pushTooltipRRow(rows: TradingViewEventMarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value)) {
    return;
  }
  rows.push({ label, value: `${formatPlainNumber(value)}R` });
}

function formatSignedPercent(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  const abs = Math.abs(value);
  const digits = abs >= 100 ? 1 : abs >= 10 ? 2 : 2;
  const rounded = value.toFixed(digits).replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "");
  if (value > 0) {
    return `+${rounded}%`;
  }
  if (value < 0) {
    return `${rounded}%`;
  }
  return "0%";
}

function formatPlainNumber(value: number): string {
  const abs = Math.abs(value);
  const digits = abs >= 1000 ? 2 : abs >= 1 ? 4 : 6;
  return value.toFixed(digits).replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "");
}

function formatLocalTimestamp(value: number): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  const yyyy = String(date.getFullYear());
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}`;
}

function readFirstNumber(detail: Record<string, unknown> | undefined, keys: string[]): number | null {
  if (!detail) {
    return null;
  }
  const lowered = new Map<string, unknown>();
  for (const [key, value] of Object.entries(detail)) {
    lowered.set(key.trim().toLowerCase(), value);
  }
  for (const key of keys) {
    const value = lowered.get(key);
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return null;
}

function readFirstString(detail: Record<string, unknown> | undefined, keys: string[]): string {
  if (!detail) {
    return "";
  }
  const lowered = new Map<string, unknown>();
  for (const [key, value] of Object.entries(detail)) {
    lowered.set(key.trim().toLowerCase(), value);
  }
  for (const key of keys) {
    const value = lowered.get(key);
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function findNearestCandleIndex(ts: number, candles: TradingViewCandle[]): number {
  if (candles.length === 0) {
    return -1;
  }
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const value = candles[mid].ts;
    if (value === ts) {
      return mid;
    }
    if (value < ts) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  if (right < 0) {
    return left < candles.length ? left : -1;
  }
  if (left >= candles.length) {
    return right;
  }
  return Math.abs(candles[right].ts - ts) <= Math.abs(candles[left].ts - ts) ? right : left;
}

function estimateCandleHalfWidth(timeScale: IChartApi["timeScale"], candles: TradingViewCandle[], index: number): number {
  const current = timeScale.timeToCoordinate(Math.floor(candles[index].ts / 1000) as Time);
  if (!Number.isFinite(current)) {
    return 10;
  }
  const gaps: number[] = [];
  if (index > 0) {
    const previous = timeScale.timeToCoordinate(Math.floor(candles[index - 1].ts / 1000) as Time);
    if (Number.isFinite(previous)) {
      gaps.push(Math.abs(Number(current) - Number(previous)));
    }
  }
  if (index + 1 < candles.length) {
    const next = timeScale.timeToCoordinate(Math.floor(candles[index + 1].ts / 1000) as Time);
    if (Number.isFinite(next)) {
      gaps.push(Math.abs(Number(next) - Number(current)));
    }
  }
  if (gaps.length === 0) {
    return 10;
  }
  const minGap = Math.min(...gaps.filter((gap) => gap > 0));
  if (!Number.isFinite(minGap)) {
    return 10;
  }
  return clamp(minGap * 0.5, 8, 18);
}

function estimateEventArrowLength(candleHalfWidth: number): number {
  return clamp(candleHalfWidth * 2 + 14, 28, 42);
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}
