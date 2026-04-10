import { Fragment, memo, useEffect, useMemo, useState, type KeyboardEvent as ReactKeyboardEvent, type ReactNode } from "react";
import type { BubbleCandleEvent, BubbleDatum, DataSourceStatus, PositionItem } from "../app/types";
import { useAppStore } from "../app/store";
import { AppConfig } from "../app/config";
import { buildSignalLinkItems, resolveVisualHistoryDateFromEntryTime } from "./signalLinks";
import tradingViewBrandIcon from "../assets/icon-tradingview.svg";
import okxBrandIcon from "../assets/icon-okx.png";
import binanceBrandIcon from "../assets/icon-binance.png";
import bitgetBrandIcon from "../assets/icon-bitget.png";

type SignalColumn = {
  key: string;
  label: string;
  render: (item: BubbleDatum) => ReactNode;
};

type PositionColumn = {
  key: string;
  label: string;
  render: (item: PositionItem) => ReactNode;
};

type TableTab = "signal" | "position" | "history";
type PositionColumnsMode = "position" | "history";
type EventDrawerTab = "signal" | "position" | "history";
type SignalLinkItem = ReturnType<typeof buildSignalLinkItems>[number];
type PositionEventsSnapshot = {
  positionId?: number;
  positionKey?: string;
  isOpen?: boolean;
  count: number;
  total: number;
  truncated: boolean;
  events: BubbleCandleEvent[];
};

type PositionEventsLoadState = {
  status: "idle" | "loading" | "ready" | "error";
  snapshot?: PositionEventsSnapshot;
  error?: string;
};

const MOBILE_LAYOUT_MAX_WIDTH = 1023;
const EVENT_PANEL_LIMIT = 5000;
const BRAND_ICON_SRC_BY_TONE: Record<string, string> = {
  tradingview: tradingViewBrandIcon,
  binance: binanceBrandIcon,
  okx: okxBrandIcon,
  bitget: bitgetBrandIcon
};
const MOBILE_SIGNAL_SUMMARY_KEYS = ["exchange", "symbol", "timeframe", "high_side", "trending_timestamp"] as const;
const MOBILE_POSITION_SUMMARY_KEYS = [
  "exchange",
  "symbol",
  "position_side",
  "leverage_multiplier",
  "floating_profit",
  "holding_time"
] as const;
const MOBILE_HISTORY_SUMMARY_KEYS = [
  "exchange",
  "symbol",
  "position_side",
  "leverage_multiplier",
  "floating_profit",
  "holding_time"
] as const;

const MOBILE_SIGNAL_SUMMARY_KEY_SET = new Set<string>(MOBILE_SIGNAL_SUMMARY_KEYS);
const MOBILE_POSITION_SUMMARY_KEY_SET = new Set<string>(MOBILE_POSITION_SUMMARY_KEYS);
const MOBILE_HISTORY_SUMMARY_KEY_SET = new Set<string>(MOBILE_HISTORY_SUMMARY_KEYS);
const EVENT_META_KEY_PRIORITY = [
  "timeframe",
  "action",
  "high_side",
  "mid_side",
  "entry_price",
  "sl_price",
  "tp_price",
  "price",
  "result_status",
  "order_type",
  "position_side",
  "quantity",
  "realized_pnl"
] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeEventLevel(value: unknown): BubbleCandleEvent["level"] {
  if (typeof value !== "string") return "info";
  const normalized = value.trim().toLowerCase();
  if (normalized === "warning") return "warning";
  if (normalized === "success") return "success";
  if (normalized === "error") return "error";
  return "info";
}

function normalizePositionEvent(payload: unknown): BubbleCandleEvent | null {
  if (!isRecord(payload)) return null;
  const id = typeof payload.id === "string" ? payload.id.trim() : "";
  const source = typeof payload.source === "string" ? payload.source.trim() : "";
  const type = typeof payload.type === "string" ? payload.type.trim() : "";
  const title = typeof payload.title === "string" ? payload.title.trim() : "";
  const summary = typeof payload.summary === "string" ? payload.summary.trim() : "";
  const eventAtRaw = payload.event_at_ms;
  const eventAtMs = typeof eventAtRaw === "number" && Number.isFinite(eventAtRaw) ? Math.trunc(eventAtRaw) : NaN;
  if (!id || !Number.isFinite(eventAtMs)) return null;
  return {
    id,
    source,
    type,
    level: normalizeEventLevel(payload.level),
    eventAtMs,
    title,
    summary,
    detail: isRecord(payload.detail) ? payload.detail : undefined
  };
}

function normalizePositionEventsSnapshot(payload: unknown): PositionEventsSnapshot | null {
  if (!isRecord(payload)) return null;
  const eventsRaw = Array.isArray(payload.events) ? payload.events : [];
  const events = eventsRaw
    .map((item) => normalizePositionEvent(item))
    .filter((item): item is BubbleCandleEvent => item !== null)
    .sort((left, right) => left.eventAtMs - right.eventAtMs);
  const countRaw = payload.count;
  const totalRaw = payload.total;
  const positionIdRaw = payload.position_id;
  return {
    positionId: typeof positionIdRaw === "number" && Number.isFinite(positionIdRaw) ? Math.trunc(positionIdRaw) : undefined,
    positionKey: typeof payload.position_key === "string" ? payload.position_key.trim() || undefined : undefined,
    isOpen: typeof payload.is_open === "boolean" ? payload.is_open : undefined,
    count: typeof countRaw === "number" && Number.isFinite(countRaw) ? Math.max(0, Math.trunc(countRaw)) : events.length,
    total: typeof totalRaw === "number" && Number.isFinite(totalRaw) ? Math.max(0, Math.trunc(totalRaw)) : events.length,
    truncated: payload.truncated === true,
    events
  };
}

function formatEventPanelTime(ts?: number): string {
  if (typeof ts !== "number" || !Number.isFinite(ts) || ts <= 0) return "--";
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return "--";
  const hh = `${date.getHours()}`.padStart(2, "0");
  const mi = `${date.getMinutes()}`.padStart(2, "0");
  const ss = `${date.getSeconds()}`.padStart(2, "0");
  return `${hh}:${mi}:${ss}`;
}

function formatEventPanelMeta(event: BubbleCandleEvent): string {
  const detail = event.detail;
  if (!detail) return "-";
  const allowed = Object.entries(detail).filter(([key, value]) => {
    if (/_json$/i.test(key)) return false;
    if (value == null) return false;
    if (typeof value === "object") return false;
    if (typeof value === "string" && (value.length === 0 || value.length > 120)) return false;
    return true;
  });
  if (allowed.length === 0) return "-";
  const picked: Array<[string, unknown]> = [];
  const used = new Set<string>();
  for (const key of EVENT_META_KEY_PRIORITY) {
    const found = allowed.find(([entryKey]) => entryKey === key);
    if (!found || used.has(found[0])) continue;
    picked.push(found);
    used.add(found[0]);
    if (picked.length >= 3) break;
  }
  for (const entry of allowed) {
    if (used.has(entry[0])) continue;
    picked.push(entry);
    if (picked.length >= 3) break;
  }
  if (picked.length === 0) return "-";
  return picked.map(([key, value]) => `${key}=${String(value)}`).join(" · ");
}

function readEventDetailNumber(detail: Record<string, unknown> | undefined, key: string): number | undefined {
  if (!detail) return undefined;
  const value = detail[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function normalizeEventTypeTokens(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function deriveEventDisplayTypes(event: BubbleCandleEvent): string[] {
  const tokens = normalizeEventTypeTokens(event.type);
  const detail = event.detail;
  const action = readEventDetailNumber(detail, "action");
  const trendingTimestamp = readEventDetailNumber(detail, "trending_timestamp");
  const hasArmed = tokens.some((item) => item === "ARMED") || action === 4;
  const hasTrendDetected = tokens.some((item) => item === "TREND_DETECTED");

  if (hasArmed && typeof trendingTimestamp === "number" && trendingTimestamp > 0) {
    const merged: string[] = [];
    merged.push("TREND_DETECTED");
    merged.push("ARMED");
    for (const token of tokens) {
      if (token === "TREND_DETECTED" || token === "ARMED") continue;
      if (!merged.includes(token)) {
        merged.push(token);
      }
    }
    return merged;
  }

  if (tokens.length > 0) {
    return tokens;
  }
  return ["-"];
}

function mapEventTypeTokenToTitle(token: string): string {
  switch (token) {
    case "TREND_DETECTED":
      return "趋势检测";
    case "ARMED":
      return "Armed";
    case "HIGH_SIDE_CHANGED":
      return "高周期方向变化";
    case "MID_SIDE_CHANGED":
      return "中周期状态变化";
    case "TRAILING_STOP":
      return "移动止损";
    case "TRAILING_TP":
      return "移动止盈";
    case "TRAILING_TP_SL":
      return "移动止盈止损";
    case "R_PROTECT_2R":
      return "2R 保本保护";
    case "R_PROTECT_4R":
      return "4R 部分平仓保护";
    case "SIGNAL":
      return "策略信号";
    case "SIGNAL_CLOSED":
      return "信号关闭";
    case "ENTRY":
      return "仓位开仓";
    case "EXIT":
      return "仓位平仓";
    case "EXECUTION":
      return "执行记录";
    default:
      return token;
  }
}

function formatEventPanelType(event: BubbleCandleEvent): string {
  return deriveEventDisplayTypes(event).join(", ");
}

function formatEventPanelTitle(event: BubbleCandleEvent): string {
  const tokens = deriveEventDisplayTypes(event);
  if (tokens.length <= 1) {
    return event.title || tokens[0] || "-";
  }
  return tokens.map((token) => mapEventTypeTokenToTitle(token)).join(", ");
}

function buildPositionEventsUrl(tab: EventDrawerTab, item: PositionItem): string {
  const origin = typeof window !== "undefined" ? window.location.origin : "http://127.0.0.1";
  const baseUrl = tab === "position" ? `${AppConfig.positionUrl}/events` : `${AppConfig.historyUrl}/events`;
  const url = new URL(baseUrl, origin);
  url.searchParams.set("event_limit", String(EVENT_PANEL_LIMIT));
  if (typeof item.positionId === "number" && Number.isFinite(item.positionId) && item.positionId > 0) {
    url.searchParams.set("position_id", String(Math.trunc(item.positionId)));
  }
  if (item.exchange && item.exchange !== "--") url.searchParams.set("exchange", item.exchange);
  if (item.symbol && item.symbol !== "--") url.searchParams.set("symbol", item.symbol);
  if (item.positionSide) url.searchParams.set("position_side", item.positionSide);
  if (item.marginMode) url.searchParams.set("margin_mode", item.marginMode);
  if (item.entryTime) url.searchParams.set("entry_time", item.entryTime);
  if (item.exitTime) url.searchParams.set("exit_time", item.exitTime);
  if (item.updatedTime) url.searchParams.set("updated_time", item.updatedTime);
  if (item.strategyName) url.searchParams.set("strategy", item.strategyName);
  if (item.strategyVersion) url.searchParams.set("version", item.strategyVersion);
  return url.toString();
}

function buildSignalEventsUrl(item: BubbleDatum): string {
  const origin = typeof window !== "undefined" ? window.location.origin : "http://127.0.0.1";
  const url = new URL(`${AppConfig.restUrl}/events`, origin);
  url.searchParams.set("event_limit", String(EVENT_PANEL_LIMIT));
  if (item.exchange) url.searchParams.set("exchange", item.exchange);
  if (item.symbol) url.searchParams.set("symbol", item.symbol);
  if (item.timeframe) url.searchParams.set("timeframe", item.timeframe);
  if (item.strategy) url.searchParams.set("strategy", item.strategy);
  if (item.strategyVersion) url.searchParams.set("version", item.strategyVersion);
  if (item.comboKey) url.searchParams.set("combo_key", item.comboKey);
  if (item.groupId) url.searchParams.set("group_id", item.groupId);
  if (typeof item.triggerTimestamp === "number" && Number.isFinite(item.triggerTimestamp) && item.triggerTimestamp > 0) {
    url.searchParams.set("trigger_timestamp", String(Math.trunc(item.triggerTimestamp)));
  }
  if (
    typeof item.trendingTimestamp === "number" &&
    Number.isFinite(item.trendingTimestamp) &&
    item.trendingTimestamp > 0
  ) {
    url.searchParams.set("trending_timestamp", String(Math.trunc(item.trendingTimestamp)));
  }
  return url.toString();
}

function eventPanelCountText(snapshot?: PositionEventsSnapshot): string {
  if (!snapshot) return "0";
  if (snapshot.truncated && snapshot.total > snapshot.events.length) {
    return `${snapshot.events.length}/${snapshot.total}`;
  }
  return String(snapshot.total || snapshot.count || snapshot.events.length);
}

function normalizeSymbolKey(symbol: string): string {
  return symbol.trim().toUpperCase();
}

function simplifySymbolKey(symbol: string): string {
  const raw = normalizeSymbolKey(symbol);
  if (!raw) return "";
  const parts = raw.split(/[/:_-]/).filter(Boolean);
  let base = parts.length > 1 ? parts[0] : raw;
  base = base.replace(/USDT(?:\.P|P)?$/i, "");
  base = base.replace(/\.P$/i, "");
  return base || raw;
}

function useMobileLayout(maxWidth: number): boolean {
  const query = `(max-width: ${maxWidth}px)`;
  const [matches, setMatches] = useState(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
    return window.matchMedia(query).matches;
  });

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return;
    const media = window.matchMedia(query);
    const handleChange = () => setMatches(media.matches);
    handleChange();
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", handleChange);
      return () => media.removeEventListener("change", handleChange);
    }
    media.addListener(handleChange);
    return () => media.removeListener(handleChange);
  }, [query]);

  return matches;
}

type RenderColumn<Row> = {
  key: string;
  label: string;
  render: (item: Row) => ReactNode;
};

function toColumnMap<Row>(columns: RenderColumn<Row>[]): Map<string, RenderColumn<Row>> {
  const map = new Map<string, RenderColumn<Row>>();
  for (const column of columns) {
    map.set(column.key, column);
  }
  return map;
}

function renderSignalLinks(rowKey: string, symbol: string, linkItems: SignalLinkItem[]): ReactNode {
  if (linkItems.length === 0) {
    return <span className="signals-mobile-card__links-empty">暂无链接</span>;
  }
  return (
    <div className="signals-links">
      {linkItems.map((link) => (
        <a
          key={`${rowKey}-${link.key}`}
          className="details-link signals-links__item"
          href={link.url}
          target="_blank"
          rel="noopener noreferrer"
          title={`${link.label} - 打开 ${symbol} K 线`}
          aria-label={`${link.label} - 打开 ${symbol} K 线`}
        >
          <span
            className={`details-link__icon details-link__icon--${link.iconTone}${BRAND_ICON_SRC_BY_TONE[link.iconTone] ? " details-link__icon--image" : ""}`}
          >
            {BRAND_ICON_SRC_BY_TONE[link.iconTone] ? (
              <img
                className={`details-link__icon-image details-link__icon-image--${link.iconTone}`}
                src={BRAND_ICON_SRC_BY_TONE[link.iconTone]}
                alt=""
                aria-hidden="true"
              />
            ) : (
              link.iconText
            )}
          </span>
        </a>
      ))}
    </div>
  );
}

function renderPositionEventDrawer(loadState: PositionEventsLoadState | undefined): ReactNode {
  if (!loadState || loadState.status === "idle") {
    return <div className="signals-event-drawer__state">点击右侧箭头加载事件</div>;
  }
  if (loadState.status === "loading") {
    return <div className="signals-event-drawer__state">事件加载中...</div>;
  }
  if (loadState.status === "error") {
    return <div className="signals-event-drawer__state signals-event-drawer__state--error">{loadState.error || "事件加载失败"}</div>;
  }

  const snapshot = loadState.snapshot;
  if (!snapshot || snapshot.events.length === 0) {
    return <div className="signals-event-drawer__state">当前无事件</div>;
  }

  return (
    <>
      <div className="signals-event-drawer__table">
        {snapshot.events.map((event) => {
          const typeText = formatEventPanelType(event);
          const titleText = formatEventPanelTitle(event);
          const summaryText = event.summary || "-";
          const metaText = formatEventPanelMeta(event);
          return (
            <article key={event.id} className={`signals-event-drawer__row level-${event.level}`}>
              <span className="signals-event-drawer__cell signals-event-drawer__cell--time">{formatEventPanelTime(event.eventAtMs)}</span>
              <span className="signals-event-drawer__cell signals-event-drawer__cell--type">
                <span className="signals-event-drawer__type-pill">{typeText}</span>
              </span>
              <span className="signals-event-drawer__cell signals-event-drawer__cell--title" title={titleText}>
                {titleText}
              </span>
              <span className="signals-event-drawer__cell signals-event-drawer__cell--summary" title={summaryText}>
                {summaryText}
              </span>
              <span className="signals-event-drawer__cell signals-event-drawer__cell--meta" title={metaText}>
                {metaText}
              </span>
            </article>
          );
        })}
      </div>
      {snapshot.truncated && snapshot.total > snapshot.events.length ? (
        <div className="signals-event-drawer__footnote">仅显示最近 {snapshot.events.length} 条事件，共 {snapshot.total} 条。</div>
      ) : null}
    </>
  );
}

function formatSignalTrendType(value?: number, mode: "zh" | "longShort" = "zh"): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const normalized = Math.trunc(value);
  if (normalized === 1) return mode === "longShort" ? "LONG" : "看多";
  if (normalized === -1) return mode === "longShort" ? "SHORT" : "看空";
  return String(normalized);
}

function resolveSignalTrendValue(item: BubbleDatum): number | undefined {
  if (typeof item.highSide === "number" && Number.isFinite(item.highSide)) {
    const normalized = Math.trunc(item.highSide);
    if (normalized === 1 || normalized === -1) {
      return normalized;
    }
  }
  return undefined;
}

function formatLeverageMultiplier(value?: number): string {
  const normalized = formatNumeric(value);
  if (normalized === "--") return "--";
  return `${normalized}x`;
}

function renderMobileSignalSummaryValue(item: BubbleDatum, key: (typeof MOBILE_SIGNAL_SUMMARY_KEYS)[number], fallback?: SignalColumn): ReactNode {
  if (key === "high_side") {
    return formatSignalTrendType(resolveSignalTrendValue(item), "longShort");
  }
  if (key === "trending_timestamp") {
    return formatTimestamp(item.trendingTimestamp);
  }
  if (fallback) {
    return fallback.render(item);
  }
  return "--";
}

function formatNumeric(value?: number): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
  if (abs >= 1) {
    return value.toLocaleString("en-US", { maximumFractionDigits: 6 });
  }
  return value.toLocaleString("en-US", { maximumFractionDigits: 10 });
}

function formatTimestamp(value?: number): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return "--";
  const normalized = value < 1_000_000_000_000 ? value * 1000 : value;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return "--";
  const yyyy = date.getFullYear();
  const mm = `${date.getMonth() + 1}`.padStart(2, "0");
  const dd = `${date.getDate()}`.padStart(2, "0");
  const hh = `${date.getHours()}`.padStart(2, "0");
  const mi = `${date.getMinutes()}`.padStart(2, "0");
  const ss = `${date.getSeconds()}`.padStart(2, "0");
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}:${ss}`;
}

function parsePositionTimeMs(value?: string): number | undefined {
  if (!value) return undefined;
  const raw = value.trim();
  if (!raw) return undefined;

  if (/^\d{10,13}$/.test(raw)) {
    const ts = Number(raw);
    if (!Number.isNaN(ts)) {
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
    return Number.isNaN(ts) ? undefined : ts;
  }

  const direct = Date.parse(raw);
  if (!Number.isNaN(direct)) return direct;

  const normalized = raw.replace(" ", "T");
  const normalizedParsed = Date.parse(normalized);
  if (!Number.isNaN(normalizedParsed)) return normalizedParsed;

  const match = raw.match(/^(\d{4})[/-](\d{2})[/-](\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
  if (!match) return undefined;

  const [, yyyy, mm, dd, hh, mi, ss] = match;
  const date = new Date(Number(yyyy), Number(mm) - 1, Number(dd), Number(hh), Number(mi), Number(ss));
  const ts = date.getTime();
  return Number.isNaN(ts) ? undefined : ts;
}

function formatPositionTimeInLocalTimezone(value?: string): string {
  const ts = parsePositionTimeMs(value);
  if (typeof ts !== "number") return "--";
  return formatTimestamp(ts);
}

function formatHoldingDuration(entryTime?: string, nowMs = Date.now()): string {
  const entryMs = parsePositionTimeMs(entryTime);
  if (typeof entryMs !== "number") return "--";

  const durationMs = Math.max(0, nowMs - entryMs);
  const totalSeconds = Math.floor(durationMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${hours}h${minutes}m${seconds}s`;
}

function formatHistoryHoldingDuration(item: PositionItem): string {
  if (typeof item.holdingTime === "string" && item.holdingTime.trim().length > 0) {
    return item.holdingTime;
  }
  const closeMs = parsePositionTimeMs(item.exitTime) ?? parsePositionTimeMs(item.updatedTime);
  if (typeof closeMs !== "number") return "--";
  return formatHoldingDuration(item.entryTime, closeMs);
}

function normalizeRatePercent(value?: number): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function formatRatePercent(value?: number): string {
  const normalized = normalizeRatePercent(value);
  if (typeof normalized !== "number") return "--";
  return `${normalized.toFixed(2)}%`;
}

function formatAmountWithRate(amount?: number, rate?: number): string {
  const normalizedRate = normalizeRatePercent(rate);
  if ((typeof amount !== "number" || !Number.isFinite(amount)) && typeof normalizedRate !== "number") {
    return "--";
  }
  return `${formatNumeric(amount)} (${formatRatePercent(rate)})`;
}

function negateNonZero(value?: number): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  if (value === 0) return 0;
  return -Math.abs(value);
}

function formatLossAmountWithRate(amount?: number, rate?: number): string {
  const displayAmount = negateNonZero(amount);
  const displayRate = negateNonZero(rate);
  const normalizedRate = normalizeRatePercent(displayRate);
  if ((typeof displayAmount !== "number" || !Number.isFinite(displayAmount)) && typeof normalizedRate !== "number") {
    return "--";
  }
  return `${formatNumeric(displayAmount)} (${formatRatePercent(displayRate)})`;
}

function calculatePositionTargetRate(item: PositionItem, targetPrice?: number): number | undefined {
  if (typeof targetPrice !== "number" || !Number.isFinite(targetPrice) || targetPrice <= 0) return undefined;
  if (typeof item.entryPrice !== "number" || !Number.isFinite(item.entryPrice) || item.entryPrice <= 0) return undefined;
  const leverage =
    typeof item.leverageMultiplier === "number" && Number.isFinite(item.leverageMultiplier) && item.leverageMultiplier > 0
      ? item.leverageMultiplier
      : 1;

  const tone = resolvePositionRowTone(item);
  if (tone === "long") {
    return ((targetPrice - item.entryPrice) / item.entryPrice) * leverage * 100;
  }
  if (tone === "short") {
    return ((item.entryPrice - targetPrice) / item.entryPrice) * leverage * 100;
  }
  return undefined;
}

function renderPositionTargetPriceWithRate(item: PositionItem, targetPrice?: number): ReactNode {
  const priceText = formatNumeric(targetPrice);
  const rate = calculatePositionTargetRate(item, targetPrice);
  if (typeof rate !== "number" || !Number.isFinite(rate)) {
    return priceText;
  }
  const tone = resolveRateTone(rate);
  return (
    <>
      {priceText} <span className={`position-rate position-rate--${tone}`}>({rate.toFixed(2)}%)</span>
    </>
  );
}

function formatPositionSideLongShort(item: PositionItem): string {
  const tone = resolvePositionRowTone(item);
  if (tone === "long") return "LONG";
  if (tone === "short") return "SHORT";
  return "--";
}

function renderMobilePositionSummaryValue(
  item: PositionItem,
  key: (typeof MOBILE_POSITION_SUMMARY_KEYS)[number] | (typeof MOBILE_HISTORY_SUMMARY_KEYS)[number],
  fallback?: PositionColumn
): ReactNode {
  if (key === "position_side") {
    return formatPositionSideLongShort(item);
  }
  if (key === "leverage_multiplier") {
    return formatLeverageMultiplier(item.leverageMultiplier);
  }
  if (fallback) return fallback.render(item);
  return "--";
}

function formatStableFloatingAmount(value?: number): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  }
  if (abs >= 1) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 4,
      maximumFractionDigits: 4
    });
  }
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 6,
    maximumFractionDigits: 6
  });
}

function formatFloatingAmountWithRateStable(amount?: number, rate?: number): string {
  const normalizedRate = normalizeRatePercent(rate);
  if ((typeof amount !== "number" || !Number.isFinite(amount)) && typeof normalizedRate !== "number") {
    return "--";
  }
  return `${formatStableFloatingAmount(amount)} (${formatRatePercent(rate)})`;
}

function resolveSignedTone(value?: number): "positive" | "negative" | "neutral" {
  if (typeof value !== "number" || !Number.isFinite(value)) return "neutral";
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

function resolveRateTone(value?: number): "positive" | "negative" | "neutral" {
  const normalized = normalizeRatePercent(value);
  return resolveSignedTone(normalized);
}

function resolveAmountRateTone(amount?: number, rate?: number): "positive" | "negative" | "neutral" {
  if (typeof amount === "number" && Number.isFinite(amount)) {
    return resolveSignedTone(amount);
  }
  return resolveRateTone(rate);
}

function isPositionFeedLive(status: DataSourceStatus): boolean {
  if (status.mode === "mock") return false;
  if (status.positionStatus !== "ok") return false;

  if (status.wsStatus === "open") {
    return status.heartbeatStale !== true;
  }

  // WS 不可用时允许 REST 回退继续驱动持仓刷新。
  if (status.wsStatus === "idle" || status.wsStatus === "closed" || status.wsStatus === "error" || status.wsStatus === "connecting") {
    return status.restStatus === "ok";
  }

  return false;
}

const SIGNAL_COLUMNS: SignalColumn[] = [
  {
    key: "exchange",
    label: "交易所",
    render: (item) => item.exchange || "--"
  },
  {
    key: "symbol",
    label: "交易对",
    render: (item) => item.symbol || "--"
  },
  {
    key: "timeframe",
    label: "周期",
    render: (item) => item.timeframe || "--"
  },
  {
    key: "high_side",
    label: "方向",
    render: (item) => {
      return formatSignalTrendType(resolveSignalTrendValue(item), "longShort");
    }
  },
  {
    key: "trending_timestamp",
    label: "趋势时间",
    render: (item) => formatTimestamp(item.trendingTimestamp)
  },
  {
    key: "strategy",
    label: "策略",
    render: (item) => item.strategy || "--"
  },
  {
    key: "strategy_version",
    label: "版本",
    render: (item) => item.strategyVersion || "--"
  }
];

function buildPositionColumns(nowMs: number, mode: PositionColumnsMode): PositionColumn[] {
  if (mode === "history") {
    return [
      { key: "exchange", label: "交易所", render: (item) => item.exchange || "--" },
      { key: "symbol", label: "交易对", render: (item) => item.symbol || "--" },
      { key: "position_side", label: "方向", render: (item) => formatPositionSideLongShort(item) },
      { key: "leverage_multiplier", label: "杠杆", render: (item) => formatLeverageMultiplier(item.leverageMultiplier) },
      { key: "margin_amount", label: "保证金", render: (item) => formatNumeric(item.marginAmount) },
      { key: "entry_price", label: "开仓价格", render: (item) => formatNumeric(item.entryPrice) },
      { key: "exit_price", label: "平仓价格", render: (item) => formatNumeric(item.exitPrice) },
      { key: "entry_quantity", label: "仓位数量", render: (item) => formatNumeric(item.entryQuantity) },
      { key: "take_profit_price", label: "止盈价格", render: (item) => formatNumeric(item.takeProfitPrice) },
      { key: "stop_loss_price", label: "止损价格", render: (item) => formatNumeric(item.stopLossPrice) },
      {
        key: "floating_profit",
        label: "已实现收益",
        render: (item) => {
          const realizedAmount = typeof item.profitAmount === "number" ? item.profitAmount : item.unrealizedProfitAmount;
          const realizedRate = typeof item.profitRate === "number" ? item.profitRate : item.unrealizedProfitRate;
          const tone = resolveAmountRateTone(realizedAmount, realizedRate);
          return (
            <span className={`position-rate position-rate--${tone}`}>
              {formatAmountWithRate(realizedAmount, realizedRate)}
            </span>
          );
        }
      },
      {
        key: "max_floating_profit",
        label: "最大浮盈",
        render: (item) => formatAmountWithRate(item.maxFloatingProfitAmount, item.maxFloatingProfitRate)
      },
      {
        key: "max_floating_loss",
        label: "最大浮亏",
        render: (item) => formatLossAmountWithRate(item.maxFloatingLossAmount, item.maxFloatingLossRate)
      },
      { key: "entry_time", label: "开仓时间", render: (item) => formatPositionTimeInLocalTimezone(item.entryTime) },
      {
        key: "close_time",
        label: "平仓时间",
        render: (item) => formatPositionTimeInLocalTimezone(item.exitTime || item.updatedTime)
      },
      { key: "holding_time", label: "持仓时长", render: (item) => formatHistoryHoldingDuration(item) },
      { key: "strategy_name", label: "策略", render: (item) => item.strategyName || "--" },
      { key: "strategy_version", label: "版本", render: (item) => item.strategyVersion || "--" }
    ];
  }

  return [
    { key: "exchange", label: "交易所", render: (item) => item.exchange || "--" },
    { key: "symbol", label: "交易对", render: (item) => item.symbol || "--" },
    { key: "position_side", label: "方向", render: (item) => formatPositionSideLongShort(item) },
    { key: "leverage_multiplier", label: "杠杆", render: (item) => formatLeverageMultiplier(item.leverageMultiplier) },
    { key: "margin_amount", label: "保证金", render: (item) => formatNumeric(item.marginAmount) },
    { key: "entry_price", label: "开仓价格", render: (item) => formatNumeric(item.entryPrice) },
    { key: "entry_quantity", label: "仓位数量", render: (item) => formatNumeric(item.entryQuantity) },
    { key: "current_price", label: "当前价格", render: (item) => formatNumeric(item.currentPrice) },
    {
      key: "floating_profit",
      label: "浮动收益",
      render: (item) => {
        const tone = resolveAmountRateTone(item.unrealizedProfitAmount, item.unrealizedProfitRate);
        return (
          <span className={`position-rate position-rate--${tone}`}>
            {formatFloatingAmountWithRateStable(item.unrealizedProfitAmount, item.unrealizedProfitRate)}
          </span>
        );
      }
    },
    {
      key: "max_floating_profit",
      label: "最大浮盈",
      render: (item) => formatAmountWithRate(item.maxFloatingProfitAmount, item.maxFloatingProfitRate)
    },
    {
      key: "max_floating_loss",
      label: "最大浮亏",
      render: (item) => formatLossAmountWithRate(item.maxFloatingLossAmount, item.maxFloatingLossRate)
    },
    {
      key: "take_profit_price",
      label: "止盈价格",
      render: (item) => renderPositionTargetPriceWithRate(item, item.takeProfitPrice)
    },
    {
      key: "stop_loss_price",
      label: "止损价格",
      render: (item) => renderPositionTargetPriceWithRate(item, item.stopLossPrice)
    },
    { key: "entry_time", label: "开仓时间", render: (item) => formatPositionTimeInLocalTimezone(item.entryTime) },
    {
      key: "holding_time",
      label: "持仓时长",
      render: (item) => item.holdingTime || formatHoldingDuration(item.entryTime, nowMs)
    },
    { key: "strategy_name", label: "策略", render: (item) => item.strategyName || "--" },
    { key: "strategy_version", label: "版本", render: (item) => item.strategyVersion || "--" }
  ];
}

function compareSignalRows(a: BubbleDatum, b: BubbleDatum): number {
  const symbol = a.symbol.localeCompare(b.symbol);
  if (symbol !== 0) return symbol;
  const timeframe = (a.timeframe || "").localeCompare(b.timeframe || "");
  if (timeframe !== 0) return timeframe;
  const highSideA = resolveSignalTrendValue(a) ?? 0;
  const highSideB = resolveSignalTrendValue(b) ?? 0;
  if (highSideA !== highSideB) return highSideA - highSideB;
  const strategy = (a.strategy || "").localeCompare(b.strategy || "");
  if (strategy !== 0) return strategy;
  return a.id.localeCompare(b.id);
}

function comparePositionRows(a: PositionItem, b: PositionItem): number {
  const symbol = a.symbol.localeCompare(b.symbol);
  if (symbol !== 0) return symbol;
  const timeframe = a.timeframe.localeCompare(b.timeframe);
  if (timeframe !== 0) return timeframe;
  const side = (a.positionSide || "").localeCompare(b.positionSide || "");
  if (side !== 0) return side;
  return a.exchange.localeCompare(b.exchange);
}

function resolveHistoryCloseTimeMs(item: PositionItem): number {
  const closeMs = parsePositionTimeMs(item.exitTime) ?? parsePositionTimeMs(item.updatedTime);
  if (typeof closeMs === "number" && Number.isFinite(closeMs)) {
    return closeMs;
  }
  return parsePositionTimeMs(item.entryTime) || 0;
}

function isClosedPosition(item: PositionItem): boolean {
  const status = (item.status || "").trim().toLowerCase();
  if (!status) return Boolean(item.exitTime);
  if (status.includes("closed") || status.includes("close")) return true;
  if (status.includes("平仓") || status.includes("已平")) return true;
  return Boolean(item.exitTime);
}

function compareHistoryRows(a: PositionItem, b: PositionItem): number {
  const closeDiff = resolveHistoryCloseTimeMs(b) - resolveHistoryCloseTimeMs(a);
  if (closeDiff !== 0) return closeDiff;
  const symbol = a.symbol.localeCompare(b.symbol);
  if (symbol !== 0) return symbol;
  const timeframe = a.timeframe.localeCompare(b.timeframe);
  if (timeframe !== 0) return timeframe;
  return a.exchange.localeCompare(b.exchange);
}

function buildPositionRowKey(item: PositionItem, index: number, mode: PositionColumnsMode): string {
  const suffix = mode === "history" ? "-history" : "";
  return `${item.exchange}-${item.symbol}-${item.timeframe}-${item.positionId || index}${suffix}`;
}

type SignalRowClass = "bullish" | "bearish" | "none";

function resolveSignalRowClass(item: BubbleDatum): SignalRowClass {
  const highSide = resolveSignalTrendValue(item);
  if (highSide === 1) return "bullish";
  if (highSide === -1) return "bearish";
  return "none";
}

type PositionRowTone = "long" | "short" | "none";

function resolvePositionRowTone(item: PositionItem): PositionRowTone {
  const side = (item.positionSide || "").trim().toLowerCase();
  if (side === "long") return "long";
  if (side === "short") return "short";
  return "none";
}

function hasActivePositionForSignal(item: BubbleDatum, activePositionKeySet: Set<string>): boolean {
  const symbol = normalizeSymbolKey(item.symbol || "");
  if (!symbol) return false;
  if (activePositionKeySet.has(symbol)) return true;
  const simplified = simplifySymbolKey(symbol);
  return simplified ? activePositionKeySet.has(simplified) : false;
}

export const SignalsTable = memo(function SignalsTable() {
  const [activeTab, setActiveTab] = useState<TableTab>("signal");
  const [positionNowMs, setPositionNowMs] = useState(() => Date.now());
  const [expandedMobileRowByTab, setExpandedMobileRowByTab] = useState<Partial<Record<TableTab, string>>>({});
  const [expandedEventRowByTab, setExpandedEventRowByTab] = useState<Partial<Record<EventDrawerTab, string>>>({});
  const [eventStateByKey, setEventStateByKey] = useState<Record<string, PositionEventsLoadState>>({});
  const isMobileLayout = useMobileLayout(MOBILE_LAYOUT_MAX_WIDTH);
  const signalRows = useAppStore((s) => s.dataList);
  const allDataList = useAppStore((s) => s.allDataList);
  const positionFeedLive = useAppStore((s) => isPositionFeedLive(s.dataSourceStatus));
  const positionSnapshot = useAppStore((s) => s.positionSnapshot);
  const historySnapshot = useAppStore((s) => s.historySnapshot);
  const requestMoreHistory = useAppStore((s) => s.requestMoreHistory);
  const positionRows = positionSnapshot?.positions || [];
  const historyRows = historySnapshot?.positions || [];

  const sortedSignalRows = useMemo(() => {
    return signalRows
      .filter((item) => {
        const side = resolveSignalTrendValue(item);
        return side === 1 || side === -1;
      })
      .sort(compareSignalRows);
  }, [signalRows]);

  const sortedPositionRows = useMemo(() => {
    return [...positionRows].sort(comparePositionRows);
  }, [positionRows]);

  const activePositionKeySet = useMemo(() => {
    const set = new Set<string>();
    for (const item of sortedPositionRows) {
      const side = (item.positionSide || "").trim().toLowerCase();
      if (side !== "long" && side !== "short") continue;
      const symbol = normalizeSymbolKey(item.symbol || "");
      if (!symbol) continue;
      set.add(symbol);
      const simplified = simplifySymbolKey(symbol);
      if (simplified) {
        set.add(simplified);
      }
    }
    return set;
  }, [sortedPositionRows]);

  const sortedHistoryRows = useMemo(() => {
    return [...historyRows].sort(compareHistoryRows);
  }, [historyRows]);

  const positionRowsWithKey = useMemo(() => {
    return sortedPositionRows.map((item, index) => ({
      item,
      rowKey: buildPositionRowKey(item, index, "position")
    }));
  }, [sortedPositionRows]);

  const historyRowsWithKey = useMemo(() => {
    return sortedHistoryRows.map((item, index) => ({
      item,
      rowKey: buildPositionRowKey(item, index, "history")
    }));
  }, [sortedHistoryRows]);

  const positionCount = useMemo(() => {
    const countFromApi = positionSnapshot?.count;
    if (typeof countFromApi === "number" && Number.isFinite(countFromApi) && countFromApi > 0) {
      return Math.max(0, Math.trunc(countFromApi));
    }
    return sortedPositionRows.length;
  }, [positionSnapshot?.count, sortedPositionRows.length]);
  const historyClosedCount = useMemo(() => {
    return sortedHistoryRows.reduce((count, item) => (isClosedPosition(item) ? count + 1 : count), 0);
  }, [sortedHistoryRows]);
  const signalCount = sortedSignalRows.length;

  const signalLinkItemsById = useMemo(() => {
    const map = new Map<string, ReturnType<typeof buildSignalLinkItems>>();
    for (const item of sortedSignalRows) {
      map.set(item.id, buildSignalLinkItems(item, allDataList));
    }
    return map;
  }, [allDataList, sortedSignalRows]);
  const positionLinkItemsByRowKey = useMemo(() => {
    const map = new Map<string, ReturnType<typeof buildSignalLinkItems>>();
    for (const row of positionRowsWithKey) {
      map.set(row.rowKey, buildSignalLinkItems(row.item, allDataList));
    }
    return map;
  }, [allDataList, positionRowsWithKey]);
  const historyLinkItemsByRowKey = useMemo(() => {
    const map = new Map<string, ReturnType<typeof buildSignalLinkItems>>();
    for (const row of historyRowsWithKey) {
      map.set(
        row.rowKey,
        buildSignalLinkItems(row.item, allDataList, {
          visualHistory: {
            enabled: true,
            date:
              resolveVisualHistoryDateFromEntryTime(row.item.entryTime) ||
              resolveVisualHistoryDateFromEntryTime(row.item.updatedTime),
            exchange: row.item.exchange,
            symbol: row.item.symbol,
            strategy: row.item.strategyName,
            version: row.item.strategyVersion
          }
        })
      );
    }
    return map;
  }, [allDataList, historyRowsWithKey]);

  const positionColumns = useMemo(() => buildPositionColumns(positionNowMs, "position"), [positionNowMs]);
  const historyColumns = useMemo(() => buildPositionColumns(positionNowMs, "history"), [positionNowMs]);
  const signalColumnsByKey = useMemo(() => toColumnMap<BubbleDatum>(SIGNAL_COLUMNS), []);
  const positionColumnsByKey = useMemo(() => toColumnMap<PositionItem>(positionColumns), [positionColumns]);
  const historyColumnsByKey = useMemo(() => toColumnMap<PositionItem>(historyColumns), [historyColumns]);
  const signalDetailColumns = useMemo(() => {
    return SIGNAL_COLUMNS.filter((column) => !MOBILE_SIGNAL_SUMMARY_KEY_SET.has(column.key));
  }, []);
  const positionDetailColumns = useMemo(() => {
    return positionColumns.filter((column) => !MOBILE_POSITION_SUMMARY_KEY_SET.has(column.key));
  }, [positionColumns]);
  const historyDetailColumns = useMemo(() => {
    return historyColumns.filter((column) => !MOBILE_HISTORY_SUMMARY_KEY_SET.has(column.key));
  }, [historyColumns]);
  const shouldTickHoldingTime = activeTab === "position" && sortedPositionRows.length > 0 && positionFeedLive;

  useEffect(() => {
    if (!shouldTickHoldingTime) return;
    setPositionNowMs(Date.now());
    const timer = window.setInterval(() => {
      setPositionNowMs(Date.now());
    }, 1000);
    return () => window.clearInterval(timer);
  }, [shouldTickHoldingTime]);

  useEffect(() => {
    setExpandedMobileRowByTab((prev) => {
      const next: Partial<Record<TableTab, string>> = { ...prev };
      let changed = false;
      if (prev.signal && !sortedSignalRows.some((item) => item.id === prev.signal)) {
        next.signal = undefined;
        changed = true;
      }
      if (prev.position && !positionRowsWithKey.some((row) => row.rowKey === prev.position)) {
        next.position = undefined;
        changed = true;
      }
      if (prev.history && !historyRowsWithKey.some((row) => row.rowKey === prev.history)) {
        next.history = undefined;
        changed = true;
      }
      return changed ? next : prev;
    });
  }, [historyRowsWithKey, positionRowsWithKey, sortedSignalRows]);

  useEffect(() => {
    setExpandedEventRowByTab((prev) => {
      const next: Partial<Record<EventDrawerTab, string>> = { ...prev };
      let changed = false;
      if (prev.signal && !sortedSignalRows.some((item) => item.id === prev.signal)) {
        next.signal = undefined;
        changed = true;
      }
      if (prev.position && !positionRowsWithKey.some((row) => row.rowKey === prev.position)) {
        next.position = undefined;
        changed = true;
      }
      if (prev.history && !historyRowsWithKey.some((row) => row.rowKey === prev.history)) {
        next.history = undefined;
        changed = true;
      }
      return changed ? next : prev;
    });
  }, [historyRowsWithKey, positionRowsWithKey]);

  const toggleMobileRow = (tab: TableTab, rowKey: string) => {
    setExpandedMobileRowByTab((prev) => ({
      ...prev,
      [tab]: prev[tab] === rowKey ? undefined : rowKey
    }));
  };
  const handleMobileRowKeyDown = (event: ReactKeyboardEvent<HTMLElement>, tab: TableTab, rowKey: string) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    toggleMobileRow(tab, rowKey);
  };

  const loadEventSnapshot = async (cacheKey: string, url: string) => {
    const current = eventStateByKey[cacheKey];
    if (current?.status === "loading" || current?.status === "ready") {
      return;
    }
    setEventStateByKey((prev) => ({
      ...prev,
      [cacheKey]: { status: "loading" }
    }));

    try {
      const response = await fetch(url);
      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        const message =
          isRecord(payload) && typeof payload.error === "string" && payload.error.trim().length > 0
            ? payload.error.trim()
            : `${response.status} ${response.statusText}`.trim();
        throw new Error(message || "事件加载失败");
      }
      const snapshot = normalizePositionEventsSnapshot(payload);
      if (!snapshot) {
        throw new Error("事件响应无效");
      }
      setEventStateByKey((prev) => ({
        ...prev,
        [cacheKey]: {
          status: "ready",
          snapshot
        }
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "事件加载失败";
      setEventStateByKey((prev) => ({
        ...prev,
        [cacheKey]: {
          status: "error",
          error: message
        }
      }));
    }
  };

  const loadSignalEvents = async (rowKey: string, item: BubbleDatum) => {
    await loadEventSnapshot(`signal:${rowKey}`, buildSignalEventsUrl(item));
  };

  const loadPositionEvents = async (tab: Exclude<EventDrawerTab, "signal">, rowKey: string, item: PositionItem) => {
    await loadEventSnapshot(`${tab}:${rowKey}`, buildPositionEventsUrl(tab, item));
  };

  const toggleSignalEventRow = (rowKey: string, item: BubbleDatum) => {
    const isExpanded = expandedEventRowByTab.signal === rowKey;
    setExpandedEventRowByTab((prev) => ({
      ...prev,
      signal: isExpanded ? undefined : rowKey
    }));
    if (!isExpanded) {
      void loadSignalEvents(rowKey, item);
    }
  };

  const toggleEventRow = (tab: EventDrawerTab, rowKey: string, item: PositionItem) => {
    const isExpanded = expandedEventRowByTab[tab] === rowKey;
    setExpandedEventRowByTab((prev) => ({
      ...prev,
      [tab]: isExpanded ? undefined : rowKey
    }));
    if (!isExpanded) {
      void loadPositionEvents(tab, rowKey, item);
    }
  };

  return (
    <section className="signals-panel">
      <div className="signals-panel__tabs" role="tablist" aria-label="列表标签">
        <button
          type="button"
          className={`timeframe-tab signals-panel__tab ${activeTab === "signal" ? "timeframe-tab--active signals-panel__tab--active" : ""}`}
          role="tab"
          aria-selected={activeTab === "signal"}
          onClick={() => setActiveTab("signal")}
        >
          {signalCount > 0 ? `signal (${signalCount})` : "signal"}
        </button>
        <button
          type="button"
          className={`timeframe-tab signals-panel__tab ${activeTab === "position" ? "timeframe-tab--active signals-panel__tab--active" : ""}`}
          role="tab"
          aria-selected={activeTab === "position"}
          onClick={() => setActiveTab("position")}
        >
          {positionCount > 0 ? `position (${positionCount})` : "position"}
        </button>
        <button
          type="button"
          className={`timeframe-tab signals-panel__tab ${activeTab === "history" ? "timeframe-tab--active signals-panel__tab--active" : ""}`}
          role="tab"
          aria-selected={activeTab === "history"}
          onClick={() => setActiveTab("history")}
        >
          {historyClosedCount > 0 ? `history (${historyClosedCount})` : "history"}
        </button>
      </div>

      {isMobileLayout ? (
        <div className="signals-mobile-wrap">
          {activeTab === "signal" ? (
            sortedSignalRows.length === 0 ? (
              <div className="signals-table__empty signals-mobile-empty">暂无可显示的信号数据</div>
            ) : (
              <div className="signals-mobile-list">
                {sortedSignalRows.map((item, index) => {
                  const rowKey = item.id;
                  const detailId = `signals-mobile-signal-${index}`;
                  const expanded = expandedMobileRowByTab.signal === rowKey;
                  const eventExpanded = expandedEventRowByTab.signal === rowKey;
                  const eventState = eventStateByKey[`signal:${rowKey}`];
                  const rowTone = resolveSignalRowClass(item);
                  const isActiveSignal = hasActivePositionForSignal(item, activePositionKeySet);
                  const linkItems = signalLinkItemsById.get(item.id) || [];
                  return (
                    <article
                      key={rowKey}
                      className={`signals-mobile-card signals-mobile-card--signal signals-mobile-card--signal-${rowTone} ${isActiveSignal ? "signals-mobile-card--signal-active" : ""}`.trim()}
                    >
                      <div className="signals-mobile-card__summary-shell">
                        <div
                          className="signals-mobile-card__summary"
                          role="button"
                          tabIndex={0}
                          aria-expanded={expanded}
                          aria-controls={detailId}
                          onClick={() => toggleMobileRow("signal", rowKey)}
                          onKeyDown={(event) => handleMobileRowKeyDown(event, "signal", rowKey)}
                        >
                          <div className="signals-mobile-card__summary-grid">
                            {MOBILE_SIGNAL_SUMMARY_KEYS.map((key) => {
                              const column = signalColumnsByKey.get(key);
                              return (
                                <div key={`${rowKey}-summary-${key}`} className="signals-mobile-card__summary-item">
                                  <span className="signals-mobile-card__summary-value">
                                    {renderMobileSignalSummaryValue(item, key, column)}
                                  </span>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                        <button
                          type="button"
                          className={`signals-mobile-card__event-toggle ${eventExpanded ? "is-expanded" : ""}`.trim()}
                          aria-label={eventExpanded ? "收起事件面板" : "展开事件面板"}
                          aria-expanded={eventExpanded}
                          onClick={(event) => {
                            event.stopPropagation();
                            toggleSignalEventRow(rowKey, item);
                          }}
                        >
                          {eventExpanded ? "▴" : "▾"}
                        </button>
                      </div>
                      {eventExpanded ? (
                        <div className="signals-event-drawer signals-event-drawer--mobile">
                          <div className="signals-event-drawer__header">
                            <span className="signals-event-drawer__title">Event Panel</span>
                            <span className="signals-event-drawer__count">{eventPanelCountText(eventState?.snapshot)}</span>
                          </div>
                          {renderPositionEventDrawer(eventState)}
                        </div>
                      ) : null}
                      {expanded ? (
                        <div id={detailId} className="signals-mobile-card__details">
                          {signalDetailColumns.map((column) => (
                            <div key={`${rowKey}-detail-${column.key}`} className="signals-mobile-card__detail-row">
                              <span>{column.label}</span>
                              <span>{column.render(item)}</span>
                            </div>
                          ))}
                          <div className="signals-mobile-card__links">
                            <div className="signals-mobile-card__links-title">链接</div>
                            {renderSignalLinks(rowKey, item.symbol, linkItems)}
                          </div>
                        </div>
                      ) : null}
                    </article>
                  );
                })}
              </div>
            )
          ) : activeTab === "position" ? (
            sortedPositionRows.length === 0 ? (
              <div className="signals-table__empty signals-mobile-empty">暂无可显示的持仓数据</div>
            ) : (
              <div className="signals-mobile-list">
                {positionRowsWithKey.map((row, index) => {
                  const item = row.item;
                  const rowKey = row.rowKey;
                  const detailId = `signals-mobile-position-${index}`;
                  const expanded = expandedMobileRowByTab.position === rowKey;
                  const eventExpanded = expandedEventRowByTab.position === rowKey;
                  const eventState = eventStateByKey[`position:${rowKey}`];
                  const rowTone = resolvePositionRowTone(item);
                  const linkItems = positionLinkItemsByRowKey.get(rowKey) || [];
                  return (
                    <article key={rowKey} className={`signals-mobile-card signals-mobile-card--position signals-mobile-card--position-${rowTone}`}>
                      <div className="signals-mobile-card__summary-shell">
                        <div
                          className="signals-mobile-card__summary"
                          role="button"
                          tabIndex={0}
                          aria-expanded={expanded}
                          aria-controls={detailId}
                          onClick={() => toggleMobileRow("position", rowKey)}
                          onKeyDown={(event) => handleMobileRowKeyDown(event, "position", rowKey)}
                        >
                          <div className="signals-mobile-card__summary-grid">
                            {MOBILE_POSITION_SUMMARY_KEYS.map((key) => {
                              const column = positionColumnsByKey.get(key);
                              return (
                                <div key={`${rowKey}-summary-${key}`} className="signals-mobile-card__summary-item">
                                  <span className="signals-mobile-card__summary-value">
                                    {renderMobilePositionSummaryValue(item, key, column)}
                                  </span>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                        <button
                          type="button"
                          className={`signals-mobile-card__event-toggle ${eventExpanded ? "is-expanded" : ""}`.trim()}
                          aria-label={eventExpanded ? "收起事件面板" : "展开事件面板"}
                          aria-expanded={eventExpanded}
                          onClick={(event) => {
                            event.stopPropagation();
                            toggleEventRow("position", rowKey, item);
                          }}
                        >
                          {eventExpanded ? "▴" : "▾"}
                        </button>
                      </div>
                      {eventExpanded ? (
                        <div className="signals-event-drawer signals-event-drawer--mobile">
                          <div className="signals-event-drawer__header">
                            <span className="signals-event-drawer__title">Event Panel</span>
                            <span className="signals-event-drawer__count">{eventPanelCountText(eventState?.snapshot)}</span>
                          </div>
                          {renderPositionEventDrawer(eventState)}
                        </div>
                      ) : null}
                      {expanded ? (
                        <div id={detailId} className="signals-mobile-card__details">
                          {positionDetailColumns.map((column) => (
                            <div key={`${rowKey}-detail-${column.key}`} className="signals-mobile-card__detail-row">
                              <span>{column.label}</span>
                              <span>{column.render(item)}</span>
                            </div>
                          ))}
                          <div className="signals-mobile-card__links">
                            <div className="signals-mobile-card__links-title">链接</div>
                            {renderSignalLinks(rowKey, item.symbol, linkItems)}
                          </div>
                        </div>
                      ) : null}
                    </article>
                  );
                })}
              </div>
            )
          ) : (
            <>
              {sortedHistoryRows.length === 0 ? (
                <div className="signals-table__empty signals-mobile-empty">暂无可显示的历史仓位数据</div>
              ) : (
                <div className="signals-mobile-list">
                  {historyRowsWithKey.map((row, index) => {
                    const item = row.item;
                    const rowKey = row.rowKey;
                    const detailId = `signals-mobile-history-${index}`;
                    const expanded = expandedMobileRowByTab.history === rowKey;
                    const eventExpanded = expandedEventRowByTab.history === rowKey;
                    const eventState = eventStateByKey[`history:${rowKey}`];
                    const rowTone = resolvePositionRowTone(item);
                    const linkItems = historyLinkItemsByRowKey.get(rowKey) || [];
                    return (
                      <article key={rowKey} className={`signals-mobile-card signals-mobile-card--position signals-mobile-card--position-${rowTone}`}>
                        <div className="signals-mobile-card__summary-shell">
                          <div
                            className="signals-mobile-card__summary"
                            role="button"
                            tabIndex={0}
                            aria-expanded={expanded}
                            aria-controls={detailId}
                            onClick={() => toggleMobileRow("history", rowKey)}
                            onKeyDown={(event) => handleMobileRowKeyDown(event, "history", rowKey)}
                          >
                            <div className="signals-mobile-card__summary-grid">
                              {MOBILE_HISTORY_SUMMARY_KEYS.map((key) => {
                                const column = historyColumnsByKey.get(key);
                                return (
                                  <div key={`${rowKey}-summary-${key}`} className="signals-mobile-card__summary-item">
                                    <span className="signals-mobile-card__summary-value">
                                      {renderMobilePositionSummaryValue(item, key, column)}
                                    </span>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                          <button
                            type="button"
                            className={`signals-mobile-card__event-toggle ${eventExpanded ? "is-expanded" : ""}`.trim()}
                            aria-label={eventExpanded ? "收起事件面板" : "展开事件面板"}
                            aria-expanded={eventExpanded}
                            onClick={(event) => {
                              event.stopPropagation();
                              toggleEventRow("history", rowKey, item);
                            }}
                          >
                            {eventExpanded ? "▴" : "▾"}
                          </button>
                        </div>
                        {eventExpanded ? (
                          <div className="signals-event-drawer signals-event-drawer--mobile">
                            <div className="signals-event-drawer__header">
                              <span className="signals-event-drawer__title">Event Panel</span>
                              <span className="signals-event-drawer__count">{eventPanelCountText(eventState?.snapshot)}</span>
                            </div>
                            {renderPositionEventDrawer(eventState)}
                          </div>
                        ) : null}
                        {expanded ? (
                          <div id={detailId} className="signals-mobile-card__details">
                            {historyDetailColumns.map((column) => (
                              <div key={`${rowKey}-detail-${column.key}`} className="signals-mobile-card__detail-row">
                                <span>{column.label}</span>
                                <span>{column.render(item)}</span>
                              </div>
                            ))}
                            <div className="signals-mobile-card__links">
                              <div className="signals-mobile-card__links-title">链接</div>
                              {renderSignalLinks(rowKey, item.symbol, linkItems)}
                            </div>
                          </div>
                        ) : null}
                      </article>
                    );
                  })}
                </div>
              )}
              <div className="signals-table__history-more">
                <button
                  type="button"
                  className="signals-table__history-more-btn"
                  onClick={requestMoreHistory}
                  aria-label="加载更早历史仓位"
                  title="加载更早历史仓位"
                >
                  <span aria-hidden="true">⌄</span>
                </button>
              </div>
            </>
          )}
        </div>
      ) : activeTab === "signal" ? (
        <div className="signals-table-wrap">
          <table className="signals-table">
            <thead>
              <tr>
                {SIGNAL_COLUMNS.map((column) => (
                  <th key={column.key}>{column.label}</th>
                ))}
                <th>链接</th>
                <th className="signals-table__toggle-head" aria-label="事件面板" />
              </tr>
            </thead>
            <tbody>
              {sortedSignalRows.length === 0 ? (
                <tr>
                  <td className="signals-table__empty" colSpan={SIGNAL_COLUMNS.length + 2}>
                    暂无可显示的信号数据
                  </td>
                </tr>
              ) : (
                sortedSignalRows.map((item) => {
                  const eventExpanded = expandedEventRowByTab.signal === item.id;
                  const eventState = eventStateByKey[`signal:${item.id}`];
                  const isActiveSignal = hasActivePositionForSignal(item, activePositionKeySet);
                  return (
                    <Fragment key={item.id}>
                      <tr
                        className={`signals-table__row signals-table__row--${resolveSignalRowClass(item)} ${isActiveSignal ? "signals-table__row--signal-active" : ""}`.trim()}
                      >
                        {SIGNAL_COLUMNS.map((column) => (
                          <td key={`${item.id}-${column.key}`}>{column.render(item)}</td>
                        ))}
                        <td className="signals-table__links-cell">{renderSignalLinks(item.id, item.symbol, signalLinkItemsById.get(item.id) || [])}</td>
                        <td className="signals-table__toggle-cell">
                          <button
                            type="button"
                            className={`signals-table__event-toggle ${eventExpanded ? "is-expanded" : ""}`.trim()}
                            aria-label={eventExpanded ? "收起事件面板" : "展开事件面板"}
                            aria-expanded={eventExpanded}
                            onClick={() => toggleSignalEventRow(item.id, item)}
                          >
                            {eventExpanded ? "▴" : "▾"}
                          </button>
                        </td>
                      </tr>
                      {eventExpanded ? (
                        <tr className="signals-table__drawer-row">
                          <td className="signals-table__drawer-cell" colSpan={SIGNAL_COLUMNS.length + 2}>
                            <div className="signals-event-drawer">
                              <div className="signals-event-drawer__header">
                                <span className="signals-event-drawer__title">Event Panel</span>
                                <span className="signals-event-drawer__count">{eventPanelCountText(eventState?.snapshot)}</span>
                              </div>
                              {renderPositionEventDrawer(eventState)}
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      ) : activeTab === "position" ? (
        <div className="signals-table-wrap">
          <table className="signals-table">
            <thead>
              <tr>
                {positionColumns.map((column) => (
                  <th key={column.key}>{column.label}</th>
                ))}
                <th>链接</th>
                <th className="signals-table__toggle-head" aria-label="事件面板" />
              </tr>
            </thead>
            <tbody>
              {sortedPositionRows.length === 0 ? (
                <tr>
                  <td className="signals-table__empty" colSpan={positionColumns.length + 2}>
                    暂无可显示的持仓数据
                  </td>
                </tr>
              ) : (
                positionRowsWithKey.map((row) => {
                  const eventExpanded = expandedEventRowByTab.position === row.rowKey;
                  const eventState = eventStateByKey[`position:${row.rowKey}`];
                  return (
                    <Fragment key={row.rowKey}>
                      <tr className={`signals-table__row signals-table__row--position-${resolvePositionRowTone(row.item)}`}>
                        {positionColumns.map((column) => (
                          <td key={`${row.rowKey}-${column.key}`}>{column.render(row.item)}</td>
                        ))}
                        <td className="signals-table__links-cell">{renderSignalLinks(row.rowKey, row.item.symbol, positionLinkItemsByRowKey.get(row.rowKey) || [])}</td>
                        <td className="signals-table__toggle-cell">
                          <button
                            type="button"
                            className={`signals-table__event-toggle ${eventExpanded ? "is-expanded" : ""}`.trim()}
                            aria-label={eventExpanded ? "收起事件面板" : "展开事件面板"}
                            aria-expanded={eventExpanded}
                            onClick={() => toggleEventRow("position", row.rowKey, row.item)}
                          >
                            {eventExpanded ? "▴" : "▾"}
                          </button>
                        </td>
                      </tr>
                      {eventExpanded ? (
                        <tr className="signals-table__drawer-row">
                          <td className="signals-table__drawer-cell" colSpan={positionColumns.length + 2}>
                            <div className="signals-event-drawer">
                              <div className="signals-event-drawer__header">
                                <span className="signals-event-drawer__title">Event Panel</span>
                                <span className="signals-event-drawer__count">{eventPanelCountText(eventState?.snapshot)}</span>
                              </div>
                              {renderPositionEventDrawer(eventState)}
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="signals-table-wrap">
          <table className="signals-table">
            <thead>
              <tr>
                {historyColumns.map((column) => (
                  <th key={column.key}>{column.label}</th>
                ))}
                <th>链接</th>
                <th className="signals-table__toggle-head" aria-label="事件面板" />
              </tr>
            </thead>
            <tbody>
              {sortedHistoryRows.length === 0 ? (
                <tr>
                  <td className="signals-table__empty" colSpan={historyColumns.length + 2}>
                    暂无可显示的历史仓位数据
                  </td>
                </tr>
              ) : (
                historyRowsWithKey.map((row) => {
                  const eventExpanded = expandedEventRowByTab.history === row.rowKey;
                  const eventState = eventStateByKey[`history:${row.rowKey}`];
                  return (
                    <Fragment key={row.rowKey}>
                      <tr className={`signals-table__row signals-table__row--position-${resolvePositionRowTone(row.item)}`}>
                        {historyColumns.map((column) => (
                          <td key={`${row.rowKey}-${column.key}`}>{column.render(row.item)}</td>
                        ))}
                        <td className="signals-table__links-cell">{renderSignalLinks(row.rowKey, row.item.symbol, historyLinkItemsByRowKey.get(row.rowKey) || [])}</td>
                        <td className="signals-table__toggle-cell">
                          <button
                            type="button"
                            className={`signals-table__event-toggle ${eventExpanded ? "is-expanded" : ""}`.trim()}
                            aria-label={eventExpanded ? "收起事件面板" : "展开事件面板"}
                            aria-expanded={eventExpanded}
                            onClick={() => toggleEventRow("history", row.rowKey, row.item)}
                          >
                            {eventExpanded ? "▴" : "▾"}
                          </button>
                        </td>
                      </tr>
                      {eventExpanded ? (
                        <tr className="signals-table__drawer-row">
                          <td className="signals-table__drawer-cell" colSpan={historyColumns.length + 2}>
                            <div className="signals-event-drawer">
                              <div className="signals-event-drawer__header">
                                <span className="signals-event-drawer__title">Event Panel</span>
                                <span className="signals-event-drawer__count">{eventPanelCountText(eventState?.snapshot)}</span>
                              </div>
                              {renderPositionEventDrawer(eventState)}
                            </div>
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })
              )}
            </tbody>
          </table>
          <div className="signals-table__history-more">
            <button
              type="button"
              className="signals-table__history-more-btn"
              onClick={requestMoreHistory}
              aria-label="加载更早历史仓位"
              title="加载更早历史仓位"
            >
              <span aria-hidden="true">⌄</span>
            </button>
          </div>
        </div>
      )}
    </section>
  );
});
