import type { CSSProperties, ReactNode } from "react";
import { memo, useEffect, useMemo, useRef, useState } from "react";
import { AppConfig } from "../app/config";
import type { BubbleCandleEvent, BubbleDatum, PositionItem } from "../app/types";
import { useAppStore } from "../app/store";
import tradingViewBrandIcon from "../assets/icon-tradingview.svg";
import okxBrandIcon from "../assets/icon-okx.png";
import binanceBrandIcon from "../assets/icon-binance.png";
import bitgetBrandIcon from "../assets/icon-bitget.png";
import {
  buildSignalLinkItems,
  resolveSymbolExchanges
} from "./signalLinks";
import { BubbleKlinePanel } from "./BubbleKlinePanel";

type DetailRow = {
  label: string;
  value: string;
};

type PositionSummaryCell = {
  key: string;
  label: string;
  value: ReactNode;
  tone?: "positive" | "negative" | "neutral";
};

type SignalEventsSnapshot = {
  count: number;
  total: number;
  truncated: boolean;
  events: BubbleCandleEvent[];
};

type SignalEventsLoadState = {
  status: "idle" | "loading" | "ready" | "error";
  snapshot?: SignalEventsSnapshot;
  error?: string;
};

type EventPanelPlacement = {
  mode: "side" | "stacked";
  top: number;
  left: number;
  width: number;
  maxHeight: number;
};

const BRAND_ICON_SRC_BY_TONE: Record<string, string> = {
  tradingview: tradingViewBrandIcon,
  binance: binanceBrandIcon,
  okx: okxBrandIcon,
  bitget: bitgetBrandIcon
};

const EVENT_PANEL_LIMIT = 5000;
const EVENT_PANEL_GAP = 16;
const EVENT_PANEL_SIDE_WIDTH = 420;
const EVENT_PANEL_STACKED_GAP = 12;

const KNOWN_QUOTES = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "EUR", "GBP", "JPY"] as const;

function formatLocalDateTime(timestamp?: number): string {
  if (typeof timestamp !== "number" || !Number.isFinite(timestamp) || timestamp <= 0) {
    return "--";
  }
  const normalized = timestamp < 1_000_000_000_000 ? timestamp * 1000 : timestamp;
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

function formatValue(value: string | number | undefined): string {
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "--";
  if (typeof value === "string") return value.trim() || "--";
  return "--";
}

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

function normalizeSignalEvent(payload: unknown): BubbleCandleEvent | null {
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

function normalizeSignalEventsSnapshot(payload: unknown): SignalEventsSnapshot | null {
  if (!isRecord(payload)) return null;
  const eventsRaw = Array.isArray(payload.events) ? payload.events : [];
  const events = eventsRaw
    .map((item) => normalizeSignalEvent(item))
    .filter((item): item is BubbleCandleEvent => item !== null)
    .sort((left, right) => left.eventAtMs - right.eventAtMs);
  const countRaw = payload.count;
  const totalRaw = payload.total;
  return {
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
  return allowed
    .slice(0, 3)
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(" · ");
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

  if (hasArmed && typeof trendingTimestamp === "number" && trendingTimestamp > 0) {
    const merged: string[] = ["TREND_DETECTED", "ARMED"];
    for (const token of tokens) {
      if (token === "TREND_DETECTED" || token === "ARMED") continue;
      if (!merged.includes(token)) {
        merged.push(token);
      }
    }
    return merged;
  }

  if (tokens.length > 0) return tokens;
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

function eventPanelCountText(snapshot?: SignalEventsSnapshot): string {
  if (!snapshot) return "0";
  if (snapshot.truncated && snapshot.total > snapshot.events.length) {
    return `${snapshot.events.length}/${snapshot.total}`;
  }
  return String(snapshot.total || snapshot.count || snapshot.events.length);
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

function renderSignalEventDrawer(loadState: SignalEventsLoadState | undefined): ReactNode {
  if (!loadState || loadState.status === "idle") {
    return <div className="signals-event-drawer__state">点击下方按钮加载事件</div>;
  }
  if (loadState.status === "loading") {
    return <div className="signals-event-drawer__state">事件加载中...</div>;
  }
  if (loadState.status === "error") {
    return (
      <div className="signals-event-drawer__state signals-event-drawer__state--error">
        {loadState.error || "事件加载失败"}
      </div>
    );
  }

  const snapshot = loadState.snapshot;
  if (!snapshot || snapshot.events.length === 0) {
    return <div className="signals-event-drawer__state">当前无事件</div>;
  }

  return (
    <>
      <div className="details-modal__event-sidepanel-scroll">
        <div className="signals-event-drawer__table">
          {snapshot.events.map((event) => {
            const typeText = formatEventPanelType(event);
            const titleText = formatEventPanelTitle(event);
            const summaryText = event.summary || "-";
            const metaText = formatEventPanelMeta(event);
            return (
              <article key={event.id} className={`signals-event-drawer__row level-${event.level}`}>
                <span className="signals-event-drawer__cell signals-event-drawer__cell--time">
                  {formatEventPanelTime(event.eventAtMs)}
                </span>
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
      </div>
      {snapshot.truncated && snapshot.total > snapshot.events.length ? (
        <div className="signals-event-drawer__footnote">
          仅显示最近 {snapshot.events.length} 条事件，共 {snapshot.total} 条。
        </div>
      ) : null}
    </>
  );
}

function formatSignalDirection(highSide?: number, side?: number): string {
  const raw =
    typeof highSide === "number" && Number.isFinite(highSide)
      ? Math.trunc(highSide)
      : typeof side === "number" && Number.isFinite(side)
      ? Math.trunc(side)
      : undefined;
  if (typeof raw !== "number") return "--";
  if (raw === 1 || raw === 8) return "LONG";
  if (raw === -1 || raw === -8) return "SHORT";
  if (raw === 255) return "LONG-RANGE";
  if (raw === -255) return "SHORT-RANGE";
  if (raw === 0) return "RANGE";
  return String(raw);
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

  return undefined;
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

function formatNumeric(value?: number, maxFractionDigits = 6): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
  if (abs >= 1) {
    return value.toLocaleString("en-US", { maximumFractionDigits: Math.max(2, maxFractionDigits) });
  }
  return value.toLocaleString("en-US", { maximumFractionDigits });
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

function normalizeRatePercent(value?: number): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function formatRatePercent(value?: number): string {
  const normalized = normalizeRatePercent(value);
  if (typeof normalized !== "number") return "--";
  return `${normalized.toFixed(2)}%`;
}

function formatFloatingAmountWithRate(amount?: number, rate?: number): string {
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

function resolveAmountRateTone(amount?: number, rate?: number): "positive" | "negative" | "neutral" {
  if (typeof amount === "number" && Number.isFinite(amount)) {
    return resolveSignedTone(amount);
  }
  return resolveSignedTone(normalizeRatePercent(rate));
}

function formatLeverageMultiplier(value?: number): string {
  const normalized = formatNumeric(value, 4);
  if (normalized === "--") return "--";
  return `${normalized}x`;
}

function normalizeExchange(value?: string): string {
  return value?.trim().toLowerCase() || "";
}

function resolvePositionSideLongShort(item: PositionItem): "LONG" | "SHORT" | "--" {
  const side = (item.positionSide || "").trim().toLowerCase();
  if (side === "long" || side === "buy") return "LONG";
  if (side === "short" || side === "sell") return "SHORT";
  return "--";
}

function buildSymbolVariants(symbol?: string): Set<string> {
  const out = new Set<string>();
  const raw = symbol?.trim().toUpperCase() || "";
  if (!raw) return out;

  const noSuffix = raw.replace(/\.P$/i, "");
  out.add(noSuffix);

  const compact = noSuffix.replace(/[^A-Z0-9]/g, "");
  if (compact) out.add(compact);

  const splitParts = noSuffix.split(/[/:_-]+/).filter(Boolean);
  if (splitParts.length >= 2) {
    const base = splitParts[0];
    const quote = splitParts[1];
    out.add(`${base}/${quote}`);
    out.add(`${base}-${quote}`);
    out.add(`${base}${quote}`);
  } else {
    const matchedQuote = KNOWN_QUOTES.find((quote) => compact.endsWith(quote) && compact.length > quote.length);
    if (matchedQuote) {
      const base = compact.slice(0, compact.length - matchedQuote.length);
      out.add(`${base}/${matchedQuote}`);
      out.add(`${base}-${matchedQuote}`);
      out.add(`${base}${matchedQuote}`);
    }
  }

  return out;
}

function symbolsLikelyMatch(left?: string, right?: string): boolean {
  const leftVariants = buildSymbolVariants(left);
  const rightVariants = buildSymbolVariants(right);
  if (leftVariants.size === 0 || rightVariants.size === 0) return false;
  for (const value of leftVariants) {
    if (rightVariants.has(value)) return true;
  }
  return false;
}

function scorePositionMatch(
  selectedExchange: string,
  selectedTimeframe: string,
  item: PositionItem
): number {
  const exchangeMatch =
    selectedExchange.length > 0 &&
    selectedExchange === normalizeExchange(item.exchange);
  const timeframeMatch =
    selectedTimeframe.length > 0 &&
    selectedTimeframe === (item.timeframe || "").trim().toLowerCase();

  if (exchangeMatch && timeframeMatch) return 4;
  if (exchangeMatch) return 3;
  if (timeframeMatch) return 2;
  return 1;
}

export const BubbleDetailsModal = memo(function BubbleDetailsModal() {
  const selectedBubbleId = useAppStore((s) => s.selectedBubbleId);
  const dataMap = useAppStore((s) => s.dataMap);
  const allDataList = useAppStore((s) => s.allDataList);
  const positionSnapshot = useAppStore((s) => s.positionSnapshot);
  const clearSelectedBubbleId = useAppStore((s) => s.clearSelectedBubbleId);
  const modalRef = useRef<HTMLElement | null>(null);
  const eventToggleRef = useRef<HTMLDivElement | null>(null);
  const eventSidePanelRef = useRef<HTMLElement | null>(null);
  const [topOffset, setTopOffset] = useState(84);
  const [positionNowMs, setPositionNowMs] = useState(() => Date.now());
  const [eventPanelOpen, setEventPanelOpen] = useState(false);
  const [eventStateByBubbleId, setEventStateByBubbleId] = useState<Record<string, SignalEventsLoadState>>({});
  const [eventPanelPlacement, setEventPanelPlacement] = useState<EventPanelPlacement | undefined>();
  const selected = selectedBubbleId ? dataMap.get(selectedBubbleId) : undefined;

  const exchangesText = useMemo(() => {
    if (!selected) return "--";
    return resolveSymbolExchanges(selected, allDataList).join(" / ") || "--";
  }, [allDataList, selected]);

  const matchedPosition = useMemo(() => {
    if (!selected || !positionSnapshot?.positions?.length) return undefined;
    const selectedExchange = normalizeExchange(selected.exchange);
    const selectedTimeframe = (selected.timeframe || "").trim().toLowerCase();
    let best: PositionItem | undefined;
    let bestScore = -1;
    let bestUpdatedTs = -1;

    for (const item of positionSnapshot.positions) {
      if (!symbolsLikelyMatch(selected.symbol, item.symbol)) continue;
      const score = scorePositionMatch(selectedExchange, selectedTimeframe, item);
      const updatedTs = parsePositionTimeMs(item.updatedTime) ?? parsePositionTimeMs(item.entryTime) ?? 0;
      if (score > bestScore || (score === bestScore && updatedTs > bestUpdatedTs)) {
        best = item;
        bestScore = score;
        bestUpdatedTs = updatedTs;
      }
    }

    return best;
  }, [positionSnapshot?.positions, selected]);

  useEffect(() => {
    if (!matchedPosition) return;
    const timer = window.setInterval(() => {
      setPositionNowMs(Date.now());
    }, 1000);
    return () => window.clearInterval(timer);
  }, [matchedPosition]);

  useEffect(() => {
    setEventPanelOpen(false);
    setEventPanelPlacement(undefined);
  }, [selectedBubbleId]);

  const positionSummaryCells = useMemo<PositionSummaryCell[]>(() => {
    if (!matchedPosition) return [];
    const floatingTone = resolveAmountRateTone(
      matchedPosition.unrealizedProfitAmount,
      matchedPosition.unrealizedProfitRate
    );
    return [
      {
        key: "exchange",
        label: "交易所",
        value: matchedPosition.exchange || "--"
      },
      {
        key: "symbol",
        label: "交易对",
        value: matchedPosition.symbol || selected?.symbol || "--"
      },
      {
        key: "side",
        label: "方向",
        value: resolvePositionSideLongShort(matchedPosition)
      },
      {
        key: "leverage",
        label: "杠杆",
        value: formatLeverageMultiplier(matchedPosition.leverageMultiplier)
      },
      {
        key: "floating",
        label: "浮动收益",
        value: formatFloatingAmountWithRate(
          matchedPosition.unrealizedProfitAmount,
          matchedPosition.unrealizedProfitRate
        ),
        tone: floatingTone
      },
      {
        key: "holding",
        label: "持仓时长",
        value: matchedPosition.holdingTime || formatHoldingDuration(matchedPosition.entryTime, positionNowMs)
      }
    ];
  }, [matchedPosition, positionNowMs, selected?.symbol]);

  useEffect(() => {
    if (!selectedBubbleId) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      clearSelectedBubbleId();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [clearSelectedBubbleId, selectedBubbleId]);

  useEffect(() => {
    if (!selectedBubbleId) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as HTMLElement | null;
      if (!target) return;
      if (target.closest(".details-modal")) return;
      if (target.closest(".details-modal__event-sidepanel")) return;
      // 气泡区交由 Canvas 内命中逻辑决定“切换交易对/空白关闭”。
      if (target.closest(".canvas-wrap")) return;

      const topbar = target.closest(".topbar");
      if (topbar) {
        const interactive = target.closest(
          ".timeframe-tab, .topbar__search, .status-indicator, input, label, button, a"
        );
        if (interactive) return;
        clearSelectedBubbleId();
        return;
      }
    };

    window.addEventListener("pointerdown", onPointerDown, true);
    return () => window.removeEventListener("pointerdown", onPointerDown, true);
  }, [clearSelectedBubbleId, selectedBubbleId]);

  useEffect(() => {
    const topbar = document.querySelector<HTMLElement>(".topbar");
    if (!topbar) return;
    const viewport = window.visualViewport;

    const updateTopOffset = () => {
      const viewportHeight = viewport?.height || window.innerHeight;
      const bottom = topbar.getBoundingClientRect().bottom;
      const next = Math.max(12, Math.min(viewportHeight - 120, Math.ceil(bottom + 12)));
      setTopOffset(next);
    };

    updateTopOffset();
    const observer = new ResizeObserver(() => updateTopOffset());
    observer.observe(topbar);
    window.addEventListener("resize", updateTopOffset);
    viewport?.addEventListener("resize", updateTopOffset);
    viewport?.addEventListener("scroll", updateTopOffset);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", updateTopOffset);
      viewport?.removeEventListener("resize", updateTopOffset);
      viewport?.removeEventListener("scroll", updateTopOffset);
    };
  }, []);

  const modalStyle = useMemo(
    () => ({ "--details-modal-top": `${topOffset}px` }) as CSSProperties,
    [topOffset]
  );

  const eventLoadState = selected ? eventStateByBubbleId[selected.id] : undefined;

  const sidePanelStyle = useMemo(() => {
    if (!eventPanelPlacement) {
      return {
        visibility: "hidden",
        pointerEvents: "none"
      } as CSSProperties;
    }
    return {
      top: `${eventPanelPlacement.top}px`,
      left: `${eventPanelPlacement.left}px`,
      width: `${eventPanelPlacement.width}px`,
      maxHeight: `${eventPanelPlacement.maxHeight}px`,
      visibility: "visible",
      pointerEvents: "auto"
    } as CSSProperties;
  }, [eventPanelPlacement]);

  const detailRows = useMemo<DetailRow[]>(() => {
    if (!selected) return [];
    return [
      { label: "交易所", value: exchangesText },
      { label: "周期", value: formatValue(selected.timeframe) },
      { label: "方向", value: formatSignalDirection(selected.highSide, selected.side) },
      { label: "趋势时间", value: formatLocalDateTime(selected.trendingTimestamp) },
      { label: "策略", value: formatValue(selected.strategy) },
      { label: "版本", value: formatValue(selected.strategyVersion) }
    ];
  }, [exchangesText, selected]);

  const linkItems = useMemo(() => {
    if (!selected) return [];
    return buildSignalLinkItems(selected, allDataList);
  }, [allDataList, selected]);

  const updateEventPanelPlacement = () => {
    if (!eventPanelOpen) return;
    const modalEl = modalRef.current;
    const triggerEl = eventToggleRef.current;
    if (!modalEl || !triggerEl) return;

    const viewportWidth = window.innerWidth;
    const viewportHeight = window.visualViewport?.height || window.innerHeight;
    const modalRect = modalEl.getBoundingClientRect();
    const triggerRect = triggerEl.getBoundingClientRect();
    const sideWidth = Math.min(EVENT_PANEL_SIDE_WIDTH, Math.max(280, viewportWidth - 24));
    const canSidePlace = modalRect.right + EVENT_PANEL_GAP + Math.min(360, sideWidth) <= viewportWidth - 12;
    const panelEl = eventSidePanelRef.current;
    const panelHeight = panelEl?.offsetHeight ?? 0;

    let top = canSidePlace
      ? Math.max(12, triggerRect.top - 8)
      : Math.max(12, modalRect.bottom + EVENT_PANEL_STACKED_GAP);
    const left = canSidePlace
      ? modalRect.right + EVENT_PANEL_GAP
      : Math.max(12, Math.min(modalRect.left, viewportWidth - sideWidth - 12));

    if (panelHeight > 0) {
      top = Math.max(12, Math.min(top, viewportHeight - panelHeight - 12));
    }

    setEventPanelPlacement({
      mode: canSidePlace ? "side" : "stacked",
      top,
      left,
      width: sideWidth,
      maxHeight: Math.max(180, viewportHeight - top - 12)
    });
  };

  useEffect(() => {
    if (!eventPanelOpen) return;
    const schedule = () => {
      window.requestAnimationFrame(() => {
        updateEventPanelPlacement();
      });
    };

    schedule();
    const viewport = window.visualViewport;
    const resizeObserver = new ResizeObserver(() => schedule());
    if (modalRef.current) resizeObserver.observe(modalRef.current);
    if (eventToggleRef.current) resizeObserver.observe(eventToggleRef.current);
    if (eventSidePanelRef.current) resizeObserver.observe(eventSidePanelRef.current);

    const modalBody = modalRef.current?.querySelector<HTMLElement>(".details-modal__body");
    window.addEventListener("resize", schedule);
    viewport?.addEventListener("resize", schedule);
    viewport?.addEventListener("scroll", schedule);
    modalBody?.addEventListener("scroll", schedule, { passive: true });
    return () => {
      resizeObserver.disconnect();
      window.removeEventListener("resize", schedule);
      viewport?.removeEventListener("resize", schedule);
      viewport?.removeEventListener("scroll", schedule);
      modalBody?.removeEventListener("scroll", schedule);
    };
  }, [eventPanelOpen, selectedBubbleId, topOffset, matchedPosition]);

  const loadSignalEvents = async (item: BubbleDatum) => {
    const current = eventStateByBubbleId[item.id];
    if (current?.status === "loading" || current?.status === "ready") return;
    setEventStateByBubbleId((prev) => ({
      ...prev,
      [item.id]: { status: "loading" }
    }));

    try {
      const response = await fetch(buildSignalEventsUrl(item));
      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        const message =
          isRecord(payload) && typeof payload.error === "string" && payload.error.trim().length > 0
            ? payload.error.trim()
            : `${response.status} ${response.statusText}`.trim();
        throw new Error(message || "事件加载失败");
      }
      const snapshot = normalizeSignalEventsSnapshot(payload);
      if (!snapshot) {
        throw new Error("事件响应无效");
      }
      setEventStateByBubbleId((prev) => ({
        ...prev,
        [item.id]: {
          status: "ready",
          snapshot
        }
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "事件加载失败";
      setEventStateByBubbleId((prev) => ({
        ...prev,
        [item.id]: {
          status: "error",
          error: message
        }
      }));
    }
  };

  const toggleEventPanel = () => {
    if (!selected) return;
    setEventPanelOpen((prev) => {
      const next = !prev;
      if (next) {
        void loadSignalEvents(selected);
      }
      return next;
    });
  };

  if (!selected) return null;

  return (
    <>
      <section
        ref={modalRef}
        className="details-modal"
        style={modalStyle}
        role="dialog"
        aria-modal="true"
        aria-label={`${selected.symbol} 详情`}
      >
        <header className="details-modal__header">
        <div>
          <div className="details-modal__title">{selected.symbol}</div>
          <div className="details-modal__subtitle">信号详情</div>
        </div>
        <button
          type="button"
          className="details-modal__close"
          onClick={clearSelectedBubbleId}
          aria-label="关闭详情"
        >
          x
        </button>
      </header>
        <div className="details-modal__body">
          <div className="details-modal__content">
          {matchedPosition ? (
            <section className="details-modal__position" aria-label="持仓信息">
              <div className="details-modal__position-title">持仓信息</div>
              <div className="details-modal__position-grid">
                {positionSummaryCells.map((cell) => (
                  <div key={cell.key} className="details-modal__position-item">
                    <span className="details-modal__position-item-label">{cell.label}</span>
                    <span
                      className={`details-modal__position-item-value ${
                        cell.tone ? `position-rate position-rate--${cell.tone}` : ""
                      }`}
                    >
                      {cell.value}
                    </span>
                  </div>
                ))}
              </div>
            </section>
          ) : null}
          {detailRows.map((row) => (
            <div key={row.label} className="details-modal__row">
              <span>{row.label}</span>
              <span>{row.value}</span>
            </div>
          ))}
          </div>
          <div className="details-modal__links">
            <div className="details-modal__links-title">Links</div>
            <div className="details-modal__links-list">
            {linkItems.map((link) => (
              <a
                key={link.key}
                className="details-link"
                href={link.url}
                target="_blank"
                rel="noopener noreferrer"
                title={`${link.label} - 打开 ${selected.symbol} K 线`}
                aria-label={`${link.label} - 打开 ${selected.symbol} K 线`}
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
            <BubbleKlinePanel selected={selected} matchedPosition={matchedPosition} allDataList={allDataList} />
            <div ref={eventToggleRef} className="details-modal__event-trigger-row">
              <button
                type="button"
                className={`details-modal__event-trigger ${eventPanelOpen ? "is-open" : ""}`}
                onClick={toggleEventPanel}
                aria-expanded={eventPanelOpen}
                aria-controls={selected ? `bubble-event-panel-${selected.id}` : undefined}
              >
                <span className="details-modal__event-trigger-dot" aria-hidden="true" />
                <span className="details-modal__event-trigger-text">EVENT PANEL</span>
                {eventLoadState?.snapshot ? (
                  <span className="details-modal__event-trigger-count">
                    {eventPanelCountText(eventLoadState.snapshot)}
                  </span>
                ) : null}
                <span className="details-modal__event-trigger-chevron" aria-hidden="true">
                  {eventPanelOpen ? "▴" : "▸"}
                </span>
              </button>
            </div>
          </div>
        </div>
      </section>
      {eventPanelOpen ? (
        <aside
          id={`bubble-event-panel-${selected.id}`}
          ref={eventSidePanelRef}
          className={`details-modal__event-sidepanel ${
            eventPanelPlacement?.mode === "stacked" ? "details-modal__event-sidepanel--stacked" : ""
          }`}
          style={sidePanelStyle}
          aria-label={`${selected.symbol} 事件列表`}
        >
          <div className="signals-event-drawer details-modal__event-sidepanel-card">
            <div className="signals-event-drawer__header details-modal__event-sidepanel-header">
              <span className="details-modal__event-trigger-dot" aria-hidden="true" />
              <span className="signals-event-drawer__title">Event Panel</span>
              <span className="signals-event-drawer__count">
                {eventPanelCountText(eventLoadState?.snapshot)}
              </span>
              <button
                type="button"
                className="details-modal__event-sidepanel-close"
                onClick={() => setEventPanelOpen(false)}
                aria-label="关闭事件列表"
              >
                ×
              </button>
            </div>
            {renderSignalEventDrawer(eventLoadState)}
          </div>
        </aside>
      ) : null}
    </>
  );
});
