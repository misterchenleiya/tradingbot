import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
  type CSSProperties
} from "react";
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  createChart,
  HistogramSeries,
  LineSeries,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type PriceFormat,
  type Time
} from "lightweight-charts";
import { SHORTCUT_HELP_ROWS } from "../shortcuts";
import type {
  Candle,
  HistoryEvent,
  HistoryPosition,
  IntegrityGap,
  IntegrityResponse,
  IntegrityTimeframe,
  TimeframeCandles
} from "../types";
import {
  buildEMAFromCandles,
  buildSMAFromCandles,
  parseIndicatorsForTimeframe,
  type ParsedIndicator
} from "../utils/indicators";

interface ChartPanelProps {
  position: HistoryPosition | null;
  candlesByTF: Record<string, TimeframeCandles>;
  activeTimeframe: string;
  onTimeframeChange: (timeframe: string) => void;
  chartMaximized: boolean;
  onToggleMaximized: () => void;
  loading: boolean;
  loadProgress: string;
  events: HistoryEvent[];
  integrity: IntegrityResponse | null;
}

export interface ChartPanelHandle {
  resetView: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
  panLeft: () => void;
  panRight: () => void;
  toggleShortcutModal: () => void;
}

type MarkerKind =
  | "ENTRY"
  | "EXIT"
  | "TP"
  | "SL"
  | "ARMED"
  | "TREND_N"
  | "HIGH_H"
  | "MID_M"
  | "R_2R"
  | "R_4R";
type MarkerTone = "bull" | "bear" | "neutral";
type PositionLevelKind = "ENTRY" | "TP" | "SL";

interface IndicatorView {
  id: string;
  kind: ParsedIndicator["kind"];
  period?: number;
  label: string;
  lineColor: string;
  labelColor: string;
}

interface MarkerTooltipRow {
  label: string;
  value: string;
}

interface RawOverlayMarker {
  id: string;
  kind: MarkerKind;
  tone: MarkerTone | null;
  x: number;
  rawY: number;
  candleTS: number;
  eventAtMS: number;
  price: number;
  markerSide: "long" | "short";
  tooltipTitle: string;
  tooltipRows: MarkerTooltipRow[];
}

interface OverlayMarker {
  id: string;
  kind: MarkerKind;
  tone: MarkerTone | null;
  x: number;
  y: number;
  size: number;
  arrowLength: number;
  guideLength: number;
  price: number;
  clampDirection: "top" | "bottom" | null;
  clampGuideTop: number;
  clampGuideHeight: number;
  tooltipTitle: string;
  tooltipRows: MarkerTooltipRow[];
}

interface MarkerTooltipState {
  markerID: string;
  kind: MarkerKind;
  price: number;
  clampDirection: "top" | "bottom" | null;
  title: string;
  rows: MarkerTooltipRow[];
  left: number;
  top: number;
}

interface PositionLevelOverlay {
  id: string;
  kind: PositionLevelKind;
  y: number;
  lineLength: number;
  side: "long" | "short";
  pnlText: string;
  pnlTone: "positive" | "negative";
}

interface LogicalRangeSnapshot {
  from: number;
  to: number;
  updatedAtMS: number;
}

interface ViewportCacheRow extends LogicalRangeSnapshot {
  key: string;
}

interface ViewportCacheEnvelope {
  version: number;
  rows: ViewportCacheRow[];
}

interface ViewportTransferSnapshot {
  sourceKey: string;
  targetKey: string;
  positionKey: string;
  sourceBars: number;
  range: { from: number; to: number };
}

interface GapOverlayCandle {
  id: string;
  x: number;
  width: number;
  wickTop: number;
  wickHeight: number;
  bodyTop: number;
  bodyHeight: number;
}

const MAX_GAP_PLACEHOLDER_BARS = 2400;
const MAX_TPSL_MARKERS_PER_CANDLE = 24;
const MAX_TOTAL_MARKERS = 1800;
const MAX_MARKER_SCAN_EVENTS = 6000;
const VIEWPORT_CACHE_KEY = "visual-history:chart-viewport-cache:v1";
const VIEWPORT_CACHE_VERSION = 1;
const MAX_VIEWPORT_CACHE_ROWS = 240;
const PRICE_AXIS_MIN_WIDTH = 52;
const PRICE_AXIS_MAX_WIDTH = 96;
const SCIENTIFIC_LEADING_ZERO_THRESHOLD = 5;
const PRICE_AXIS_FONT = "11px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
const CHART_TIME_AXIS_HEIGHT = 34;
const CHART_MARKER_BOTTOM_PADDING = 8;

interface IndicatorPalette {
  lineColor: string;
  labelColor: string;
}

const EMA_STYLE_BY_PERIOD: Record<number, IndicatorPalette> = {
  5: { lineColor: "rgba(255,255,255,0.5)", labelColor: "#ffffff" },
  10: { lineColor: "rgba(250,250,0,0.5)", labelColor: "#fafa00" },
  20: { lineColor: "rgba(248,215,0,0.5)", labelColor: "#f8d700" },
  60: { lineColor: "rgba(0,46,253,0.5)", labelColor: "#002efd" },
  120: { lineColor: "rgba(191,0,225,0.5)", labelColor: "#bf00e1" }
};
const EMA_FALLBACK_STYLES: IndicatorPalette[] = [
  { lineColor: "rgba(48,209,88,0.5)", labelColor: "#30d158" },
  { lineColor: "rgba(167,139,250,0.5)", labelColor: "#a78bfa" },
  { lineColor: "rgba(249,115,22,0.5)", labelColor: "#f97316" }
];
const SMA_COLORS = ["#74b9ff", "#ffd166", "#f72585", "#06d6a0", "#f77f00", "#8ecae6"];
const OTHER_INDICATOR_COLOR = "#ffffff";

type EventLegendPreviewKind =
  | "ENTRY"
  | "EXIT"
  | "TP"
  | "SL"
  | "ARMED"
  | "TREND_N"
  | "HIGH_H"
  | "MID_M"
  | "R_2R"
  | "R_4R";

interface EventLegendRow {
  kind: EventLegendPreviewKind;
  label: string;
  description: string;
}

const EVENT_LEGEND_ROWS: EventLegendRow[] = [
  {
    kind: "ENTRY",
    label: "ENTRY",
    description: "绿色右箭头，指向开仓 K 线左边缘。"
  },
  {
    kind: "EXIT",
    label: "EXIT",
    description: "红色左箭头，指向平仓 K 线右边缘。"
  },
  {
    kind: "TP",
    label: "TP",
    description: "绿色实心圆，按真实止盈价格定位。"
  },
  {
    kind: "SL",
    label: "SL",
    description: "红色空心圆，按真实止损价格定位。"
  },
  {
    kind: "ARMED",
    label: "ARMED",
    description: "黄色实心圆，表示小周期进入 armed。"
  },
  {
    kind: "TREND_N",
    label: "TREND",
    description: "绿色空心圆，表示趋势检测完成。"
  },
  {
    kind: "HIGH_H",
    label: "HIGH",
    description: "高周期状态变化。绿色=1，红色=-1，灰色=255/-255。"
  },
  {
    kind: "MID_M",
    label: "MID",
    description: "中周期状态变化。绿色=1，红色=-1，灰色=255/-255。"
  },
  {
    kind: "R_2R",
    label: "2R",
    description: "保本保护事件，定位到生效后的 SL 价格。"
  },
  {
    kind: "R_4R",
    label: "4R",
    description: "部分平仓保护事件，定位到生效后的 SL 价格。"
  }
];

function resolveEMAStyle(period: number | undefined, fallbackIndex: number): IndicatorPalette {
  if (period != null) {
    const direct = EMA_STYLE_BY_PERIOD[period];
    if (direct) {
      return direct;
    }
  }
  return EMA_FALLBACK_STYLES[fallbackIndex % EMA_FALLBACK_STYLES.length] ?? {
    lineColor: "rgba(255,255,255,0.5)",
    labelColor: "#ffffff"
  };
}

type DynamicPriceDisplay = {
  key: string;
  axisWidth: number;
  priceFormat: PriceFormat;
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

function buildDynamicPriceDisplay(candles: Candle[]): DynamicPriceDisplay {
  if (!Array.isArray(candles) || candles.length === 0) {
    return {
      key: "empty",
      axisWidth: PRICE_AXIS_MIN_WIDTH,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 }
    };
  }

  const values: number[] = [];
  let maxAbs = 0;
  let minPositiveAbs = Number.POSITIVE_INFINITY;
  for (const candle of candles) {
    values.push(candle.open, candle.high, candle.low, candle.close);
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

export const ChartPanel = forwardRef<ChartPanelHandle, ChartPanelProps>(function ChartPanel(
  props: ChartPanelProps,
  ref
): JSX.Element {
  const {
    position,
    candlesByTF,
    activeTimeframe,
    onTimeframeChange,
    chartMaximized,
    onToggleMaximized,
    loading,
    loadProgress,
    events,
    integrity
  } = props;

  const chartBodyRef = useRef<HTMLDivElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const indicatorSeriesRef = useRef<Array<ISeriesApi<"Line">>>([]);
  const priceDisplayKeyRef = useRef("");
  const currentViewportKeyRef = useRef("");
  const lastAppliedViewportKeyRef = useRef("");
  const lastFittedPositionKeyRef = useRef("");
  const viewportCacheRef = useRef<Map<string, LogicalRangeSnapshot>>(loadViewportCache());
  const hydratedViewportKeysRef = useRef<Set<string>>(new Set());
  const currentDataLengthRef = useRef(0);
  const pendingViewportTransferRef = useRef<ViewportTransferSnapshot | null>(null);

  const [overlayTick, setOverlayTick] = useState(0);
  const [indicatorVisibility, setIndicatorVisibility] = useState<Record<string, boolean>>({});
  const [showShortcutModal, setShowShortcutModal] = useState(false);
  const [hoveredMarker, setHoveredMarker] = useState<MarkerTooltipState | null>(null);
  const [integrityDismissed, setIntegrityDismissed] = useState(false);
  const [showIntegrityDetails, setShowIntegrityDetails] = useState(false);

  const normalizedCandlesByTF = useMemo(() => {
    const out: Record<string, TimeframeCandles> = {};
    for (const [rawKey, frame] of Object.entries(candlesByTF)) {
      const normalizedKey = rawKey.trim().toLowerCase();
      if (!normalizedKey) {
        continue;
      }
      if (!out[normalizedKey]) {
        out[normalizedKey] = frame;
      }
    }
    return out;
  }, [candlesByTF]);

  const timeframes = useMemo(() => {
    const keys = Object.keys(normalizedCandlesByTF);
    if (keys.length > 0) {
      return sortTimeframes(keys);
    }
    return position?.timeframes || [];
  }, [normalizedCandlesByTF, position?.timeframes]);

  const normalizedActiveTimeframe = activeTimeframe.trim().toLowerCase();
  const resolvedActiveTimeframe = useMemo(() => {
    return normalizedActiveTimeframe && normalizedCandlesByTF[normalizedActiveTimeframe]
      ? normalizedActiveTimeframe
      : timeframes.find((item) => normalizedCandlesByTF[item]) || "";
  }, [normalizedActiveTimeframe, normalizedCandlesByTF, timeframes]);

  const activeData = useMemo(() => {
    if (!resolvedActiveTimeframe) {
      return [];
    }
    return normalizedCandlesByTF[resolvedActiveTimeframe]?.candles || [];
  }, [resolvedActiveTimeframe, normalizedCandlesByTF]);

  const normalizedData = useMemo(() => normalizeCandles(activeData), [activeData]);
  const latestCandle = normalizedData.length > 0 ? normalizedData[normalizedData.length - 1] : null;

  const indicatorViews = useMemo(() => {
    const parsed = parseIndicatorsForTimeframe(position?.indicators || {}, resolvedActiveTimeframe);
    const out: IndicatorView[] = [];
    let emaCount = 0;
    let smaCount = 0;
    for (const item of parsed) {
      if (item.kind === "EMA") {
        const style = resolveEMAStyle(item.period, emaCount);
        emaCount += 1;
        out.push({
          id: item.id,
          kind: item.kind,
          period: item.period,
          label: item.label,
          lineColor: style.lineColor,
          labelColor: style.labelColor
        });
        continue;
      }
      if (item.kind === "SMA") {
        const color = SMA_COLORS[smaCount % SMA_COLORS.length];
        smaCount += 1;
        out.push({
          id: item.id,
          kind: item.kind,
          period: item.period,
          label: item.label,
          lineColor: color,
          labelColor: color
        });
        continue;
      }
      out.push({
        id: item.id,
        kind: item.kind,
        period: item.period,
        label: item.label,
        lineColor: OTHER_INDICATOR_COLOR,
        labelColor: OTHER_INDICATOR_COLOR
      });
    }
    return out;
  }, [position?.indicators, resolvedActiveTimeframe]);

  const visibleIndicators = useMemo(
    () => indicatorViews.filter((item) => indicatorVisibility[item.id] !== false),
    [indicatorViews, indicatorVisibility]
  );

  const activeIntegrity = useMemo(() => {
    if (!integrity) {
      return null;
    }
    const frames = Array.isArray(integrity.timeframes) ? integrity.timeframes : [];
    return frames.find((item) => item.timeframe === resolvedActiveTimeframe) || null;
  }, [integrity, resolvedActiveTimeframe]);

  const integrityHasIssues = Boolean(integrity && !integrity.check.ok);
  const shouldShowIntegrityWarning = Boolean(position && normalizedData.length > 0 && integrityHasIssues);

  useEffect(() => {
    setIndicatorVisibility((prev) => {
      const next: Record<string, boolean> = {};
      let changed = false;
      for (const item of indicatorViews) {
        if (Object.prototype.hasOwnProperty.call(prev, item.id)) {
          next[item.id] = prev[item.id];
        } else {
          next[item.id] = true;
          changed = true;
        }
      }
      for (const key of Object.keys(prev)) {
        if (!Object.prototype.hasOwnProperty.call(next, key)) {
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [indicatorViews]);

  useEffect(() => {
    setIntegrityDismissed(false);
    setShowIntegrityDetails(false);
  }, [
    position?.id,
    resolvedActiveTimeframe,
    integrity?.summary?.incomplete_timeframes,
    integrity?.summary?.missing_bars,
    integrity?.summary?.discontinuities
  ]);

  useEffect(() => {
    const nextKey = buildViewportKey(position?.position_key, resolvedActiveTimeframe);
    const prevKey = currentViewportKeyRef.current;
    if (prevKey && prevKey !== nextKey) {
      const chart = chartRef.current;
      if (chart) {
        const currentRange = chart.timeScale().getVisibleLogicalRange();
        if (isValidLogicalRange(currentRange)) {
          saveViewportRange(viewportCacheRef.current, prevKey, currentRange);
          pendingViewportTransferRef.current = {
            sourceKey: prevKey,
            targetKey: nextKey,
            positionKey: (position?.position_key || "").trim(),
            sourceBars: currentDataLengthRef.current,
            range: { from: Number(currentRange.from), to: Number(currentRange.to) }
          };
        } else {
          pendingViewportTransferRef.current = null;
        }
      }
    }
    currentViewportKeyRef.current = nextKey;
  }, [position?.position_key, resolvedActiveTimeframe]);

  useImperativeHandle(
    ref,
    () => ({
      resetView: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        scheduleFitAllData(chart);
      },
      zoomIn: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        zoomChart(chart, 0.82);
        setOverlayTick((value) => value + 1);
      },
      zoomOut: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        zoomChart(chart, 1.22);
        setOverlayTick((value) => value + 1);
      },
      panLeft: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        panChart(chart, -1);
        setOverlayTick((value) => value + 1);
      },
      panRight: () => {
        const chart = chartRef.current;
        if (!chart) {
          return;
        }
        panChart(chart, 1);
        setOverlayTick((value) => value + 1);
      },
      toggleShortcutModal: () => {
        setShowShortcutModal((prev) => !prev);
      }
    }),
    []
  );

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: "#1e1e1e" },
        textColor: "#98989d",
        fontSize: 11
      },
      localization: {
        timeFormatter: (time: Time) => formatChartAxisTime(time)
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.06)" },
        horzLines: { color: "rgba(255,255,255,0.06)" }
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.12)",
        scaleMargins: { top: 0.1, bottom: 0.25 }
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.12)",
        minBarSpacing: 0.5,
        maxBarSpacing: 60,
        fixLeftEdge: false,
        rightOffset: 0,
        timeVisible: true,
        secondsVisible: false
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "rgba(255,255,255,0.2)", width: 1 },
        horzLine: { color: "rgba(255,255,255,0.2)", width: 1 }
      }
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#00c897",
      downColor: "#ff4757",
      wickUpColor: "#00c897",
      wickDownColor: "#ff4757",
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

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    const repaint = () => {
      const key = currentViewportKeyRef.current;
      if (key && hydratedViewportKeysRef.current.has(key)) {
        const range = chart.timeScale().getVisibleLogicalRange();
        if (isValidLogicalRange(range)) {
          saveViewportRange(viewportCacheRef.current, key, range);
        }
      }
      setOverlayTick((value) => value + 1);
    };
    const resize = () => {
      const width = container.clientWidth;
      const height = container.clientHeight;
      if (width > 0 && height > 0) {
        chart.applyOptions({ width, height });
        repaint();
      }
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
      indicatorSeriesRef.current = [];
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    if (!chart || !candleSeries || !volumeSeries) {
      return;
    }

    if (normalizedData.length === 0) {
      currentDataLengthRef.current = 0;
      candleSeries.setData([]);
      volumeSeries.setData([]);
      for (const line of indicatorSeriesRef.current) {
        chart.removeSeries(line);
      }
      indicatorSeriesRef.current = [];
      setOverlayTick((value) => value + 1);
      return;
    }

    const candleData: CandlestickData<Time>[] = normalizedData.map((item) => ({
      time: Math.floor(item.ts / 1000) as Time,
      open: item.open,
      high: item.high,
      low: item.low,
      close: item.close
    }));
    candleSeries.setData(candleData);

    const priceDisplay = buildDynamicPriceDisplay(normalizedData);
    if (priceDisplayKeyRef.current !== priceDisplay.key) {
      priceDisplayKeyRef.current = priceDisplay.key;
      chart.applyOptions({
        rightPriceScale: {
          minimumWidth: priceDisplay.axisWidth
        }
      });
      candleSeries.applyOptions({ priceFormat: priceDisplay.priceFormat });
    }

    volumeSeries.setData(
      normalizedData.map((item) => ({
        time: Math.floor(item.ts / 1000) as Time,
        value: item.volume,
        color: item.close >= item.open ? "rgba(0,200,151,0.25)" : "rgba(255,71,87,0.25)"
      }))
    );
    currentDataLengthRef.current = normalizedData.length;

    for (const line of indicatorSeriesRef.current) {
      chart.removeSeries(line);
    }
    indicatorSeriesRef.current = [];

    for (const item of visibleIndicators) {
      if (item.period == null || !Number.isFinite(item.period) || item.period <= 0) {
        continue;
      }
      let lineData: LineData<Time>[] = [];
      if (item.kind === "EMA") {
        lineData = buildEMAFromCandles(normalizedData, item.period).map((point) => ({
          time: point.time as Time,
          value: point.value
        }));
      } else if (item.kind === "SMA") {
        lineData = buildSMAFromCandles(normalizedData, item.period).map((point) => ({
          time: point.time as Time,
          value: point.value
        }));
      } else {
        continue;
      }

      const lineSeries = chart.addSeries(LineSeries, {
        color: item.lineColor,
        lineWidth: 1,
        lastValueVisible: false,
        priceLineVisible: false,
        priceFormat: priceDisplay.priceFormat
      });
      lineSeries.setData(lineData);
      indicatorSeriesRef.current.push(lineSeries);
    }

    setOverlayTick((value) => value + 1);
  }, [normalizedData, visibleIndicators]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || normalizedData.length === 0) {
      return;
    }
    const positionKey = (position?.position_key || "").trim();
    const viewportKey = buildViewportKey(positionKey, resolvedActiveTimeframe);
    if (!viewportKey || lastAppliedViewportKeyRef.current === viewportKey) {
      return;
    }
    lastAppliedViewportKeyRef.current = viewportKey;

    const cached = readViewportRange(viewportCacheRef.current, viewportKey);
    if (cached) {
      pendingViewportTransferRef.current = null;
      hydratedViewportKeysRef.current.add(viewportKey);
      chart.timeScale().setVisibleLogicalRange({ from: cached.from, to: cached.to });
      return;
    }
    if (!positionKey) {
      return;
    }
    const transfer = pendingViewportTransferRef.current;
    if (
      transfer &&
      transfer.targetKey === viewportKey &&
      transfer.positionKey === positionKey &&
      transfer.sourceKey !== viewportKey
    ) {
      const adapted = adaptTransferredRange(transfer.range, transfer.sourceBars, normalizedData.length);
      pendingViewportTransferRef.current = null;
      if (adapted) {
        hydratedViewportKeysRef.current.add(viewportKey);
        chart.timeScale().setVisibleLogicalRange(adapted);
        saveViewportRange(viewportCacheRef.current, viewportKey, adapted);
        lastFittedPositionKeyRef.current = positionKey;
        return;
      }
    }
    if (lastFittedPositionKeyRef.current === positionKey) {
      // 同一仓位切换时间周期时保持当前视图，不自动重置。
      hydratedViewportKeysRef.current.add(viewportKey);
      return;
    }
    lastFittedPositionKeyRef.current = positionKey;
    hydratedViewportKeysRef.current.add(viewportKey);
    scheduleFitAllData(chart);
  }, [position?.position_key, resolvedActiveTimeframe, normalizedData.length]);

  useEffect(() => {
    if (!showShortcutModal) {
      return;
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setShowShortcutModal(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [showShortcutModal]);

  useEffect(() => {
    const flushViewport = () => {
      const chart = chartRef.current;
      const key = currentViewportKeyRef.current;
      if (!chart || !key) {
        return;
      }
      const range = chart.timeScale().getVisibleLogicalRange();
      if (!isValidLogicalRange(range)) {
        return;
      }
      saveViewportRange(viewportCacheRef.current, key, range);
    };
    const onVisibilityChange = () => {
      if (document.hidden) {
        flushViewport();
      }
    };
    window.addEventListener("beforeunload", flushViewport);
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      window.removeEventListener("beforeunload", flushViewport);
      document.removeEventListener("visibilitychange", onVisibilityChange);
      flushViewport();
    };
  }, []);

  const overlayMarkers = useMemo(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const chartContainer = containerRef.current;
    if (!chart || !candleSeries || !chartContainer || normalizedData.length === 0) {
      return [];
    }
    return buildOverlayMarkers(
      chart,
      candleSeries,
      normalizedData,
      events,
      position,
      resolvedActiveTimeframe,
      overlayTick,
      chartContainer.clientWidth,
      chartContainer.clientHeight
    );
  }, [normalizedData, events, position, resolvedActiveTimeframe, overlayTick]);

  const gapOverlayCandles = useMemo(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!chart || !candleSeries || !activeIntegrity || normalizedData.length === 0) {
      return [];
    }
    return buildGapOverlayCandles(chart, candleSeries, normalizedData, activeIntegrity, overlayTick);
  }, [activeIntegrity, normalizedData, overlayTick]);

  const openPositionLevels = useMemo(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const chartContainer = containerRef.current;
    if (!chart || !candleSeries || !chartContainer || normalizedData.length === 0) {
      return [];
    }
    return buildOpenPositionLevelOverlays(candleSeries, normalizedData, position, events, chartContainer.clientWidth, chartContainer.clientHeight);
  }, [normalizedData, position, events, overlayTick]);

  useEffect(() => {
    if (!hoveredMarker) {
      return;
    }
    if (!overlayMarkers.some((item) => item.id === hoveredMarker.markerID)) {
      setHoveredMarker(null);
    }
  }, [hoveredMarker, overlayMarkers]);

  return (
    <section className="vh-chart-panel">
      <div className="vh-toolbar">
        <div className="vh-toolbar-left">
          <div className="vh-timeframes">
            {timeframes.map((timeframe) => (
              <button
                key={timeframe}
                className={timeframe === resolvedActiveTimeframe ? "vh-timeframe-btn is-active" : "vh-timeframe-btn"}
                onClick={() => onTimeframeChange(timeframe)}
                type="button"
              >
                {timeframe}
              </button>
            ))}
          </div>
          <div className="vh-indicator-legend">
            {indicatorViews.length === 0 ? <span className="vh-indicator-empty">No Indicators</span> : null}
            {indicatorViews.map((item) => {
              const active = indicatorVisibility[item.id] !== false;
              return (
                <button
                  key={item.id}
                  type="button"
                  className={active ? "vh-indicator-btn is-active" : "vh-indicator-btn"}
                  style={{
                    color: active ? item.labelColor : "#6f7785",
                    borderColor: active ? "rgba(255,255,255,0.24)" : "rgba(255,255,255,0.1)"
                  }}
                  onClick={() => {
                    setIndicatorVisibility((prev) => ({
                      ...prev,
                      [item.id]: !(prev[item.id] ?? true)
                    }));
                  }}
                  title={item.label}
                >
                  {item.label}
                </button>
              );
            })}
          </div>
        </div>
        <div className="vh-toolbar-right">
          <button
            type="button"
            className={chartMaximized ? "vh-toolbar-icon-btn is-active" : "vh-toolbar-icon-btn"}
            onClick={onToggleMaximized}
            title={chartMaximized ? "Restore chart window (Shift + F)" : "Fullscreen chart window (Shift + F)"}
            aria-label={chartMaximized ? "Restore chart window" : "Fullscreen chart window"}
          >
            {chartMaximized ? (
              <svg viewBox="0 0 16 16" aria-hidden="true">
                <path
                  d="M3.5 6V3.5H6M10 3.5H12.5V6M6 12.5H3.5V10M12.5 10V12.5H10"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.35"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            ) : (
              <svg viewBox="0 0 16 16" aria-hidden="true">
                <path
                  d="M6 3.5H3.5V6M10 3.5H12.5V6M3.5 10V12.5H6M12.5 10V12.5H10"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.35"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            )}
          </button>
          <button
            type="button"
            className="vh-toolbar-icon-btn vh-help-btn"
            onClick={() => setShowShortcutModal(true)}
            title="Keyboard shortcuts"
            aria-label="Keyboard shortcuts"
          >
            ?
          </button>
        </div>
      </div>

      <div className="vh-chart-body" ref={chartBodyRef}>
        {loading ? <div className="vh-chart-loading">{loadProgress || "加载中..."}</div> : null}

        {shouldShowIntegrityWarning && !integrityDismissed && integrity ? (
          <div className="vh-integrity-banner">
            <div className="vh-integrity-banner-copy">
              <div className="vh-integrity-banner-title">历史 K 线不完整，但仍可继续分析</div>
              <div className="vh-integrity-banner-text">{buildIntegrityBannerText(integrity, activeIntegrity)}</div>
            </div>
            <div className="vh-integrity-banner-actions">
              <button type="button" className="vh-integrity-action-btn" onClick={() => setIntegrityDismissed(true)}>
                继续使用
              </button>
              <button
                type="button"
                className="vh-integrity-action-btn is-secondary"
                onClick={() => setShowIntegrityDetails((current) => !current)}
              >
                {showIntegrityDetails ? "收起详情" : "查看详情"}
              </button>
            </div>
          </div>
        ) : null}

        {shouldShowIntegrityWarning && integrityDismissed ? (
          <button type="button" className="vh-integrity-pill" onClick={() => setIntegrityDismissed(false)}>
            数据异常
          </button>
        ) : null}

        {shouldShowIntegrityWarning && showIntegrityDetails && integrity ? (
          <div className="vh-integrity-details">
            {(Array.isArray(integrity.timeframes) ? integrity.timeframes : []).map((item) => (
              <div key={item.timeframe} className="vh-integrity-detail-row">
                <div className="vh-integrity-detail-head">
                  <span>{item.timeframe}</span>
                  <span>{item.complete && item.continuous ? "完整" : "异常"}</span>
                </div>
                <div className="vh-integrity-detail-meta">
                  {buildIntegrityDetailText(item)}
                </div>
              </div>
            ))}
          </div>
        ) : null}

        {latestCandle ? (
          <div className="vh-ohlc-overlay" style={{ top: shouldShowIntegrityWarning && !integrityDismissed ? 80 : 10 }}>
            <span>O {latestCandle.open.toFixed(2)}</span>
            <span>H {latestCandle.high.toFixed(2)}</span>
            <span>L {latestCandle.low.toFixed(2)}</span>
            <span>C {latestCandle.close.toFixed(2)}</span>
          </div>
        ) : null}

        <div className="vh-chart-canvas" ref={containerRef} />

        <div className="vh-gap-overlay">
          {gapOverlayCandles.map((item) => (
            <div key={item.id} className="vh-gap-candle" style={{ left: `${item.x}px`, width: `${item.width}px` }}>
              <span className="vh-gap-candle-wick" style={{ top: `${item.wickTop}px`, height: `${item.wickHeight}px` }} />
              <span className="vh-gap-candle-body" style={{ top: `${item.bodyTop}px`, height: `${item.bodyHeight}px` }} />
            </div>
          ))}
        </div>

        <div className="vh-level-overlay">
          {openPositionLevels.map((level) => {
            const levelStyle: CSSProperties & Record<"--vh-level-line-length", string> = {
              top: `${level.y}px`,
              "--vh-level-line-length": `${level.lineLength}px`
            };
            const entryLineClass =
              level.kind === "ENTRY"
                ? level.side === "short"
                  ? "vh-level-item is-entry is-short"
                  : "vh-level-item is-entry"
                : level.kind === "TP"
                  ? "vh-level-item is-tp"
                  : "vh-level-item is-sl";
            return (
              <div key={level.id} className={entryLineClass} style={levelStyle}>
                {level.kind === "ENTRY" ? (
                  <div className={`vh-level-entry-box is-${level.pnlTone}`}>{level.pnlText}</div>
                ) : (
                  <div className="vh-level-boxes">
                    <div className="vh-level-type-box">{level.kind}</div>
                    <div className="vh-level-pnl-box">{level.pnlText}</div>
                  </div>
                )}
                <span className="vh-level-line" />
              </div>
            );
          })}
        </div>

        <div className="vh-event-overlay">
          {overlayMarkers.map((marker) => {
            if (marker.kind === "ENTRY" || marker.kind === "EXIT") {
              const arrowStyle: CSSProperties & Record<"--vh-arrow-line-length", string> = {
                left: `${marker.x}px`,
                top: `${marker.y}px`,
                "--vh-arrow-line-length": `${marker.arrowLength}px`
              };
              return (
                <div
                  key={marker.id}
                  className={marker.kind === "ENTRY" ? "vh-chart-arrow is-entry" : "vh-chart-arrow is-exit"}
                  style={arrowStyle}
                >
                  <span className="vh-chart-arrow-label">{marker.kind}</span>
                  <span className="vh-chart-arrow-line" />
                  <span className="vh-chart-arrow-head" />
                </div>
              );
            }
            return (
              <div key={marker.id}>
                {marker.guideLength > 0 ? (
                  <div
                    className={`vh-chart-r-guide ${marker.kind === "R_4R" ? "is-r-4r" : "is-r-2r"}`}
                    style={{
                      left: `${marker.x}px`,
                      top: `${marker.y}px`,
                      width: `${marker.guideLength}px`
                    }}
                  />
                ) : null}
                {marker.clampGuideHeight > 0 && isPriceBoundMarker(marker.kind) ? (
                  <div
                    className={buildClampGuideClassName(marker.kind)}
                    style={{
                      left: `${marker.x}px`,
                      top: `${marker.clampGuideTop}px`,
                      height: `${marker.clampGuideHeight}px`
                    }}
                  />
                ) : null}
                <div
                  className={buildMarkerBadgeClassName(marker)}
                  style={{
                    left: `${marker.x}px`,
                    top: `${marker.y}px`,
                    width: `${marker.size}px`,
                    height: `${marker.size}px`,
                    fontSize: `${
                      marker.kind === "ARMED"
                        ? Math.max(11, Math.min(16, marker.size * 0.52))
                        : isRMarker(marker.kind)
                          ? Math.max(10, Math.min(13, marker.size * 0.42))
                        : Math.max(9, Math.min(12, marker.size * 0.38))
                    }px`
                  }}
                  onMouseEnter={(event) => {
                    setHoveredMarker(buildMarkerTooltipState(marker, event.clientX, event.clientY, chartBodyRef.current));
                  }}
                  onMouseMove={(event) => {
                    setHoveredMarker(buildMarkerTooltipState(marker, event.clientX, event.clientY, chartBodyRef.current));
                  }}
                  onMouseLeave={() => {
                    setHoveredMarker((current) => (current?.markerID === marker.id ? null : current));
                  }}
                >
                  {markerBadgeText(marker.kind)}
                </div>
              </div>
            );
          })}
        </div>

        {hoveredMarker ? (
          <div
            className="vh-marker-tooltip"
            style={{
              left: `${hoveredMarker.left}px`,
              top: `${hoveredMarker.top}px`
            }}
          >
            <div className="vh-marker-tooltip-title">{hoveredMarker.title}</div>
            <div className="vh-marker-tooltip-value">{formatMarkerPrice(hoveredMarker.price)}</div>
            {hoveredMarker.rows.length > 0 ? (
              <div className="vh-marker-tooltip-rows">
                {hoveredMarker.rows.map((row) => (
                  <div key={`${hoveredMarker.markerID}-${row.label}`} className="vh-marker-tooltip-row">
                    <span className="vh-marker-tooltip-key">{row.label}</span>
                    <span className="vh-marker-tooltip-data">{row.value}</span>
                  </div>
                ))}
              </div>
            ) : null}
            {hoveredMarker.clampDirection ? (
              <div className="vh-marker-tooltip-note">
                {hoveredMarker.clampDirection === "top" ? "超出当前显示范围（顶部）" : "超出当前显示范围（底部）"}
              </div>
            ) : null}
          </div>
        ) : null}

        {activeData.length === 0 && !loading ? <div className="vh-chart-empty">请选择仓位并点击“加载K线”</div> : null}
      </div>

      {showShortcutModal ? (
        <div className="vh-shortcut-modal-backdrop" onClick={() => setShowShortcutModal(false)}>
          <div className="vh-shortcut-modal" onClick={(event) => event.stopPropagation()}>
            <header className="vh-shortcut-modal-header">
              <span>Keyboard Shortcuts & Event Legend</span>
              <button type="button" className="vh-shortcut-close-btn" onClick={() => setShowShortcutModal(false)}>
                x
              </button>
            </header>
            <div className="vh-shortcut-modal-body custom-scrollbar">
              <section className="vh-shortcut-section">
                <div className="vh-shortcut-section-title">Keyboard Shortcuts</div>
                <table className="vh-shortcut-table">
                  <thead>
                    <tr>
                      <th>Action</th>
                      <th>Windows/Linux</th>
                      <th>macOS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {SHORTCUT_HELP_ROWS.map((row) => (
                      <tr key={row.action}>
                        <td>{row.action}</td>
                        <td>{row.winLinux}</td>
                        <td>{row.mac}</td>
                      </tr>
                    ))}
                    <tr>
                      <td>Example (15m / 1h)</td>
                      <td>1 = 15m, 2 = 1h</td>
                      <td>1 = 15m, 2 = 1h</td>
                    </tr>
                  </tbody>
                </table>
              </section>

              <section className="vh-shortcut-section vh-shortcut-section-legend">
                <div className="vh-shortcut-section-title">Event Legend</div>
                <div className="vh-event-legend-grid">
                  {EVENT_LEGEND_ROWS.map((row) => (
                    <div key={row.kind} className="vh-event-legend-row">
                      <div className="vh-event-legend-preview">{renderEventLegendPreview(row.kind)}</div>
                      <div className="vh-event-legend-label">{row.label}</div>
                      <div className="vh-event-legend-desc">{row.description}</div>
                    </div>
                  ))}
                </div>
              </section>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
});

function buildOverlayMarkers(
  chart: IChartApi,
  candleSeries: ISeriesApi<"Candlestick">,
  candles: Candle[],
  events: HistoryEvent[],
  position: HistoryPosition | null,
  activeTimeframe: string,
  overlayTick: number,
  chartWidth: number,
  chartHeight: number
): OverlayMarker[] {
  void overlayTick;

  const timeScale = chart.timeScale();
  const markerByCandleKind = new Map<string, RawOverlayMarker>();
  const eventPool =
    events.length > MAX_MARKER_SCAN_EVENTS ? events.slice(events.length - MAX_MARKER_SCAN_EVENTS) : events;
  const sortedEvents = [...eventPool].sort((a, b) => a.event_at_ms - b.event_at_ms);
  const side = normalizePositionSide(position?.position_side || "");
  const normalizedActiveTimeframe = activeTimeframe.trim().toLowerCase();

  for (const event of sortedEvents) {
    const kinds = classifyEventKinds(event);
    if (kinds.length === 0) {
      continue;
    }
    if (!shouldRenderEventOnTimeframe(event, normalizedActiveTimeframe, kinds)) {
      continue;
    }
    const nearest = findNearestCandle(event.event_at_ms, candles);
    if (!nearest) {
      continue;
    }
    const eventTime = Math.floor(nearest.ts / 1000) as Time;
    const x = timeScale.timeToCoordinate(eventTime);
    if (!Number.isFinite(x)) {
      continue;
    }

    for (const kind of kinds) {
      const markerSide = resolveMarkerSideForKind(kind, event, side);
      const markerTone = resolveMarkerTone(kind, event, markerSide);
      const markerPrice = resolveMarkerPrice(kind, event, nearest, position, markerSide);
      const rawY = candleSeries.priceToCoordinate(markerPrice);
      if (!Number.isFinite(rawY)) {
        continue;
      }
      const markerKey = `${kind}|${nearest.ts}`;
      const candidate: RawOverlayMarker = {
        id: `${event.id}-${kind}`,
        kind,
        tone: markerTone,
        x: Number(x),
        rawY: Number(rawY),
        candleTS: nearest.ts,
        eventAtMS: event.event_at_ms,
        price: markerPrice,
        markerSide,
        tooltipTitle: markerTooltipTitle(kind),
        tooltipRows: buildMarkerTooltipRows(kind, event)
      };
      const prev = markerByCandleKind.get(markerKey);
      if (prev && prev.eventAtMS > candidate.eventAtMS) {
        continue;
      }
      if (!prev && markerByCandleKind.size >= MAX_TOTAL_MARKERS) {
        break;
      }
      markerByCandleKind.set(markerKey, candidate);
    }
    if (markerByCandleKind.size >= MAX_TOTAL_MARKERS) {
      break;
    }
  }

  const rawMarkers = Array.from(markerByCandleKind.values()).sort((a, b) => {
    if (a.eventAtMS !== b.eventAtMS) {
      return a.eventAtMS - b.eventAtMS;
    }
    return a.id.localeCompare(b.id);
  });
  if (rawMarkers.length === 0 || chartHeight <= 0) {
    return [];
  }

  const candleWidth = computeCandleWidth(chart, candles);
  const circleSize = Math.max(10, Math.min(32, candleWidth));
  const candleHalfWidth = Math.max(3, candleWidth * 0.5);
  const markerRadius = circleSize * 0.5;
  const clampTop = markerRadius + 4;
  const clampBottom = Math.max(
    clampTop,
    chartHeight - CHART_TIME_AXIS_HEIGHT - CHART_MARKER_BOTTOM_PADDING - markerRadius
  );
  const out: OverlayMarker[] = [];
  const rGuideLength = clampValue(Math.round(chartWidth * 0.1), 56, 120);

  const groupMap = new Map<string, RawOverlayMarker[]>();
  for (const item of rawMarkers) {
    if (item.kind === "TP" || item.kind === "SL") {
      const key = `${item.kind}-${item.candleTS}`;
      const group = groupMap.get(key) || [];
      group.push(item);
      groupMap.set(key, group);
      continue;
    }
    if (isDirectionalCircleMarker(item.kind)) {
      out.push(layoutDirectionalCircleMarker(item, circleSize, clampTop, clampBottom));
      continue;
    }
    const clampDirection = isPriceBoundMarker(item.kind)
      ? item.rawY < clampTop
        ? "top"
        : item.rawY > clampBottom
          ? "bottom"
          : null
      : null;
    const { guideTop, guideHeight } = buildClampGuide(
      clampValue(item.rawY, clampTop, clampBottom),
      circleSize,
      clampDirection
    );
    out.push({
      id: item.id,
      kind: item.kind,
      tone: item.tone,
      x: item.kind === "ENTRY" ? item.x - candleHalfWidth : item.x + candleHalfWidth,
      y: clampValue(item.rawY, clampTop, clampBottom),
      size: circleSize,
      arrowLength: estimateArrowLineLength(item.kind),
      guideLength: isRMarker(item.kind) ? rGuideLength : 0,
      price: item.price,
      clampDirection,
      clampGuideTop: guideTop,
      clampGuideHeight: guideHeight,
      tooltipTitle: item.tooltipTitle,
      tooltipRows: item.tooltipRows
    });
  }

  for (const group of groupMap.values()) {
    out.push(...layoutTpSlMarkers(group, side, circleSize, clampTop, clampBottom));
  }

  return out;
}

function shouldRenderEventOnTimeframe(event: HistoryEvent, activeTimeframe: string, kinds: MarkerKind[]): boolean {
  if (
    kinds.includes("TP") ||
    kinds.includes("SL") ||
    kinds.includes("R_2R") ||
    kinds.includes("R_4R") ||
    kinds.includes("TREND_N") ||
    kinds.includes("HIGH_H") ||
    kinds.includes("MID_M")
  ) {
    // TP/SL 与生命周期类事件不做当前周期强约束，按仓位全量事件渲染。
    return true;
  }
  if (!activeTimeframe) {
    return true;
  }
  const timeframe = readFirstString(event.detail, ["timeframe"]).trim().toLowerCase();
  if (!timeframe) {
    return true;
  }
  return timeframe === activeTimeframe;
}

function buildOpenPositionLevelOverlays(
  candleSeries: ISeriesApi<"Candlestick">,
  candles: Candle[],
  position: HistoryPosition | null,
  events: HistoryEvent[],
  chartWidth: number,
  chartHeight: number
): PositionLevelOverlay[] {
  void candleSeries;
  void candles;
  void position;
  void events;
  void chartWidth;
  void chartHeight;
  return [];
}

function resolveLatestMarkerPriceFromEvents(events: HistoryEvent[], kind: "TP" | "SL"): number | null {
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const event = events[i];
    const kinds = classifyEventKinds(event);
    if (!kinds.includes(kind)) {
      continue;
    }
    const value = readMarkerPrice(event.detail, kind);
    if (value != null && value > 0) {
      return value;
    }
  }
  return null;
}

function resolvePositionQuantity(position: HistoryPosition, entryPrice: number): number {
  if (Number.isFinite(position.quantity) && position.quantity > 0) {
    return position.quantity;
  }
  if (Number.isFinite(position.notional_usd) && position.notional_usd > 0 && entryPrice > 0) {
    return position.notional_usd / entryPrice;
  }
  return 0;
}

function calculateOpenPnL(
  targetPrice: number,
  entryPrice: number,
  quantity: number,
  side: "long" | "short"
): { amount: number; ratio: number } {
  const delta = side === "long" ? targetPrice - entryPrice : entryPrice - targetPrice;
  const amount = Number.isFinite(quantity) && quantity > 0 ? delta * quantity : 0;
  const ratio = entryPrice > 0 ? (delta / entryPrice) * 100 : 0;
  return { amount, ratio };
}

function formatAmountRatio(amount: number, ratio: number): string {
  return `${formatSignedNumber(amount, 3)} (${formatSignedNumber(ratio, 2)}%)`;
}

function formatSignedNumber(value: number, fallbackDigits: number): string {
  const abs = Math.abs(value);
  let digits = fallbackDigits;
  if (abs >= 1000) {
    digits = 2;
  } else if (abs >= 100) {
    digits = 2;
  } else if (abs >= 1) {
    digits = Math.min(fallbackDigits, 3);
  } else if (abs > 0) {
    digits = Math.max(fallbackDigits, 4);
  }
  const normalized = Number.isFinite(value) ? value : 0;
  const text = Math.abs(normalized).toFixed(digits).replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "");
  const sign = normalized > 0 ? "+" : normalized < 0 ? "-" : "";
  return `${sign}${text}`;
}

function resolvePnLTone(amount: number, ratio: number): "positive" | "negative" {
  const basis = amount !== 0 ? amount : ratio;
  if (basis < 0) {
    return "negative";
  }
  return "positive";
}

function firstPositiveNumber(...values: Array<number | null | undefined>): number | null {
  for (const value of values) {
    if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
      continue;
    }
    return value;
  }
  return null;
}

function buildGapOverlayCandles(
  chart: IChartApi,
  candleSeries: ISeriesApi<"Candlestick">,
  candles: Candle[],
  integrity: IntegrityTimeframe,
  overlayTick: number
): GapOverlayCandle[] {
  void overlayTick;
  const gaps = normalizeIntegrityGaps(integrity.gaps);
  if (gaps.length === 0 || candles.length === 0) {
    return [];
  }
  const durMS = timeframeToMilliseconds(integrity.timeframe);
  if (durMS <= 0) {
    return [];
  }
  const timeScale = chart.timeScale();
  const candleWidth = Math.max(6, computeCandleWidth(chart, candles) * 0.78);
  const spacing = estimateGapSpacingPx(candles, timeScale, durMS, candleWidth);
  const placeholders: GapOverlayCandle[] = [];

  for (const gap of gaps) {
    if (placeholders.length >= MAX_GAP_PLACEHOLDER_BARS) {
      break;
    }
    const gapBars = Math.max(1, calculateGapBars(gap, durMS));
    const stepBars = Math.max(1, Math.ceil(gapBars / MAX_GAP_PLACEHOLDER_BARS));
    const stepMS = durMS * stepBars;
    let ts = gap.start_ts;
    let seen = 0;
    while (ts <= gap.end_ts && placeholders.length < MAX_GAP_PLACEHOLDER_BARS) {
      const synthetic = interpolateGapCandle(candles, ts);
      if (!synthetic) {
        ts += stepMS;
        continue;
      }
      const x = resolveGapCoordinate(ts, candles, timeScale, durMS, spacing);
      const openY = candleSeries.priceToCoordinate(synthetic.open);
      const highY = candleSeries.priceToCoordinate(synthetic.high);
      const lowY = candleSeries.priceToCoordinate(synthetic.low);
      const closeY = candleSeries.priceToCoordinate(synthetic.close);
      if (!Number.isFinite(x) || !Number.isFinite(openY) || !Number.isFinite(highY) || !Number.isFinite(lowY) || !Number.isFinite(closeY)) {
        ts += stepMS;
        continue;
      }
      const top = Math.min(Number(highY), Number(lowY));
      const bottom = Math.max(Number(highY), Number(lowY));
      const bodyTop = Math.min(Number(openY), Number(closeY));
      const bodyBottom = Math.max(Number(openY), Number(closeY));
      placeholders.push({
        id: `${integrity.timeframe}-${ts}`,
        x: Number(x),
        width: candleWidth,
        wickTop: top,
        wickHeight: Math.max(1, bottom - top),
        bodyTop,
        bodyHeight: Math.max(2, bodyBottom - bodyTop)
      });
      seen += 1;
      if (seen >= MAX_GAP_PLACEHOLDER_BARS) {
        break;
      }
      ts += stepMS;
    }
  }

  return placeholders;
}

function interpolateGapCandle(candles: Candle[], targetTS: number): Candle | null {
  const { prev, next } = findGapNeighbors(candles, targetTS);
  if (!prev && !next) {
    return null;
  }
  if (!prev) {
    return { ...next!, ts: targetTS };
  }
  if (!next) {
    return { ...prev, ts: targetTS };
  }
  const span = next.ts - prev.ts;
  if (span <= 0) {
    return { ...prev, ts: targetTS };
  }
  const weight = (targetTS - prev.ts) / span;
  return {
    ts: targetTS,
    open: lerp(prev.open, next.open, weight),
    high: lerp(prev.high, next.high, weight),
    low: lerp(prev.low, next.low, weight),
    close: lerp(prev.close, next.close, weight),
    volume: lerp(prev.volume, next.volume, weight)
  };
}

function findGapNeighbors(candles: Candle[], targetTS: number): { prev: Candle | null; next: Candle | null } {
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const value = candles[mid].ts;
    if (value === targetTS) {
      return { prev: candles[mid], next: candles[mid] };
    }
    if (value < targetTS) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return {
    prev: right >= 0 ? candles[right] : null,
    next: left < candles.length ? candles[left] : null
  };
}

function lerp(start: number, end: number, weight: number): number {
  return start + (end - start) * weight;
}

function layoutTpSlMarkers(
  group: RawOverlayMarker[],
  side: "long" | "short",
  markerSize: number,
  clampTop: number,
  clampBottom: number
): OverlayMarker[] {
  if (group.length === 0) {
    return [];
  }
  const minGap = markerSize + 3;
  const anchor = tpSlAnchor(group[0].kind, side);
  const sorted = [...group].sort((a, b) => {
    if (a.eventAtMS !== b.eventAtMS) {
      return b.eventAtMS - a.eventAtMS;
    }
    return b.id.localeCompare(a.id);
  }).slice(0, MAX_TPSL_MARKERS_PER_CANDLE);
  const placed: OverlayMarker[] = [];
  let previousY: number | null = null;

  for (const item of sorted) {
    let y = clampValue(item.rawY, clampTop, clampBottom);
    if (previousY != null) {
      if (anchor === "above") {
        y = Math.max(y, previousY + minGap);
      } else {
        y = Math.min(y, previousY - minGap);
      }
    }
    y = clampValue(y, clampTop, clampBottom);
    previousY = y;

    const clampDirection = item.rawY < clampTop ? "top" : item.rawY > clampBottom ? "bottom" : null;
    const { guideTop, guideHeight } = buildClampGuide(y, markerSize, clampDirection);

    placed.push({
      id: item.id,
      kind: item.kind,
      tone: item.tone,
      x: item.x,
      y,
      size: markerSize,
      arrowLength: estimateArrowLineLength(item.kind),
      guideLength: 0,
      price: item.price,
      clampDirection,
      clampGuideTop: guideTop,
      clampGuideHeight: guideHeight,
      tooltipTitle: item.tooltipTitle,
      tooltipRows: item.tooltipRows
    });
  }

  return placed;
}

function layoutDirectionalCircleMarker(
  marker: RawOverlayMarker,
  markerSize: number,
  clampTop: number,
  clampBottom: number
): OverlayMarker {
  const verticalOffset = Math.max(6, markerSize * 0.7);
  const targetY = marker.markerSide === "long" ? marker.rawY - verticalOffset : marker.rawY + verticalOffset;
  const y = clampValue(targetY, clampTop, clampBottom);
  const clampDirection = targetY < clampTop ? "top" : targetY > clampBottom ? "bottom" : null;
  return {
    id: marker.id,
    kind: marker.kind,
    tone: marker.tone,
    x: marker.x,
    y,
    size: markerSize,
    arrowLength: estimateArrowLineLength(marker.kind),
    guideLength: 0,
    price: marker.price,
    clampDirection,
    clampGuideTop: 0,
    clampGuideHeight: 0,
    tooltipTitle: marker.tooltipTitle,
    tooltipRows: marker.tooltipRows
  };
}

function calculateGapBars(gap: IntegrityGap, durMS: number): number {
  if (gap.bars > 0) {
    return gap.bars;
  }
  if (durMS <= 0 || gap.end_ts < gap.start_ts) {
    return 0;
  }
  return Math.floor((gap.end_ts-gap.start_ts)/durMS) + 1;
}

function estimateGapSpacingPx(
  candles: Candle[],
  timeScale: ReturnType<IChartApi["timeScale"]>,
  durMS: number,
  fallback: number
): number {
  let total = 0;
  let count = 0;
  for (let i = 1; i < candles.length; i += 1) {
    const prev = candles[i - 1];
    const next = candles[i];
    const left = timeScale.timeToCoordinate(Math.floor(prev.ts / 1000) as Time);
    const right = timeScale.timeToCoordinate(Math.floor(next.ts / 1000) as Time);
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      continue;
    }
    const bars = Math.max(1, Math.round((next.ts - prev.ts) / durMS));
    const px = Math.abs(Number(right) - Number(left)) / bars;
    if (!Number.isFinite(px) || px <= 0) {
      continue;
    }
    total += px;
    count += 1;
  }
  if (count <= 0) {
    return Math.max(1, fallback * 0.9);
  }
  return Math.max(1, total / count);
}

function resolveGapCoordinate(
  ts: number,
  candles: Candle[],
  timeScale: ReturnType<IChartApi["timeScale"]>,
  durMS: number,
  spacing: number
): number {
  const direct = timeScale.timeToCoordinate(Math.floor(ts / 1000) as Time);
  if (Number.isFinite(direct)) {
    return Number(direct);
  }
  const { prev, next } = findGapNeighbors(candles, ts);
  if (prev && next) {
    const left = timeScale.timeToCoordinate(Math.floor(prev.ts / 1000) as Time);
    const right = timeScale.timeToCoordinate(Math.floor(next.ts / 1000) as Time);
    if (Number.isFinite(left) && Number.isFinite(right) && next.ts > prev.ts) {
      const weight = (ts - prev.ts) / (next.ts - prev.ts);
      return Number(left) + (Number(right)-Number(left))*weight;
    }
  }
  if (prev) {
    const left = timeScale.timeToCoordinate(Math.floor(prev.ts / 1000) as Time);
    if (Number.isFinite(left)) {
      const bars = Math.max(1, Math.round((ts - prev.ts) / durMS));
      return Number(left) + spacing*bars;
    }
  }
  if (next) {
    const right = timeScale.timeToCoordinate(Math.floor(next.ts / 1000) as Time);
    if (Number.isFinite(right)) {
      const bars = Math.max(1, Math.round((next.ts - ts) / durMS));
      return Number(right) - spacing*bars;
    }
  }
  return Number.NaN;
}

function buildClampGuide(
  markerY: number,
  markerSize: number,
  clampDirection: "top" | "bottom" | null
): { guideTop: number; guideHeight: number } {
  if (!clampDirection) {
    return { guideTop: 0, guideHeight: 0 };
  }
  const guideLength = 26;
  const radius = markerSize * 0.5;
  if (clampDirection === "top") {
    return {
      guideTop: markerY + radius,
      guideHeight: guideLength
    };
  }
  return {
    guideTop: markerY - radius - guideLength,
    guideHeight: guideLength
  };
}

function clampValue(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

function estimateArrowLineLength(kind: MarkerKind): number {
  // Keep shaft visually aligned with the label width.
  const text = kind === "ENTRY" ? "ENTRY" : kind === "EXIT" ? "EXIT" : markerBadgeText(kind);
  return Math.max(26, Math.round(text.length * 7 + 6));
}

function markerTooltipTitle(kind: MarkerKind): string {
  switch (kind) {
    case "TP":
      return "移动止盈";
    case "SL":
      return "移动止损";
    case "ARMED":
      return "Armed";
    case "TREND_N":
      return "趋势检测";
    case "HIGH_H":
      return "高周期方向变化";
    case "MID_M":
      return "中周期状态变化";
    case "R_2R":
      return "2R 保本保护";
    case "R_4R":
      return "4R 部分平仓保护";
    case "ENTRY":
      return "仓位开仓";
    case "EXIT":
      return "仓位平仓";
    default:
      return markerBadgeText(kind);
  }
}

function markerBadgeText(kind: MarkerKind): string {
  switch (kind) {
    case "ARMED":
      return "A";
    case "TREND_N":
      return "N";
    case "HIGH_H":
      return "H";
    case "MID_M":
      return "M";
    case "R_2R":
      return "2R";
    case "R_4R":
      return "4R";
    default:
      return kind;
  }
}

function buildMarkerTooltipRows(kind: MarkerKind, event: HistoryEvent): MarkerTooltipRow[] {
  const rows: MarkerTooltipRow[] = [];
  switch (kind) {
    case "TREND_N":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipTimestampRow(rows, "trending_timestamp", readFirstNumber(event.detail, ["trending_timestamp"]));
      pushTooltipIntegerRow(rows, "high_side", readFirstNumber(event.detail, ["high_side", "highside"]));
      pushTooltipIntegerRow(rows, "mid_side", readFirstNumber(event.detail, ["mid_side", "midside"]));
      return rows;
    case "HIGH_H":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipIntegerRow(rows, "high_side", readFirstNumber(event.detail, ["high_side", "highside"]));
      return rows;
    case "MID_M":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipIntegerRow(rows, "mid_side", readFirstNumber(event.detail, ["mid_side", "midside"]));
      return rows;
    case "ARMED":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipStringRow(rows, "strategy", readFirstString(event.detail, ["strategy"]));
      pushTooltipStringRow(rows, "strategy_version", readFirstString(event.detail, ["strategy_version"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipTimestampRow(rows, "entry_watch_timestamp", readFirstNumber(event.detail, ["entry_watch_timestamp"]));
      return rows;
    case "TP":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipStringRow(rows, "order_type", readFirstString(event.detail, ["order_type"]));
      pushTooltipIntegerRow(rows, "has_position", readFirstNumber(event.detail, ["has_position"]));
      pushTooltipPriceRow(rows, "tp_price", readFirstNumber(event.detail, ["tp_price", "take_profit_price", "tp"]));
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]));
      return rows;
    case "SL":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipStringRow(rows, "order_type", readFirstString(event.detail, ["order_type"]));
      pushTooltipIntegerRow(rows, "has_position", readFirstNumber(event.detail, ["has_position"]));
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]));
      pushTooltipPriceRow(rows, "tp_price", readFirstNumber(event.detail, ["tp_price", "take_profit_price", "tp"]));
      return rows;
    case "R_2R":
    case "R_4R":
      pushTooltipStringRow(rows, "timeframe", readFirstString(event.detail, ["timeframe"]));
      pushTooltipIntegerRow(rows, "action", readFirstNumber(event.detail, ["action", "signal_action"]));
      pushTooltipPriceRow(rows, "sl_price", readFirstNumber(event.detail, ["sl_price", "stop_loss_price", "sl"]));
      pushTooltipPriceRow(rows, "entry_price", readFirstNumber(event.detail, ["entry_price", "entry"]));
      pushTooltipPriceRow(rows, "initial_sl", readFirstNumber(event.detail, ["initial_sl"]));
      pushTooltipRatioRow(rows, "initial_risk_pct", readFirstNumber(event.detail, ["initial_risk_pct", "initialriskpct"]));
      pushTooltipRatioRow(rows, "max_favorable_profit_pct", readFirstNumber(event.detail, ["max_favorable_profit_pct", "maxfavorableprofitpct"]));
      pushTooltipRRow(rows, "mfer", readFirstNumber(event.detail, ["mfer"]));
      pushTooltipIntegerRow(rows, "profit_protect_stage", readFirstNumber(event.detail, ["profit_protect_stage", "profitprotectstage"]));
      return rows;
    default:
      return rows;
  }
}

function buildMarkerBadgeClassName(marker: OverlayMarker): string {
  switch (marker.kind) {
    case "TP":
      return "vh-chart-badge is-tp";
    case "SL":
      return "vh-chart-badge is-sl";
    case "ARMED":
      return "vh-chart-badge is-armed";
    case "R_2R":
      return "vh-chart-badge is-r-2r";
    case "R_4R":
      return "vh-chart-badge is-r-4r";
    case "TREND_N":
      return "vh-chart-badge is-trend-n";
    case "HIGH_H":
      return marker.tone === "neutral"
        ? "vh-chart-badge is-high-h is-neutral"
        : marker.tone === "bear"
          ? "vh-chart-badge is-high-h is-bear"
          : "vh-chart-badge is-high-h is-bull";
    case "MID_M":
      return marker.tone === "neutral"
        ? "vh-chart-badge is-mid-m is-neutral"
        : marker.tone === "bear"
          ? "vh-chart-badge is-mid-m is-bear"
          : "vh-chart-badge is-mid-m is-bull";
    default:
      return "vh-chart-badge";
  }
}

function buildClampGuideClassName(kind: MarkerKind): string {
  switch (kind) {
    case "TP":
      return "vh-chart-clamp-guide is-tp";
    case "R_2R":
      return "vh-chart-clamp-guide is-r-2r";
    case "R_4R":
      return "vh-chart-clamp-guide is-r-4r";
    default:
      return "vh-chart-clamp-guide is-sl";
  }
}

function renderEventLegendPreview(kind: EventLegendPreviewKind): JSX.Element {
  switch (kind) {
    case "ENTRY":
      return (
        <div className="vh-event-legend-arrow is-entry">
          <span className="vh-event-legend-arrow-line" />
          <span className="vh-event-legend-arrow-head" />
        </div>
      );
    case "EXIT":
      return (
        <div className="vh-event-legend-arrow is-exit">
          <span className="vh-event-legend-arrow-line" />
          <span className="vh-event-legend-arrow-head" />
        </div>
      );
    case "TP":
      return <span className="vh-event-legend-badge is-tp">TP</span>;
    case "SL":
      return <span className="vh-event-legend-badge is-sl">SL</span>;
    case "ARMED":
      return <span className="vh-event-legend-badge is-armed">A</span>;
    case "TREND_N":
      return <span className="vh-event-legend-badge is-trend-n">N</span>;
    case "HIGH_H":
      return (
        <div className="vh-event-legend-variants">
          <span className="vh-event-legend-badge is-high-h is-bull">H</span>
          <span className="vh-event-legend-badge is-high-h is-bear">H</span>
          <span className="vh-event-legend-badge is-high-h is-neutral">H</span>
        </div>
      );
    case "MID_M":
      return (
        <div className="vh-event-legend-variants">
          <span className="vh-event-legend-badge is-mid-m is-bull">M</span>
          <span className="vh-event-legend-badge is-mid-m is-bear">M</span>
          <span className="vh-event-legend-badge is-mid-m is-neutral">M</span>
        </div>
      );
    case "R_2R":
      return <span className="vh-event-legend-badge is-r-2r">2R</span>;
    case "R_4R":
      return <span className="vh-event-legend-badge is-r-4r">4R</span>;
    default:
      return <span className="vh-event-legend-badge">{kind}</span>;
  }
}

function isDirectionalCircleMarker(kind: MarkerKind): boolean {
  return kind === "ARMED" || kind === "TREND_N" || kind === "HIGH_H" || kind === "MID_M";
}

function isRMarker(kind: MarkerKind): boolean {
  return kind === "R_2R" || kind === "R_4R";
}

function isPriceBoundMarker(kind: MarkerKind): boolean {
  return kind === "TP" || kind === "SL" || isRMarker(kind);
}

function buildIntegrityBannerText(integrity: IntegrityResponse, activeIntegrity: IntegrityTimeframe | null): string {
  if (activeIntegrity && (!activeIntegrity.complete || !activeIntegrity.continuous)) {
    return `${activeIntegrity.timeframe} ${buildIntegrityDetailText(activeIntegrity)}。系统未自动修复，图表已用灰色斜线 K 线标出缺口。`;
  }
  const parts: string[] = [];
  if (integrity.summary.missing_bars > 0) {
    parts.push(`缺失 ${integrity.summary.missing_bars} 根`);
  }
  if (integrity.summary.discontinuities > 0) {
    parts.push(`连续性断点 ${integrity.summary.discontinuities} 处`);
  }
  if (parts.length === 0) {
    return "系统未自动修复，图表已用灰色斜线 K 线标出异常区间。";
  }
  return `${parts.join("，")}。系统未自动修复，图表已用灰色斜线 K 线标出异常区间。`;
}

function buildIntegrityDetailText(item: IntegrityTimeframe): string {
  const parts: string[] = [];
  const gaps = normalizeIntegrityGaps(item.gaps);
  if (!item.complete) {
    parts.push(`覆盖不完整，实际 ${item.actual_bars}/${item.expected_bars} 根`);
  }
  if (!item.continuous) {
    const discontinuities = gaps.filter((gap) => gap.kind === "internal_gap").length;
    if (discontinuities > 0) {
      parts.push(`内部断点 ${discontinuities} 处`);
    }
  }
  if (parts.length === 0) {
    return "覆盖完整，连续性正常";
  }
  return parts.join("，");
}

function normalizeIntegrityGaps(gaps: IntegrityGap[] | undefined): IntegrityGap[] {
  if (!Array.isArray(gaps) || gaps.length === 0) {
    return [];
  }
  return gaps;
}

function timeframeToMilliseconds(value: string): number {
  const normalized = value.trim().toLowerCase();
  if (!normalized || normalized.length < 2) {
    return 0;
  }
  const step = Number(normalized.slice(0, -1));
  if (!Number.isFinite(step) || step <= 0) {
    return 0;
  }
  switch (normalized.slice(-1)) {
    case "m":
      return step * 60_000;
    case "h":
      return step * 3_600_000;
    case "d":
      return step * 86_400_000;
    case "w":
      return step * 604_800_000;
    default:
      return 0;
  }
}

function formatChartAxisTime(time: Time): string {
  const date = chartTimeToDate(time);
  if (!date) {
    return "";
  }
  const yyyy = String(date.getFullYear());
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}

function chartTimeToDate(time: Time): Date | null {
  if (typeof time === "number") {
    return new Date(time * 1000);
  }
  if (typeof time === "string") {
    const parsed = new Date(time);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed;
  }
  if (time && typeof time === "object" && "year" in time && "month" in time && "day" in time) {
    const year = Number(time.year);
    const month = Number(time.month);
    const day = Number(time.day);
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
      return null;
    }
    return new Date(year, month - 1, day, 0, 0, 0, 0);
  }
  return null;
}

function classifyEventKinds(event: HistoryEvent): MarkerKind[] {
  const typeToken = normalizeEventToken(event.type);
  if (typeToken === "ENTRY") {
    return ["ENTRY"];
  }
  if (typeToken === "EXIT") {
    return ["EXIT"];
  }
  const kinds = new Set<MarkerKind>();
  addKindsByTypeToken(kinds, typeToken);
  addKindsByMarkerToken(kinds, normalizeEventToken(readFirstString(event.detail, ["marker"])));
  addKindsByTypeToken(kinds, normalizeEventToken(readFirstString(event.detail, ["event_type", "order_type", "action"])));
  addKindsByProfitProtectState(kinds, event);
  const changedFields = readChangedFieldSet(event);
  if (changedFields.has("trending_timestamp")) {
    kinds.add("TREND_N");
  }
  if (changedFields.has("high_side")) {
    kinds.add("HIGH_H");
  }
  if (changedFields.has("mid_side")) {
    kinds.add("MID_M");
  }
  if (readSignalAction(event.detail) === 4) {
    kinds.add("ARMED");
  }

  // 没有明确事件类型时，不做宽松文本匹配，避免把无关事件误判成 TP/SL 导致渲染爆炸。
  if (kinds.size === 0) {
    return [];
  }
  return Array.from(kinds);
}

function readChangedFieldSet(event: HistoryEvent): Set<string> {
  const raw = `${readFirstString(event.detail, ["changed_fields"])} ${event.summary ?? ""}`;
  const out = new Set<string>();
  const normalized = raw.trim().toLowerCase();
  if (!normalized) {
    return out;
  }
  const parts = normalized.split(/[\s,;|]+/u);
  for (const part of parts) {
    const token = part.trim();
    if (!token) continue;
    out.add(token);
  }
  return out;
}

function normalizePositionSide(side: string): "long" | "short" {
  const normalized = side.trim().toLowerCase();
  if (normalized === "short") {
    return "short";
  }
  return "long";
}

function resolveMarkerSideFromValue(value: number | null): "long" | "short" | null {
  if (value == null) {
    return null;
  }
  if (value < 0) {
    return "short";
  }
  if (value > 0) {
    return "long";
  }
  return null;
}

function resolveMarkerSideForKind(kind: MarkerKind, event: HistoryEvent, fallback: "long" | "short"): "long" | "short" {
  if (kind === "MID_M") {
    const midSide = resolveMarkerSideFromValue(readFirstNumber(event.detail, ["mid_side", "midside"]));
    if (midSide) {
      return midSide;
    }
  }
  const highSide = readFirstNumber(event.detail, ["high_side", "highside"]);
  if (highSide != null) {
    if (highSide < 0) return "short";
    if (highSide > 0) return "long";
  }
  const sideValue = readFirstString(event.detail, ["position_side", "side"]);
  if (sideValue) {
    const normalized = sideValue.trim().toLowerCase();
    if (normalized === "short" || normalized === "-1") {
      return "short";
    }
    if (normalized === "long" || normalized === "1") {
      return "long";
    }
  }
  return fallback;
}

function resolveMarkerTone(kind: MarkerKind, event: HistoryEvent, fallbackSide: "long" | "short"): MarkerTone | null {
  if (kind === "HIGH_H") {
    const highSide = readFirstNumber(event.detail, ["high_side", "highside"]);
    if (highSide != null) {
      if (highSide === 255 || highSide === -255) return "neutral";
      if (highSide > 0) return "bull";
      if (highSide < 0) return "bear";
    }
    return fallbackSide === "long" ? "bull" : "bear";
  }
  if (kind === "MID_M") {
    const midSide = readFirstNumber(event.detail, ["mid_side", "midside"]);
    if (midSide != null) {
      if (midSide === 255 || midSide === -255) return "neutral";
      if (midSide > 0) return "bull";
      if (midSide < 0) return "bear";
    }
    return fallbackSide === "long" ? "bull" : "bear";
  }
  if (kind === "TREND_N") {
    return "bull";
  }
  return null;
}

function resolveMarkerPrice(
  kind: MarkerKind,
  event: HistoryEvent,
  candle: Candle,
  position: HistoryPosition | null,
  side: "long" | "short"
): number {
  if (kind === "ARMED" || kind === "TREND_N" || kind === "HIGH_H" || kind === "MID_M") {
    return side === "long" ? candle.high : candle.low;
  }
  if (kind === "TP") {
    return readMarkerPrice(event.detail, "TP") ?? (side === "long" ? candle.high : candle.low);
  }
  if (kind === "SL") {
    return readMarkerPrice(event.detail, "SL") ?? (side === "long" ? candle.low : candle.high);
  }
  if (kind === "R_2R" || kind === "R_4R") {
    return resolveProfitProtectPrice(kind, event, candle, position, side);
  }
  if (kind === "ENTRY") {
    return (
      readFirstNumber(event.detail, ["entry_price", "price", "fill_price", "avg_px"]) ??
      (Number.isFinite(position?.entry_price) ? Number(position?.entry_price) : null) ??
      candle.close
    );
  }
  return (
    readFirstNumber(event.detail, ["exit_price", "price", "fill_price", "close_avg_px"]) ??
    (Number.isFinite(position?.exit_price) ? Number(position?.exit_price) : null) ??
    candle.close
  );
}

function readMarkerPrice(detail: Record<string, unknown> | undefined, kind: "TP" | "SL"): number | null {
  const directKeys = kind === "TP" ? ["tp_price", "take_profit_price", "tp"] : ["sl_price", "stop_loss_price", "sl"];
  return readFirstNumber(detail, directKeys);
}

function resolveProfitProtectPrice(
  kind: "R_2R" | "R_4R",
  event: HistoryEvent,
  candle: Candle,
  position: HistoryPosition | null,
  side: "long" | "short"
): number {
  const directSL = readMarkerPrice(event.detail, "SL");
  if (directSL != null) {
    return directSL;
  }
  const entryPrice =
    readFirstNumber(event.detail, ["entry_price", "entry", "price", "fill_price", "avg_px"]) ??
    (Number.isFinite(position?.entry_price) ? Number(position?.entry_price) : null);
  if (entryPrice == null || !Number.isFinite(entryPrice) || entryPrice <= 0) {
    return kind === "R_2R" || kind === "R_4R"
      ? side === "long"
        ? candle.low
        : candle.high
      : candle.close;
  }
  if (kind === "R_2R") {
    return entryPrice;
  }
  const initialRiskPct = readFirstNumber(event.detail, ["initial_risk_pct", "initialriskpct"]);
  if (initialRiskPct != null && Number.isFinite(initialRiskPct) && initialRiskPct > 0) {
    return side === "long" ? entryPrice * (1 + 2 * initialRiskPct) : entryPrice * (1 - 2 * initialRiskPct);
  }
  return entryPrice;
}

function normalizeEventToken(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
}

function addKindsByTypeToken(target: Set<MarkerKind>, token: string): void {
  if (!token) {
    return;
  }
  switch (token) {
    case "ARMED":
      target.add("ARMED");
      return;
    case "TREND_DETECTED":
      target.add("TREND_N");
      return;
    case "HIGH_SIDE_CHANGED":
      target.add("HIGH_H");
      return;
    case "MID_SIDE_CHANGED":
      target.add("MID_M");
      return;
    case "R_PROTECT_2R":
    case "R_BREAK_EVEN":
      target.add("R_2R");
      return;
    case "R_PROTECT_4R":
    case "R_PARTIAL_PROTECT":
      target.add("R_4R");
      return;
    case "TRAILING_TP":
    case "TP":
    case "TAKE_PROFIT":
      target.add("TP");
      return;
    case "TRAILING_STOP":
    case "TRAILING_SL":
    case "SL":
    case "STOP_LOSS":
      target.add("SL");
      return;
    case "TRAILING_TP_SL":
    case "TP_SL":
    case "TAKE_PROFIT_STOP_LOSS":
      target.add("TP");
      target.add("SL");
      return;
    default:
  }
}

function addKindsByMarkerToken(target: Set<MarkerKind>, markerToken: string): void {
  if (!markerToken) {
    return;
  }
  if (markerToken === "ARMED" || markerToken.startsWith("ARMED_")) {
    target.add("ARMED");
  }
  if (markerToken === "N" || markerToken.startsWith("TREND_") || markerToken.includes("TREND")) {
    target.add("TREND_N");
  }
  if (markerToken === "H" || markerToken.startsWith("HIGH_")) {
    target.add("HIGH_H");
  }
  if (markerToken === "M" || markerToken.startsWith("MID_")) {
    target.add("MID_M");
  }
  if (markerToken === "2R" || markerToken === "R_2R" || markerToken === "R2" || markerToken === "BREAK_EVEN_2R") {
    target.add("R_2R");
  }
  if (markerToken === "4R" || markerToken === "R_4R" || markerToken === "R4" || markerToken === "PARTIAL_4R") {
    target.add("R_4R");
  }
  if (markerToken === "TP" || markerToken.startsWith("TP_") || markerToken.includes("TAKE_PROFIT")) {
    target.add("TP");
  }
  if (markerToken === "SL" || markerToken.startsWith("SL_") || markerToken.includes("STOP_LOSS")) {
    target.add("SL");
  }
}

function addKindsByProfitProtectState(target: Set<MarkerKind>, event: HistoryEvent): void {
  const changedFields = readChangedFieldSet(event);
  const action = readSignalAction(event.detail);
  if (!changedFields.has("profit_protect_stage") && action !== 32) {
    return;
  }
  const stage = Math.round(readFirstNumber(event.detail, ["profit_protect_stage", "profitprotectstage"]) ?? 0);
  if (stage >= 2 || action === 32) {
    target.add("R_4R");
    return;
  }
  if (stage >= 1) {
    target.add("R_2R");
  }
}

function readSignalAction(detail: Record<string, unknown> | undefined): number {
  const value = readFirstNumber(detail, ["action", "signal_action"]);
  if (value == null || !Number.isFinite(value)) {
    return 0;
  }
  return Math.round(value);
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
    if (!lowered.has(key)) {
      continue;
    }
    const value = toNumber(lowered.get(key));
    if (value != null) {
      return value;
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
    if (!lowered.has(key)) {
      continue;
    }
    const value = lowered.get(key);
    if (typeof value === "string") {
      const normalized = value.trim();
      if (normalized) {
        return normalized;
      }
    }
  }
  return "";
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function buildMarkerTooltipState(
  marker: OverlayMarker,
  clientX: number,
  clientY: number,
  bodyElement: HTMLDivElement | null
): MarkerTooltipState {
  const tooltipWidth = marker.tooltipRows.length >= 6 ? 304 : 276;
  const tooltipHeight = 56 + marker.tooltipRows.length * 20 + (marker.clampDirection ? 24 : 0);
  const fallback = {
    markerID: marker.id,
    kind: marker.kind,
    price: marker.price,
    clampDirection: marker.clampDirection,
    title: marker.tooltipTitle,
    rows: marker.tooltipRows,
    left: marker.x + 14,
    top: Math.max(8, marker.y - tooltipHeight)
  };
  if (!bodyElement) {
    return fallback;
  }
  const rect = bodyElement.getBoundingClientRect();
  const localX = clientX - rect.left;
  const localY = clientY - rect.top;
  return {
    markerID: marker.id,
    kind: marker.kind,
    price: marker.price,
    clampDirection: marker.clampDirection,
    title: marker.tooltipTitle,
    rows: marker.tooltipRows,
    left: clampValue(localX + 14, 8, Math.max(8, rect.width - tooltipWidth - 8)),
    top: clampValue(localY - tooltipHeight - 10, 8, Math.max(8, rect.height - tooltipHeight - 8))
  };
}

function formatMarkerPrice(price: number): string {
  const abs = Math.abs(price);
  let digits = 6;
  if (abs >= 1000) {
    digits = 2;
  } else if (abs >= 1) {
    digits = 4;
  }
  const fixed = price.toFixed(digits);
  return fixed.replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "");
}

function pushTooltipStringRow(target: MarkerTooltipRow[], label: string, value: string): void {
  const normalized = value.trim();
  if (!normalized) {
    return;
  }
  target.push({ label, value: normalized });
}

function pushTooltipIntegerRow(target: MarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value)) {
    return;
  }
  target.push({ label, value: String(Math.round(value)) });
}

function pushTooltipPriceRow(target: MarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return;
  }
  target.push({ label, value: formatMarkerPrice(value) });
}

function pushTooltipRatioRow(target: MarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return;
  }
  target.push({ label, value: `${formatCompactNumber(value, 4)} (${formatCompactNumber(value * 100, 2)}%)` });
}

function pushTooltipRRow(target: MarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return;
  }
  target.push({ label, value: `${formatCompactNumber(value, 2)}R` });
}

function pushTooltipTimestampRow(target: MarkerTooltipRow[], label: string, value: number | null): void {
  if (value == null || !Number.isFinite(value) || value <= 0) {
    return;
  }
  const text = formatTooltipTimestamp(Math.round(value));
  if (!text) {
    return;
  }
  target.push({ label, value: text });
}

function formatCompactNumber(value: number, digits: number): string {
  const normalized = Number.isFinite(value) ? value : 0;
  return normalized.toFixed(digits).replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "");
}

function formatTooltipTimestamp(ts: number): string {
  if (!Number.isFinite(ts) || ts <= 0) {
    return "";
  }
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const yyyy = String(date.getFullYear());
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}`;
}

function tpSlAnchor(kind: MarkerKind, side: "long" | "short"): "above" | "below" {
  if (kind === "TP") {
    return side === "long" ? "above" : "below";
  }
  return side === "long" ? "below" : "above";
}

function computeCandleWidth(chart: IChartApi, candles: Candle[]): number {
  if (candles.length <= 1) {
    return 12;
  }
  const timeScale = chart.timeScale();
  let minDiff = Number.POSITIVE_INFINITY;
  for (let i = 1; i < candles.length; i += 1) {
    const prev = timeScale.timeToCoordinate(Math.floor(candles[i - 1].ts / 1000) as Time);
    const next = timeScale.timeToCoordinate(Math.floor(candles[i].ts / 1000) as Time);
    if (!Number.isFinite(prev) || !Number.isFinite(next)) {
      continue;
    }
    const diff = Math.abs(Number(next) - Number(prev));
    if (diff > 0.1 && diff < minDiff) {
      minDiff = diff;
    }
  }
  if (!Number.isFinite(minDiff)) {
    return 12;
  }
  return minDiff * 0.78;
}

function buildViewportKey(positionKey: string | undefined, timeframe: string): string {
  const key = (positionKey || "").trim();
  const tf = timeframe.trim().toLowerCase();
  if (!key || !tf) {
    return "";
  }
  return `${key}|${tf}`;
}

function isValidLogicalRange(
  range: { from: number; to: number } | null | undefined
): range is { from: number; to: number } {
  if (!range) {
    return false;
  }
  if (!Number.isFinite(range.from) || !Number.isFinite(range.to)) {
    return false;
  }
  return range.to > range.from;
}

function readViewportRange(cache: Map<string, LogicalRangeSnapshot>, key: string): LogicalRangeSnapshot | null {
  const value = cache.get(key);
  if (!value) {
    return null;
  }
  if (!Number.isFinite(value.from) || !Number.isFinite(value.to) || value.to <= value.from) {
    return null;
  }
  return value;
}

function saveViewportRange(
  cache: Map<string, LogicalRangeSnapshot>,
  key: string,
  range: { from: number; to: number }
): void {
  if (!key || !isValidLogicalRange(range)) {
    return;
  }
  const previous = cache.get(key);
  const from = Number(range.from);
  const to = Number(range.to);
  if (previous && Math.abs(previous.from - from) < 1e-6 && Math.abs(previous.to - to) < 1e-6) {
    return;
  }
  cache.set(key, { from, to, updatedAtMS: Date.now() });
  trimViewportCache(cache);
  persistViewportCache(cache);
}

function adaptTransferredRange(
  sourceRange: { from: number; to: number },
  sourceBars: number,
  targetBars: number
): { from: number; to: number } | null {
  if (!isValidLogicalRange(sourceRange) || targetBars <= 0) {
    return null;
  }
  const span = Math.max(2, Number(sourceRange.to) - Number(sourceRange.from));
  if (sourceBars <= 0) {
    return { from: Number(sourceRange.from), to: Number(sourceRange.from) + span };
  }
  const sourceLast = sourceBars - 1;
  const targetLast = targetBars - 1;
  const rightOffset = sourceLast - Number(sourceRange.to);
  let to = targetLast - rightOffset;
  let from = to - span;

  const minLogical = -50;
  const maxLogical = targetLast + 50;
  if (to > maxLogical) {
    const delta = to - maxLogical;
    to -= delta;
    from -= delta;
  }
  if (from < minLogical) {
    const delta = minLogical - from;
    from += delta;
    to += delta;
  }
  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) {
    return null;
  }
  return { from, to };
}

function trimViewportCache(cache: Map<string, LogicalRangeSnapshot>): void {
  if (cache.size <= MAX_VIEWPORT_CACHE_ROWS) {
    return;
  }
  const rows = Array.from(cache.entries()).sort((a, b) => b[1].updatedAtMS - a[1].updatedAtMS);
  cache.clear();
  for (const [key, value] of rows.slice(0, MAX_VIEWPORT_CACHE_ROWS)) {
    cache.set(key, value);
  }
}

function loadViewportCache(): Map<string, LogicalRangeSnapshot> {
  const out = new Map<string, LogicalRangeSnapshot>();
  if (typeof window === "undefined" || !window.localStorage) {
    return out;
  }
  const raw = window.localStorage.getItem(VIEWPORT_CACHE_KEY);
  if (!raw) {
    return out;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return out;
  }
  if (!parsed || typeof parsed !== "object") {
    return out;
  }
  const payload = parsed as Partial<ViewportCacheEnvelope>;
  if (Number(payload.version) !== VIEWPORT_CACHE_VERSION || !Array.isArray(payload.rows)) {
    return out;
  }
  for (const row of payload.rows) {
    if (!row || typeof row !== "object") {
      continue;
    }
    const item = row as Partial<ViewportCacheRow>;
    const key = typeof item.key === "string" ? item.key.trim() : "";
    const from = Number(item.from);
    const to = Number(item.to);
    if (!key || !Number.isFinite(from) || !Number.isFinite(to) || to <= from) {
      continue;
    }
    const updatedAtMS = Number.isFinite(item.updatedAtMS) ? Number(item.updatedAtMS) : 0;
    out.set(key, { from, to, updatedAtMS });
  }
  trimViewportCache(out);
  return out;
}

function persistViewportCache(cache: Map<string, LogicalRangeSnapshot>): void {
  if (typeof window === "undefined" || !window.localStorage) {
    return;
  }
  const rows: ViewportCacheRow[] = [];
  for (const [key, value] of cache.entries()) {
    rows.push({
      key,
      from: value.from,
      to: value.to,
      updatedAtMS: value.updatedAtMS
    });
  }
  rows.sort((a, b) => b.updatedAtMS - a.updatedAtMS);
  const payload: ViewportCacheEnvelope = {
    version: VIEWPORT_CACHE_VERSION,
    rows: rows.slice(0, MAX_VIEWPORT_CACHE_ROWS)
  };
  try {
    window.localStorage.setItem(VIEWPORT_CACHE_KEY, JSON.stringify(payload));
  } catch {
    // Ignore cache write failure (quota/private mode) to keep chart usable.
  }
}

function zoomChart(chart: IChartApi, scaleFactor: number): void {
  const timeScale = chart.timeScale();
  const current = timeScale.getVisibleLogicalRange();
  if (!current || !Number.isFinite(current.from) || !Number.isFinite(current.to)) {
    return;
  }
  const span = Math.max(2, current.to - current.from);
  const center = (current.from + current.to) / 2;
  const nextSpan = Math.max(2, span * scaleFactor);
  timeScale.setVisibleLogicalRange({
    from: center - nextSpan / 2,
    to: center + nextSpan / 2
  });
}

function panChart(chart: IChartApi, bars: number): void {
  if (!Number.isFinite(bars) || bars === 0) {
    return;
  }
  const timeScale = chart.timeScale();
  const current = timeScale.getVisibleLogicalRange();
  if (!current || !Number.isFinite(current.from) || !Number.isFinite(current.to)) {
    return;
  }
  timeScale.setVisibleLogicalRange({
    from: current.from + bars,
    to: current.to + bars
  });
}

function scheduleFitAllData(chart: IChartApi): void {
  const apply = () => {
    const timeScale = chart.timeScale();
    timeScale.fitContent();
  };

  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(apply);
  });
}

function normalizeCandles(candles: TimeframeCandles["candles"]): TimeframeCandles["candles"] {
  if (candles.length <= 1) {
    return candles;
  }
  const bucket = new Map<number, TimeframeCandles["candles"][number]>();
  for (const candle of candles) {
    bucket.set(candle.ts, candle);
  }
  return Array.from(bucket.values()).sort((a, b) => a.ts - b.ts);
}

function sortTimeframes(values: string[]): string[] {
  const normalized = values.map((item) => item.trim()).filter((item) => item.length > 0);
  return [...normalized].sort((a, b) => {
    const leftWeight = timeframeOrderWeight(a);
    const rightWeight = timeframeOrderWeight(b);
    if (leftWeight === rightWeight) {
      return a.localeCompare(b);
    }
    return leftWeight - rightWeight;
  });
}

function timeframeOrderWeight(value: string): number {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return Number.MAX_SAFE_INTEGER;
  }
  const unit = normalized.slice(-1);
  const step = Number(normalized.slice(0, -1));
  if (!Number.isFinite(step) || step <= 0) {
    return Number.MAX_SAFE_INTEGER;
  }
  switch (unit) {
    case "m":
      return step;
    case "h":
      return step * 60;
    case "d":
      return step * 60 * 24;
    case "w":
      return step * 60 * 24 * 7;
    default:
      return Number.MAX_SAFE_INTEGER;
  }
}

function findNearestCandle(ts: number, candles: TimeframeCandles["candles"]) {
  if (!candles.length) {
    return null;
  }
  let left = 0;
  let right = candles.length - 1;
  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const value = candles[mid].ts;
    if (value === ts) {
      return candles[mid];
    }
    if (value < ts) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  const prev = right >= 0 ? candles[right] : undefined;
  const next = left < candles.length ? candles[left] : undefined;
  if (!prev) return next || null;
  if (!next) return prev;
  return Math.abs(prev.ts - ts) <= Math.abs(next.ts - ts) ? prev : next;
}
