import type { BubbleRuntime, GroupBubbleRuntime } from "../physics/Engine";
import type { ColorMetric, TrendType } from "../../app/types";
import { sideToTrendType } from "../../app/trend";
import {
  resolveBubbleColors,
  resolveBubbleColorsByTrend
} from "../../utils/color";
import type { ImageCache } from "../../utils/imageCache";
import type { BubblePresetRuntime } from "./bubblePresets";

const PULSE_BORDER_PERIOD_MS = 3000;
const PULSE_BORDER_RGB = "255, 232, 43";
const PULSE_PHASE_BUCKETS = 32;
const PULSE_RADIUS_BUCKET_STEP = 1;
const PULSE_SPRITE_CACHE_LIMIT = 256;
const TEXT_METRICS_CACHE_LIMIT = 4096;
const STATIC_OVERLAY_CACHE_LIMIT = 512;
const STATIC_OVERLAY_RADIUS_BUCKET_STEP = 1;

type TextBoxMetrics = {
  width: number;
  ascent: number;
  descent: number;
  height: number;
};

type BackgroundSurface = {
  cssWidth: number;
  cssHeight: number;
  dpr: number;
  pixelWidth: number;
  pixelHeight: number;
  canvas: HTMLCanvasElement;
};

type StaticOverlaySprite = {
  canvas: HTMLCanvasElement | OffscreenCanvas;
  radius: number;
  size: number;
};

type PulseBorderSprite = {
  canvas: HTMLCanvasElement | OffscreenCanvas;
  radius: number;
  size: number;
};

type PulseBorderMode = "position" | "armed";

let backgroundSurfaceCache: BackgroundSurface | null = null;
const textMetricsCache = new Map<string, TextBoxMetrics>();
const staticOverlayCache = new Map<string, StaticOverlaySprite>();
const pulseBorderCache = new Map<string, PulseBorderSprite>();

export function drawBackground(
  ctx: CanvasRenderingContext2D,
  width: number,
  height: number,
  dpr: number
): void {
  const background = getOrCreateBackgroundSurface(width, height, dpr);
  if (!background) {
    const gradient = ctx.createRadialGradient(
      width * 0.5,
      height * 0.45,
      20,
      width * 0.5,
      height * 0.6,
      Math.max(width, height)
    );
    gradient.addColorStop(0, "#2c2c2e");
    gradient.addColorStop(0.6, "#232326");
    gradient.addColorStop(1, "#1a1a1d");
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, width, height);
    return;
  }

  ctx.save();
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.drawImage(background.canvas, 0, 0, ctx.canvas.width, ctx.canvas.height);
  ctx.restore();
}

export function drawBubble(
  ctx: CanvasRenderingContext2D,
  bubble: BubbleRuntime,
  colorMetric: ColorMetric,
  imageCache: ImageCache,
  positionRateBySymbol: Map<string, number>,
  presetRuntime: BubblePresetRuntime,
  nowMs: number
): void {
  const trendType = resolveSignalTrendType(bubble);
  if (trendType === "none") {
    return;
  }

  const colors = resolveBubbleRenderColors(bubble, colorMetric);
  const change = colorMetric === "change7d" ? bubble.display.change7d : bubble.display.change24h;
  const positionRate = resolvePositionRate(bubble, positionRateBySymbol);
  const activePosition = isActivePosition(positionRate);
  const armed = isArmedSignal(bubble);

  presetRuntime.drawBubbleSkin(ctx, bubble, colors, trendType);
  if (activePosition) {
    drawLogo(ctx, bubble, imageCache);
    drawText(ctx, bubble, colorMetric, change, colors.text, positionRate);
    drawPulseBorder(ctx, bubble, nowMs, "position");
    return;
  }
  if (armed) {
    drawPulseBorder(ctx, bubble, nowMs, "armed");
  }
  if (!drawStaticOverlay(ctx, bubble, imageCache, colors.text)) {
    drawLogo(ctx, bubble, imageCache);
    drawText(ctx, bubble, colorMetric, change, colors.text);
  }
}

export function drawGroupBubble(
  ctx: CanvasRenderingContext2D,
  group: GroupBubbleRuntime,
  presetRuntime: BubblePresetRuntime
): void {
  const trendType = sideToTrendType(group.highSide);
  if (trendType === "none") return;
  const colors = resolveBubbleColorsByTrend(trendType);

  ctx.save();
  ctx.globalAlpha = 0.28;
  presetRuntime.drawBubbleSkin(ctx, group, colors, trendType);
  ctx.restore();

  const outline = group.highSide > 0
    ? "rgba(64, 236, 124, 0.22)"
    : "rgba(255, 74, 66, 0.24)";
  ctx.save();
  ctx.strokeStyle = outline;
  ctx.lineWidth = Math.max(1.5, group.radius * 0.022);
  ctx.shadowBlur = Math.max(6, group.radius * 0.16);
  ctx.shadowColor = outline;
  ctx.beginPath();
  ctx.arc(group.x, group.y, Math.max(1, group.radius - ctx.lineWidth * 0.5), 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();
}

function getOrCreateBackgroundSurface(
  width: number,
  height: number,
  dpr: number
): BackgroundSurface | null {
  if (typeof document === "undefined") {
    return null;
  }
  const safeWidth = Math.max(1, width);
  const safeHeight = Math.max(1, height);
  const safeDpr = Number.isFinite(dpr) && dpr > 0 ? dpr : 1;
  const pixelWidth = Math.max(1, Math.round(safeWidth * safeDpr));
  const pixelHeight = Math.max(1, Math.round(safeHeight * safeDpr));

  if (
    backgroundSurfaceCache &&
    backgroundSurfaceCache.cssWidth === safeWidth &&
    backgroundSurfaceCache.cssHeight === safeHeight &&
    backgroundSurfaceCache.dpr === safeDpr &&
    backgroundSurfaceCache.pixelWidth === pixelWidth &&
    backgroundSurfaceCache.pixelHeight === pixelHeight
  ) {
    return backgroundSurfaceCache;
  }

  const canvas = document.createElement("canvas");
  canvas.width = pixelWidth;
  canvas.height = pixelHeight;
  const bgCtx = canvas.getContext("2d");
  if (!bgCtx) {
    return null;
  }

  const gradient = bgCtx.createRadialGradient(
    pixelWidth * 0.5,
    pixelHeight * 0.45,
    Math.max(1, 20 * safeDpr),
    pixelWidth * 0.5,
    pixelHeight * 0.6,
    Math.max(pixelWidth, pixelHeight)
  );
  gradient.addColorStop(0, "#2c2c2e");
  gradient.addColorStop(0.6, "#232326");
  gradient.addColorStop(1, "#1a1a1d");
  bgCtx.fillStyle = gradient;
  bgCtx.fillRect(0, 0, pixelWidth, pixelHeight);

  backgroundSurfaceCache = {
    cssWidth: safeWidth,
    cssHeight: safeHeight,
    dpr: safeDpr,
    pixelWidth,
    pixelHeight,
    canvas
  };
  return backgroundSurfaceCache;
}

function buildTextMetricsKey(text: string, fontSize: number): string {
  const normalizedFont = Math.max(1, Math.round(fontSize * 100) / 100);
  return `${text}\u0000${normalizedFont.toFixed(2)}`;
}

function enforceCacheLimit<T>(cache: Map<string, T>, limit: number): void {
  if (cache.size <= limit) return;
  const overflow = cache.size - limit;
  for (let i = 0; i < overflow; i += 1) {
    const oldestKey = cache.keys().next().value;
    if (typeof oldestKey !== "string") break;
    cache.delete(oldestKey);
  }
}

export function resolveBubbleRenderColors(
  bubble: BubbleRuntime,
  colorMetric: ColorMetric
) {
  const trendType = resolveSignalTrendType(bubble);
  if (!trendType) {
    const change = colorMetric === "change7d" ? bubble.display.change7d : bubble.display.change24h;
    return resolveBubbleColors(change);
  }
  if (trendType === "none") {
    return resolveBubbleColorsByTrend("none");
  }
  return resolveBubbleColorsByTrend(trendType);
}

function resolveSignalTrendType(bubble: BubbleRuntime): TrendType | null {
  const isSignalBubble =
    typeof bubble.data.highSide === "number" ||
    Boolean(bubble.data.trendType) ||
    Boolean(bubble.data.timeframe) ||
    Boolean(bubble.data.strategy);
  if (!isSignalBubble) return null;

  if (typeof bubble.data.highSide === "number" && Number.isFinite(bubble.data.highSide)) {
    const normalizedHighSide = Math.trunc(bubble.data.highSide);
    if (normalizedHighSide === 1 || normalizedHighSide === -1) {
      return sideToTrendType(normalizedHighSide);
    }
  }
  return "none";
}

function isArmedSignal(bubble: BubbleRuntime): boolean {
  const action = bubble.data.action;
  return typeof action === "number" && Number.isFinite(action) && (Math.trunc(action) & 4) !== 0;
}

function drawPulseBorder(
  ctx: CanvasRenderingContext2D,
  bubble: BubbleRuntime,
  nowMs: number,
  mode: PulseBorderMode
): void {
  const phase = (nowMs % PULSE_BORDER_PERIOD_MS) / PULSE_BORDER_PERIOD_MS;
  const radius = Math.max(1, bubble.radius);
  const bucketRadius = bucketRadiusByStep(radius, PULSE_RADIUS_BUCKET_STEP);
  const phaseBucket = Math.max(
    0,
    Math.min(PULSE_PHASE_BUCKETS - 1, Math.round(phase * (PULSE_PHASE_BUCKETS - 1)))
  );
  const key = `${mode}|${bucketRadius.toFixed(2)}|${phaseBucket}`;

  let sprite = pulseBorderCache.get(key);
  if (!sprite) {
    const created = createPulseBorderSprite(bucketRadius, phaseBucket, mode);
    if (!created) return;
    if (pulseBorderCache.size >= PULSE_SPRITE_CACHE_LIMIT) {
      const oldestKey = pulseBorderCache.keys().next().value;
      if (typeof oldestKey === "string") {
        pulseBorderCache.delete(oldestKey);
      }
    }
    pulseBorderCache.set(key, created);
    sprite = created;
  } else {
    pulseBorderCache.delete(key);
    pulseBorderCache.set(key, sprite);
  }

  const scale = radius / sprite.radius;
  const drawSize = sprite.size * scale;
  const half = drawSize * 0.5;
  ctx.drawImage(sprite.canvas, bubble.x - half, bubble.y - half, drawSize, drawSize);
}

function drawPulseHaloLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  cx: number,
  cy: number,
  innerRadius: number,
  outerRadius: number,
  alpha: number
): void {
  if (outerRadius <= innerRadius) return;

  const gradient = ctx.createRadialGradient(cx, cy, innerRadius, cx, cy, outerRadius);
  gradient.addColorStop(0, `rgba(${PULSE_BORDER_RGB}, ${Math.max(0, alpha).toFixed(3)})`);
  gradient.addColorStop(0.45, `rgba(${PULSE_BORDER_RGB}, ${(alpha * 0.6).toFixed(3)})`);
  gradient.addColorStop(1, `rgba(${PULSE_BORDER_RGB}, 0)`);

  ctx.save();
  ctx.shadowBlur = Math.max(8, outerRadius * 0.09);
  ctx.shadowColor = `rgba(${PULSE_BORDER_RGB}, ${(alpha * 0.65).toFixed(3)})`;
  ctx.fillStyle = gradient;
  ctx.beginPath();
  ctx.arc(cx, cy, outerRadius, 0, Math.PI * 2);
  ctx.arc(cx, cy, innerRadius, 0, Math.PI * 2, true);
  ctx.fill();
  ctx.restore();
}

function createPulseBorderSprite(
  radius: number,
  phaseBucket: number,
  mode: PulseBorderMode
): PulseBorderSprite | null {
  const safeBucket = Math.max(0, Math.min(PULSE_PHASE_BUCKETS - 1, phaseBucket));
  const phaseRatio = PULSE_PHASE_BUCKETS > 1 ? safeBucket / (PULSE_PHASE_BUCKETS - 1) : 0;
  const pulse = 0.5 + 0.5 * Math.sin(phaseRatio * Math.PI * 2);
  const includeHalo = mode === "position";
  const edgeAlpha = includeHalo ? 0.58 + pulse * 0.24 : 0.52 + pulse * 0.32;
  const edgeLineWidth = Math.max(1.8, radius * (includeHalo ? 0.035 : 0.038));
  const edgeRadius = radius + edgeLineWidth * 0.45;
  const outerRadius1 = includeHalo ? radius * (1.66 + pulse * 0.2) : edgeRadius;
  const outerRadius2 = includeHalo ? radius * (2.22 + pulse * 0.34) : edgeRadius;
  const haloShadow = includeHalo ? Math.max(8, outerRadius2 * 0.09) : 0;
  const edgeShadow = includeHalo ? Math.max(6, radius * 0.28) : 0;
  const maxOuter = includeHalo
    ? Math.max(edgeRadius + edgeLineWidth * 0.5, outerRadius1, outerRadius2)
    : edgeRadius + edgeLineWidth * 0.5;
  const pad = Math.ceil(maxOuter + Math.max(haloShadow, edgeShadow) + 4);
  const size = Math.max(8, pad * 2);
  const surface = createSurface(size);
  if (!surface) return null;

  const { canvas, ctx } = surface;
  const cx = size * 0.5;
  const cy = size * 0.5;
  ctx.save();
  ctx.globalCompositeOperation = "lighter";
  if (includeHalo) {
    drawPulseHaloLayer(
      ctx,
      cx,
      cy,
      radius * 0.98,
      outerRadius1,
      0.3 + pulse * 0.22
    );
    drawPulseHaloLayer(
      ctx,
      cx,
      cy,
      radius * 1.08,
      outerRadius2,
      0.16 + pulse * 0.13
    );
  }
  ctx.strokeStyle = `rgba(${PULSE_BORDER_RGB}, ${edgeAlpha.toFixed(3)})`;
  ctx.lineWidth = edgeLineWidth;
  ctx.shadowBlur = edgeShadow;
  ctx.shadowColor = `rgba(${PULSE_BORDER_RGB}, ${(includeHalo ? 0.35 + pulse * 0.4 : 0).toFixed(3)})`;
  ctx.beginPath();
  ctx.arc(cx, cy, edgeRadius, 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();

  return {
    canvas,
    radius,
    size
  };
}

function drawLogo(ctx: CanvasRenderingContext2D, bubble: BubbleRuntime, imageCache: ImageCache): void {
  const logoSize = Math.min(bubble.radius * 0.9, 42);
  if (logoSize < 10) return;
  const logoY = bubble.y - bubble.radius * 0.15;
  const logoX = bubble.x;
  const img = imageCache.get(bubble.data.logoUrl);
  if (!img) return;

  ctx.save();
  ctx.beginPath();
  ctx.arc(logoX, logoY, logoSize * 0.5, 0, Math.PI * 2);
  ctx.clip();
  ctx.drawImage(img, logoX - logoSize * 0.5, logoY - logoSize * 0.5, logoSize, logoSize);
  ctx.restore();
}

function drawStaticOverlay(
  ctx: CanvasRenderingContext2D,
  bubble: BubbleRuntime,
  imageCache: ImageCache,
  textColor: string
): boolean {
  const radius = Math.max(2, bubble.radius);
  const bucketRadius = bucketRadiusByStep(radius, STATIC_OVERLAY_RADIUS_BUCKET_STEP);
  const symbol = simplifySymbolLabel(bubble.data.symbol || "--");
  const logoUrl = bubble.data.logoUrl || "";
  const logo = imageCache.get(logoUrl);
  const logoReady = Boolean(logoUrl) && Boolean(logo);
  const key = `${bucketRadius.toFixed(2)}|${textColor}|${symbol}|${logoUrl}|${logoReady ? 1 : 0}`;

  let sprite = staticOverlayCache.get(key);
  if (!sprite) {
    const created = createStaticOverlaySprite(bucketRadius, symbol, textColor, logo);
    if (!created) return false;
    if (staticOverlayCache.size >= STATIC_OVERLAY_CACHE_LIMIT) {
      const oldestKey = staticOverlayCache.keys().next().value;
      if (typeof oldestKey === "string") {
        staticOverlayCache.delete(oldestKey);
      }
    }
    staticOverlayCache.set(key, created);
    sprite = created;
  } else {
    staticOverlayCache.delete(key);
    staticOverlayCache.set(key, sprite);
  }

  const scale = radius / sprite.radius;
  const drawSize = sprite.size * scale;
  const half = drawSize * 0.5;
  ctx.drawImage(sprite.canvas, bubble.x - half, bubble.y - half, drawSize, drawSize);
  return true;
}

function createStaticOverlaySprite(
  radius: number,
  symbol: string,
  textColor: string,
  logo: HTMLImageElement | null
): StaticOverlaySprite | null {
  const pad = Math.ceil(Math.max(3, radius * 0.08));
  const size = Math.max(8, Math.ceil((radius + pad) * 2));
  const surface = createSurface(size);
  if (!surface) return null;

  const { canvas, ctx } = surface;
  const cx = size * 0.5;
  const cy = size * 0.5;
  const logoSize = Math.min(radius * 0.9, 42);
  if (logo && logoSize >= 10) {
    const logoY = cy - radius * 0.15;
    const logoX = cx;
    ctx.save();
    ctx.beginPath();
    ctx.arc(logoX, logoY, logoSize * 0.5, 0, Math.PI * 2);
    ctx.clip();
    ctx.drawImage(logo, logoX - logoSize * 0.5, logoY - logoSize * 0.5, logoSize, logoSize);
    ctx.restore();
  }

  const fontSize = resolveSymbolFontSize(ctx, symbol, radius);
  ctx.save();
  ctx.fillStyle = textColor;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.font = `${fontSize}px 'Space Grotesk', sans-serif`;
  ctx.fillText(symbol, cx, cy);
  ctx.restore();

  return {
    canvas,
    radius,
    size
  };
}

function drawText(
  ctx: CanvasRenderingContext2D,
  bubble: BubbleRuntime,
  colorMetric: ColorMetric,
  change: number,
  textColor: string,
  positionRate?: number
): void {
  void colorMetric;
  void change;

  const symbol = simplifySymbolLabel(bubble.data.symbol || "--");
  ctx.save();
  ctx.fillStyle = textColor;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";

  if (bubble.radius >= 18 && isActivePosition(positionRate)) {
    const profitText = formatRatePercent(positionRate);
    const innerPadding = Math.max(2, bubble.radius * 0.08);
    const innerRadius = Math.max(1, bubble.radius - innerPadding);
    const centerLineGap = Math.max(1, bubble.radius * 0.015);
    const outerPadding = Math.max(1.2, bubble.radius * 0.04);
    const horizontalPadding = Math.max(
      1.2,
      bubble.radius * 0.055
    );

    // 先按几何中线计算一版布局，再按“固定下移为主 + 字形补偿为辅”确定分隔线。
    const baseSymbolLayout = resolveAnchoredHalfTextLayout(
      ctx,
      symbol,
      bubble.y,
      bubble.y,
      innerRadius,
      centerLineGap,
      outerPadding,
      horizontalPadding,
      "topHalfBottom"
    );
    const baseProfitLayout = resolveAnchoredHalfTextLayout(
      ctx,
      profitText,
      bubble.y,
      bubble.y,
      innerRadius,
      centerLineGap,
      outerPadding,
      horizontalPadding,
      "bottomHalfTop"
    );
    const visualDownShift = resolveVisualDownShift(
      baseSymbolLayout,
      baseProfitLayout,
      bubble.radius
    );
    const fixedDownShift = Math.min(14, Math.max(4, bubble.radius * 0.12));
    const dividerY = bubble.y + fixedDownShift + visualDownShift;
    const symbolLayout = resolveAnchoredHalfTextLayout(
      ctx,
      symbol,
      dividerY,
      bubble.y,
      innerRadius,
      centerLineGap,
      outerPadding,
      horizontalPadding,
      "topHalfBottom"
    );
    const profitLayout = resolveAnchoredHalfTextLayout(
      ctx,
      profitText,
      dividerY,
      bubble.y,
      innerRadius,
      centerLineGap,
      outerPadding,
      horizontalPadding,
      "bottomHalfTop"
    );

    // 显示持仓收益时，使用字形 bbox 精确锚定边界，降低“视觉上浮”问题。
    ctx.save();
    ctx.beginPath();
    ctx.arc(bubble.x, bubble.y, Math.max(1, bubble.radius - 1), 0, Math.PI * 2);
    ctx.clip();
    ctx.textBaseline = "alphabetic";
    ctx.font = `${symbolLayout.fontSize}px 'Space Grotesk', sans-serif`;
    ctx.fillText(symbol, bubble.x, symbolLayout.baselineY);
    ctx.font = `${profitLayout.fontSize}px 'Space Grotesk', sans-serif`;
    ctx.fillText(profitText, bubble.x, profitLayout.baselineY);
    ctx.restore();
  } else {
    // 无持仓收益时，恢复交易对单行居中显示。
    const fontSize = resolveSymbolFontSize(ctx, symbol, bubble.radius);
    ctx.font = `${fontSize}px 'Space Grotesk', sans-serif`;
    ctx.fillText(symbol, bubble.x, bubble.y);
  }
  ctx.restore();
}

function simplifySymbolLabel(symbol: string): string {
  const raw = symbol.trim().toUpperCase();
  if (!raw) return "--";

  // 优先按常见分隔符取基础币种，如 BTC/USDT -> BTC。
  const parts = raw.split(/[/:_-]/).filter(Boolean);
  let base = parts.length > 1 ? parts[0] : raw;

  // 兼容无分隔符写法，如 SOMIUSDT / RECALLUSDT.P。
  base = base.replace(/USDT(?:\.P|P)?$/i, "");
  base = base.replace(/\.P$/i, "");

  return base || raw;
}

function normalizeSymbolKey(symbol: string): string {
  return symbol.trim().toUpperCase();
}

function normalizeRatePercent(value: number): number {
  return Math.abs(value) <= 1 ? value * 100 : value;
}

function formatRatePercent(value: number): string {
  const normalized = normalizeRatePercent(value);
  const sign = normalized > 0 ? "+" : "";
  return `${sign}${normalized.toFixed(2)}%`;
}

function resolvePositionRate(bubble: BubbleRuntime, positionRateBySymbol: Map<string, number>): number | undefined {
  const rawKey = normalizeSymbolKey(bubble.data.symbol || "");
  if (!rawKey) return undefined;
  const direct = positionRateBySymbol.get(rawKey);
  if (typeof direct === "number") return direct;
  const simplified = simplifySymbolLabel(rawKey);
  const fallback = positionRateBySymbol.get(simplified);
  return typeof fallback === "number" ? fallback : undefined;
}

function isActivePosition(positionRate?: number): positionRate is number {
  return typeof positionRate === "number" && Number.isFinite(positionRate);
}

function resolveSymbolFontSize(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  symbol: string,
  radius: number
): number {
  const symbolLength = Math.max(1, Array.from(symbol).length);
  const targetRatio =
    symbolLength <= 3 ? 0.5 : symbolLength <= 5 ? 0.7 : 0.9;
  const targetWidth = Math.max(8, radius * 2 * targetRatio);
  const minSize = 6;
  const maxSize = Math.max(minSize, radius * 0.8);
  const seedSize = Math.max(minSize, Math.min(maxSize, radius * 0.45));

  const measuredWidth = Math.max(1, measureTextBoxMetrics(ctx, symbol, seedSize).width);
  const fittedSize = seedSize * (targetWidth / measuredWidth);

  return Math.max(minSize, Math.min(maxSize, fittedSize));
}

type HalfTextAnchor = "topHalfBottom" | "bottomHalfTop";

interface AnchoredHalfTextLayout {
  fontSize: number;
  baselineY: number;
  textHeight: number;
}

function resolveAnchoredHalfTextLayout(
  ctx: CanvasRenderingContext2D,
  text: string,
  dividerY: number,
  bubbleCenterY: number,
  innerRadius: number,
  centerLineGap: number,
  outerPadding: number,
  horizontalPadding: number,
  anchor: HalfTextAnchor
): AnchoredHalfTextLayout {
  const minSize = 6;
  const maxSize = Math.max(minSize, innerRadius * 1.02);

  const measureAt = (fontSize: number) => {
    const measured = measureTextBoxMetrics(ctx, text, fontSize);
    const anchorEdge =
      anchor === "topHalfBottom"
        ? dividerY - centerLineGap
        : dividerY + centerLineGap;
    const baselineY =
      anchor === "topHalfBottom"
        ? anchorEdge - measured.descent
        : anchorEdge + measured.ascent;
    const textTop = baselineY - measured.ascent;
    const textBottom = baselineY + measured.descent;
    const centerY = (textTop + textBottom) * 0.5;
    return {
      textWidth: measured.width,
      textHeight: measured.height,
      baselineY,
      textTop,
      textBottom,
      centerY
    };
  };

  const canFit = (fontSize: number): boolean => {
    const { textWidth, textTop, textBottom, centerY } = measureAt(fontSize);
    const topLimit = bubbleCenterY - innerRadius + outerPadding;
    const bottomLimit = bubbleCenterY + innerRadius - outerPadding;
    if (textTop < topLimit || textBottom > bottomLimit) {
      return false;
    }
    const topChord = chordWidthAtY(innerRadius, Math.abs(textTop - bubbleCenterY));
    const centerChord = chordWidthAtY(innerRadius, Math.abs(centerY - bubbleCenterY));
    const bottomChord = chordWidthAtY(innerRadius, Math.abs(textBottom - bubbleCenterY));
    const maxWidth = Math.max(
      1,
      Math.min(topChord, centerChord, bottomChord) - horizontalPadding * 2
    );
    return textWidth <= maxWidth;
  };

  if (!canFit(minSize)) {
    const fallback = measureAt(minSize);
    return {
      fontSize: minSize,
      baselineY: fallback.baselineY,
      textHeight: fallback.textHeight
    };
  }

  let low = minSize;
  let high = maxSize;
  let best = minSize;
  for (let i = 0; i < 14; i += 1) {
    const mid = (low + high) * 0.5;
    if (canFit(mid)) {
      best = mid;
      low = mid;
    } else {
      high = mid;
    }
  }

  const result = measureAt(best);
  return {
    fontSize: best,
    baselineY: result.baselineY,
    textHeight: result.textHeight
  };
}

function resolveVisualDownShift(
  symbolLayout: AnchoredHalfTextLayout,
  profitLayout: AnchoredHalfTextLayout,
  radius: number
): number {
  const heightDiff = symbolLayout.textHeight - profitLayout.textHeight;
  if (heightDiff <= 0) return 0;
  return Math.min(radius * 0.03, Math.max(0, heightDiff * 0.08));
}

function chordWidthAtY(radius: number, deltaY: number): number {
  const safeRadius = Math.max(0, radius);
  const safeDelta = Math.min(Math.max(0, deltaY), safeRadius);
  const remain = safeRadius * safeRadius - safeDelta * safeDelta;
  if (remain <= 0) return 0;
  return Math.sqrt(remain) * 2;
}

function measureTextBoxMetrics(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  text: string,
  fontSize: number
): TextBoxMetrics {
  const cacheKey = buildTextMetricsKey(text, fontSize);
  const cached = textMetricsCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const normalizedFontSize = Math.max(1, Math.round(fontSize * 100) / 100);
  ctx.font = `${normalizedFontSize}px 'Space Grotesk', sans-serif`;
  const metrics = ctx.measureText(text);
  const width = Math.max(1, metrics.width);

  const safeAscent = Math.max(0, metrics.actualBoundingBoxAscent);
  const safeDescent = Math.max(0, metrics.actualBoundingBoxDescent);
  const measuredHeight = safeAscent + safeDescent;

  let result: TextBoxMetrics;
  if (measuredHeight > 0) {
    result = {
      width,
      ascent: safeAscent > 0 ? safeAscent : measuredHeight * 0.78,
      descent: safeDescent > 0 ? safeDescent : measuredHeight * 0.22,
      height: measuredHeight
    };
  } else {
    const fallbackHeight = normalizedFontSize * 1.08;
    result = {
      width,
      ascent: fallbackHeight * 0.78,
      descent: fallbackHeight * 0.22,
      height: fallbackHeight
    };
  }

  textMetricsCache.set(cacheKey, result);
  enforceCacheLimit(textMetricsCache, TEXT_METRICS_CACHE_LIMIT);
  return result;
}

function bucketRadiusByStep(radius: number, step: number): number {
  const safeStep = Math.max(0.5, step);
  return Math.max(safeStep, Math.round(radius / safeStep) * safeStep);
}

function createSurface(size: number): {
  canvas: HTMLCanvasElement | OffscreenCanvas;
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D;
} | null {
  if (typeof OffscreenCanvas !== "undefined") {
    const canvas = new OffscreenCanvas(size, size);
    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    return { canvas, ctx };
  }

  if (typeof document === "undefined") {
    return null;
  }

  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d");
  if (!ctx) return null;
  return { canvas, ctx };
}
