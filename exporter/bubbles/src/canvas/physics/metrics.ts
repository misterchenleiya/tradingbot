import type { BubbleDatum, SizeMetric } from "../../app/types";
import { clamp, lerp } from "../../utils/math";

const EPSILON = 1e-9;
const MAX_STEP_CHANGE = 0.2;
const TREND_SCORE_NORMALIZER = 0.12;
const STRENGTH_SCORE_NORMALIZER = 0.015;

function metricValue(item: BubbleDatum, metric: SizeMetric): number {
  switch (metric) {
    case "marketCap":
      return item.marketCap;
    case "volume24h":
      return item.volume24h;
    case "price":
      return item.price;
    case "rank":
      return Math.max(1, item.rank);
    default:
      return item.marketCap;
  }
}

export function computeRadiusTargets(
  data: BubbleDatum[],
  metric: SizeMetric,
  rMin: number,
  rMax: number,
  baseRadius?: number
): Map<string, number> {
  const out = new Map<string, number>();
  if (data.length === 0) {
    return out;
  }

  const safeMin = Math.max(1, Math.min(rMin, rMax));
  const safeMax = Math.max(safeMin, Math.max(rMin, rMax));
  const defaultRadius = clamp(baseRadius ?? (safeMin + safeMax) * 0.5, safeMin, safeMax);

  if (data.some(isSignalLikeBubble)) {
    for (const item of data) {
      const scale = computeTrendDrivenScale(item);
      const target = scale === undefined
        ? defaultRadius
        : clamp(defaultRadius * scale, safeMin, safeMax);
      out.set(item.id, target);
    }
    return out;
  }

  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  const values = new Map<string, number>();

  for (const item of data) {
    const v = Math.log10(metricValue(item, metric) + 1);
    values.set(item.id, v);
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return out;
  }

  const range = max - min;
  for (const item of data) {
    const v = values.get(item.id) ?? min;
    const norm = range === 0 ? 0.5 : (v - min) / range;
    out.set(item.id, lerp(safeMin, safeMax, norm));
  }
  return out;
}

function isSignalLikeBubble(item: BubbleDatum): boolean {
  return (
    typeof item.highSide === "number" ||
    typeof item.side === "number" ||
    Boolean(item.timeframe) ||
    Boolean(item.strategy)
  );
}

function resolveTrendDirection(item: BubbleDatum): -1 | 0 | 1 {
  if (typeof item.highSide === "number" && Number.isFinite(item.highSide)) {
    const highSide = Math.trunc(item.highSide);
    if (highSide === 1) return 1;
    if (highSide === -1) return -1;
    return 0;
  }

  const side =
    typeof item.side === "number" && Number.isFinite(item.side) ? Math.trunc(item.side) : 0;
  if (side === 1 || side === 8) return 1;
  if (side === -1 || side === -8) return -1;
  return 0;
}

function extractCloseSeriesOldToNew(item: BubbleDatum): number[] {
  if (!Array.isArray(item.ohlcv) || item.ohlcv.length === 0) {
    return [];
  }
  const closes: number[] = [];
  for (let i = item.ohlcv.length - 1; i >= 0; i -= 1) {
    const close = item.ohlcv[i]?.close;
    if (!Number.isFinite(close) || close <= 0) {
      continue;
    }
    closes.push(close);
  }
  return closes;
}

function computeTrendDrivenScale(item: BubbleDatum): number | undefined {
  const trendDirection = resolveTrendDirection(item);
  if (trendDirection === 0) {
    return undefined;
  }

  const closes = extractCloseSeriesOldToNew(item);
  if (closes.length < 2) {
    return undefined;
  }

  const alignedSteps: number[] = [];
  let positiveSteps = 0;
  let negativeSteps = 0;

  for (let i = 0; i < closes.length - 1; i += 1) {
    const from = closes[i];
    const to = closes[i + 1];
    const delta = (to - from) / Math.max(Math.abs(from), EPSILON);
    const aligned = trendDirection * clamp(delta, -MAX_STEP_CHANGE, MAX_STEP_CHANGE);
    alignedSteps.push(aligned);
    if (aligned > 0) positiveSteps += 1;
    if (aligned < 0) negativeSteps += 1;
  }

  if (alignedSteps.length === 0) {
    return undefined;
  }

  const oldestClose = closes[0];
  const latestClose = closes[closes.length - 1];
  const netAlignedChange =
    trendDirection * ((latestClose - oldestClose) / Math.max(Math.abs(oldestClose), EPSILON));
  const trendScore = clamp(netAlignedChange / TREND_SCORE_NORMALIZER, -1, 1);
  const consistency = clamp((positiveSteps - negativeSteps) / alignedSteps.length, -1, 1);

  const segment = Math.max(1, Math.floor(alignedSteps.length / 2));
  const earlyMean = average(alignedSteps.slice(0, segment));
  const recentMean = average(alignedSteps.slice(alignedSteps.length - segment));
  const strengthening = clamp(
    (recentMean - earlyMean) / STRENGTH_SCORE_NORMALIZER,
    -1,
    1
  );

  if (trendScore > 0 && consistency > 0 && strengthening > 0) {
    const growScore = clamp(
      trendScore * 0.5 + consistency * 0.3 + strengthening * 0.2,
      0,
      1
    );
    return 1 + 4 * Math.pow(growScore, 1.35);
  }

  const oppositePenalty = Math.max(0, -trendScore);
  const divergencePenalty = Math.max(0, -consistency);
  const weakeningPenalty = Math.max(0, -strengthening);
  const shrinkScore = clamp(
    oppositePenalty * 0.5 + divergencePenalty * 0.2 + weakeningPenalty * 0.3,
    0,
    1
  );
  return 1 - 0.5 * Math.pow(shrinkScore, 1.1);
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  let sum = 0;
  for (const value of values) {
    sum += value;
  }
  return sum / values.length;
}
