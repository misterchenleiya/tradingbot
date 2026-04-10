import type { TrendType } from "../../app/types";
import type { BubbleColors } from "../../utils/color";

const DEFAULT_PRESET_ASSET_URL = new URL("../../../bubbles.ndjson", import.meta.url).toString();
const PARSE_YIELD_BATCH = 220;
const MAX_PRESETS = 5;
const MAX_SPRITE_CACHE = 768;
const RADIUS_BUCKET_STEP = 1;
const TAU = Math.PI * 2;

const COMPOSITE_SET = new Set<GlobalCompositeOperation>([
  "source-over",
  "source-in",
  "source-out",
  "source-atop",
  "destination-over",
  "destination-in",
  "destination-out",
  "destination-atop",
  "lighter",
  "copy",
  "xor",
  "multiply",
  "screen",
  "overlay",
  "darken",
  "lighten",
  "color-dodge",
  "color-burn",
  "hard-light",
  "soft-light",
  "difference",
  "exclusion",
  "hue",
  "saturation",
  "color",
  "luminosity"
]);

type GradientStop = {
  offset: number;
  color: string;
};

type GradientDef = {
  gradientId: string;
  x0?: number;
  y0?: number;
  r0?: number;
  x1?: number;
  y1?: number;
  r1?: number;
  stops: GradientStop[];
};

type StyleSnapshot = {
  globalAlpha: number;
  shadowBlur: number;
  shadowColor: string;
  shadowOffsetX: number;
  shadowOffsetY: number;
  globalCompositeOperation: GlobalCompositeOperation;
  lineWidth: number;
  fillStyle?: string;
  strokeStyle?: string;
  fillIsGradient: boolean;
  gradientId?: string;
};

type CircleSample = {
  x: number;
  y: number;
  r: number;
  style: StyleSnapshot;
};

type PresetSample = {
  bodyAlpha: number;
  bodyLineWidthRatio: number;
  bodyShadowBlurRatio: number;
  bodyShadowOffsetXRatio: number;
  bodyShadowOffsetYRatio: number;
  bodyComposite: GlobalCompositeOperation;
  hasGradientFill: boolean;
  strokeLightness: number;
  highlightEnabled: boolean;
  highlightDxRatio: number;
  highlightDyRatio: number;
  highlightRadiusRatio: number;
  highlightAlpha: number;
  highlightComposite: GlobalCompositeOperation;
  stop0Alpha: number;
  stop1Offset: number;
  stop1Alpha: number;
  stop2Alpha: number;
};

type PresetCluster = {
  signature: string;
  count: number;
  bodyAlpha: number;
  bodyLineWidthRatio: number;
  bodyShadowBlurRatio: number;
  bodyShadowOffsetXRatio: number;
  bodyShadowOffsetYRatio: number;
  hasGradientFillCount: number;
  strokeLightness: number;
  highlightEnabledCount: number;
  highlightDxRatio: number;
  highlightDyRatio: number;
  highlightRadiusRatio: number;
  highlightAlpha: number;
  stop0Alpha: number;
  stop1Offset: number;
  stop1Alpha: number;
  stop2Alpha: number;
  bodyCompositeCounts: Map<GlobalCompositeOperation, number>;
  highlightCompositeCounts: Map<GlobalCompositeOperation, number>;
};

type BuildAccumulator = {
  groups: Map<string, CircleSample[]>;
  gradients: Map<string, GradientDef>;
  unresolvedStops: Map<string, GradientStop[]>;
  stats: BubblePresetBuildStats;
};

type SpriteRecord = {
  canvas: HTMLCanvasElement | OffscreenCanvas;
  radius: number;
  size: number;
};

type BubbleStyleLayer = {
  globalAlpha: number;
  globalCompositeOperation: GlobalCompositeOperation;
  lineWidthRatio: number;
  shadowBlurRatio: number;
  shadowOffsetXRatio: number;
  shadowOffsetYRatio: number;
};

type BubbleStyleGradient = {
  focusXRatio: number;
  focusYRatio: number;
  innerRadiusRatio: number;
  outerRadiusRatio: number;
  midStop: number;
};

type BubbleStyleHighlight = {
  enabled: boolean;
  centerXRatio: number;
  centerYRatio: number;
  radiusRatio: number;
  globalAlpha: number;
  globalCompositeOperation: GlobalCompositeOperation;
  stop0Alpha: number;
  stop1Offset: number;
  stop1Alpha: number;
  stop2Alpha: number;
};

type BubbleStyleRim = {
  enabled: boolean;
  lineWidthRatio: number;
  globalAlpha: number;
  globalCompositeOperation: GlobalCompositeOperation;
};

type BubbleStyleGlow = {
  enabled: boolean;
  blurRatio: number;
  globalAlpha: number;
  shadowColorAlpha: number;
  globalCompositeOperation: GlobalCompositeOperation;
};

export type BubbleStylePreset = {
  id: string;
  usageCount: number;
  signature: string;
  lightnessScore: number;
  gradientFillRate: number;
  body: BubbleStyleLayer;
  bodyGradient: BubbleStyleGradient;
  highlight: BubbleStyleHighlight;
  rim: BubbleStyleRim;
  glow: BubbleStyleGlow;
};

export type BubblePresetBuildStats = {
  sourceUrl: string;
  totalLines: number;
  parsedRecords: number;
  parseErrors: number;
  circleRecords: number;
  circleGroups: number;
  extractedSamples: number;
  gradientDefs: number;
  gradientStops: number;
  missingStyleRecords: number;
  missingGradientRefs: number;
  groupsWithoutHighlight: number;
  presetsBuilt: number;
  presetOccurrences: Array<{ id: string; count: number; signature: string }>;
};

export type BubblePresetRuntime = {
  presets: BubbleStylePreset[];
  stats: BubblePresetBuildStats;
  clearCache: () => void;
  pickPreset: (trendType: TrendType | null, bubbleId: string) => BubbleStylePreset;
  drawBubbleSkin: (
    ctx: CanvasRenderingContext2D,
    bubble: BubbleSkinTarget,
    colors: BubbleColors,
    trendType: TrendType | null
  ) => void;
};

export type BubbleSkinTarget = {
  id: string;
  x: number;
  y: number;
  radius: number;
};

export function createDefaultBubblePresetRuntime(): BubblePresetRuntime {
  const fallbackStats: BubblePresetBuildStats = {
    sourceUrl: "fallback",
    totalLines: 0,
    parsedRecords: 0,
    parseErrors: 0,
    circleRecords: 0,
    circleGroups: 0,
    extractedSamples: 0,
    gradientDefs: 0,
    gradientStops: 0,
    missingStyleRecords: 0,
    missingGradientRefs: 0,
    groupsWithoutHighlight: 0,
    presetsBuilt: 1,
    presetOccurrences: [{ id: "fallback-1", count: 1, signature: "fallback" }]
  };
  return createBubblePresetRuntime([defaultPreset()], fallbackStats);
}

export async function loadBubblePresets(customUrl?: string): Promise<BubblePresetRuntime> {
  const candidateUrls = customUrl ? [customUrl] : resolvePresetUrls();
  let lastError: unknown = null;

  for (const sourceUrl of candidateUrls) {
    try {
      const response = await fetch(sourceUrl, { cache: "no-store" });
      if (!response.ok || !response.body) {
        throw new Error(`request failed: ${response.status}`);
      }

      const { presets, stats } = await buildPresetsFromResponse(sourceUrl, response);
      const runtime = createBubblePresetRuntime(presets, stats);

      console.info("[bubble-presets] build stats", {
        sourceUrl,
        totalLines: stats.totalLines,
        parsedRecords: stats.parsedRecords,
        circleRecords: stats.circleRecords,
        circleGroups: stats.circleGroups,
        extractedSamples: stats.extractedSamples,
        gradientDefs: stats.gradientDefs,
        gradientStops: stats.gradientStops,
        presetsBuilt: stats.presetsBuilt,
        parseErrors: stats.parseErrors,
        missingStyleRecords: stats.missingStyleRecords,
        missingGradientRefs: stats.missingGradientRefs,
        groupsWithoutHighlight: stats.groupsWithoutHighlight,
        presetOccurrences: stats.presetOccurrences
      });

      return runtime;
    } catch (err) {
      lastError = err;
      console.warn("[bubble-presets] preset source failed", {
        sourceUrl,
        error: err instanceof Error ? err.message : String(err)
      });
    }
  }

  console.warn("[bubble-presets] fallback to built-in preset", {
    error: lastError instanceof Error ? lastError.message : String(lastError)
  });
  return createDefaultBubblePresetRuntime();
}

function resolvePresetUrls(): string[] {
  const base = normalizeBasePath(import.meta.env.BASE_URL || "/");
  const urls = [`${base}bubbles.ndjson`, DEFAULT_PRESET_ASSET_URL];
  return Array.from(new Set(urls));
}

function normalizeBasePath(rawBase: string): string {
  const trimmed = rawBase.trim();
  if (trimmed.length === 0) {
    return "/";
  }
  let base = trimmed;
  if (!base.startsWith("/")) {
    base = `/${base}`;
  }
  if (!base.endsWith("/")) {
    base = `${base}/`;
  }
  return base;
}

async function buildPresetsFromResponse(
  sourceUrl: string,
  response: Response
): Promise<{ presets: BubbleStylePreset[]; stats: BubblePresetBuildStats }> {
  const stats: BubblePresetBuildStats = {
    sourceUrl,
    totalLines: 0,
    parsedRecords: 0,
    parseErrors: 0,
    circleRecords: 0,
    circleGroups: 0,
    extractedSamples: 0,
    gradientDefs: 0,
    gradientStops: 0,
    missingStyleRecords: 0,
    missingGradientRefs: 0,
    groupsWithoutHighlight: 0,
    presetsBuilt: 0,
    presetOccurrences: []
  };

  const acc: BuildAccumulator = {
    groups: new Map(),
    gradients: new Map(),
    unresolvedStops: new Map(),
    stats
  };

  await parseNdjsonStream(response, stats, (record) => {
    stats.parsedRecords += 1;
    handleRecord(record, acc);
  });

  for (const [gradientId, stops] of acc.unresolvedStops.entries()) {
    if (stops.length === 0) continue;
    const gradient = acc.gradients.get(gradientId);
    if (!gradient) continue;
    gradient.stops.push(...stops);
    gradient.stops.sort((a, b) => a.offset - b.offset);
  }

  stats.gradientDefs = acc.gradients.size;
  stats.gradientStops = Array.from(acc.gradients.values()).reduce((sum, item) => sum + item.stops.length, 0);

  const samples: PresetSample[] = [];
  for (const circles of acc.groups.values()) {
    const sample = extractPresetSample(circles, acc.gradients, stats);
    if (!sample) continue;
    samples.push(sample);
  }

  stats.circleGroups = acc.groups.size;
  stats.extractedSamples = samples.length;

  const presets = buildPresets(samples, stats);
  stats.presetsBuilt = presets.length;

  return { presets, stats };
}

async function parseNdjsonStream(
  response: Response,
  stats: BubblePresetBuildStats,
  onRecord: (record: Record<string, unknown>) => void
): Promise<void> {
  const reader = response.body?.getReader();
  if (!reader) return;

  const decoder = new TextDecoder();
  let buffer = "";
  let parsedSinceYield = 0;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = buffer.slice(0, newlineIndex);
      buffer = buffer.slice(newlineIndex + 1);
      parseLine(line, stats, onRecord);
      parsedSinceYield += 1;
      if (parsedSinceYield >= PARSE_YIELD_BATCH) {
        parsedSinceYield = 0;
        await yieldMainThread();
      }
      newlineIndex = buffer.indexOf("\n");
    }
  }

  buffer += decoder.decode();
  if (buffer.trim().length > 0) {
    parseLine(buffer, stats, onRecord);
  }
}

function parseLine(
  line: string,
  stats: BubblePresetBuildStats,
  onRecord: (record: Record<string, unknown>) => void
): void {
  stats.totalLines += 1;
  const trimmed = line.trim();
  if (trimmed.length === 0) return;

  try {
    const parsed = JSON.parse(trimmed);
    if (parsed && typeof parsed === "object") {
      onRecord(parsed as Record<string, unknown>);
    }
  } catch {
    stats.parseErrors += 1;
  }
}

async function yieldMainThread(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

function handleRecord(record: Record<string, unknown>, acc: BuildAccumulator): void {
  const type = asString(record.type);
  if (!type) return;

  if (type === "circle") {
    acc.stats.circleRecords += 1;
    const circle = parseCircleRecord(record, acc.stats);
    if (!circle) return;
    const key = resolveCircleGroupKey(record);
    const group = acc.groups.get(key);
    if (group) {
      group.push(circle);
    } else {
      acc.groups.set(key, [circle]);
    }
    return;
  }

  if (type === "arc" || type === "fill" || type === "stroke") {
    acc.stats.circleRecords += 1;
    const circle = parseCircleRecord(record, acc.stats);
    if (!circle) return;
    const key = resolveCircleGroupKey(record);
    const group = acc.groups.get(key);
    if (group) {
      group.push(circle);
    } else {
      acc.groups.set(key, [circle]);
    }
    return;
  }

  if (type === "radialGradient") {
    parseRadialGradientRecord(record, acc);
    return;
  }

  if (type === "colorStop") {
    parseColorStopRecord(record, acc);
  }
}

function resolveCircleGroupKey(record: Record<string, unknown>): string {
  const tsWall = asNumber(record.tsWall) ?? -1;
  const frameId = asNumber(record.frameId) ?? -1;
  const canvasId = asString(record.canvasId) || "canvas-unknown";
  return `${tsWall}|${frameId}|${canvasId}`;
}

function parseCircleRecord(
  record: Record<string, unknown>,
  stats: BubblePresetBuildStats
): CircleSample | null {
  const x = asNumber(record.x);
  const y = asNumber(record.y);
  const r = asNumber(record.r);
  if (!isFiniteNumber(x) || !isFiniteNumber(y) || !isFiniteNumber(r) || r <= 0) {
    return null;
  }

  const styleRecord = asRecord(record.style);
  const legacyStyle = asRecord(record.fill) || asRecord(record.stroke);
  if (!styleRecord && !legacyStyle) {
    stats.missingStyleRecords += 1;
    return null;
  }

  return {
    x,
    y,
    r,
    style: normalizeStyleSnapshot((styleRecord || legacyStyle) as Record<string, unknown>)
  };
}

function parseRadialGradientRecord(record: Record<string, unknown>, acc: BuildAccumulator): void {
  const gradientId = asString(record.gradientId) || asString(record.id);
  if (!gradientId) return;

  const existing = acc.gradients.get(gradientId);
  const next: GradientDef = existing || { gradientId, stops: [] };

  const x0 = asNumber(record.x0) ?? asNumber(record.fx) ?? asNumber(record.x);
  const y0 = asNumber(record.y0) ?? asNumber(record.fy) ?? asNumber(record.y);
  const r0 = asNumber(record.r0) ?? asNumber(record.innerRadius);
  const x1 = asNumber(record.x1) ?? asNumber(record.cx) ?? asNumber(record.x);
  const y1 = asNumber(record.y1) ?? asNumber(record.cy) ?? asNumber(record.y);
  const r1 = asNumber(record.r1) ?? asNumber(record.radius);

  if (isFiniteNumber(x0)) next.x0 = x0;
  if (isFiniteNumber(y0)) next.y0 = y0;
  if (isFiniteNumber(r0)) next.r0 = r0;
  if (isFiniteNumber(x1)) next.x1 = x1;
  if (isFiniteNumber(y1)) next.y1 = y1;
  if (isFiniteNumber(r1)) next.r1 = r1;

  const stops = parseGradientStops(record.stops);
  if (stops.length > 0) {
    next.stops.push(...stops);
  }

  next.stops.sort((a, b) => a.offset - b.offset);
  acc.gradients.set(gradientId, next);
}

function parseColorStopRecord(record: Record<string, unknown>, acc: BuildAccumulator): void {
  const gradientId = asString(record.gradientId);
  const offset = asNumber(record.offset);
  const color = asString(record.color);
  if (!gradientId || !isFiniteNumber(offset) || !color) return;

  const stop: GradientStop = {
    offset: clamp(offset, 0, 1),
    color
  };

  const gradient = acc.gradients.get(gradientId);
  if (gradient) {
    gradient.stops.push(stop);
    gradient.stops.sort((a, b) => a.offset - b.offset);
    return;
  }

  const pending = acc.unresolvedStops.get(gradientId);
  if (pending) {
    pending.push(stop);
    return;
  }
  acc.unresolvedStops.set(gradientId, [stop]);
}

function parseGradientStops(rawStops: unknown): GradientStop[] {
  if (!Array.isArray(rawStops)) return [];

  const out: GradientStop[] = [];
  for (const item of rawStops) {
    const stopObj = asRecord(item);
    if (!stopObj) continue;
    const offset = asNumber(stopObj.offset);
    const color = asString(stopObj.color);
    if (!isFiniteNumber(offset) || !color) continue;
    out.push({ offset: clamp(offset, 0, 1), color });
  }
  out.sort((a, b) => a.offset - b.offset);
  return out;
}

function normalizeStyleSnapshot(style: Record<string, unknown>): StyleSnapshot {
  const fillStyle = style.fillStyle;
  const fillRecord = asRecord(fillStyle);
  const fillIsGradient = Boolean(
    fillRecord &&
      ((asString(fillRecord.kind) || "").toLowerCase() === "gradient" ||
        (asString(fillRecord.type) || "").toLowerCase() === "gradient")
  );

  return {
    globalAlpha: clamp(asNumber(style.globalAlpha) ?? asNumber(style.alpha) ?? 1, 0, 1),
    shadowBlur: Math.max(0, asNumber(style.shadowBlur) ?? 0),
    shadowColor: asString(style.shadowColor) || "rgba(0, 0, 0, 0)",
    shadowOffsetX: asNumber(style.shadowOffsetX) ?? 0,
    shadowOffsetY: asNumber(style.shadowOffsetY) ?? 0,
    globalCompositeOperation: normalizeComposite(
      style.globalCompositeOperation ?? style.comp,
      "source-over"
    ),
    lineWidth: Math.max(0, asNumber(style.lineWidth) ?? 0),
    fillStyle: typeof fillStyle === "string" ? fillStyle : undefined,
    strokeStyle: asString(style.strokeStyle) || undefined,
    fillIsGradient,
    gradientId: fillIsGradient ? asString(fillRecord?.gradientId) : undefined
  };
}

function extractPresetSample(
  circles: CircleSample[],
  gradients: Map<string, GradientDef>,
  stats: BubblePresetBuildStats
): PresetSample | null {
  if (circles.length === 0) return null;

  const sorted = [...circles].sort((a, b) => b.r - a.r);
  const body = sorted[0];
  if (!body || body.r <= 0) return null;

  const highlight =
    sorted.find((item) => item !== body && item.style.fillIsGradient && item.r < body.r * 0.98) ||
    sorted.find((item) => item !== body && item.r < body.r * 0.92);

  if (!highlight) {
    stats.groupsWithoutHighlight += 1;
  }

  const highlightEnabled = Boolean(highlight);
  const highlightDxRatio = highlight ? (highlight.x - body.x) / body.r : -0.33;
  const highlightDyRatio = highlight ? (highlight.y - body.y) / body.r : -0.34;
  const highlightRadiusRatio = highlight ? highlight.r / body.r : 0.52;

  let stop0Alpha = 0.95;
  let stop1Offset = 0.45;
  let stop1Alpha = 0.42;
  let stop2Alpha = 0;

  if (highlight?.style.gradientId) {
    const gradient = gradients.get(highlight.style.gradientId);
    if (gradient && gradient.stops.length > 0) {
      const profile = toStopProfile(gradient.stops);
      stop0Alpha = profile.stop0Alpha;
      stop1Offset = profile.stop1Offset;
      stop1Alpha = profile.stop1Alpha;
      stop2Alpha = profile.stop2Alpha;
    } else {
      stats.missingGradientRefs += 1;
    }
  }

  return {
    bodyAlpha: body.style.globalAlpha,
    bodyLineWidthRatio: body.style.lineWidth / body.r,
    bodyShadowBlurRatio: body.style.shadowBlur / body.r,
    bodyShadowOffsetXRatio: body.style.shadowOffsetX / body.r,
    bodyShadowOffsetYRatio: body.style.shadowOffsetY / body.r,
    bodyComposite: body.style.globalCompositeOperation,
    hasGradientFill: body.style.fillIsGradient,
    strokeLightness: colorLightness(body.style.strokeStyle || body.style.fillStyle || "#999999"),
    highlightEnabled,
    highlightDxRatio,
    highlightDyRatio,
    highlightRadiusRatio,
    highlightAlpha: highlight ? highlight.style.globalAlpha : 0.7,
    highlightComposite: highlight
      ? normalizeComposite(highlight.style.globalCompositeOperation, "screen")
      : "screen",
    stop0Alpha,
    stop1Offset,
    stop1Alpha,
    stop2Alpha
  };
}

function toStopProfile(stops: GradientStop[]): {
  stop0Alpha: number;
  stop1Offset: number;
  stop1Alpha: number;
  stop2Alpha: number;
} {
  if (stops.length === 0) {
    return {
      stop0Alpha: 0.95,
      stop1Offset: 0.45,
      stop1Alpha: 0.42,
      stop2Alpha: 0
    };
  }

  const first = stops[0];
  const middle = stops[Math.floor(stops.length / 2)];
  const last = stops[stops.length - 1];

  return {
    stop0Alpha: alphaFromColor(first.color),
    stop1Offset: clamp(middle.offset, 0, 1),
    stop1Alpha: alphaFromColor(middle.color),
    stop2Alpha: alphaFromColor(last.color)
  };
}

function buildPresets(samples: PresetSample[], stats: BubblePresetBuildStats): BubbleStylePreset[] {
  if (samples.length === 0) {
    stats.presetOccurrences = [{ id: "fallback-1", count: 1, signature: "fallback" }];
    return [defaultPreset()];
  }

  const clusters = new Map<string, PresetCluster>();
  for (const sample of samples) {
    const signature = sampleSignature(sample);
    const existing = clusters.get(signature);
    if (existing) {
      appendSample(existing, sample);
      continue;
    }

    const cluster: PresetCluster = {
      signature,
      count: 1,
      bodyAlpha: sample.bodyAlpha,
      bodyLineWidthRatio: sample.bodyLineWidthRatio,
      bodyShadowBlurRatio: sample.bodyShadowBlurRatio,
      bodyShadowOffsetXRatio: sample.bodyShadowOffsetXRatio,
      bodyShadowOffsetYRatio: sample.bodyShadowOffsetYRatio,
      hasGradientFillCount: sample.hasGradientFill ? 1 : 0,
      strokeLightness: sample.strokeLightness,
      highlightEnabledCount: sample.highlightEnabled ? 1 : 0,
      highlightDxRatio: sample.highlightDxRatio,
      highlightDyRatio: sample.highlightDyRatio,
      highlightRadiusRatio: sample.highlightRadiusRatio,
      highlightAlpha: sample.highlightAlpha,
      stop0Alpha: sample.stop0Alpha,
      stop1Offset: sample.stop1Offset,
      stop1Alpha: sample.stop1Alpha,
      stop2Alpha: sample.stop2Alpha,
      bodyCompositeCounts: new Map([[sample.bodyComposite, 1]]),
      highlightCompositeCounts: new Map([[sample.highlightComposite, 1]])
    };

    clusters.set(signature, cluster);
  }

  const sorted = Array.from(clusters.values()).sort((a, b) => b.count - a.count);
  const minClusterCount = Math.max(1, Math.floor(samples.length * 0.04));
  const filtered = sorted.filter((item) => item.count >= minClusterCount);
  const selected = (filtered.length > 0 ? filtered : sorted).slice(0, MAX_PRESETS);

  const presets = selected.map((cluster, index) => clusterToPreset(cluster, index));
  stats.presetOccurrences = presets.map((preset) => ({
    id: preset.id,
    count: preset.usageCount,
    signature: preset.signature
  }));

  return presets.length > 0 ? presets : [defaultPreset()];
}

function appendSample(cluster: PresetCluster, sample: PresetSample): void {
  cluster.count += 1;
  cluster.bodyAlpha += sample.bodyAlpha;
  cluster.bodyLineWidthRatio += sample.bodyLineWidthRatio;
  cluster.bodyShadowBlurRatio += sample.bodyShadowBlurRatio;
  cluster.bodyShadowOffsetXRatio += sample.bodyShadowOffsetXRatio;
  cluster.bodyShadowOffsetYRatio += sample.bodyShadowOffsetYRatio;
  cluster.hasGradientFillCount += sample.hasGradientFill ? 1 : 0;
  cluster.strokeLightness += sample.strokeLightness;
  cluster.highlightEnabledCount += sample.highlightEnabled ? 1 : 0;
  cluster.highlightDxRatio += sample.highlightDxRatio;
  cluster.highlightDyRatio += sample.highlightDyRatio;
  cluster.highlightRadiusRatio += sample.highlightRadiusRatio;
  cluster.highlightAlpha += sample.highlightAlpha;
  cluster.stop0Alpha += sample.stop0Alpha;
  cluster.stop1Offset += sample.stop1Offset;
  cluster.stop1Alpha += sample.stop1Alpha;
  cluster.stop2Alpha += sample.stop2Alpha;

  bumpCount(cluster.bodyCompositeCounts, sample.bodyComposite);
  bumpCount(cluster.highlightCompositeCounts, sample.highlightComposite);
}

function bumpCount(map: Map<GlobalCompositeOperation, number>, key: GlobalCompositeOperation): void {
  const current = map.get(key) || 0;
  map.set(key, current + 1);
}

function clusterToPreset(cluster: PresetCluster, index: number): BubbleStylePreset {
  const avg = (value: number): number => value / cluster.count;
  const gradientFillRate = cluster.hasGradientFillCount / cluster.count;
  const lightness = clamp(avg(cluster.strokeLightness), 0, 1);
  const highlightRate = cluster.highlightEnabledCount / cluster.count;

  const bodyLineWidthRatio = clampWithFallback(avg(cluster.bodyLineWidthRatio), 0.035, 0.008, 0.09);
  const bodyShadowBlurRatio = clampWithFallback(
    avg(cluster.bodyShadowBlurRatio),
    0.22 + highlightRate * 0.22,
    0,
    0.85
  );

  return {
    id: `ndjson-${index + 1}`,
    usageCount: cluster.count,
    signature: cluster.signature,
    lightnessScore: lightness,
    gradientFillRate,
    body: {
      globalAlpha: clampWithFallback(avg(cluster.bodyAlpha), 0.9, 0.45, 1),
      globalCompositeOperation: pickDominant(cluster.bodyCompositeCounts, "source-over"),
      lineWidthRatio: bodyLineWidthRatio,
      shadowBlurRatio: bodyShadowBlurRatio,
      shadowOffsetXRatio: clamp(avg(cluster.bodyShadowOffsetXRatio), -0.5, 0.5),
      shadowOffsetYRatio: clamp(avg(cluster.bodyShadowOffsetYRatio), -0.5, 0.5)
    },
    bodyGradient: {
      focusXRatio: clamp(avg(cluster.highlightDxRatio) * 0.82, -0.75, 0.75),
      focusYRatio: clamp(avg(cluster.highlightDyRatio) * 0.82, -0.75, 0.75),
      innerRadiusRatio: 0.08,
      outerRadiusRatio: 1,
      midStop: clamp(0.54 + (1 - gradientFillRate) * 0.15, 0.45, 0.8)
    },
    highlight: {
      enabled: true,
      centerXRatio: clampWithFallback(avg(cluster.highlightDxRatio), -0.33, -0.85, 0.85),
      centerYRatio: clampWithFallback(avg(cluster.highlightDyRatio), -0.34, -0.85, 0.85),
      radiusRatio: clampWithFallback(avg(cluster.highlightRadiusRatio), 0.52, 0.18, 1.45),
      globalAlpha: clampWithFallback(avg(cluster.highlightAlpha), 0.82, 0.15, 1),
      globalCompositeOperation: pickDominant(cluster.highlightCompositeCounts, "screen"),
      stop0Alpha: clampWithFallback(avg(cluster.stop0Alpha), 0.98, 0, 1),
      stop1Offset: clampWithFallback(avg(cluster.stop1Offset), 0.45, 0.05, 0.95),
      stop1Alpha: clampWithFallback(avg(cluster.stop1Alpha), 0.46, 0, 1),
      stop2Alpha: clampWithFallback(avg(cluster.stop2Alpha), 0, 0, 1)
    },
    rim: {
      enabled: true,
      lineWidthRatio: clamp(bodyLineWidthRatio, 0.01, 0.08),
      globalAlpha: clamp(0.2 + lightness * 0.55, 0.16, 0.88),
      globalCompositeOperation: "source-over"
    },
    glow: {
      enabled: true,
      blurRatio: clamp(bodyShadowBlurRatio + 0.26, 0.2, 1.05),
      globalAlpha: clamp(0.26 + highlightRate * 0.42, 0.18, 0.85),
      shadowColorAlpha: clamp(0.32 + lightness * 0.5, 0.22, 0.95),
      globalCompositeOperation: lightness >= 0.5 ? "lighter" : "screen"
    }
  };
}

function sampleSignature(sample: PresetSample): string {
  const parts = [
    round(sample.highlightDxRatio, 2),
    round(sample.highlightDyRatio, 2),
    round(sample.highlightRadiusRatio, 2),
    bucketLineWidth(sample.bodyLineWidthRatio),
    round(sample.bodyShadowBlurRatio, 3),
    sample.bodyComposite,
    sample.highlightComposite,
    sample.highlightEnabled ? "h1" : "h0",
    bucket(sample.strokeLightness)
  ];
  return parts.join("|");
}

function bucketLineWidth(value: number): string {
  if (value < 0.025) return "lw-s";
  if (value < 0.042) return "lw-m";
  return "lw-l";
}

function round(value: number, digits: number): string {
  return value.toFixed(digits);
}

function bucket(value: number): string {
  if (value < 0.33) return "dark";
  if (value < 0.66) return "mid";
  return "light";
}

function pickDominant(
  counts: Map<GlobalCompositeOperation, number>,
  fallback: GlobalCompositeOperation
): GlobalCompositeOperation {
  let winner: GlobalCompositeOperation = fallback;
  let max = -1;
  for (const [op, count] of counts.entries()) {
    if (count > max) {
      winner = op;
      max = count;
    }
  }
  return winner;
}

function clampWithFallback(value: number, fallback: number, min: number, max: number): number {
  if (!isFiniteNumber(value)) return fallback;
  return clamp(value, min, max);
}

function defaultPreset(): BubbleStylePreset {
  return {
    id: "fallback-1",
    usageCount: 1,
    signature: "fallback",
    lightnessScore: 0.5,
    gradientFillRate: 1,
    body: {
      globalAlpha: 0.92,
      globalCompositeOperation: "source-over",
      lineWidthRatio: 0.035,
      shadowBlurRatio: 0.24,
      shadowOffsetXRatio: 0,
      shadowOffsetYRatio: 0
    },
    bodyGradient: {
      focusXRatio: 0,
      focusYRatio: -0.43,
      innerRadiusRatio: 0.08,
      outerRadiusRatio: 1,
      midStop: 0.56
    },
    highlight: {
      enabled: true,
      centerXRatio: 0,
      centerYRatio: -0.52,
      radiusRatio: 0.28,
      globalAlpha: 0.88,
      globalCompositeOperation: "source-over",
      stop0Alpha: 0.88,
      stop1Offset: 0.36,
      stop1Alpha: 0.22,
      stop2Alpha: 0
    },
    rim: {
      enabled: true,
      lineWidthRatio: 0.035,
      globalAlpha: 0.54,
      globalCompositeOperation: "source-over"
    },
    glow: {
      enabled: true,
      blurRatio: 0.52,
      globalAlpha: 0.46,
      shadowColorAlpha: 0.62,
      globalCompositeOperation: "lighter"
    }
  };
}

function createBubblePresetRuntime(
  presets: BubbleStylePreset[],
  stats: BubblePresetBuildStats
): BubblePresetRuntime {
  const spriteCache = new Map<string, SpriteRecord>();
  const sortedByLightness = [...presets].sort((a, b) => a.lightnessScore - b.lightnessScore);

  const clearCache = () => {
    spriteCache.clear();
  };

  const pickPreset = (trendType: TrendType | null, bubbleId: string): BubbleStylePreset => {
    if (sortedByLightness.length === 1) {
      return sortedByLightness[0];
    }

    const last = sortedByLightness.length - 1;
    const secondDark = Math.min(1, last);
    const secondLight = Math.max(last - 1, 0);
    const mid = Math.floor(last / 2);

    if (trendType === "bullish") return sortedByLightness[last];
    if (trendType === "bullishPullback") return sortedByLightness[secondLight];
    if (trendType === "bearish") return sortedByLightness[0];
    if (trendType === "bearishPullback") return sortedByLightness[secondDark];

    const hash = hashString(bubbleId);
    return sortedByLightness[(mid + (hash % sortedByLightness.length)) % sortedByLightness.length];
  };

  const drawBubbleSkin = (
    ctx: CanvasRenderingContext2D,
    bubble: BubbleSkinTarget,
    colors: BubbleColors,
    trendType: TrendType | null
  ) => {
    const radius = Math.max(2, bubble.radius);
    const preset = pickPreset(trendType, bubble.id);
    const bucketRadius = bucketRadiusByStep(radius, RADIUS_BUCKET_STEP);
    const paletteKey = `${colors.core}|${colors.edge}|${colors.highlight}|${colors.rim}|${colors.glow}`;
    const key = `${preset.id}|${bucketRadius.toFixed(2)}|${paletteKey}`;

    let sprite = spriteCache.get(key);
    if (!sprite) {
      const created = createSprite(preset, bucketRadius, colors);
      if (!created) return;

      if (spriteCache.size >= MAX_SPRITE_CACHE) {
        const oldestKey = spriteCache.keys().next().value;
        if (typeof oldestKey === "string") {
          spriteCache.delete(oldestKey);
        }
      }
      spriteCache.set(key, created);
      sprite = created;
    } else {
      // 命中时刷新 LRU，避免热点 sprite 被意外淘汰。
      spriteCache.delete(key);
      spriteCache.set(key, sprite);
    }

    const scale = radius / sprite.radius;
    const drawSize = sprite.size * scale;
    const half = drawSize * 0.5;
    ctx.drawImage(sprite.canvas, bubble.x - half, bubble.y - half, drawSize, drawSize);
  };

  return {
    presets,
    stats,
    clearCache,
    pickPreset,
    drawBubbleSkin
  };
}

function createSprite(
  preset: BubbleStylePreset,
  radius: number,
  colors: BubbleColors
): SpriteRecord | null {
  const glowBlur = preset.glow.enabled ? radius * preset.glow.blurRatio : 0;
  const bodyShadow = radius * preset.body.shadowBlurRatio;
  const shadowShift = Math.max(
    Math.abs(radius * preset.body.shadowOffsetXRatio),
    Math.abs(radius * preset.body.shadowOffsetYRatio)
  );
  const rimWidth = preset.rim.enabled ? Math.max(1, radius * preset.rim.lineWidthRatio) : 0;
  const pad = Math.ceil(glowBlur + bodyShadow + shadowShift + rimWidth + 4);
  const size = Math.max(8, Math.ceil((radius + pad) * 2));

  const surface = createSurface(size);
  if (!surface) return null;

  const { canvas, ctx } = surface;
  const cx = size * 0.5;
  const cy = size * 0.5;

  drawGlowLayer(ctx, preset, cx, cy, radius, colors);
  drawBodyLayer(ctx, preset, cx, cy, radius, colors);
  drawDepthLayer(ctx, cx, cy, radius);
  drawHighlightLayer(ctx, preset, cx, cy, radius, colors);
  drawSpecularLayer(ctx, preset, cx, cy, radius);
  drawRimLayer(ctx, preset, cx, cy, radius, colors);

  return {
    canvas,
    radius,
    size
  };
}

function drawGlowLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  preset: BubbleStylePreset,
  cx: number,
  cy: number,
  radius: number,
  colors: BubbleColors
): void {
  if (!preset.glow.enabled) return;

  ctx.save();
  ctx.globalCompositeOperation = preset.glow.globalCompositeOperation;
  ctx.globalAlpha = preset.glow.globalAlpha;
  ctx.shadowBlur = Math.max(0, radius * preset.glow.blurRatio);
  ctx.shadowColor = withAlpha(colors.glow, preset.glow.shadowColorAlpha);
  ctx.fillStyle = withAlpha(colors.core, 0.12);
  ctx.beginPath();
  ctx.arc(cx, cy, radius * 0.99, 0, TAU);
  ctx.fill();
  ctx.globalAlpha = preset.glow.globalAlpha * 0.55;
  ctx.shadowBlur = Math.max(0, radius * preset.glow.blurRatio * 1.2);
  ctx.fillStyle = withAlpha(colors.glow, 0.48);
  ctx.beginPath();
  ctx.arc(cx, cy, radius * 0.9, 0, TAU);
  ctx.fill();
  ctx.restore();
}

function drawBodyLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  preset: BubbleStylePreset,
  cx: number,
  cy: number,
  radius: number,
  colors: BubbleColors
): void {
  const focusX = cx + radius * preset.bodyGradient.focusXRatio;
  const focusY = cy + radius * preset.bodyGradient.focusYRatio;
  const innerR = Math.max(0.5, radius * preset.bodyGradient.innerRadiusRatio);
  const outerR = Math.max(innerR + 0.5, radius * preset.bodyGradient.outerRadiusRatio);
  const midStop = clamp(preset.bodyGradient.midStop, 0.05, 0.95);

  const gradient = ctx.createRadialGradient(focusX, focusY, innerR, cx, cy, outerR);
  gradient.addColorStop(0, withAlpha(colors.core, preset.body.globalAlpha));
  gradient.addColorStop(midStop, withAlpha(colors.core, preset.body.globalAlpha * 0.96));
  gradient.addColorStop(1, withAlpha(colors.edge, preset.body.globalAlpha));

  ctx.save();
  ctx.globalCompositeOperation = preset.body.globalCompositeOperation;
  ctx.shadowBlur = Math.max(0, radius * preset.body.shadowBlurRatio);
  ctx.shadowColor = withAlpha(colors.glow, 0.55);
  ctx.shadowOffsetX = radius * preset.body.shadowOffsetXRatio;
  ctx.shadowOffsetY = radius * preset.body.shadowOffsetYRatio;
  ctx.fillStyle = gradient;
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, TAU);
  ctx.fill();
  ctx.restore();
}

function drawDepthLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  cx: number,
  cy: number,
  radius: number
): void {
  const depthGradient = ctx.createRadialGradient(
    cx,
    cy + radius * 0.36,
    Math.max(1, radius * 0.08),
    cx,
    cy,
    radius * 1.02
  );
  depthGradient.addColorStop(0, "rgba(0, 0, 0, 0)");
  depthGradient.addColorStop(0.75, "rgba(0, 0, 0, 0.04)");
  depthGradient.addColorStop(1, "rgba(0, 0, 0, 0.15)");

  ctx.save();
  ctx.globalCompositeOperation = "multiply";
  ctx.fillStyle = depthGradient;
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, TAU);
  ctx.fill();
  ctx.restore();
}

function drawHighlightLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  preset: BubbleStylePreset,
  cx: number,
  cy: number,
  radius: number,
  colors: BubbleColors
): void {
  if (!preset.highlight.enabled || radius < 6) return;

  const hx = cx + radius * preset.highlight.centerXRatio;
  const hy = cy + radius * preset.highlight.centerYRatio;
  const hr = Math.max(1, radius * preset.highlight.radiusRatio);
  const stop1 = clamp(preset.highlight.stop1Offset, 0.05, 0.95);

  const gradient = ctx.createRadialGradient(hx, hy, 0, hx, hy, hr);
  gradient.addColorStop(
    0,
    withAlpha(colors.highlight, preset.highlight.globalAlpha * preset.highlight.stop0Alpha)
  );
  gradient.addColorStop(
    stop1,
    withAlpha(colors.highlight, preset.highlight.globalAlpha * preset.highlight.stop1Alpha)
  );
  gradient.addColorStop(
    1,
    withAlpha(colors.highlight, preset.highlight.globalAlpha * preset.highlight.stop2Alpha)
  );

  ctx.save();
  ctx.globalCompositeOperation = preset.highlight.globalCompositeOperation;
  ctx.fillStyle = gradient;
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, TAU);
  ctx.fill();
  ctx.restore();
}

function drawSpecularLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  preset: BubbleStylePreset,
  cx: number,
  cy: number,
  radius: number
): void {
  if (radius < 8) return;

  const sx = cx + radius * preset.highlight.centerXRatio * 0.35;
  const sy = cy + radius * (preset.highlight.centerYRatio * 0.75 - 0.08);
  const sheenRadius = radius * clamp(preset.highlight.radiusRatio * 1.08, 0.34, 0.98);
  const stop = clamp(preset.highlight.stop1Offset * 0.86, 0.12, 0.86);

  const sheen = ctx.createRadialGradient(0, -sheenRadius * 0.18, 0, 0, 0, sheenRadius);
  sheen.addColorStop(0, "rgba(255, 255, 255, 0.98)");
  sheen.addColorStop(stop, "rgba(255, 255, 255, 0.38)");
  sheen.addColorStop(1, "rgba(255, 255, 255, 0)");

  ctx.save();
  ctx.globalCompositeOperation = "screen";
  ctx.globalAlpha = clamp(0.28 + preset.highlight.globalAlpha * 0.52, 0.2, 0.92);
  ctx.translate(sx, sy);
  ctx.scale(1, 0.68);
  ctx.fillStyle = sheen;
  ctx.beginPath();
  ctx.arc(0, 0, sheenRadius, 0, TAU);
  ctx.fill();
  ctx.restore();

  ctx.save();
  ctx.globalCompositeOperation = "screen";
  ctx.strokeStyle = "rgba(255, 255, 255, 0.38)";
  ctx.lineWidth = Math.max(0.9, radius * 0.028);
  ctx.beginPath();
  ctx.arc(cx, cy - radius * 0.03, radius * 0.94, -Math.PI * 0.92, -Math.PI * 0.08);
  ctx.stroke();
  ctx.restore();
}

function drawRimLayer(
  ctx: CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D,
  preset: BubbleStylePreset,
  cx: number,
  cy: number,
  radius: number,
  colors: BubbleColors
): void {
  if (!preset.rim.enabled) return;

  const lineWidth = Math.max(0.8, radius * preset.rim.lineWidthRatio);
  ctx.save();
  ctx.globalCompositeOperation = preset.rim.globalCompositeOperation;
  ctx.strokeStyle = withAlpha(colors.rim, preset.rim.globalAlpha);
  ctx.lineWidth = lineWidth;
  ctx.beginPath();
  ctx.arc(cx, cy, Math.max(0, radius - lineWidth * 0.5), 0, TAU);
  ctx.stroke();
  ctx.restore();
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

function hashString(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return hash;
}

function bucketRadiusByStep(radius: number, step: number): number {
  return Math.max(step, Math.round(radius / step) * step);
}

function alphaFromColor(color: string): number {
  const rgba = parseColor(color);
  if (!rgba) return 1;
  return clamp(rgba.a, 0, 1);
}

function colorLightness(color: string): number {
  const rgba = parseColor(color);
  if (!rgba) return 0.5;
  const r = rgba.r / 255;
  const g = rgba.g / 255;
  const b = rgba.b / 255;
  return clamp((Math.max(r, g, b) + Math.min(r, g, b)) * 0.5, 0, 1);
}

function withAlpha(color: string, alphaMultiplier: number): string {
  const rgba = parseColor(color);
  if (!rgba) return color;
  const nextAlpha = clamp(rgba.a * alphaMultiplier, 0, 1);
  return `rgba(${Math.round(rgba.r)}, ${Math.round(rgba.g)}, ${Math.round(rgba.b)}, ${nextAlpha.toFixed(3)})`;
}

function parseColor(input: string): { r: number; g: number; b: number; a: number } | null {
  const color = input.trim();

  const rgbaMatch = /^rgba?\(\s*([0-9.]+)\s*,\s*([0-9.]+)\s*,\s*([0-9.]+)(?:\s*,\s*([0-9.]+))?\s*\)$/i.exec(
    color
  );
  if (rgbaMatch) {
    const r = Number(rgbaMatch[1]);
    const g = Number(rgbaMatch[2]);
    const b = Number(rgbaMatch[3]);
    const a = rgbaMatch[4] === undefined ? 1 : Number(rgbaMatch[4]);
    if ([r, g, b, a].every(isFiniteNumber)) {
      return {
        r: clamp(r, 0, 255),
        g: clamp(g, 0, 255),
        b: clamp(b, 0, 255),
        a: clamp(a, 0, 1)
      };
    }
  }

  const hexMatch = /^#([0-9a-f]{3}|[0-9a-f]{4}|[0-9a-f]{6}|[0-9a-f]{8})$/i.exec(color);
  if (hexMatch) {
    const hex = hexMatch[1];
    if (hex.length === 3 || hex.length === 4) {
      const r = Number.parseInt(`${hex[0]}${hex[0]}`, 16);
      const g = Number.parseInt(`${hex[1]}${hex[1]}`, 16);
      const b = Number.parseInt(`${hex[2]}${hex[2]}`, 16);
      const a = hex.length === 4 ? Number.parseInt(`${hex[3]}${hex[3]}`, 16) / 255 : 1;
      return { r, g, b, a };
    }
    const r = Number.parseInt(hex.slice(0, 2), 16);
    const g = Number.parseInt(hex.slice(2, 4), 16);
    const b = Number.parseInt(hex.slice(4, 6), 16);
    const a = hex.length === 8 ? Number.parseInt(hex.slice(6, 8), 16) / 255 : 1;
    return { r, g, b, a };
  }

  return null;
}

function normalizeComposite(raw: unknown, fallback: GlobalCompositeOperation): GlobalCompositeOperation {
  if (typeof raw !== "string") return fallback;
  if (COMPOSITE_SET.has(raw as GlobalCompositeOperation)) {
    return raw as GlobalCompositeOperation;
  }
  return fallback;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object") return null;
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function clamp(value: number, min: number, max: number): number {
  if (value < min) return min;
  if (value > max) return max;
  return value;
}
