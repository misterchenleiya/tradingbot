import type { BubbleDatum, BubbleUpdate, ColorMetric, SizeMetric } from "../../app/types";
import { EngineDefaults } from "../../app/config";
import { clamp, lerp } from "../../utils/math";
import { SpatialHash } from "./SpatialHash";
import { computeForces } from "./forces";
import { resolveCircleCollision } from "./collision";
import { computeRadiusTargets } from "./metrics";
import {
  computeGroupPadding,
  measurePackedChildrenRadius,
  packGroupChildren
} from "./groupPacking";
import {
  buildDensityGrid,
  computeCentroid,
  type DensityGrid,
  computeMeanVelocity,
  computeQuadrantCounts,
  computeQuadrantImbalanceRatio,
  computeSoftWallForce,
  sampleDensityForce
} from "./distribution";

export type BubbleRuntime = {
  id: string;
  data: BubbleDatum;
  display: {
    price: number;
    marketCap: number;
    volume24h: number;
    change24h: number;
    change7d: number;
  };
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
  targetRadius: number;
  mass: number;
  noisePhase: number;
  isDragged: boolean;
  groupBubbleId?: string;
  groupOffsetX: number;
  groupOffsetY: number;
};

export type GroupBubbleRuntime = {
  id: string;
  groupId: string;
  highSide: 1 | -1;
  motionMode: "free" | "pinned";
  packedRadius: number;
  layoutScale: number;
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
  targetRadius: number;
  mass: number;
  noisePhase: number;
  isDragged: boolean;
  childIds: string[];
  pinnedSlotKey?: PinnedGroupSlotKey;
};

type MotionBody = BubbleRuntime | GroupBubbleRuntime;

type PinnedGroupSlotKey =
  | "top-left"
  | "top-right"
  | "bottom-left"
  | "bottom-right"
  | "top-center"
  | "bottom-center"
  | "center-left"
  | "center-right";

type PreviousGroupChildLayout = {
  groupBubbleId: string;
  x: number;
  y: number;
};

type EngineParams = {
  noiseStrength: number;
  noiseSpeed: number;
  damping: number;
  restitution: number;
  densityGridCols: number;
  densityGridRows: number;
  densityStrength: number;
  softWallStrength: number;
  softWallBandRatio: number;
  maxSpeed: number;
  driftCancelStrength: number;
  centroidReturnStrength: number;
  centroidDeadZoneRatio: number;
  imbalanceCheckInterval: number;
  imbalanceThresholdRatio: number;
  imbalanceBoostMultiplier: number;
  imbalanceBoostDuration: number;
  throwStrength: number;
  explodeStrength: number;
  explodeRadius: number;
  radiusSmoothing: number;
  dataSmoothing: number;
};

const GROUP_BUBBLE_PREFIX = "group-bubble|";
const STABILIZE_ITERATIONS = 6;
const GROUP_LAYOUT_RADIUS_DELTA_PX = 2;
const GROUP_LAYOUT_RADIUS_DELTA_RATIO = 0.06;
const GROUP_STABILIZE_RADIUS_DELTA_PX = 16;
const GROUP_STABILIZE_RADIUS_DELTA_RATIO = 0.14;
const GROUP_GROWTH_SMOOTHING_MULTIPLIER = 1.75;
const GROUP_PINNED_DIAMETER_RATIO = 0.48;
const GROUP_PINNED_SECONDARY_DIAMETER_RATIO = 0.36;
const GROUP_PINNED_CHILDREN_THRESHOLD = 16;
const PINNED_SLOT_GAP_PX = 12;
const PINNED_SLOT_STICKINESS_SCORE = 220;
const PINNED_SLOT_MIN_SCALE = 0;
const PINNED_SLOT_SAFE_PADDING_PX = 8;
const PINNED_SLOT_KEYS: PinnedGroupSlotKey[] = [
  "top-left",
  "top-right",
  "bottom-left",
  "bottom-right",
  "top-center",
  "bottom-center",
  "center-left",
  "center-right"
];
const PINNED_SLOT_ANCHORS: Record<PinnedGroupSlotKey, { xRatio: number; yRatio: number }> = {
  "top-left": { xRatio: 0.24, yRatio: 0.24 },
  "top-right": { xRatio: 0.76, yRatio: 0.24 },
  "bottom-left": { xRatio: 0.24, yRatio: 0.76 },
  "bottom-right": { xRatio: 0.76, yRatio: 0.76 },
  "top-center": { xRatio: 0.5, yRatio: 0.2 },
  "bottom-center": { xRatio: 0.5, yRatio: 0.8 },
  "center-left": { xRatio: 0.2, yRatio: 0.5 },
  "center-right": { xRatio: 0.8, yRatio: 0.5 }
};

function normalizeGroupId(value?: string): string {
  return (value || "").trim().toLowerCase();
}

function resolveEffectiveHighSide(item: BubbleDatum): 1 | -1 | undefined {
  if (typeof item.highSide !== "number" || !Number.isFinite(item.highSide)) {
    return undefined;
  }
  const normalized = Math.trunc(item.highSide);
  if (normalized === 1 || normalized === -1) {
    return normalized;
  }
  return undefined;
}

function buildGroupBubbleId(groupId: string): string {
  return `${GROUP_BUBBLE_PREFIX}${groupId}`;
}

function isPinnedGroup(group: GroupBubbleRuntime): boolean {
  return group.motionMode === "pinned";
}

function isPinnedMotionBody(body: MotionBody): body is GroupBubbleRuntime {
  return "motionMode" in body && body.motionMode === "pinned";
}

function isGroupedBubble(bubble: BubbleRuntime): boolean {
  return typeof bubble.groupBubbleId === "string" && bubble.groupBubbleId.length > 0;
}

function isSignalLikeBubbleData(
  item: Pick<BubbleDatum, "highSide" | "side" | "timeframe" | "strategy">
): boolean {
  return (
    typeof item.highSide === "number" ||
    typeof item.side === "number" ||
    Boolean(item.timeframe) ||
    Boolean(item.strategy)
  );
}

function sameOhlcvBar(
  left?: BubbleDatum["ohlcv"] extends Array<infer T> ? T : never,
  right?: BubbleDatum["ohlcv"] extends Array<infer T> ? T : never
): boolean {
  if (left === right) return true;
  if (!left || !right) return !left && !right;
  return (
    left.ts === right.ts &&
    left.open === right.open &&
    left.high === right.high &&
    left.low === right.low &&
    left.close === right.close &&
    left.volume === right.volume
  );
}

function sameOhlcvSeries(left?: BubbleDatum["ohlcv"], right?: BubbleDatum["ohlcv"]): boolean {
  if (left === right) return true;
  if (!left || !right) return !left && !right;
  if (left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    if (!sameOhlcvBar(left[i], right[i])) return false;
  }
  return true;
}

function exceedsRadiusDelta(prev: number, next: number, px: number, ratio: number): boolean {
  const delta = Math.abs(next - prev);
  const threshold = Math.max(px, Math.max(Math.abs(prev), Math.abs(next)) * ratio);
  return delta > threshold;
}

function seedHash(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export class Engine {
  private bubbles: BubbleRuntime[] = [];
  private bubbleMap = new Map<string, BubbleRuntime>();
  private groupBubbles: GroupBubbleRuntime[] = [];
  private groupBubbleMap = new Map<string, GroupBubbleRuntime>();
  private allData: BubbleDatum[] = [];
  private dataMap = new Map<string, BubbleDatum>();
  private width = 800;
  private height = 600;
  private centerX = 400;
  private centerY = 300;
  private rMin = 10;
  private rMax = 80;
  private baseRadius = 30;
  private maxBubbles = 200;
  private sizeMetric: SizeMetric = "marketCap";
  private colorMetric: ColorMetric = "change24h";
  private readonly spatial: SpatialHash;
  private time = 0;
  private neighbors: number[] = [];
  private readonly params: EngineParams;
  private readonly layoutSeed: number;
  private imbalanceCheckElapsed = 0;
  private imbalanceBoostRemaining = 0;
  private activityScore = 1;
  private densityGrid: DensityGrid | null = null;
  private densityGridReuseRemaining = 0;

  constructor(params?: Partial<EngineParams>) {
    this.params = { ...EngineDefaults, ...params };
    this.spatial = new SpatialHash(this.rMax * 2);
    this.layoutSeed = Math.floor(Math.random() * 0xffffffff) >>> 0;
  }

  getBubbles(): BubbleRuntime[] {
    return this.bubbles;
  }

  getGroupBubbles(): GroupBubbleRuntime[] {
    return this.groupBubbles;
  }

  getColorMetric(): ColorMetric {
    return this.colorMetric;
  }

  getActivityScore(): number {
    return this.activityScore;
  }

  setViewport(width: number, height: number): void {
    this.width = width;
    this.height = height;
    this.centerX = width * 0.5;
    this.centerY = height * 0.5;
    this.densityGrid = null;
    this.densityGridReuseRemaining = 0;
    this.layoutPinnedGroups();
    this.stabilizeActiveBodies();
    this.syncGroupedChildPositions();
  }

  setRadiusRange(rMin: number, rMax: number): void {
    this.rMin = rMin;
    this.rMax = rMax;
    this.baseRadius = clamp(this.baseRadius, this.rMin, this.rMax);
    this.spatial.setCellSize(this.rMax * 2);
    this.refreshRuntimeLayout();
  }

  setBaseRadius(radius: number): void {
    if (!Number.isFinite(radius) || radius <= 0) return;
    this.baseRadius = clamp(radius, this.rMin, this.rMax);
    this.refreshRuntimeLayout();
  }

  setMetrics(sizeMetric: SizeMetric, colorMetric: ColorMetric): void {
    this.sizeMetric = sizeMetric;
    this.colorMetric = colorMetric;
    this.refreshRuntimeLayout();
  }

  setMaxBubbles(count: number): void {
    this.maxBubbles = count;
    this.rebuildBubbles();
  }

  setSnapshot(data: BubbleDatum[]): void {
    this.dataMap = new Map();
    for (const item of data) {
      this.dataMap.set(item.id, item);
    }
    this.allData = Array.from(this.dataMap.values());
    this.sortAllData();
    this.rebuildBubbles();
  }

  applyUpdates(updates: BubbleUpdate[]): void {
    let hasChanges = false;
    let needsResort = false;
    let needsLayoutRefresh = false;
    const touchedIds = new Set<string>();

    for (const update of updates) {
      let existing = this.dataMap.get(update.id);
      let resolvedId = update.id;
      if (!existing && update.symbol) {
        const symbol = update.symbol.toUpperCase();
        for (const [key, value] of this.dataMap.entries()) {
          if (value.symbol.toUpperCase() === symbol) {
            existing = value;
            resolvedId = key;
            break;
          }
        }
      }
      if (!existing) continue;

      const merged = { ...existing, ...update, id: existing.id };
      this.dataMap.set(resolvedId, merged);
      hasChanges = true;
      if (
        existing.rank !== merged.rank ||
        existing.marketCap !== merged.marketCap ||
        existing.symbol !== merged.symbol
      ) {
        needsResort = true;
      }
      if (this.updateAffectsLayout(existing, merged)) {
        needsLayoutRefresh = true;
      }
      const bubble = this.bubbleMap.get(merged.id);
      if (bubble) {
        bubble.data = merged;
      }
      touchedIds.add(merged.id);
    }

    if (!hasChanges) {
      return;
    }
    if (needsResort) {
      this.allData = Array.from(this.dataMap.values());
      this.sortAllData();
      this.rebuildBubbles();
      return;
    }

    if (touchedIds.size > 0) {
      for (let i = 0; i < this.allData.length; i += 1) {
        const id = this.allData[i].id;
        if (!touchedIds.has(id)) continue;
        const latest = this.dataMap.get(id);
        if (latest) {
          this.allData[i] = latest;
        }
      }
    }

    if (needsLayoutRefresh) {
      this.refreshRuntimeLayout();
    }
  }

  resetLayout(): void {
    for (const bubble of this.bubbles) {
      bubble.groupBubbleId = undefined;
      bubble.groupOffsetX = 0;
      bubble.groupOffsetY = 0;
      const spawn = this.randomSpawnPosition(bubble.id, bubble.radius, "reset");
      bubble.x = spawn.x;
      bubble.y = spawn.y;
      bubble.vx = 0;
      bubble.vy = 0;
      bubble.isDragged = false;
    }
    for (const group of this.groupBubbles) {
      const spawn = this.randomSpawnPosition(group.id, group.radius, "reset");
      group.x = spawn.x;
      group.y = spawn.y;
      group.vx = 0;
      group.vy = 0;
      group.isDragged = false;
    }
    this.refreshGroupStructures({ forceRepack: false, forceStabilize: true });
  }

  tick(dt: number): void {
    if (dt <= 0) return;
    this.time += dt;

    const dataSmoothing = 1 - Math.exp(-this.params.dataSmoothing * dt);
    const radiusSmoothing = 1 - Math.exp(-this.params.radiusSmoothing * dt);

    for (const bubble of this.bubbles) {
      const group = bubble.groupBubbleId ? this.groupBubbleMap.get(bubble.groupBubbleId) : undefined;
      const groupScale = group ? group.layoutScale : 1;
      const effectiveTargetRadius = bubble.targetRadius * groupScale;
      bubble.display.price = lerp(bubble.display.price, bubble.data.price, dataSmoothing);
      bubble.display.marketCap = lerp(bubble.display.marketCap, bubble.data.marketCap, dataSmoothing);
      bubble.display.volume24h = lerp(bubble.display.volume24h, bubble.data.volume24h, dataSmoothing);
      bubble.display.change24h = lerp(bubble.display.change24h, bubble.data.change24h, dataSmoothing);
      bubble.display.change7d = lerp(bubble.display.change7d, bubble.data.change7d, dataSmoothing);
      bubble.radius = lerp(bubble.radius, effectiveTargetRadius, radiusSmoothing);
      if (bubble.radius > effectiveTargetRadius) {
        bubble.radius = effectiveTargetRadius;
      }
      bubble.mass = Math.max(1, bubble.radius * bubble.radius);
      if (isGroupedBubble(bubble)) {
        bubble.vx = 0;
        bubble.vy = 0;
        bubble.isDragged = false;
      }
    }
    for (const group of this.groupBubbles) {
      const groupRadiusSmoothing =
        group.targetRadius >= group.radius
          ? 1 -
            Math.exp(
              -(this.params.radiusSmoothing * GROUP_GROWTH_SMOOTHING_MULTIPLIER) * dt
            )
          : radiusSmoothing;
      group.radius = lerp(group.radius, group.targetRadius, groupRadiusSmoothing);
      group.mass = Math.max(1, group.radius * group.radius);
    }

    this.layoutPinnedGroups();

    const activeBodies = this.getActiveBodies();
    const dynamicBodies = activeBodies.filter((body) => !isPinnedMotionBody(body));
    const densityGrid = this.resolveDensityGrid(activeBodies);

    this.imbalanceCheckElapsed += dt;
    if (this.imbalanceCheckElapsed >= this.params.imbalanceCheckInterval) {
      const counts = computeQuadrantCounts(dynamicBodies, this.centerX, this.centerY);
      const imbalanceRatio = computeQuadrantImbalanceRatio(counts);
      if (imbalanceRatio < this.params.imbalanceThresholdRatio) {
        this.imbalanceBoostRemaining = Math.max(
          this.imbalanceBoostRemaining,
          this.params.imbalanceBoostDuration
        );
      }
      this.imbalanceCheckElapsed = 0;
    }
    if (this.imbalanceBoostRemaining > 0) {
      this.imbalanceBoostRemaining = Math.max(0, this.imbalanceBoostRemaining - dt);
    }

    const densityMultiplier =
      this.imbalanceBoostRemaining > 0 ? this.params.imbalanceBoostMultiplier : 1;
    const densityStrength = this.params.densityStrength * densityMultiplier;
    const maxDensityForce = densityStrength * 2.2;
    const maxSpeed = Math.max(20, this.params.maxSpeed);
    let speedSum = 0;
    let speedCount = 0;

    for (const body of dynamicBodies) {
      if (body.isDragged) {
        continue;
      }
      const { ax, ay } = computeForces(body, this.centerX, this.centerY, this.time, {
        noiseStrength: this.params.noiseStrength,
        noiseSpeed: this.params.noiseSpeed
      });
      const density = sampleDensityForce(
        densityGrid,
        body.x,
        body.y,
        densityStrength,
        maxDensityForce
      );
      const wall = computeSoftWallForce(
        body.x,
        body.y,
        body.radius,
        this.width,
        this.height,
        this.params.softWallBandRatio,
        this.params.softWallStrength
      );

      body.vx += (ax + density.ax + wall.ax) * dt;
      body.vy += (ay + density.ay + wall.ay) * dt;

      const damp = Math.exp(-this.params.damping * dt);
      body.vx *= damp;
      body.vy *= damp;
      this.clampVelocity(body, maxSpeed);
      speedSum += Math.hypot(body.vx, body.vy);
      speedCount += 1;

      body.x += body.vx * dt;
      body.y += body.vy * dt;
      this.applyBounds(body);
    }

    if (this.params.driftCancelStrength > 0) {
      const meanVelocity = computeMeanVelocity(activeBodies);
      if (meanVelocity.count > 0) {
        for (const body of dynamicBodies) {
          if (body.isDragged) continue;
          body.vx -= meanVelocity.vx * this.params.driftCancelStrength;
          body.vy -= meanVelocity.vy * this.params.driftCancelStrength;
          this.clampVelocity(body, maxSpeed);
        }
      }
    }

    if (this.params.centroidReturnStrength > 0) {
      const centroid = computeCentroid(activeBodies);
      if (centroid.count > 0) {
        const dx = this.centerX - centroid.x;
        const dy = this.centerY - centroid.y;
        const distance = Math.hypot(dx, dy);
        const sceneScale = Math.max(1, Math.min(this.width, this.height));
        const offsetRatio = distance / sceneScale;
        const deadZone = clamp(this.params.centroidDeadZoneRatio, 0, 0.45);
        if (offsetRatio > deadZone && distance > 1e-6) {
          const ratio = clamp((offsetRatio - deadZone) / Math.max(1e-6, 1 - deadZone), 0, 1);
          const nx = dx / distance;
          const ny = dy / distance;
          const pull = this.params.centroidReturnStrength * ratio * dt;
          for (const body of dynamicBodies) {
            if (body.isDragged) continue;
            body.vx += nx * pull;
            body.vy += ny * pull;
            this.clampVelocity(body, maxSpeed);
          }
        }
      }
    }

    const collisionPairs = this.solveCollisions(activeBodies);
    this.syncGroupedChildPositions();

    const meanSpeed = speedCount > 0 ? speedSum / speedCount : 0;
    const speedScore = clamp(meanSpeed / Math.max(20, maxSpeed * 0.35), 0, 1);
    const collisionScore = clamp(collisionPairs / Math.max(1, activeBodies.length * 0.45), 0, 1);
    const targetActivity = clamp(speedScore * 0.7 + collisionScore * 0.3, 0, 1);
    const activitySmoothing = 1 - Math.exp(-5 * dt);
    this.activityScore = lerp(this.activityScore, targetActivity, activitySmoothing);
  }

  explode(x: number, y: number): void {
    const radius = this.params.explodeRadius;
    const strength = this.params.explodeStrength;
    for (const body of this.getActiveBodies()) {
      const dx = body.x - x;
      const dy = body.y - y;
      const dist = Math.hypot(dx, dy);
      if (dist > radius || dist === 0) continue;
      const force = strength / Math.max(dist, 12);
      const nx = dx / dist;
      const ny = dy / dist;
      body.vx += nx * force;
      body.vy += ny * force;
    }
  }

  pick(x: number, y: number): BubbleRuntime | null {
    let best: BubbleRuntime | null = null;
    let minDist = Infinity;
    for (const bubble of this.bubbles) {
      const dx = x - bubble.x;
      const dy = y - bubble.y;
      const dist = Math.hypot(dx, dy);
      if (dist <= bubble.radius && dist < minDist) {
        minDist = dist;
        best = bubble;
      }
    }
    return best;
  }

  pickGroup(x: number, y: number): GroupBubbleRuntime | null {
    let best: GroupBubbleRuntime | null = null;
    let minDist = Infinity;
    for (const group of this.groupBubbles) {
      const dx = x - group.x;
      const dy = y - group.y;
      const dist = Math.hypot(dx, dy);
      if (dist <= group.radius && dist < minDist) {
        minDist = dist;
        best = group;
      }
    }
    return best;
  }

  focus(id: string): void {
    const bubble = this.bubbleMap.get(id);
    if (!bubble) return;
    const group = bubble.groupBubbleId ? this.groupBubbleMap.get(bubble.groupBubbleId) : undefined;
    const target = group ?? bubble;
    target.x = this.centerX;
    target.y = this.centerY;
    target.vx = 0;
    target.vy = 0;
    this.syncGroupedChildPositions();
  }

  setDragged(bubble: BubbleRuntime | null, dragged: boolean): void {
    if (!bubble) return;
    const group = bubble.groupBubbleId ? this.groupBubbleMap.get(bubble.groupBubbleId) : undefined;
    if (group) {
      if (isPinnedGroup(group)) {
        return;
      }
      group.isDragged = dragged;
      return;
    }
    bubble.isDragged = dragged;
  }

  setDraggedPosition(bubble: BubbleRuntime, x: number, y: number): void {
    const group = bubble.groupBubbleId ? this.groupBubbleMap.get(bubble.groupBubbleId) : undefined;
    if (group && isPinnedGroup(group)) {
      return;
    }
    const target = group ?? bubble;
    target.x = clamp(x, target.radius, this.width - target.radius);
    target.y = clamp(y, target.radius, this.height - target.radius);
    this.syncGroupedChildPositions();
  }

  applyThrow(bubble: BubbleRuntime, vx: number, vy: number): void {
    const group = bubble.groupBubbleId ? this.groupBubbleMap.get(bubble.groupBubbleId) : undefined;
    if (group && isPinnedGroup(group)) {
      return;
    }
    const target = group ?? bubble;
    target.vx = vx * this.params.throwStrength;
    target.vy = vy * this.params.throwStrength;
  }

  private sortAllData(): void {
    this.allData.sort((a, b) => {
      const rankA = a.rank || 0;
      const rankB = b.rank || 0;
      if (rankA && rankB && rankA !== rankB) return rankA - rankB;
      const capDiff = b.marketCap - a.marketCap;
      if (capDiff !== 0) return capDiff;
      const symbolDiff = a.symbol.localeCompare(b.symbol);
      if (symbolDiff !== 0) return symbolDiff;
      return a.id.localeCompare(b.id);
    });
  }

  private rebuildBubbles(): void {
    const visible = this.allData.slice(0, this.maxBubbles);
    const radiusTargets = computeRadiusTargets(
      visible,
      this.sizeMetric,
      this.rMin,
      this.rMax,
      this.baseRadius
    );

    const next: BubbleRuntime[] = [];
    const nextMap = new Map<string, BubbleRuntime>();

    for (const item of visible) {
      const existing = this.bubbleMap.get(item.id);
      if (existing) {
        existing.data = item;
        existing.targetRadius = radiusTargets.get(item.id) ?? existing.targetRadius;
        existing.groupBubbleId = undefined;
        existing.groupOffsetX = 0;
        existing.groupOffsetY = 0;
        next.push(existing);
        nextMap.set(item.id, existing);
        continue;
      }

      const targetRadius = radiusTargets.get(item.id) ?? this.rMin;
      const rvx = this.seededUnit(item.id, "vx");
      const rvy = this.seededUnit(item.id, "vy");
      const rphase = this.seededUnit(item.id, "phase");
      const spawn = this.randomSpawnPosition(item.id, targetRadius, "spawn");
      const bubble: BubbleRuntime = {
        id: item.id,
        data: item,
        display: {
          price: item.price,
          marketCap: item.marketCap,
          volume24h: item.volume24h,
          change24h: item.change24h,
          change7d: item.change7d
        },
        x: spawn.x,
        y: spawn.y,
        vx: (rvx - 0.5) * 8,
        vy: (rvy - 0.5) * 8,
        radius: targetRadius,
        targetRadius,
        mass: Math.max(1, targetRadius * targetRadius),
        noisePhase: rphase * Math.PI * 2,
        isDragged: false,
        groupOffsetX: 0,
        groupOffsetY: 0
      };
      next.push(bubble);
      nextMap.set(item.id, bubble);
    }

    this.bubbles = next;
    this.bubbleMap = nextMap;
    this.refreshGroupStructures({ forceRepack: true, forceStabilize: true });
    this.densityGridReuseRemaining = 0;
  }

  private refreshRuntimeLayout(): void {
    if (this.bubbles.length === 0) return;
    const radiusTargets = computeRadiusTargets(
      this.bubbles.map((bubble) => bubble.data),
      this.sizeMetric,
      this.rMin,
      this.rMax,
      this.baseRadius
    );
    for (const bubble of this.bubbles) {
      const target = radiusTargets.get(bubble.id);
      if (target !== undefined) {
        bubble.targetRadius = target;
      }
    }
    this.refreshGroupStructures({ forceRepack: false, forceStabilize: false });
  }

  private refreshGroupStructures(options?: {
    forceRepack?: boolean;
    forceStabilize?: boolean;
  }): void {
    const forceRepack = options?.forceRepack === true;
    const forceStabilize = options?.forceStabilize === true;
    const previousGroupMap = this.groupBubbleMap;
    const previousBubbleParents = new Map<string, string>();
    const previousChildLayouts = new Map<string, PreviousGroupChildLayout>();
    for (const bubble of this.bubbles) {
      if (bubble.groupBubbleId) {
        previousBubbleParents.set(bubble.id, bubble.groupBubbleId);
        previousChildLayouts.set(bubble.id, {
          groupBubbleId: bubble.groupBubbleId,
          x: bubble.groupOffsetX,
          y: bubble.groupOffsetY
        });
      }
      bubble.groupBubbleId = undefined;
      bubble.groupOffsetX = 0;
      bubble.groupOffsetY = 0;
    }

    const buckets = new Map<string, BubbleRuntime[]>();
    const groupSides = new Map<string, 1 | -1>();
    const invalidGroups = new Set<string>();

    for (const bubble of this.bubbles) {
      const groupId = normalizeGroupId(bubble.data.groupId);
      const highSide = resolveEffectiveHighSide(bubble.data);
      if (!groupId || !highSide) continue;
      const existingSide = groupSides.get(groupId);
      if (existingSide && existingSide !== highSide) {
        invalidGroups.add(groupId);
        continue;
      }
      groupSides.set(groupId, highSide);
      const bucket = buckets.get(groupId);
      if (bucket) {
        bucket.push(bubble);
      } else {
        buckets.set(groupId, [bubble]);
      }
    }

    for (const invalidGroupId of invalidGroups) {
      buckets.delete(invalidGroupId);
      groupSides.delete(invalidGroupId);
    }

    const nextGroups: GroupBubbleRuntime[] = [];
    const nextGroupMap = new Map<string, GroupBubbleRuntime>();
    let shouldStabilize = forceStabilize || previousGroupMap.size !== buckets.size;

    for (const [groupId, children] of buckets.entries()) {
      if (children.length < 2) continue;
      const highSide = groupSides.get(groupId);
      if (highSide !== 1 && highSide !== -1) continue;

      const groupRuntimeId = buildGroupBubbleId(groupId);
      const previousGroup = previousGroupMap.get(groupRuntimeId);
      const previousTargetRadius = previousGroup?.targetRadius ?? previousGroup?.radius;
      const canReuseLayout =
        !forceRepack &&
        this.canReuseGroupLayout(groupRuntimeId, previousGroup, children, previousChildLayouts);
      const packInput = children.map((bubble) => ({
        id: bubble.id,
        radius: this.resolveGroupChildRadius(bubble)
      }));
      const packing = canReuseLayout
        ? this.buildReusedGroupPacking(children, previousChildLayouts)
        : packGroupChildren(groupId, packInput);
      const center = previousGroup
        ? { x: previousGroup.x, y: previousGroup.y }
        : this.computeBubbleCentroid(children);
      const velocity = previousGroup
        ? { vx: previousGroup.vx, vy: previousGroup.vy }
        : this.computeBubbleVelocity(children);
      const noisePhase = previousGroup
        ? previousGroup.noisePhase
        : this.seededUnit(groupRuntimeId, "phase") * Math.PI * 2;
      const targetRadius = packing.radius;

      const group: GroupBubbleRuntime = previousGroup
        ? previousGroup
        : {
            id: groupRuntimeId,
            groupId,
            highSide,
            motionMode: "free",
            packedRadius: targetRadius,
            layoutScale: 1,
            x: center.x,
            y: center.y,
            vx: velocity.vx,
            vy: velocity.vy,
            radius: targetRadius,
            targetRadius,
            mass: Math.max(1, targetRadius * targetRadius),
            noisePhase,
            isDragged: false,
            childIds: []
      };

      group.groupId = groupId;
      group.highSide = highSide;
      group.motionMode = previousGroup?.motionMode ?? "free";
      group.packedRadius = targetRadius;
      group.layoutScale = previousGroup?.layoutScale ?? 1;
      group.x = center.x;
      group.y = center.y;
      group.vx = velocity.vx;
      group.vy = velocity.vy;
      group.targetRadius = targetRadius * group.layoutScale;
      const currentRadiusFloor = this.measureCurrentGroupRadiusFloor(
        children,
        packing.children,
        previousGroup?.layoutScale ?? 1
      );
      if (!previousGroup) {
        group.radius = group.targetRadius;
      } else if (group.radius < currentRadiusFloor) {
        group.radius = currentRadiusFloor;
      }
      group.mass = Math.max(1, group.radius * group.radius);
      group.childIds = packing.children.map((item) => item.id);
      nextGroups.push(group);
      nextGroupMap.set(group.id, group);

      if (!previousGroup) {
        shouldStabilize = true;
      } else if (
        !canReuseLayout ||
        exceedsRadiusDelta(
          previousTargetRadius ?? targetRadius,
          targetRadius,
          GROUP_STABILIZE_RADIUS_DELTA_PX,
          GROUP_STABILIZE_RADIUS_DELTA_RATIO
        )
      ) {
        shouldStabilize = true;
      }

      const packedOffsets = new Map(packing.children.map((item) => [item.id, item]));
      for (const bubble of children) {
        const packed = packedOffsets.get(bubble.id);
        if (!packed) continue;
        bubble.groupBubbleId = group.id;
        bubble.groupOffsetX = packed.x;
        bubble.groupOffsetY = packed.y;
        bubble.vx = 0;
        bubble.vy = 0;
      }
    }

    this.groupBubbles = nextGroups;
    this.groupBubbleMap = nextGroupMap;
    if (this.updateGroupMotionModes()) {
      shouldStabilize = true;
    }

    for (const bubble of this.bubbles) {
      if (bubble.groupBubbleId) continue;
      const previousParentId = previousBubbleParents.get(bubble.id);
      if (!previousParentId) continue;
      const previousParent = previousGroupMap.get(previousParentId);
      if (!previousParent) continue;
      bubble.vx = previousParent.vx;
      bubble.vy = previousParent.vy;
    }

    this.syncGroupedChildPositions();
    if (shouldStabilize) {
      this.stabilizeActiveBodies();
    }
    this.syncGroupedChildPositions();
    this.densityGridReuseRemaining = 0;
  }

  private solveCollisions(activeBodies: MotionBody[]): number {
    let maxCollisionRadius = this.rMax;
    for (const body of activeBodies) {
      if (body.radius > maxCollisionRadius) {
        maxCollisionRadius = body.radius;
      }
    }
    this.spatial.setCellSize(Math.max(this.rMax * 2, maxCollisionRadius * 2));
    this.spatial.clear();
    for (let i = 0; i < activeBodies.length; i += 1) {
      const body = activeBodies[i];
      this.spatial.insert(i, body.x, body.y);
    }

    let collisionPairs = 0;
    for (let i = 0; i < activeBodies.length; i += 1) {
      const body = activeBodies[i];
      const neighbors = this.spatial.query(body.x, body.y, this.neighbors);
      for (let n = 0; n < neighbors.length; n += 1) {
        const j = neighbors[n];
        if (j <= i) continue;
        const other = activeBodies[j];
        const dx = other.x - body.x;
        const dy = other.y - body.y;
        const radiusSum = body.radius + other.radius;
        if (dx * dx + dy * dy >= radiusSum * radiusSum) continue;
        const staticBody = isPinnedMotionBody(body);
        const staticOther = isPinnedMotionBody(other);
        if (staticBody && staticOther) {
          continue;
        }
        collisionPairs += 1;
        this.resolveBodyCollision(body, other, this.params.restitution, staticBody, staticOther);
      }
    }
    return collisionPairs;
  }

  private stabilizeActiveBodies(): void {
    const activeBodies = this.getActiveBodies();
    if (activeBodies.length <= 1) {
      return;
    }

    for (let iter = 0; iter < STABILIZE_ITERATIONS; iter += 1) {
      let changed = false;
      for (let i = 0; i < activeBodies.length; i += 1) {
        const body = activeBodies[i];
        for (let j = i + 1; j < activeBodies.length; j += 1) {
          const other = activeBodies[j];
          const dx = other.x - body.x;
          const dy = other.y - body.y;
          const radiusSum = body.radius + other.radius;
          if (dx * dx + dy * dy >= radiusSum * radiusSum) continue;
          const staticBody = isPinnedMotionBody(body);
          const staticOther = isPinnedMotionBody(other);
          if (staticBody && staticOther) {
            continue;
          }
          this.resolveBodyCollision(body, other, 0, staticBody, staticOther);
          changed = true;
        }
        if (isPinnedMotionBody(body)) {
          continue;
        }
        this.applyBounds(body);
      }
      if (!changed) {
        break;
      }
    }
  }

  private getActiveBodies(): MotionBody[] {
    const out: MotionBody[] = [];
    for (const group of this.groupBubbles) {
      out.push(group);
    }
    for (const bubble of this.bubbles) {
      if (!isGroupedBubble(bubble)) {
        out.push(bubble);
      }
    }
    return out;
  }

  private syncGroupedChildPositions(): void {
    for (const bubble of this.bubbles) {
      if (!bubble.groupBubbleId) continue;
      const group = this.groupBubbleMap.get(bubble.groupBubbleId);
      if (!group) {
        bubble.groupBubbleId = undefined;
        bubble.groupOffsetX = 0;
        bubble.groupOffsetY = 0;
        continue;
      }
      bubble.x = group.x + bubble.groupOffsetX * group.layoutScale;
      bubble.y = group.y + bubble.groupOffsetY * group.layoutScale;
    }
  }

  private updateAffectsLayout(previous: BubbleDatum, next: BubbleDatum): boolean {
    if (normalizeGroupId(previous.groupId) !== normalizeGroupId(next.groupId)) {
      return true;
    }
    if (resolveEffectiveHighSide(previous) !== resolveEffectiveHighSide(next)) {
      return true;
    }
    if (
      previous.timeframe !== next.timeframe ||
      previous.strategy !== next.strategy ||
      previous.strategyVersion !== next.strategyVersion
    ) {
      return true;
    }

    const signalLike = isSignalLikeBubbleData(previous) || isSignalLikeBubbleData(next);
    if (signalLike) {
      return !sameOhlcvSeries(previous.ohlcv, next.ohlcv);
    }

    return (
      previous.marketCap !== next.marketCap ||
      previous.volume24h !== next.volume24h ||
      previous.price !== next.price ||
      previous.rank !== next.rank
    );
  }

  private resolveDensityGrid(activeBodies: MotionBody[]): DensityGrid {
    const reuseSteps = this.resolveDensityReuseSteps();
    if (!this.densityGrid || this.densityGridReuseRemaining <= 0) {
      this.densityGrid = buildDensityGrid(
        activeBodies,
        this.width,
        this.height,
        this.params.densityGridCols,
        this.params.densityGridRows
      );
      this.densityGridReuseRemaining = reuseSteps;
      return this.densityGrid;
    }
    this.densityGridReuseRemaining -= 1;
    return this.densityGrid;
  }

  private resolveDensityReuseSteps(): number {
    if (this.activityScore >= 0.45) return 0;
    if (this.activityScore >= 0.25) return 1;
    return 2;
  }

  private applyBounds(body: MotionBody): void {
    const oversizeX = body.radius * 2 >= this.width;
    const oversizeY = body.radius * 2 >= this.height;

    if (oversizeX) {
      body.x = this.width * 0.5;
      body.vx = 0;
    } else if (body.x - body.radius < 0) {
      body.x = body.radius;
      body.vx = -body.vx * this.params.restitution;
    } else if (body.x + body.radius > this.width) {
      body.x = this.width - body.radius;
      body.vx = -body.vx * this.params.restitution;
    }

    if (oversizeY) {
      body.y = this.height * 0.5;
      body.vy = 0;
    } else if (body.y - body.radius < 0) {
      body.y = body.radius;
      body.vy = -body.vy * this.params.restitution;
    } else if (body.y + body.radius > this.height) {
      body.y = this.height - body.radius;
      body.vy = -body.vy * this.params.restitution;
    }
  }

  private clampVelocity(body: MotionBody, maxSpeed: number): void {
    const safeMax = Math.max(1, maxSpeed);
    const speed = Math.hypot(body.vx, body.vy);
    if (speed <= safeMax || speed <= 0) return;
    const scale = safeMax / speed;
    body.vx *= scale;
    body.vy *= scale;
  }

  private resolveBodyCollision(
    body: MotionBody,
    other: MotionBody,
    restitution: number,
    staticBody: boolean,
    staticOther: boolean
  ): void {
    const mutableBody = body as MotionBody & { isStatic?: boolean };
    const mutableOther = other as MotionBody & { isStatic?: boolean };
    mutableBody.isStatic = staticBody;
    mutableOther.isStatic = staticOther;
    resolveCircleCollision(mutableBody, mutableOther, restitution);
    delete mutableBody.isStatic;
    delete mutableOther.isStatic;
  }

  private seededUnit(id: string, salt: string): number {
    return seedHash(`${this.layoutSeed}|${salt}|${id}`) / 4294967295;
  }

  private updateGroupMotionModes(): boolean {
    let changed = false;
    for (const group of this.groupBubbles) {
      const shouldPin = this.shouldPinGroup(group);
      const nextMode = shouldPin ? "pinned" : "free";
      if (group.motionMode !== nextMode) {
        group.motionMode = nextMode;
        changed = true;
      }
      if (nextMode === "free" && group.pinnedSlotKey) {
        group.pinnedSlotKey = undefined;
        changed = true;
      }
      if (nextMode === "free" && group.layoutScale !== 1) {
        group.layoutScale = 1;
        changed = true;
      }
      if (nextMode === "free" && group.targetRadius !== group.packedRadius) {
        group.targetRadius = group.packedRadius;
        changed = true;
      }
      if (nextMode === "free") {
        group.mass = Math.max(1, group.radius * group.radius);
      }
    }
    if (this.layoutPinnedGroups()) {
      changed = true;
    }
    return changed;
  }

  private shouldPinGroup(group: GroupBubbleRuntime): boolean {
    const safeMinSide = Math.max(1, Math.min(this.width, this.height));
    const diameter = group.packedRadius * 2;
    if (diameter >= safeMinSide * GROUP_PINNED_DIAMETER_RATIO) {
      return true;
    }
    return (
      group.childIds.length >= GROUP_PINNED_CHILDREN_THRESHOLD &&
      diameter >= safeMinSide * GROUP_PINNED_SECONDARY_DIAMETER_RATIO
    );
  }

  private layoutPinnedGroups(): boolean {
    const pinnedGroups = this.groupBubbles
      .filter(isPinnedGroup)
      .sort(
        (left, right) =>
          this.resolvePinnedPlacementRadius(right) -
            this.resolvePinnedPlacementRadius(left) ||
          left.id.localeCompare(right.id)
      );
    if (pinnedGroups.length === 0) {
      return false;
    }

    const assigned: Array<{ key: PinnedGroupSlotKey; x: number; y: number; radius: number }> = [];
    let changed = false;

    for (const group of pinnedGroups) {
      const packedRadius = this.resolvePinnedPlacementRadius(group);
      const slotKeys = this.resolvePinnedSlotOrder(group.pinnedSlotKey);
      let best:
        | {
            key: PinnedGroupSlotKey;
            x: number;
            y: number;
            radius: number;
            scale: number;
            score: number;
          }
        | undefined;

      for (const key of slotKeys) {
        const placement = this.resolvePinnedSlotPlacement(key);
        const allowedRadius = this.resolvePinnedSlotAllowedRadius(placement, assigned);
        const scale = clamp(allowedRadius / Math.max(1, packedRadius), PINNED_SLOT_MIN_SCALE, 1);
        const radius = packedRadius * scale;
        let score = Math.hypot(placement.x - group.x, placement.y - group.y);
        score += (1 - scale) * 100000;
        for (const other of assigned) {
          const distance = Math.hypot(placement.x - other.x, placement.y - other.y);
          const minDistance = radius + other.radius + PINNED_SLOT_GAP_PX;
          const overlap = Math.max(0, minDistance - distance);
          score += overlap * overlap * 1000;
        }
        if (group.pinnedSlotKey === key) {
          score -= PINNED_SLOT_STICKINESS_SCORE;
        }
        if (!best || score < best.score) {
          best = { key, x: placement.x, y: placement.y, radius, scale, score };
        }
      }

      if (!best) {
        continue;
      }

      if (
        group.pinnedSlotKey !== best.key ||
        Math.abs(group.x - best.x) > 1e-6 ||
        Math.abs(group.y - best.y) > 1e-6 ||
        Math.abs(group.layoutScale - best.scale) > 1e-6 ||
        Math.abs(group.targetRadius - best.radius) > 1e-6 ||
        group.vx !== 0 ||
        group.vy !== 0 ||
        group.isDragged
      ) {
        changed = true;
      }
      group.pinnedSlotKey = best.key;
      group.layoutScale = best.scale;
      group.targetRadius = best.radius;
      if (group.radius > group.targetRadius) {
        group.radius = group.targetRadius;
      }
      group.mass = Math.max(1, group.radius * group.radius);
      group.x = best.x;
      group.y = best.y;
      group.vx = 0;
      group.vy = 0;
      group.isDragged = false;
      this.applyPinnedChildScale(group);
      assigned.push({ key: best.key, x: best.x, y: best.y, radius: best.radius });
    }

    return changed;
  }

  private resolvePinnedPlacementRadius(group: GroupBubbleRuntime): number {
    return Math.max(group.packedRadius, group.targetRadius / Math.max(group.layoutScale, 1e-6));
  }

  private resolvePinnedSlotOrder(previousKey?: PinnedGroupSlotKey): PinnedGroupSlotKey[] {
    if (!previousKey) {
      return PINNED_SLOT_KEYS;
    }
    return [previousKey, ...PINNED_SLOT_KEYS.filter((key) => key !== previousKey)];
  }

  private resolvePinnedSlotPlacement(
    key: PinnedGroupSlotKey
  ): { x: number; y: number } {
    const anchor = PINNED_SLOT_ANCHORS[key];
    switch (key) {
      case "top-left":
      case "top-right":
      case "bottom-left":
      case "bottom-right":
      case "top-center":
      case "bottom-center":
      case "center-left":
      case "center-right":
        return {
          x: this.width * anchor.xRatio,
          y: this.height * anchor.yRatio
        };
    }
  }

  private resolvePinnedSlotAllowedRadius(
    placement: { x: number; y: number },
    assigned: Array<{ x: number; y: number; radius: number }>
  ): number {
    let allowed = Math.min(
      placement.x,
      this.width - placement.x,
      placement.y,
      this.height - placement.y
    ) - PINNED_SLOT_SAFE_PADDING_PX;
    for (const other of assigned) {
      const distance = Math.hypot(placement.x - other.x, placement.y - other.y);
      allowed = Math.min(allowed, distance - other.radius - PINNED_SLOT_GAP_PX);
    }
    return Math.max(1, allowed);
  }

  private applyPinnedChildScale(group: GroupBubbleRuntime): void {
    for (const childId of group.childIds) {
      const bubble = this.bubbleMap.get(childId);
      if (!bubble) continue;
      const targetRadius = bubble.targetRadius * group.layoutScale;
      if (bubble.radius > targetRadius) {
        bubble.radius = targetRadius;
      }
      bubble.mass = Math.max(1, bubble.radius * bubble.radius);
    }
  }

  private randomSpawnPosition(id: string, radius: number, salt: string): { x: number; y: number } {
    const safeRadius = Math.max(1, radius);
    const minX = safeRadius;
    const maxX = Math.max(minX, this.width - safeRadius);
    const minY = safeRadius;
    const maxY = Math.max(minY, this.height - safeRadius);
    const x = lerp(minX, maxX, this.seededUnit(id, `${salt}-x`));
    const y = lerp(minY, maxY, this.seededUnit(id, `${salt}-y`));
    return { x, y };
  }

  private computeBubbleCentroid(bubbles: BubbleRuntime[]): { x: number; y: number } {
    if (bubbles.length === 0) {
      return { x: this.centerX, y: this.centerY };
    }
    let x = 0;
    let y = 0;
    for (const bubble of bubbles) {
      x += bubble.x;
      y += bubble.y;
    }
    return {
      x: clamp(x / bubbles.length, 0, this.width),
      y: clamp(y / bubbles.length, 0, this.height)
    };
  }

  private computeBubbleVelocity(bubbles: BubbleRuntime[]): { vx: number; vy: number } {
    if (bubbles.length === 0) {
      return { vx: 0, vy: 0 };
    }
    let vx = 0;
    let vy = 0;
    for (const bubble of bubbles) {
      vx += bubble.vx;
      vy += bubble.vy;
    }
    return {
      vx: vx / bubbles.length,
      vy: vy / bubbles.length
    };
  }

  private resolveGroupChildRadius(bubble: BubbleRuntime): number {
    const group = bubble.groupBubbleId ? this.groupBubbleMap.get(bubble.groupBubbleId) : undefined;
    const scale = group?.layoutScale ?? 1;
    const currentUnscaledRadius = scale > 0 ? bubble.radius / scale : bubble.radius;
    return Math.max(currentUnscaledRadius, bubble.targetRadius);
  }

  private canReuseGroupLayout(
    groupRuntimeId: string,
    previousGroup: GroupBubbleRuntime | undefined,
    children: BubbleRuntime[],
    previousChildLayouts: Map<string, PreviousGroupChildLayout>
  ): boolean {
    if (!previousGroup) {
      return false;
    }
    if (previousGroup.childIds.length !== children.length) {
      return false;
    }

    const currentChildIds = new Set(children.map((bubble) => bubble.id));
    for (const childId of previousGroup.childIds) {
      if (!currentChildIds.has(childId)) {
        return false;
      }
    }

    const reused = children.map((bubble) => {
      const previousLayout = previousChildLayouts.get(bubble.id);
      if (!previousLayout || previousLayout.groupBubbleId !== groupRuntimeId) {
        return null;
      }
      return {
        x: previousLayout.x,
        y: previousLayout.y,
        radius: this.resolveGroupChildRadius(bubble)
      };
    });
    if (reused.some((item) => !item)) {
      return false;
    }

    return !this.hasPackedChildOverlap(
      reused as Array<{ radius: number; x: number; y: number }>
    );
  }

  private buildReusedGroupPacking(
    children: BubbleRuntime[],
    previousChildLayouts: Map<string, PreviousGroupChildLayout>
  ) {
    let maxChildRadius = 0;
    const packed = children.map((bubble) => {
      const previousLayout = previousChildLayouts.get(bubble.id);
      const radius = this.resolveGroupChildRadius(bubble);
      maxChildRadius = Math.max(maxChildRadius, radius);
      return {
        id: bubble.id,
        radius,
        x: previousLayout?.x ?? 0,
        y: previousLayout?.y ?? 0
      };
    });
    const padding = computeGroupPadding(maxChildRadius);
    return {
      children: packed,
      radius: measurePackedChildrenRadius(packed) + padding,
      padding
    };
  }

  private measureCurrentGroupRadiusFloor(
    children: BubbleRuntime[],
    packedChildren: Array<{ id: string; x: number; y: number }>,
    scale: number
  ): number {
    const childMap = new Map(children.map((bubble) => [bubble.id, bubble]));
    const currentPacked = packedChildren.map((item) => {
      const bubble = childMap.get(item.id);
      return {
        x: item.x * scale,
        y: item.y * scale,
        radius: bubble ? Math.max(1, bubble.radius) : 1
      };
    });
    let maxChildRadius = 0;
    for (const child of currentPacked) {
      maxChildRadius = Math.max(maxChildRadius, child.radius);
    }
    return measurePackedChildrenRadius(currentPacked) + computeGroupPadding(maxChildRadius);
  }

  private hasPackedChildOverlap(children: Array<{ radius: number; x: number; y: number }>): boolean {
    for (let i = 0; i < children.length; i += 1) {
      const current = children[i];
      for (let j = i + 1; j < children.length; j += 1) {
        const other = children[j];
        const dx = current.x - other.x;
        const dy = current.y - other.y;
        const minDistance = current.radius + other.radius;
        if (dx * dx + dy * dy < minDistance * minDistance - 1e-6) {
          return true;
        }
      }
    }
    return false;
  }
}
