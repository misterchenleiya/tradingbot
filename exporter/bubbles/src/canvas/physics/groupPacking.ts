const TAU = Math.PI * 2;
const ANGLE_STEP = Math.PI / 18;
const RING_STEP = 2;
const SEARCH_LIMIT_MULTIPLIER = 6;
const SEARCH_MAX_STEPS = 640;

type PackInput = {
  id: string;
  radius: number;
};

export type PackedGroupChild = {
  id: string;
  radius: number;
  x: number;
  y: number;
};

export type GroupPackingResult = {
  children: PackedGroupChild[];
  radius: number;
  padding: number;
};

function clampRadius(value: number): number {
  return Number.isFinite(value) && value > 0 ? value : 1;
}

function seededUnit(seed: string): number {
  let hash = 2166136261;
  for (let i = 0; i < seed.length; i += 1) {
    hash ^= seed.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) / 4294967295;
}

function overlapsAny(
  x: number,
  y: number,
  radius: number,
  placed: PackedGroupChild[]
): boolean {
  for (const node of placed) {
    const dx = x - node.x;
    const dy = y - node.y;
    const limit = radius + node.radius;
    if (dx * dx + dy * dy < limit * limit - 1e-6) {
      return true;
    }
  }
  return false;
}

function requiredRadiusForCenter(
  centerX: number,
  centerY: number,
  placed: PackedGroupChild[]
): number {
  let required = 0;
  for (const node of placed) {
    const distance = Math.hypot(node.x - centerX, node.y - centerY) + node.radius;
    if (distance > required) {
      required = distance;
    }
  }
  return required;
}

function optimizeCenter(placed: PackedGroupChild[]): { x: number; y: number; radius: number } {
  if (placed.length === 0) {
    return { x: 0, y: 0, radius: 0 };
  }

  let centerX = 0;
  let centerY = 0;
  for (const node of placed) {
    centerX += node.x;
    centerY += node.y;
  }
  centerX /= placed.length;
  centerY /= placed.length;

  let bestRadius = requiredRadiusForCenter(centerX, centerY, placed);
  let step = Math.max(4, bestRadius * 0.35);

  for (let iter = 0; iter < 24; iter += 1) {
    let improved = false;
    for (let i = 0; i < 8; i += 1) {
      const angle = (TAU / 8) * i;
      const nextX = centerX + Math.cos(angle) * step;
      const nextY = centerY + Math.sin(angle) * step;
      const nextRadius = requiredRadiusForCenter(nextX, nextY, placed);
      if (nextRadius + 1e-6 < bestRadius) {
        centerX = nextX;
        centerY = nextY;
        bestRadius = nextRadius;
        improved = true;
      }
    }
    if (!improved) {
      step *= 0.5;
      if (step < 0.25) break;
    }
  }

  return { x: centerX, y: centerY, radius: bestRadius };
}

export function computeGroupPadding(maxChildRadius: number): number {
  return Math.max(6, maxChildRadius * 0.14);
}

export function measurePackedChildrenRadius(
  children: Array<{ radius: number; x: number; y: number }>
): number {
  let required = 0;
  for (const node of children) {
    const distance = Math.hypot(node.x, node.y) + node.radius;
    if (distance > required) {
      required = distance;
    }
  }
  return required;
}

export function packGroupChildren(groupId: string, rawItems: PackInput[]): GroupPackingResult {
  const items = rawItems
    .map((item) => ({ id: item.id, radius: clampRadius(item.radius) }))
    .sort((left, right) => right.radius - left.radius || left.id.localeCompare(right.id));

  if (items.length === 0) {
    return { children: [], radius: 0, padding: 0 };
  }

  const placed: PackedGroupChild[] = [];
  let currentExtent = 0;
  const maxChildRadius = items[0].radius;

  for (const item of items) {
    if (placed.length === 0) {
      placed.push({ id: item.id, radius: item.radius, x: 0, y: 0 });
      currentExtent = item.radius;
      continue;
    }

    const angleSeed = seededUnit(`${groupId}|${item.id}`) * TAU;
    const searchLimit = Math.max(
      item.radius * 2,
      currentExtent + item.radius * SEARCH_LIMIT_MULTIPLIER
    );

    let best: { x: number; y: number; score: number; extent: number } | null = null;
    for (let step = 0; step < SEARCH_MAX_STEPS; step += 1) {
      const ring = step * RING_STEP;
      if (ring > searchLimit) break;
      const angle = angleSeed + step * ANGLE_STEP;
      const x = Math.cos(angle) * ring;
      const y = Math.sin(angle) * ring;
      if (overlapsAny(x, y, item.radius, placed)) {
        continue;
      }
      const extent = Math.max(currentExtent, Math.hypot(x, y) + item.radius);
      const score = extent * 1000 + ring;
      if (!best || score < best.score) {
        best = { x, y, score, extent };
      }
      if (ring > currentExtent + item.radius && best) {
        break;
      }
    }

    if (!best) {
      const fallbackRing = currentExtent + item.radius * 1.6;
      best = {
        x: Math.cos(angleSeed) * fallbackRing,
        y: Math.sin(angleSeed) * fallbackRing,
        score: Number.MAX_SAFE_INTEGER,
        extent: Math.max(currentExtent, fallbackRing + item.radius)
      };
    }

    placed.push({ id: item.id, radius: item.radius, x: best.x, y: best.y });
    currentExtent = Math.max(currentExtent, best.extent);
  }

  const optimized = optimizeCenter(placed);
  const normalizedChildren = placed.map((node) => ({
    ...node,
    x: node.x - optimized.x,
    y: node.y - optimized.y
  }));
  const normalizedRadius = requiredRadiusForCenter(0, 0, normalizedChildren);
  const padding = computeGroupPadding(maxChildRadius);

  return {
    children: normalizedChildren,
    radius: normalizedRadius + padding,
    padding
  };
}
