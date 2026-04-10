import { clamp } from "../../utils/math";

export type DensitySource = {
  x: number;
  y: number;
  radius: number;
};

export type PositionSource = {
  x: number;
  y: number;
  isDragged?: boolean;
};

export type VelocitySource = {
  vx: number;
  vy: number;
  isDragged?: boolean;
};

export type DensityGrid = {
  cols: number;
  rows: number;
  cellWidth: number;
  cellHeight: number;
  values: Float32Array;
};

export type DensityForce = {
  ax: number;
  ay: number;
  density: number;
};

const MIN_GRID_SIZE = 2;

function gridIndex(cols: number, x: number, y: number): number {
  return y * cols + x;
}

function clampCell(value: number, maxExclusive: number): number {
  return Math.min(maxExclusive - 1, Math.max(0, value));
}

function sampleGridNearest(grid: DensityGrid, cellX: number, cellY: number): number {
  const x = clampCell(cellX, grid.cols);
  const y = clampCell(cellY, grid.rows);
  return grid.values[gridIndex(grid.cols, x, y)] || 0;
}

export function buildDensityGrid(
  points: DensitySource[],
  width: number,
  height: number,
  cols: number,
  rows: number
): DensityGrid {
  const safeCols = Math.max(MIN_GRID_SIZE, Math.trunc(cols));
  const safeRows = Math.max(MIN_GRID_SIZE, Math.trunc(rows));
  const safeWidth = Math.max(1, width);
  const safeHeight = Math.max(1, height);
  const cellWidth = safeWidth / safeCols;
  const cellHeight = safeHeight / safeRows;
  const raw = new Float32Array(safeCols * safeRows);

  for (const point of points) {
    if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) continue;
    const weight = Math.max(1, point.radius * point.radius * 0.0025);
    const gx = clamp(point.x / cellWidth - 0.5, 0, safeCols - 1);
    const gy = clamp(point.y / cellHeight - 0.5, 0, safeRows - 1);
    const x0 = clampCell(Math.floor(gx), safeCols);
    const y0 = clampCell(Math.floor(gy), safeRows);
    const x1 = clampCell(x0 + 1, safeCols);
    const y1 = clampCell(y0 + 1, safeRows);
    const tx = gx - x0;
    const ty = gy - y0;

    raw[gridIndex(safeCols, x0, y0)] += weight * (1 - tx) * (1 - ty);
    raw[gridIndex(safeCols, x1, y0)] += weight * tx * (1 - ty);
    raw[gridIndex(safeCols, x0, y1)] += weight * (1 - tx) * ty;
    raw[gridIndex(safeCols, x1, y1)] += weight * tx * ty;
  }

  // 轻量平滑，避免密度梯度过于尖锐导致局部抖动。
  const smoothed = new Float32Array(raw.length);
  for (let y = 0; y < safeRows; y += 1) {
    for (let x = 0; x < safeCols; x += 1) {
      let sum = 0;
      let weightSum = 0;
      for (let oy = -1; oy <= 1; oy += 1) {
        for (let ox = -1; ox <= 1; ox += 1) {
          const nx = clampCell(x + ox, safeCols);
          const ny = clampCell(y + oy, safeRows);
          const kernel = ox === 0 && oy === 0 ? 4 : ox === 0 || oy === 0 ? 2 : 1;
          sum += raw[gridIndex(safeCols, nx, ny)] * kernel;
          weightSum += kernel;
        }
      }
      smoothed[gridIndex(safeCols, x, y)] = weightSum > 0 ? sum / weightSum : 0;
    }
  }

  return {
    cols: safeCols,
    rows: safeRows,
    cellWidth,
    cellHeight,
    values: smoothed
  };
}

export function sampleDensity(grid: DensityGrid, x: number, y: number): number {
  const gx = clamp(x / grid.cellWidth - 0.5, 0, grid.cols - 1);
  const gy = clamp(y / grid.cellHeight - 0.5, 0, grid.rows - 1);
  const x0 = clampCell(Math.floor(gx), grid.cols);
  const y0 = clampCell(Math.floor(gy), grid.rows);
  const x1 = clampCell(x0 + 1, grid.cols);
  const y1 = clampCell(y0 + 1, grid.rows);
  const tx = gx - x0;
  const ty = gy - y0;
  const v00 = sampleGridNearest(grid, x0, y0);
  const v10 = sampleGridNearest(grid, x1, y0);
  const v01 = sampleGridNearest(grid, x0, y1);
  const v11 = sampleGridNearest(grid, x1, y1);
  const top = v00 * (1 - tx) + v10 * tx;
  const bottom = v01 * (1 - tx) + v11 * tx;
  return top * (1 - ty) + bottom * ty;
}

export function sampleDensityForce(
  grid: DensityGrid,
  x: number,
  y: number,
  strength: number,
  maxMagnitude: number
): DensityForce {
  const epsX = Math.max(1, grid.cellWidth);
  const epsY = Math.max(1, grid.cellHeight);
  const dLeft = sampleDensity(grid, x - epsX, y);
  const dRight = sampleDensity(grid, x + epsX, y);
  const dTop = sampleDensity(grid, x, y - epsY);
  const dBottom = sampleDensity(grid, x, y + epsY);
  const gx = (dRight - dLeft) / (2 * epsX);
  const gy = (dBottom - dTop) / (2 * epsY);

  let ax = -gx * strength;
  let ay = -gy * strength;
  const magnitude = Math.hypot(ax, ay);
  const safeMax = Math.max(0, maxMagnitude);
  if (magnitude > safeMax && magnitude > 0) {
    const scale = safeMax / magnitude;
    ax *= scale;
    ay *= scale;
  }
  return { ax, ay, density: sampleDensity(grid, x, y) };
}

export function computeSoftWallForce(
  x: number,
  y: number,
  radius: number,
  width: number,
  height: number,
  bandRatio: number,
  strength: number
): { ax: number; ay: number } {
  const safeWidth = Math.max(1, width);
  const safeHeight = Math.max(1, height);
  const safeRadius = Math.max(0, radius);
  const oversizeX = safeRadius * 2 >= safeWidth;
  const oversizeY = safeRadius * 2 >= safeHeight;
  const viewportBand =
    Math.min(safeWidth, safeHeight) * clamp(bandRatio, 0.01, 0.25);
  const radiusBand = safeRadius * 1.35;
  const band = clamp(Math.max(radiusBand, viewportBand), 10, 42);
  const wallStrength = Math.max(0, strength);

  const leftDistance = x - safeRadius;
  const rightDistance = safeWidth - (x + safeRadius);
  const topDistance = y - safeRadius;
  const bottomDistance = safeHeight - (y + safeRadius);

  const pushFromWall = (distance: number): number => {
    if (distance >= band) return 0;
    const t = 1 - clamp(distance / band, 0, 1);
    return t * t * t;
  };

  const ax = oversizeX
    ? 0
    : wallStrength * (pushFromWall(leftDistance) - pushFromWall(rightDistance));
  const ay = oversizeY
    ? 0
    : wallStrength * (pushFromWall(topDistance) - pushFromWall(bottomDistance));
  return { ax, ay };
}

export function computeMeanVelocity(points: VelocitySource[]): { vx: number; vy: number; count: number } {
  let vx = 0;
  let vy = 0;
  let count = 0;
  for (const point of points) {
    if (point.isDragged) continue;
    if (!Number.isFinite(point.vx) || !Number.isFinite(point.vy)) continue;
    vx += point.vx;
    vy += point.vy;
    count += 1;
  }
  if (count <= 0) return { vx: 0, vy: 0, count: 0 };
  return { vx: vx / count, vy: vy / count, count };
}

export function computeCentroid(points: PositionSource[]): { x: number; y: number; count: number } {
  let x = 0;
  let y = 0;
  let count = 0;
  for (const point of points) {
    if (point.isDragged) continue;
    if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) continue;
    x += point.x;
    y += point.y;
    count += 1;
  }
  if (count <= 0) return { x: 0, y: 0, count: 0 };
  return { x: x / count, y: y / count, count };
}

export type QuadrantCounts = [number, number, number, number];

export function computeQuadrantCounts(
  points: PositionSource[],
  centerX: number,
  centerY: number
): QuadrantCounts {
  const counts: QuadrantCounts = [0, 0, 0, 0];
  for (const point of points) {
    const x = point.x;
    const y = point.y;
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    if (x < centerX && y < centerY) {
      counts[0] += 1;
    } else if (x >= centerX && y < centerY) {
      counts[1] += 1;
    } else if (x < centerX && y >= centerY) {
      counts[2] += 1;
    } else {
      counts[3] += 1;
    }
  }
  return counts;
}

export function computeQuadrantImbalanceRatio(counts: QuadrantCounts): number {
  const total = counts[0] + counts[1] + counts[2] + counts[3];
  if (total <= 0) return 1;
  const avg = total / 4;
  if (avg <= 0) return 1;
  const minValue = Math.min(counts[0], counts[1], counts[2], counts[3]);
  return minValue / avg;
}
