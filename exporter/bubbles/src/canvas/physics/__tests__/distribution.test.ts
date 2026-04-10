import { describe, expect, it } from "vitest";
import {
  buildDensityGrid,
  computeMeanVelocity,
  computeQuadrantCounts,
  computeQuadrantImbalanceRatio,
  computeSoftWallForce,
  sampleDensityForce
} from "../distribution";

describe("distribution helpers", () => {
  it("pushes bubble away from denser region", () => {
    const grid = buildDensityGrid(
      [
        { x: 82, y: 45, radius: 22 },
        { x: 88, y: 55, radius: 18 }
      ],
      100,
      100,
      10,
      10
    );

    const force = sampleDensityForce(grid, 58, 50, 1.1, 10);
    expect(force.ax).toBeLessThan(0);
  });

  it("returns soft wall force that pushes back into viewport", () => {
    const nearLeftTop = computeSoftWallForce(6, 8, 6, 100, 100, 0.2, 2);
    expect(nearLeftTop.ax).toBeGreaterThan(0);
    expect(nearLeftTop.ay).toBeGreaterThan(0);

    const nearRightBottom = computeSoftWallForce(94, 93, 6, 100, 100, 0.2, 2);
    expect(nearRightBottom.ax).toBeLessThan(0);
    expect(nearRightBottom.ay).toBeLessThan(0);
  });

  it("avoids creating large dead zones near edges on large viewports", () => {
    const midLeft = computeSoftWallForce(120, 400, 18, 1200, 800, 0.12, 1.15);
    expect(Math.abs(midLeft.ax)).toBeLessThan(1e-6);
    expect(Math.abs(midLeft.ay)).toBeLessThan(1e-6);
  });

  it("computes quadrant imbalance ratio", () => {
    const counts = computeQuadrantCounts(
      [
        { x: 10, y: 10 },
        { x: 20, y: 20 },
        { x: 80, y: 10 },
        { x: 82, y: 12 },
        { x: 84, y: 14 },
        { x: 86, y: 16 },
        { x: 88, y: 18 },
        { x: 90, y: 20 }
      ],
      50,
      50
    );

    expect(counts).toEqual([2, 6, 0, 0]);
    const ratio = computeQuadrantImbalanceRatio(counts);
    expect(ratio).toBe(0);
  });

  it("computes mean velocity while skipping dragged bubbles", () => {
    const mean = computeMeanVelocity([
      { vx: 8, vy: 2 },
      { vx: -2, vy: 6 },
      { vx: 50, vy: 50, isDragged: true }
    ]);

    expect(mean.count).toBe(2);
    expect(mean.vx).toBeCloseTo(3, 6);
    expect(mean.vy).toBeCloseTo(4, 6);
  });
});
