import { describe, expect, it } from "vitest";
import { resolveCircleCollision } from "../collision";

describe("resolveCircleCollision", () => {
  it("separates overlapping circles and updates velocity", () => {
    const a = { x: 0, y: 0, vx: 1, vy: 0, radius: 10, mass: 100 };
    const b = { x: 15, y: 0, vx: -1, vy: 0, radius: 10, mass: 100 };

    resolveCircleCollision(a, b, 0.8);

    const dist = Math.hypot(b.x - a.x, b.y - a.y);
    expect(dist).toBeGreaterThanOrEqual(a.radius + b.radius - 0.01);
    expect(a.vx).toBeLessThanOrEqual(1);
    expect(b.vx).toBeGreaterThanOrEqual(-1);
  });
});
