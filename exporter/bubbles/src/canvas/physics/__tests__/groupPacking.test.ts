import { describe, expect, it } from "vitest";
import { packGroupChildren } from "../groupPacking";

describe("packGroupChildren", () => {
  it("packs children without overlap and keeps them inside parent radius", () => {
    const packed = packGroupChildren("turtle|30m|long|1", [
      { id: "A", radius: 38 },
      { id: "B", radius: 26 },
      { id: "C", radius: 22 },
      { id: "D", radius: 18 }
    ]);

    expect(packed.children).toHaveLength(4);
    expect(packed.radius).toBeGreaterThan(38);

    for (let i = 0; i < packed.children.length; i += 1) {
      const current = packed.children[i];
      expect(Math.hypot(current.x, current.y) + current.radius).toBeLessThanOrEqual(packed.radius + 1e-6);
      for (let j = i + 1; j < packed.children.length; j += 1) {
        const other = packed.children[j];
        const distance = Math.hypot(current.x - other.x, current.y - other.y);
        expect(distance).toBeGreaterThanOrEqual(current.radius + other.radius - 1e-6);
      }
    }
  });

  it("is deterministic for the same group id and radii", () => {
    const left = packGroupChildren("turtle|30m|short|2", [
      { id: "SOL", radius: 30 },
      { id: "ARB", radius: 24 },
      { id: "LINK", radius: 16 }
    ]);
    const right = packGroupChildren("turtle|30m|short|2", [
      { id: "SOL", radius: 30 },
      { id: "ARB", radius: 24 },
      { id: "LINK", radius: 16 }
    ]);

    expect(right).toEqual(left);
  });

  it("keeps parent radius reasonably compact", () => {
    const packed = packGroupChildren("turtle|30m|long|3", [
      { id: "BTC", radius: 42 },
      { id: "ETH", radius: 40 }
    ]);

    expect(packed.radius).toBeLessThanOrEqual(100);
  });
});
