import { describe, expect, it } from "vitest";
import type { BubbleDatum } from "../../../app/types";
import { Engine } from "../Engine";

const GROUP_ID = "turtle|30m|long|1";

function createEngine(): Engine {
  const engine = new Engine();
  engine.setViewport(960, 640);
  engine.setRadiusRange(18, 140);
  engine.setBaseRadius(32);
  return engine;
}

function buildSignal(
  id: string,
  symbol: string,
  closesOldestToNewest: number[],
  overrides: Partial<BubbleDatum> = {}
): BubbleDatum {
  return {
    id,
    symbol,
    name: symbol,
    marketCap: 100,
    volume24h: 100,
    price: closesOldestToNewest[closesOldestToNewest.length - 1] ?? 1,
    change24h: 0,
    change7d: 0,
    rank: 1,
    exchange: "okx",
    timeframe: "30m",
    strategy: "turtle",
    strategyVersion: "v0.0.6c",
    groupId: GROUP_ID,
    highSide: 1,
    ohlcv: buildBarsFromOldestClose(closesOldestToNewest),
    ...overrides
  };
}

function buildBarsFromOldestClose(closesOldestToNewest: number[]): BubbleDatum["ohlcv"] {
  return [...closesOldestToNewest]
    .reverse()
    .map((close, index) => ({
      ts: index,
      open: close,
      high: close,
      low: close,
      close,
      volume: 1
    }));
}

describe("Engine grouped bubbles", () => {
  it("does not repack grouped layout on signal price-only updates", () => {
    const engine = createEngine();
    const snapshot = [
      buildSignal("sig-a", "A", [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]),
      buildSignal("sig-b", "B", [100, 101, 103, 104, 106, 108, 109, 111, 112, 114]),
      buildSignal("sig-c", "C", [100, 100.5, 101, 101.5, 102, 103, 104, 104.5, 105, 106])
    ];

    engine.setSnapshot(snapshot);

    const beforeGroup = engine.getGroupBubbles()[0];
    expect(beforeGroup).toBeDefined();
    const beforeTargetRadius = beforeGroup.targetRadius;
    const beforeOffsets = new Map(
      engine.getBubbles().map((bubble) => [
        bubble.id,
        {
          groupBubbleId: bubble.groupBubbleId,
          x: bubble.groupOffsetX,
          y: bubble.groupOffsetY
        }
      ])
    );

    engine.applyUpdates([
      {
        ...snapshot[0],
        price: snapshot[0].price * 1.08,
        marketCap: snapshot[0].marketCap * 1.12,
        volume24h: snapshot[0].volume24h * 0.91
      }
    ]);

    const afterGroup = engine.getGroupBubbles()[0];
    expect(afterGroup.targetRadius).toBeCloseTo(beforeTargetRadius, 6);
    for (const bubble of engine.getBubbles()) {
      const before = beforeOffsets.get(bubble.id);
      expect(before).toBeDefined();
      expect(bubble.groupBubbleId).toBe(before?.groupBubbleId);
      expect(bubble.groupOffsetX).toBeCloseTo(before?.x ?? 0, 6);
      expect(bubble.groupOffsetY).toBeCloseTo(before?.y ?? 0, 6);
    }
  });

  it("keeps group radius below a larger target until smoothing catches up", () => {
    const engine = createEngine();
    const snapshot = [
      buildSignal("sig-a", "A", [100, 100, 100, 100, 100, 100, 100, 100, 100, 100]),
      buildSignal("sig-b", "B", [100, 101, 101, 102, 102, 103, 103, 104, 104, 105]),
      buildSignal("sig-c", "C", [100, 100.5, 100.5, 101, 101, 101.5, 101.5, 102, 102, 102.5])
    ];

    engine.setSnapshot(snapshot);

    const beforeGroup = engine.getGroupBubbles()[0];
    expect(beforeGroup).toBeDefined();
    const beforeRadius = beforeGroup.radius;

    engine.applyUpdates([
      {
        ...snapshot[0],
        ohlcv: buildBarsFromOldestClose([100, 101, 103, 106, 110, 115, 121, 128, 136, 145])
      }
    ]);

    const afterGroup = engine.getGroupBubbles()[0];
    expect(afterGroup.targetRadius).toBeGreaterThan(beforeRadius);
    expect(afterGroup.radius).toBeLessThan(afterGroup.targetRadius);
    expect(afterGroup.radius).toBeGreaterThanOrEqual(beforeRadius - 1e-6);
  });

  it("pins oversized group bubbles to the viewport center instead of flipping vertically", () => {
    const engine = createEngine();
    engine.setViewport(320, 200);

    const oversizedGroup = {
      id: "group-bubble|oversized",
      groupId: GROUP_ID,
      highSide: 1 as const,
      motionMode: "free" as const,
      packedRadius: 180,
      layoutScale: 1,
      x: 48,
      y: 18,
      vx: 22,
      vy: -35,
      radius: 180,
      targetRadius: 180,
      mass: 1,
      noisePhase: 0,
      isDragged: false,
      childIds: []
    };

    (engine as any).groupBubbles = [oversizedGroup];
    (engine as any).groupBubbleMap = new Map([[oversizedGroup.id, oversizedGroup]]);

    const ySamples: number[] = [];
    for (let index = 0; index < 5; index += 1) {
      engine.tick(1 / 60);
      ySamples.push(engine.getGroupBubbles()[0]?.y ?? NaN);
    }

    expect(ySamples).toHaveLength(5);
    for (const y of ySamples) {
      expect(y).toBeCloseTo(100, 6);
    }
    const group = engine.getGroupBubbles()[0];
    expect(group.x).toBeCloseTo(160, 6);
    expect(group.y).toBeCloseTo(100, 6);
    expect(group.vx).toBe(0);
    expect(group.vy).toBe(0);
  });

  it("pins multiple giant groups into stable slots instead of letting them shake in collisions", () => {
    const engine = createEngine();
    engine.setViewport(960, 540);

    const groups = [
      {
        id: "group-bubble|g1",
        groupId: "turtle|30m|long|1",
        highSide: 1 as const,
        motionMode: "free" as const,
        packedRadius: 290,
        layoutScale: 1,
        x: 480,
        y: 270,
        vx: 18,
        vy: -12,
        radius: 290,
        targetRadius: 290,
        mass: 1,
        noisePhase: 0,
        isDragged: false,
        childIds: Array.from({ length: 96 }, (_, index) => `g1-${index}`)
      },
      {
        id: "group-bubble|g2",
        groupId: "turtle|30m|short|2",
        highSide: -1 as const,
        motionMode: "free" as const,
        packedRadius: 250,
        layoutScale: 1,
        x: 500,
        y: 250,
        vx: -16,
        vy: 10,
        radius: 250,
        targetRadius: 250,
        mass: 1,
        noisePhase: 0.5,
        isDragged: false,
        childIds: Array.from({ length: 72 }, (_, index) => `g2-${index}`)
      },
      {
        id: "group-bubble|g3",
        groupId: "turtle|30m|short|3",
        highSide: -1 as const,
        motionMode: "free" as const,
        packedRadius: 210,
        layoutScale: 1,
        x: 460,
        y: 280,
        vx: 8,
        vy: 14,
        radius: 210,
        targetRadius: 210,
        mass: 1,
        noisePhase: 1,
        isDragged: false,
        childIds: Array.from({ length: 32 }, (_, index) => `g3-${index}`)
      }
    ];

    (engine as any).groupBubbles = groups;
    (engine as any).groupBubbleMap = new Map(groups.map((group) => [group.id, group]));
    (engine as any).updateGroupMotionModes();

    const initial = engine.getGroupBubbles().map((group) => ({
      id: group.id,
      x: group.x,
      y: group.y,
      mode: group.motionMode
    }));

    for (let index = 0; index < 12; index += 1) {
      engine.tick(1 / 60);
    }

    const after = engine.getGroupBubbles();
    expect(after).toHaveLength(3);
    for (const group of after) {
      expect(group.motionMode).toBe("pinned");
      expect(group.vx).toBe(0);
      expect(group.vy).toBe(0);
      const before = initial.find((item) => item.id === group.id);
      expect(group.x).toBeCloseTo(before?.x ?? 0, 6);
      expect(group.y).toBeCloseTo(before?.y ?? 0, 6);
    }

    for (let i = 0; i < after.length; i += 1) {
      for (let j = i + 1; j < after.length; j += 1) {
        const left = after[i];
        const right = after[j];
        const distance = Math.hypot(left.x - right.x, left.y - right.y);
        expect(distance).toBeGreaterThanOrEqual(left.radius + right.radius + 10);
      }
    }
  });

  it("treats pinned groups as static obstacles for free bubbles", () => {
    const engine = createEngine();
    engine.setViewport(960, 540);

    const pinnedGroup = {
      id: "group-bubble|pinned",
      groupId: GROUP_ID,
      highSide: 1 as const,
      motionMode: "pinned" as const,
      packedRadius: 300,
      layoutScale: 1,
      x: 102,
      y: 102,
      vx: 0,
      vy: 0,
      radius: 300,
      targetRadius: 300,
      mass: 1,
      noisePhase: 0,
      isDragged: false,
      childIds: Array.from({ length: 40 }, (_, index) => `pinned-${index}`),
      pinnedSlotKey: "top-left" as const
    };
    const freeBubble = {
      id: "free",
      data: buildSignal("free", "FREE", [100, 101, 102, 103, 104, 105, 106, 107, 108, 109], {
        groupId: undefined
      }),
      display: {
        price: 109,
        marketCap: 100,
        volume24h: 100,
        change24h: 0,
        change7d: 0
      },
      x: 170,
      y: 130,
      vx: -12,
      vy: 0,
      radius: 36,
      targetRadius: 36,
      mass: 1296,
      noisePhase: 0,
      isDragged: false,
      groupOffsetX: 0,
      groupOffsetY: 0
    };

    (engine as any).groupBubbles = [pinnedGroup];
    (engine as any).groupBubbleMap = new Map([[pinnedGroup.id, pinnedGroup]]);
    (engine as any).bubbles = [freeBubble];
    (engine as any).bubbleMap = new Map([[freeBubble.id, freeBubble]]);

    const before = { x: pinnedGroup.x, y: pinnedGroup.y };
    engine.tick(1 / 60);

    const group = engine.getGroupBubbles()[0];
    const bubble = engine.getBubbles()[0];
    expect(group.motionMode).toBe("pinned");
    expect(group.vx).toBe(0);
    expect(group.vy).toBe(0);
    expect(group.x).not.toBeCloseTo(before.x, 6);
    expect(group.y).not.toBeCloseTo(before.y, 6);
    const distance = Math.hypot(group.x - bubble.x, group.y - bubble.y);
    expect(distance).toBeGreaterThanOrEqual(group.radius + bubble.radius - 1e-6);
  });

  it("scales oversized pinned groups proportionally to keep the full group within the viewport", () => {
    const engine = createEngine();
    engine.setViewport(960, 540);

    const group = {
      id: "group-bubble|scaled",
      groupId: GROUP_ID,
      highSide: 1 as const,
      motionMode: "pinned" as const,
      packedRadius: 420,
      layoutScale: 1,
      x: 480,
      y: 270,
      vx: 0,
      vy: 0,
      radius: 420,
      targetRadius: 420,
      mass: 1,
      noisePhase: 0,
      isDragged: false,
      childIds: ["child-big", "child-small"],
      pinnedSlotKey: "top-left" as const
    };
    const childBig = {
      id: "child-big",
      data: buildSignal(
        "child-big",
        "BIG",
        [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
      ),
      display: {
        price: 109,
        marketCap: 100,
        volume24h: 100,
        change24h: 0,
        change7d: 0
      },
      x: 0,
      y: 0,
      vx: 0,
      vy: 0,
      radius: 60,
      targetRadius: 60,
      mass: 3600,
      noisePhase: 0,
      isDragged: false,
      groupBubbleId: group.id,
      groupOffsetX: -180,
      groupOffsetY: -120
    };
    const childSmall = {
      id: "child-small",
      data: buildSignal(
        "child-small",
        "SMALL",
        [100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5]
      ),
      display: {
        price: 104.5,
        marketCap: 100,
        volume24h: 100,
        change24h: 0,
        change7d: 0
      },
      x: 0,
      y: 0,
      vx: 0,
      vy: 0,
      radius: 30,
      targetRadius: 30,
      mass: 900,
      noisePhase: 0,
      isDragged: false,
      groupBubbleId: group.id,
      groupOffsetX: 150,
      groupOffsetY: 140
    };

    (engine as any).groupBubbles = [group];
    (engine as any).groupBubbleMap = new Map([[group.id, group]]);
    (engine as any).bubbles = [childBig, childSmall];
    (engine as any).bubbleMap = new Map([
      [childBig.id, childBig],
      [childSmall.id, childSmall]
    ]);

    engine.tick(1 / 60);

    const scaledGroup = engine.getGroupBubbles()[0];
    const [bigBubble, smallBubble] = engine.getBubbles();

    expect(scaledGroup.motionMode).toBe("pinned");
    expect(scaledGroup.layoutScale).toBeLessThan(1);
    expect(scaledGroup.radius).toBeCloseTo(scaledGroup.packedRadius * scaledGroup.layoutScale, 6);
    expect(scaledGroup.x - scaledGroup.radius).toBeGreaterThanOrEqual(-1e-6);
    expect(scaledGroup.y - scaledGroup.radius).toBeGreaterThanOrEqual(-1e-6);
    expect(scaledGroup.x + scaledGroup.radius).toBeLessThanOrEqual(960 + 1e-6);
    expect(scaledGroup.y + scaledGroup.radius).toBeLessThanOrEqual(540 + 1e-6);

    const bubbles = [bigBubble, smallBubble];
    for (const bubble of bubbles) {
      expect(bubble.x - bubble.radius).toBeGreaterThanOrEqual(-1e-6);
      expect(bubble.y - bubble.radius).toBeGreaterThanOrEqual(-1e-6);
      expect(bubble.x + bubble.radius).toBeLessThanOrEqual(960 + 1e-6);
      expect(bubble.y + bubble.radius).toBeLessThanOrEqual(540 + 1e-6);
    }

    expect(bigBubble.radius / smallBubble.radius).toBeCloseTo(2, 6);
  });
});
