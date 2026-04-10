import { describe, expect, it } from "vitest";
import { computeRadiusTargets } from "../metrics";
import type { BubbleDatum } from "../../../app/types";

describe("computeRadiusTargets", () => {
  it("maps higher metric to larger radius within bounds", () => {
    const data: BubbleDatum[] = [
      {
        id: "a",
        symbol: "A",
        name: "A Coin",
        marketCap: 10,
        volume24h: 20,
        price: 1,
        change24h: 0,
        change7d: 0,
        rank: 2
      },
      {
        id: "b",
        symbol: "B",
        name: "B Coin",
        marketCap: 1000,
        volume24h: 2000,
        price: 10,
        change24h: 0,
        change7d: 0,
        rank: 1
      }
    ];

    const radii = computeRadiusTargets(data, "marketCap", 10, 80);
    const rA = radii.get("a") ?? 0;
    const rB = radii.get("b") ?? 0;

    expect(rA).toBeGreaterThanOrEqual(10);
    expect(rA).toBeLessThanOrEqual(80);
    expect(rB).toBeGreaterThanOrEqual(10);
    expect(rB).toBeLessThanOrEqual(80);
    expect(rB).toBeGreaterThan(rA);
  });

  it("grows radius when ohlcv trend is aligned and strengthening", () => {
    const data: BubbleDatum[] = [
      {
        id: "sig-1",
        symbol: "BTCUSDT",
        name: "BTC Signal",
        marketCap: 100,
        volume24h: 100,
        price: 1,
        change24h: 0,
        change7d: 0,
        rank: 1,
        highSide: 1,
        timeframe: "1h",
        strategy: "simple",
        ohlcv: buildBarsFromOldestClose([100, 101, 103, 106, 110, 115, 121, 128, 136, 145])
      }
    ];

    const radii = computeRadiusTargets(data, "marketCap", 15, 150, 30);
    const radius = radii.get("sig-1") ?? 0;
    expect(radius).toBeGreaterThan(30);
    expect(radius).toBeLessThanOrEqual(150);
  });

  it("shrinks radius when ohlcv trend runs against direction", () => {
    const data: BubbleDatum[] = [
      {
        id: "sig-2",
        symbol: "ETHUSDT",
        name: "ETH Signal",
        marketCap: 100,
        volume24h: 100,
        price: 1,
        change24h: 0,
        change7d: 0,
        rank: 1,
        highSide: -1,
        timeframe: "1h",
        strategy: "simple",
        // 空头趋势下，价格整体抬升，半径应向下收缩。
        ohlcv: buildBarsFromOldestClose([100, 102, 104, 106, 109, 112, 116, 120, 125, 131])
      }
    ];

    const radii = computeRadiusTargets(data, "marketCap", 15, 150, 30);
    const radius = radii.get("sig-2") ?? 0;
    expect(radius).toBeLessThan(30);
    expect(radius).toBeGreaterThanOrEqual(15);
  });

  it("falls back to base radius when ohlcv is missing", () => {
    const data: BubbleDatum[] = [
      {
        id: "sig-3",
        symbol: "SOLUSDT",
        name: "SOL Signal",
        marketCap: 100,
        volume24h: 100,
        price: 1,
        change24h: 0,
        change7d: 0,
        rank: 1,
        highSide: 1,
        timeframe: "1h",
        strategy: "simple"
      }
    ];

    const radii = computeRadiusTargets(data, "marketCap", 15, 150, 30);
    expect(radii.get("sig-3")).toBe(30);
  });
});

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
