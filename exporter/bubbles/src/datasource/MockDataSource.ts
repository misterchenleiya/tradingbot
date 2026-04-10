import type { IDataSource, DataSourceHandlers } from "./IDataSource";
import type { BubbleCandlesFetchRequest, BubbleCandlesSnapshot, BubbleDatum, BubbleUpdate } from "../app/types";
import { createSeededRng, randNormal } from "../utils/math";

export class MockDataSource implements IDataSource {
  private timer: number | null = null;
  private readonly rng: () => number;

  constructor(
    private readonly data: BubbleDatum[],
    private readonly updateIntervalMs: number,
    private readonly handlers: DataSourceHandlers
  ) {
    this.rng = createSeededRng(12345);
  }

  start(): void {
    this.handlers.onStatus?.({ restStatus: "ok", wsStatus: "idle" });
    this.handlers.onSnapshot([...this.data]);
    this.timer = window.setInterval(() => {
      const updates = this.randomUpdates();
      if (updates.length > 0) {
        this.handlers.onUpdate(updates);
      }
    }, this.updateIntervalMs);
  }

  stop(): void {
    if (this.timer !== null) {
      window.clearInterval(this.timer);
      this.timer = null;
    }
  }

  loadMoreHistory(): void {}

  async fetchCandles(_request: BubbleCandlesFetchRequest): Promise<BubbleCandlesSnapshot | undefined> {
    return undefined;
  }

  private randomUpdates(): BubbleUpdate[] {
    const updates: BubbleUpdate[] = [];
    const updateCount = Math.max(5, Math.floor(this.data.length * 0.08));
    for (let i = 0; i < updateCount; i += 1) {
      const index = Math.floor(this.rng() * this.data.length);
      const item = this.data[index];
      if (!item) continue;

      const priceDrift = 1 + randNormal(this.rng) * 0.002;
      const newPrice = Math.max(0.0001, item.price * priceDrift);
      const change24h = item.change24h + randNormal(this.rng) * 0.1;
      const change7d = item.change7d + randNormal(this.rng) * 0.08;
      const marketCap = Math.max(1, item.marketCap * priceDrift * (1 + randNormal(this.rng) * 0.0008));
      const volume24h = Math.max(1, item.volume24h * (1 + randNormal(this.rng) * 0.01));

      const updated: BubbleDatum = {
        ...item,
        price: newPrice,
        change24h,
        change7d,
        marketCap,
        volume24h
      };

      this.data[index] = updated;
      updates.push({
        id: updated.id,
        price: updated.price,
        change24h: updated.change24h,
        change7d: updated.change7d,
        marketCap: updated.marketCap,
        volume24h: updated.volume24h
      });
    }
    return updates;
  }
}
