import type { BubbleDatum } from "../../app/types";
import { createSeededRng, randNormal } from "../../utils/math";
import { fixedMockBubbles } from "./bubbles.mock";

function randomSymbol(rng: () => number): string {
  const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const len = 3 + Math.floor(rng() * 2);
  let out = "";
  for (let i = 0; i < len; i += 1) {
    out += letters[Math.floor(rng() * letters.length)];
  }
  return out;
}

export function generateRandomBubbles(count: number, seed = 42): BubbleDatum[] {
  const rng = createSeededRng(seed);
  const items: BubbleDatum[] = [...fixedMockBubbles];
  const remaining = Math.max(0, count - items.length);

  for (let i = 0; i < remaining; i += 1) {
    const logCap = 7 + rng() * 5.5; // 1e7 ~ 3e12
    const marketCap = Math.pow(10, logCap);
    const price = Math.pow(10, rng() * 4.8) * 0.1;
    const volume24h = marketCap * (0.01 + rng() * 0.12);
    const change24h = randNormal(rng) * 3.0;
    const change7d = randNormal(rng) * 6.0;
    const symbol = randomSymbol(rng);

    items.push({
      id: `mock-${i + 1}`,
      symbol,
      name: `${symbol} Coin`,
      marketCap,
      volume24h,
      price,
      change24h,
      change7d,
      rank: items.length + 1
    });
  }

  return items;
}
