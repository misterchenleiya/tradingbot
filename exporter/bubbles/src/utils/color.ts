import { clamp, smoothstep } from "./math";
import type { TrendType } from "../app/types";

const RED = [255, 74, 66] as const;
const GREEN = [64, 236, 124] as const;
const TRANSPARENT = "rgba(0, 0, 0, 0)";
const NEUTRAL_EPSILON = 0.01;

function toRgba(rgb: readonly number[], alpha = 1): string {
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${alpha})`;
}

export type BubbleColors = {
  core: string;
  edge: string;
  glow: string;
  rim: string;
  highlight: string;
  text: string;
  glowStrength: number;
};

function transparentColors(): BubbleColors {
  return {
    core: TRANSPARENT,
    edge: TRANSPARENT,
    glow: TRANSPARENT,
    rim: TRANSPARENT,
    highlight: TRANSPARENT,
    text: "rgba(255, 255, 255, 0.92)",
    glowStrength: 0
  };
}

function makePalette(base: readonly number[], strength = 0.6): BubbleColors {
  const glowStrength = clamp(strength, 0, 1);
  const alphaBoost = 0.18 + glowStrength * 0.22;
  return {
    core: toRgba(base, 0.78 + alphaBoost),
    edge: toRgba(base, 0.52 + alphaBoost * 0.5),
    glow: toRgba(base, 0.22 + glowStrength * 0.5),
    rim: toRgba(base, 0.34 + glowStrength * 0.2),
    highlight: toRgba(base, 0.28 + glowStrength * 0.2),
    text: "rgba(255, 255, 255, 0.92)",
    glowStrength
  };
}

function makeFrostedPalette(): BubbleColors {
  return {
    core: "rgba(214, 229, 245, 0.34)",
    edge: "rgba(168, 188, 212, 0.28)",
    glow: "rgba(210, 226, 243, 0.32)",
    rim: "rgba(240, 247, 255, 0.86)",
    highlight: "rgba(255, 255, 255, 0.92)",
    text: "rgba(255, 255, 255, 0.95)",
    glowStrength: 0.35
  };
}

export function resolveBubbleColors(changePercent: number): BubbleColors {
  const p = clamp(changePercent, -20, 20);
  const abs = Math.abs(p);
  const glowStrength = smoothstep(0, 1, abs / 20);
  if (abs <= NEUTRAL_EPSILON) {
    return transparentColors();
  }

  const base = p > 0 ? GREEN : RED;
  return makePalette(base, glowStrength);
}

export function resolveBubbleColorsByTrend(trendType: TrendType): BubbleColors {
  switch (trendType) {
    case "bullish":
      return makePalette([64, 236, 124], 0.82);
    case "bullishPullback":
      return makePalette([64, 236, 124], 0.74);
    case "bearish":
      return makePalette([255, 74, 66], 0.84);
    case "bearishPullback":
      return makePalette([255, 74, 66], 0.76);
    case "range":
      return makeFrostedPalette();
    case "none":
      return transparentColors();
    default:
      return transparentColors();
  }
}

export function lightenColor(color: string, amount: number): string {
  const match = /rgba\((\d+),\s*(\d+),\s*(\d+),\s*([0-9.]+)\)/.exec(color);
  if (!match) return color;
  const r = Math.min(255, Math.round(Number(match[1]) + 255 * amount));
  const g = Math.min(255, Math.round(Number(match[2]) + 255 * amount));
  const b = Math.min(255, Math.round(Number(match[3]) + 255 * amount));
  const a = Number(match[4]);
  return `rgba(${r}, ${g}, ${b}, ${a})`;
}
