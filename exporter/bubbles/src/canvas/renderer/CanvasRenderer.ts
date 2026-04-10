import type { BubbleRuntime, GroupBubbleRuntime } from "../physics/Engine";
import type { ColorMetric } from "../../app/types";
import { drawBackground, drawBubble, drawGroupBubble } from "./draw";
import type { ImageCache } from "../../utils/imageCache";
import {
  createDefaultBubblePresetRuntime,
  type BubblePresetRuntime
} from "./bubblePresets";

export type ViewportTransform = {
  scale: number;
  offsetX: number;
  offsetY: number;
};

export type RenderState = {
  width: number;
  height: number;
  dpr: number;
  nowMs: number;
  worldWidth: number;
  worldHeight: number;
  groups: GroupBubbleRuntime[];
  bubbles: BubbleRuntime[];
  colorMetric: ColorMetric;
  positionRateBySymbol: Map<string, number>;
};

export function resolveViewportTransform(
  width: number,
  height: number,
  worldWidth: number,
  worldHeight: number
): ViewportTransform {
  const safeWorldWidth = Math.max(1, worldWidth);
  const safeWorldHeight = Math.max(1, worldHeight);
  const scale = Math.max(0.0001, Math.min(width / safeWorldWidth, height / safeWorldHeight));
  const offsetX = (width - safeWorldWidth * scale) * 0.5;
  const offsetY = (height - safeWorldHeight * scale) * 0.5;
  return { scale, offsetX, offsetY };
}

export function toWorldPoint(
  x: number,
  y: number,
  worldWidth: number,
  worldHeight: number,
  transform: ViewportTransform
): { x: number; y: number } | null {
  const worldX = (x - transform.offsetX) / transform.scale;
  const worldY = (y - transform.offsetY) / transform.scale;
  if (worldX < 0 || worldY < 0 || worldX > worldWidth || worldY > worldHeight) {
    return null;
  }
  return { x: worldX, y: worldY };
}

export class CanvasRenderer {
  private bubblePresetRuntime: BubblePresetRuntime;

  constructor(private readonly imageCache: ImageCache) {
    this.bubblePresetRuntime = createDefaultBubblePresetRuntime();
  }

  setBubblePresetRuntime(runtime: BubblePresetRuntime): void {
    this.bubblePresetRuntime = runtime;
  }

  draw(ctx: CanvasRenderingContext2D, state: RenderState): void {
    drawBackground(ctx, state.width, state.height, state.dpr);
    const transform = resolveViewportTransform(
      state.width,
      state.height,
      state.worldWidth,
      state.worldHeight
    );
    ctx.save();
    ctx.translate(transform.offsetX, transform.offsetY);
    ctx.scale(transform.scale, transform.scale);
    for (const group of state.groups) {
      drawGroupBubble(ctx, group, this.bubblePresetRuntime);
    }
    for (const bubble of state.bubbles) {
      drawBubble(
        ctx,
        bubble,
        state.colorMetric,
        this.imageCache,
        state.positionRateBySymbol,
        this.bubblePresetRuntime,
        state.nowMs
      );
    }
    ctx.restore();
  }
}
