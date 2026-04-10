export type ForceParams = {
  noiseStrength: number;
  noiseSpeed: number;
};

export type ForceBody = {
  x: number;
  y: number;
  noisePhase: number;
};

export function computeForces(
  body: ForceBody,
  centerX: number,
  centerY: number,
  time: number,
  params: ForceParams
): { ax: number; ay: number } {
  // 使用随窗口尺寸变化的流场，避免气泡向中心聚集。
  const flowScaleX = Math.max(140, centerX * 0.85);
  const flowScaleY = Math.max(140, centerY * 0.85);

  const flowX =
    Math.sin((body.y / flowScaleY) * Math.PI * 2 + time * params.noiseSpeed + body.noisePhase);
  const flowY =
    Math.cos((body.x / flowScaleX) * Math.PI * 2 - time * params.noiseSpeed + body.noisePhase * 1.11);

  const jitterX = Math.sin(time * params.noiseSpeed * 1.7 + body.noisePhase * 2.3);
  const jitterY = Math.cos(time * params.noiseSpeed * 1.9 + body.noisePhase * 2.7);

  const ax = (flowX * 0.75 + jitterX * 0.25) * params.noiseStrength;
  const ay = (flowY * 0.75 + jitterY * 0.25) * params.noiseStrength;
  return { ax, ay };
}
