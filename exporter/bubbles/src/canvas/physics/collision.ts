export type CollisionBody = {
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
  mass: number;
  isDragged?: boolean;
  isStatic?: boolean;
};

export function resolveCircleCollision(
  a: CollisionBody,
  b: CollisionBody,
  restitution: number
): void {
  let dx = b.x - a.x;
  let dy = b.y - a.y;
  let dist = Math.hypot(dx, dy);
  const rSum = a.radius + b.radius;

  if (dist === 0) {
    dist = 0.0001;
    dx = rSum;
    dy = 0;
  }

  if (dist >= rSum) return;

  const nx = dx / dist;
  const ny = dy / dist;
  const overlap = rSum - dist;

  const invMassA = a.isDragged || a.isStatic ? 0 : 1 / Math.max(1, a.mass);
  const invMassB = b.isDragged || b.isStatic ? 0 : 1 / Math.max(1, b.mass);
  const invMassSum = invMassA + invMassB;

  if (invMassSum > 0) {
    const correction = overlap / invMassSum;
    a.x -= nx * correction * invMassA;
    a.y -= ny * correction * invMassA;
    b.x += nx * correction * invMassB;
    b.y += ny * correction * invMassB;
  }

  const rvx = b.vx - a.vx;
  const rvy = b.vy - a.vy;
  const velAlongNormal = rvx * nx + rvy * ny;

  if (velAlongNormal > 0) return;
  if (invMassSum === 0) return;

  const j = (-(1 + restitution) * velAlongNormal) / invMassSum;
  const impulseX = j * nx;
  const impulseY = j * ny;

  a.vx -= impulseX * invMassA;
  a.vy -= impulseY * invMassA;
  b.vx += impulseX * invMassB;
  b.vy += impulseY * invMassB;
}
