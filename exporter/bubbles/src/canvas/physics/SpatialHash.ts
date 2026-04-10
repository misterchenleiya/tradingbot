export class SpatialHash {
  private cellSize: number;
  private cells = new Map<number, number[]>();
  private static readonly CELL_OFFSET = 32768;

  constructor(cellSize: number) {
    this.cellSize = cellSize;
  }

  setCellSize(cellSize: number): void {
    this.cellSize = Math.max(20, cellSize);
  }

  clear(): void {
    this.cells.clear();
  }

  insert(index: number, x: number, y: number): void {
    const cx = Math.floor(x / this.cellSize);
    const cy = Math.floor(y / this.cellSize);
    const key = this.keyForCell(cx, cy);
    const list = this.cells.get(key);
    if (list) {
      list.push(index);
    } else {
      this.cells.set(key, [index]);
    }
  }

  query(x: number, y: number, out: number[]): number[] {
    out.length = 0;
    const cx = Math.floor(x / this.cellSize);
    const cy = Math.floor(y / this.cellSize);
    for (let ox = -1; ox <= 1; ox += 1) {
      for (let oy = -1; oy <= 1; oy += 1) {
        const key = this.keyForCell(cx + ox, cy + oy);
        const list = this.cells.get(key);
        if (list) {
          for (let i = 0; i < list.length; i += 1) {
            out.push(list[i]);
          }
        }
      }
    }
    return out;
  }

  private keyForCell(cx: number, cy: number): number {
    const x = (cx + SpatialHash.CELL_OFFSET) & 0xffff;
    const y = (cy + SpatialHash.CELL_OFFSET) & 0xffff;
    return (((x << 16) | y) >>> 0);
  }
}
