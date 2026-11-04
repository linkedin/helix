export interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface Tile<T> extends Rect {
  value: T;
}

// Squarified treemap (Bruls, Huizing and van Wijk). Keeps the input order, so
// the caller controls placement, and the tiles fill the rect exactly.
export function squarify<T>(
  items: { weight: number; value: T }[],
  rect: Rect,
): Tile<T>[] {
  const list = items.filter((item) => item.weight > 0);
  const total = list.reduce((sum, item) => sum + item.weight, 0);
  const tiles: Tile<T>[] = [];
  if (!total || rect.w <= 0 || rect.h <= 0) {
    return tiles;
  }
  const scale = (rect.w * rect.h) / total;
  // worst aspect ratio of a row with this area sum, smallest and largest area
  const worst = (sum: number, min: number, max: number, side: number) =>
    Math.max(
      (side * side * max) / (sum * sum),
      (sum * sum) / (side * side * min),
    );
  let { x, y, w, h } = rect;
  let i = 0;
  while (i < list.length) {
    const column = w >= h;
    const side = column ? h : w;
    let [sum, min, max, j] = [0, Infinity, 0, i];
    for (; j < list.length; j++) {
      const area = list[j].weight * scale;
      const [lo, hi] = [Math.min(min, area), Math.max(max, area)];
      if (
        j > i &&
        worst(sum + area, lo, hi, side) > worst(sum, min, max, side)
      ) {
        break;
      }
      [sum, min, max] = [sum + area, lo, hi];
    }
    const depth = j === list.length ? (column ? w : h) : sum / side;
    const end = column ? y + h : x + w;
    let pos = column ? y : x;
    for (let k = i; k < j; k++) {
      const len = k === j - 1 ? end - pos : (list[k].weight * scale) / depth;
      const value = list[k].value;
      tiles.push(
        column
          ? { x, y: pos, w: depth, h: len, value }
          : { x: pos, y, w: len, h: depth, value },
      );
      pos += len;
    }
    if (column) {
      x += depth;
      w -= depth;
    } else {
      y += depth;
      h -= depth;
    }
    i = j;
  }
  return tiles;
}
