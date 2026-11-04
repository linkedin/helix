import { describe, expect, it } from '@jest/globals';

import { Rect, squarify, Tile } from './treemap';

const rect: Rect = { x: 10, y: 20, w: 400, h: 250 };
const EPS = 1e-6;

function overlap(a: Rect, b: Rect) {
  return (
    Math.min(a.x + a.w, b.x + b.w) - Math.max(a.x, b.x) > EPS &&
    Math.min(a.y + a.h, b.y + b.h) - Math.max(a.y, b.y) > EPS
  );
}

function check(tiles: Tile<number>[], weights: number[]) {
  const total = weights.reduce((a, b) => a + b, 0);
  tiles.forEach((tile, i) => {
    expect(tile.w * tile.h).toBeCloseTo((weights[i] / total) * 100000, 3);
    expect(tile.x).toBeGreaterThanOrEqual(rect.x - EPS);
    expect(tile.y).toBeGreaterThanOrEqual(rect.y - EPS);
    expect(tile.x + tile.w).toBeLessThanOrEqual(rect.x + rect.w + EPS);
    expect(tile.y + tile.h).toBeLessThanOrEqual(rect.y + rect.h + EPS);
  });
  const overlapping = tiles.filter((tile, i) =>
    tiles.slice(i + 1).some((other) => overlap(tile, other)),
  );
  expect(overlapping).toEqual([]);
}

describe('squarify', () => {
  it('returns nothing for no weight or an empty rect', () => {
    expect(squarify([], rect)).toEqual([]);
    expect(squarify([{ weight: 0, value: 1 }], rect)).toEqual([]);
    expect(squarify([{ weight: 1, value: 1 }], { ...rect, w: 0 })).toEqual([]);
  });

  it('gives a single item the whole rect', () => {
    expect(squarify([{ weight: 3, value: 'a' }], rect)).toEqual([
      { ...rect, value: 'a' },
    ]);
  });

  it('sizes tiles by weight, inside the rect, without overlap, in input order', () => {
    const weights = [6, 6, 4, 3, 2, 2, 1, 0.5, 5, 1.5];
    const items = weights.map((weight, value) => ({ weight, value }));
    const tiles = squarify([...items, { weight: 0, value: 99 }], rect);

    expect(tiles.map((tile) => tile.value)).toEqual(weights.map((_, i) => i));
    check(tiles, weights);
    expect(squarify(items, rect)).toEqual(tiles);
  });

  it('keeps many equal tiles close to square', () => {
    const weights = Array.from({ length: 200 }, () => 1);
    const tiles = squarify(
      weights.map((weight, value) => ({ weight, value })),
      rect,
    );

    check(tiles, weights);
    const aspects = tiles.map((t) => Math.max(t.w / t.h, t.h / t.w));
    expect(Math.max(...aspects)).toBeLessThan(2.5);
  });
});
