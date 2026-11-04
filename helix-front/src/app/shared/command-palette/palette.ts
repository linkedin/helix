export type PaletteKind = 'page' | 'cluster' | 'resource' | 'instance';

export interface PaletteItem {
  kind: PaletteKind;
  label: string;
  hint: string;
  route: string[];
}

export interface Segment {
  text: string;
  hit: boolean;
}

export interface Ranked {
  item: PaletteItem;
  score: number;
  segments: Segment[];
}

export const CLUSTER_PAGES = [
  'Overview',
  'Resources',
  'Instances',
  'Workflows',
  'Topology',
  'Capacity',
  'Diagnostics',
  'Dashboard',
  'Configs',
];

// Subsequence match: every query character must appear in order. Consecutive
// hits and hits at word starts score higher, so "inst" beats "i...n...s...t".
export function fuzzyMatch(query: string, text: string) {
  const q = query.toLowerCase().replace(/\s+/g, '');
  const t = text.toLowerCase();
  const hits: number[] = [];
  let score = 0;
  let from = 0;
  for (const ch of q) {
    const i = t.indexOf(ch, from);
    if (i < 0) return null;
    score += 1;
    if (hits.length && i === hits[hits.length - 1] + 1) score += 4;
    if (i === 0 || /[^a-z0-9]/.test(t[i - 1])) score += 3;
    hits.push(i);
    from = i + 1;
  }
  if (t.startsWith(q)) score += 10;
  return { score: score - text.length / 100, hits };
}

export function toSegments(text: string, hits: number[]): Segment[] {
  const set = new Set(hits);
  const out: Segment[] = [];
  for (let i = 0; i < text.length; i++) {
    const hit = set.has(i);
    const last = out[out.length - 1];
    if (last && last.hit === hit) last.text += text[i];
    else out.push({ text: text[i], hit });
  }
  return out;
}

export function rank(
  query: string,
  items: PaletteItem[],
  limit = 50,
): Ranked[] {
  if (!query.trim()) {
    return items
      .slice(0, limit)
      .map((item) => ({
        item,
        score: 0,
        segments: toSegments(item.label, []),
      }));
  }
  const out: Ranked[] = [];
  for (const item of items) {
    const m = fuzzyMatch(query, item.label);
    if (m)
      out.push({
        item,
        score: m.score,
        segments: toSegments(item.label, m.hits),
      });
  }
  return out.sort((a, b) => b.score - a.score).slice(0, limit);
}

// "/helix.default/Demo/instances" -> { helix: "helix.default", cluster: "Demo" }
export function contextFromUrl(url: string) {
  const parts = url.split(/[?#]/)[0].split('/').filter(Boolean).map(safeDecode);
  return { helix: parts[0], cluster: parts[1] };
}

function safeDecode(s: string) {
  try {
    return decodeURIComponent(s);
  } catch (e) {
    return s;
  }
}
