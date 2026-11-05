const APP = 'Helix';
const SEP = ' · ';

// Builds the browser tab title from a router URL, so open tabs can be told
// apart, for example /helix.default/Demo/instances -> "Demo · Instances · Helix"
// and /helix.default/Demo/resources/db/idealState -> "db · Ideal State · Demo · Helix".
export function pageTitle(url: string): string {
  const segments = url
    .split(/[?#]/)[0]
    .split('/')
    .filter(Boolean)
    .map(safeDecode);
  const [, cluster, section, item, view] = segments;
  if (!cluster) {
    return APP;
  }
  if (!section) {
    return [cluster, APP].join(SEP);
  }
  const parts =
    item && section !== 'controller'
      ? [item, label(view), cluster]
      : [cluster, label(section), label(item)];
  return [...parts.filter(Boolean), APP].join(SEP);
}

// "idealState" -> "Ideal State"
function label(word?: string): string {
  return (word || '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/^./, (c) => c.toUpperCase());
}

function safeDecode(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}
