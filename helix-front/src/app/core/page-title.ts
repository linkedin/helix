const APP = 'Helix';
const SEP = ' · ';

// Builds the browser tab title from a router URL, so open tabs can be told
// apart, for example /helix.default/Demo/instances -> "Demo · Instances · Helix".
export function pageTitle(url: string): string {
  const segments = url
    .split(/[?#]/)[0]
    .split('/')
    .filter(Boolean)
    .map(safeDecode);
  const [, cluster, section, item] = segments;
  if (!cluster) {
    return APP;
  }
  if (!section) {
    return [cluster, APP].join(SEP);
  }
  const parts =
    item && section !== 'controller'
      ? [item, cluster]
      : [cluster, capitalize(section)];
  return [...parts, APP].join(SEP);
}

function capitalize(word: string): string {
  return word.charAt(0).toUpperCase() + word.slice(1);
}

function safeDecode(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}
