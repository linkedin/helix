import { describe, expect, it } from '@jest/globals';
import { pageTitle } from './page-title';

describe('pageTitle', () => {
  it('leads with the cluster or the item the page shows', () => {
    expect(pageTitle('/')).toBe('Helix');
    expect(pageTitle('/helix.default')).toBe('Helix');
    expect(pageTitle('/helix.default/Demo')).toBe('Demo · Helix');
    expect(pageTitle('/helix.default/Demo/instances?embed=true')).toBe(
      'Demo · Instances · Helix',
    );
    expect(
      pageTitle('/helix.default/Demo/instances/localhost_22004/resources'),
    ).toBe('localhost_22004 · Demo · Helix');
    expect(pageTitle('/helix.default/Demo/resources/my%20db/partitions')).toBe(
      'my db · Demo · Helix',
    );
    expect(pageTitle('/helix.default/Demo/controller/history')).toBe(
      'Demo · Controller · Helix',
    );
  });

  it('keeps a segment that is not valid URI encoding', () => {
    expect(pageTitle('/h/Demo/resources/100%')).toBe('100% · Demo · Helix');
  });
});
