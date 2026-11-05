import { describe, expect, it, jest } from '@jest/globals';
import { of, Subject, throwError } from 'rxjs';

import {
  contextFromUrl,
  fuzzyMatch,
  rank,
  toSegments,
  PaletteItem,
} from './palette';
import { CommandPaletteComponent } from './command-palette.component';
import { PaletteService } from './palette.service';

const item = (label: string): PaletteItem => ({
  kind: 'cluster',
  label,
  hint: '',
  route: [label],
});

describe('palette matching', () => {
  it('matches subsequences in order only', () => {
    expect(fuzzyMatch('dmo', 'Demo')).not.toBeNull();
    expect(fuzzyMatch('omd', 'Demo')).toBeNull();
    expect(fuzzyMatch('lh 1', 'localhost_12913').hits).toEqual([0, 5, 10]);
  });

  it('ranks prefixes and word starts above scattered hits', () => {
    const labels = rank('inst', [
      item('MaintenanceStats'),
      item('Instances'),
      item('my_instance'),
    ]).map((r) => r.item.label);
    expect(labels).toEqual(['Instances', 'my_instance', 'MaintenanceStats']);
  });

  it('builds highlight segments from hit positions', () => {
    expect(toSegments('Demo', [0, 1])).toEqual([
      { text: 'De', hit: true },
      { text: 'mo', hit: false },
    ]);
  });

  it('reads helix and cluster from the url', () => {
    expect(contextFromUrl('/helix.default/My%20Cluster/instances?x=1')).toEqual(
      {
        helix: 'helix.default',
        cluster: 'My Cluster',
      },
    );
    expect(contextFromUrl('/')).toEqual({
      helix: undefined,
      cluster: undefined,
    });
  });
});

describe('CommandPaletteComponent', () => {
  const key = (k: string, mods: Partial<KeyboardEvent> = {}) =>
    ({
      key: k,
      preventDefault: jest.fn(),
      ...mods,
    }) as unknown as KeyboardEvent;

  function create() {
    localStorage.clear();
    const router = { url: '/helix.default/Demo/overview', navigate: jest.fn() };
    const service = {
      clusters: jest.fn(() => of(['Demo', 'Other'])),
      resources: jest.fn(() => of(['db'])),
      instances: jest.fn(() => of(['host_1'])),
    };
    return {
      router,
      service,
      c: new CommandPaletteComponent(router as any, service as any),
    };
  }

  it('toggles on Cmd+K or Ctrl+K and closes on Escape', () => {
    const { c } = create();
    c.onKey(key('k', { metaKey: true }));
    expect(c.isOpen).toBe(true);
    c.onKey(key('K', { ctrlKey: true }));
    expect(c.isOpen).toBe(false);
    c.onKey(key('k'));
    expect(c.isOpen).toBe(false);
    c.open();
    c.onInputKey(key('Escape'));
    expect(c.isOpen).toBe(false);
  });

  it('loads pages and clusters on open, resources and instances only after 2 characters', () => {
    const { c, service } = create();
    c.open();
    expect(new Set(c.results.map((r) => r.item.kind))).toEqual(
      new Set(['page', 'cluster']),
    );
    expect(service.instances).not.toHaveBeenCalled();
    c.onQuery('h');
    expect(service.instances).not.toHaveBeenCalled();
    c.onQuery('ho');
    c.onQuery('hos');
    expect(service.instances).toHaveBeenCalledTimes(1);
    expect(service.resources).toHaveBeenCalledTimes(1);
    expect(c.results.map((r) => r.item.label)).toContain('host_1');
  });

  it('clears the loading state when closed mid-fetch', () => {
    const { c, service } = create();
    service.clusters = jest.fn(() => new Subject<string[]>());
    c.open();
    expect(c.loading).toBe(1);
    c.close();
    c.open();
    expect(c.loading).toBe(1);
  });

  it('moves with the arrows, wraps, and opens the active result on Enter', () => {
    const { c, router } = create();
    c.open();
    c.onQuery('host');
    expect(c.results.map((r) => r.item.label)).toEqual(['host_1']);
    c.onQuery('o');
    c.onInputKey(key('ArrowUp'));
    expect(c.active).toBe(c.results.length - 1);
    c.onQuery('host');
    c.onInputKey(key('Enter'));
    expect(router.navigate).toHaveBeenCalledWith([
      '/',
      'helix.default',
      'Demo',
      'instances',
      'host_1',
    ]);
    expect(c.isOpen).toBe(false);
  });

  it('lists recently opened items first on the next open', () => {
    const { c } = create();
    c.open();
    c.onQuery('db');
    c.onInputKey(key('Enter'));
    c.open();
    expect(c.results[0].item.label).toBe('db');
    expect(c.isRecent(c.results[0])).toBe(true);
    expect(c.results.filter((r) => r.item.label === 'db').length).toBe(1);
  });
});

describe('PaletteService', () => {
  it('caches names for 5 minutes and refetches after expiry or an error', () => {
    let now = 1000;
    jest.spyOn(Date, 'now').mockImplementation(() => now);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    const get = jest.fn(() => of({ clusters: ['b', 'a'] }));
    const svc = new PaletteService({ url: '/' } as any, { get } as any);
    let names: string[] = [];
    svc.clusters('h').subscribe((n) => (names = n));
    svc.clusters('h').subscribe();
    expect(names).toEqual(['a', 'b']);
    expect(get).toHaveBeenCalledTimes(1);
    now += 5 * 60 * 1000;
    svc.clusters('h').subscribe();
    expect(get).toHaveBeenCalledTimes(2);
    get.mockImplementationOnce(() => throwError(() => 'boom') as any);
    svc.clusters('x').subscribe((n) => (names = n));
    expect(names).toEqual([]);
    svc.clusters('x').subscribe();
    expect(get).toHaveBeenCalledTimes(4);
    jest.restoreAllMocks();
  });
});
