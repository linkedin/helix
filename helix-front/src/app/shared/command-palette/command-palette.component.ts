import {
  Component,
  ElementRef,
  HostListener,
  OnDestroy,
  ViewChild,
} from '@angular/core';
import { Router } from '@angular/router';
import { Observable, Subscription } from 'rxjs';

import {
  CLUSTER_PAGES,
  PaletteItem,
  PaletteKind,
  Ranked,
  contextFromUrl,
  rank,
} from './palette';
import { PaletteService } from './palette.service';

const RECENT_KEY = 'hi-palette-recent';
const DEEP_QUERY_MIN = 2;
const ICONS: Record<PaletteKind, string> = {
  page: 'tab',
  cluster: 'device_hub',
  resource: 'storage',
  instance: 'dns',
};

@Component({
  selector: 'hi-command-palette',
  templateUrl: './command-palette.component.html',
  styleUrls: ['./command-palette.component.scss'],
  providers: [PaletteService],
})
export class CommandPaletteComponent implements OnDestroy {
  @ViewChild('input') input: ElementRef<HTMLInputElement>;
  @ViewChild('list') list: ElementRef<HTMLElement>;

  readonly icons = ICONS;
  readonly shortcut = /Mac|iPhone|iPad/.test(navigator.platform)
    ? '⌘K'
    : 'Ctrl K';
  isOpen = false;
  query = '';
  active = 0;
  results: Ranked[] = [];
  loading = 0;

  private items: PaletteItem[] = [];
  private groups: PaletteItem[][] = [];
  private deepLoaded = false;
  private recent: PaletteItem[] = [];
  private subs: Subscription[] = [];
  private returnFocus: HTMLElement | null = null;

  constructor(
    private router: Router,
    private service: PaletteService,
  ) {}

  @HostListener('document:keydown', ['$event'])
  onKey(event: KeyboardEvent) {
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      this.isOpen ? this.close() : this.open();
    }
  }

  open() {
    if (this.isOpen) return;
    this.returnFocus = document.activeElement as HTMLElement;
    this.isOpen = true;
    this.query = '';
    this.active = 0;
    this.recent = this.readRecent();
    this.load();
    setTimeout(() => this.input?.nativeElement.focus());
  }

  close() {
    if (!this.isOpen) return;
    this.isOpen = false;
    this.subs.forEach((s) => s.unsubscribe());
    this.subs = [];
    this.loading = 0;
    this.returnFocus?.focus?.();
  }

  ngOnDestroy() {
    this.subs.forEach((s) => s.unsubscribe());
  }

  onInputKey(event: KeyboardEvent) {
    const n = this.results.length;
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      if (n)
        this.setActive(
          (this.active + (event.key === 'ArrowDown' ? 1 : n - 1)) % n,
        );
    } else if (event.key === 'Enter') {
      event.preventDefault();
      if (this.results[this.active]) this.go(this.results[this.active].item);
    } else if (event.key === 'Escape') {
      event.preventDefault();
      this.close();
    }
  }

  onQuery(query: string) {
    this.query = query;
    this.active = 0;
    this.loadDeep();
    this.refresh();
  }

  setActive(i: number) {
    this.active = i;
    setTimeout(() =>
      this.list?.nativeElement
        .querySelector(`#hi-palette-opt-${i}`)
        ?.scrollIntoView({ block: 'nearest' }),
    );
  }

  go(item: PaletteItem) {
    this.recent = [
      item,
      ...this.recent.filter((r) => r.route.join('/') !== item.route.join('/')),
    ].slice(0, 5);
    try {
      localStorage.setItem(RECENT_KEY, JSON.stringify(this.recent));
    } catch (e) {}
    this.close();
    this.router.navigate(['/', ...item.route]);
  }

  isRecent(r: Ranked) {
    return !this.query.trim() && this.recent.includes(r.item);
  }

  private load() {
    const { helix, cluster } = contextFromUrl(this.router.url);
    const pages: PaletteItem[] = cluster
      ? CLUSTER_PAGES.map((p) => ({
          kind: 'page',
          label: p,
          hint: cluster,
          route: [helix, cluster, p.toLowerCase()],
        }))
      : [];
    this.groups = [pages, [], [], []];
    this.deepLoaded = !cluster;
    this.items = pages;
    this.refresh();
    if (!helix) return;
    this.add(1, this.service.clusters(helix), (n) => ({
      kind: 'cluster',
      label: n,
      hint: 'Cluster',
      route: [helix, n],
    }));
  }

  // Resource and instance lists can be thousands of names, and helix-rest
  // reads every InstanceConfig to build /instances, so fetch them only once
  // the user is clearly searching rather than picking a page.
  private loadDeep() {
    if (this.deepLoaded || this.query.trim().length < DEEP_QUERY_MIN) return;
    this.deepLoaded = true;
    const { helix, cluster } = contextFromUrl(this.router.url);
    this.add(2, this.service.resources(helix, cluster), (n) => ({
      kind: 'resource',
      label: n,
      hint: `Resource in ${cluster}`,
      route: [helix, cluster, 'resources', n],
    }));
    this.add(3, this.service.instances(helix, cluster), (n) => ({
      kind: 'instance',
      label: n,
      hint: `Instance in ${cluster}`,
      route: [helix, cluster, 'instances', n],
    }));
  }

  private add(
    slot: number,
    names$: Observable<string[]>,
    make: (n: string) => PaletteItem,
  ) {
    this.loading++;
    this.subs.push(
      names$.subscribe((names) => {
        this.loading--;
        this.groups[slot] = names.map(make);
        this.items = [].concat(...this.groups);
        this.refresh();
      }),
    );
  }

  private refresh() {
    const { helix } = contextFromUrl(this.router.url);
    const recent = this.recent.filter((r) => r.route[0] === helix);
    const pool = this.query.trim()
      ? this.items
      : [
          ...recent,
          ...this.items.filter(
            (i) => !recent.some((r) => r.route.join('/') === i.route.join('/')),
          ),
        ];
    this.results = rank(this.query, pool);
    this.active = Math.max(0, Math.min(this.active, this.results.length - 1));
  }

  private readRecent(): PaletteItem[] {
    try {
      const v = JSON.parse(localStorage.getItem(RECENT_KEY) || '[]');
      return Array.isArray(v)
        ? v.filter((r) => r && Array.isArray(r.route) && ICONS[r.kind])
        : [];
    } catch (e) {
      return [];
    }
  }
}
