import {
  AfterViewInit,
  Directive,
  ElementRef,
  Input,
  OnDestroy,
} from '@angular/core';

// How many columns to use for `count` items when at most `fit` fit on a line.
// Rows are kept even, so 7 cards that fit 6 to a line become 4 and 3 rather
// than 6 and 1.
export function balancedColumns(count: number, fit: number): number {
  if (count <= 0) return 1;
  const perLine = Math.max(1, fit);
  if (count <= perLine) return count;
  return Math.ceil(count / Math.ceil(count / perLine));
}

// Lays the children of a grid out in even rows. The element keeps whatever
// columns its stylesheet gives it until it has a width to measure.
@Directive({ selector: '[hiBalancedGrid]' })
export class BalancedGridDirective implements AfterViewInit, OnDestroy {
  // narrowest a child may get, in px, before a row wraps
  @Input() hiBalancedGrid: number | '' = '';

  private resize: ResizeObserver | null = null;
  private mutation: MutationObserver | null = null;

  constructor(private readonly el: ElementRef<HTMLElement>) {}

  ngAfterViewInit() {
    if (typeof ResizeObserver !== 'undefined') {
      this.resize = new ResizeObserver(() => this.layout());
      this.resize.observe(this.el.nativeElement);
    }
    if (typeof MutationObserver !== 'undefined') {
      this.mutation = new MutationObserver(() => this.layout());
      this.mutation.observe(this.el.nativeElement, { childList: true });
    }
    this.layout();
  }

  ngOnDestroy() {
    this.resize?.disconnect();
    this.mutation?.disconnect();
  }

  layout() {
    const host = this.el.nativeElement;
    const width = host.clientWidth;
    const count = host.children.length;
    if (!width || !count) return;
    const min = Number(this.hiBalancedGrid) || 180;
    const gap = parseFloat(getComputedStyle(host).columnGap) || 12;
    const fit = Math.floor((width + gap) / (min + gap));
    host.style.gridTemplateColumns = `repeat(${balancedColumns(count, fit)}, minmax(0, 1fr))`;
  }
}
