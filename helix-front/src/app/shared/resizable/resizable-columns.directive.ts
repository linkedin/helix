import {
  AfterViewInit,
  Directive,
  ElementRef,
  Input,
  NgZone,
  OnDestroy,
  Renderer2,
} from '@angular/core';

import { readWidths, writeWidths } from './column-widths';

export const MIN_COLUMN_WIDTH = 48;
const KEY_STEP = 16;

// Makes a plain <table> behave like a production data grid: it scrolls sideways
// inside its own box instead of widening the page, and each header cell gets a
// handle on its right edge that resizes the column by dragging or with the arrow
// keys. Widths are kept per table under the given key, and a double click on a
// handle restores the natural layout.
@Directive({ selector: 'table[hiResizableColumns]' })
export class ResizableColumnsDirective implements AfterViewInit, OnDestroy {
  // storage key, e.g. 'capacity'; widths are not kept when it is empty
  @Input() hiResizableColumns = '';

  private readonly table: HTMLTableElement;
  private scroller: HTMLElement | null = null;
  private observer: MutationObserver | null = null;
  // header names last laid out, so a change in the columns re-pins the table
  private columns = '';
  private readonly unlisten: (() => void)[] = [];

  constructor(
    el: ElementRef<HTMLTableElement>,
    private readonly renderer: Renderer2,
    private readonly zone: NgZone,
  ) {
    this.table = el.nativeElement;
  }

  ngAfterViewInit(): void {
    const parent = this.renderer.parentNode(this.table);
    if (parent) {
      this.scroller = this.renderer.createElement('div');
      this.renderer.addClass(this.scroller, 'hi-table-scroll');
      this.renderer.insertBefore(parent, this.scroller, this.table);
      this.renderer.appendChild(this.scroller, this.table);
    }
    this.renderer.addClass(this.table, 'hi-resizable');
    this.zone.runOutsideAngular(() => {
      this.attach();
      // header cells come and go with *ngIf, e.g. a capacity column
      this.observer = new MutationObserver(() => this.attach());
      this.observer.observe(this.table, { childList: true, subtree: true });
    });
  }

  ngOnDestroy(): void {
    this.observer?.disconnect();
    this.unlisten.forEach((off) => off());
    // Angular removes the table through its current parent, the wrapper, which
    // it never knew about, so the wrapper is ours to remove
    this.scroller?.remove();
  }

  private headers(): HTMLTableCellElement[] {
    return Array.from(this.table.tHead?.rows[0]?.cells || []);
  }

  private name(th: HTMLTableCellElement): string {
    return (th.dataset.column || th.textContent || '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private attach(): void {
    const headers = this.headers();
    headers
      .filter((th) => !th.querySelector(':scope > .hi-col-handle'))
      .forEach((th) => this.addHandle(th));
    const saved = this.hiResizableColumns
      ? readWidths(this.hiResizableColumns)
      : {};
    const columns = headers.map((th) => this.name(th)).join('\n');
    const changed = columns !== this.columns;
    this.columns = columns;
    if (!this.table.classList.contains('hi-columns-fixed')) {
      if (headers.some((th) => saved[this.name(th)] > 0)) {
        this.freeze(saved);
      }
    } else if (changed) {
      // a column added or removed after the table was pinned, e.g. capacity
      // keys appearing, gets its saved or natural width and the total follows
      headers
        .filter((th) => !th.style.width)
        .forEach((th) =>
          this.setWidth(
            th,
            saved[this.name(th)] || th.getBoundingClientRect().width,
          ),
        );
      this.syncTableWidth();
    }
  }

  private addHandle(th: HTMLTableCellElement): void {
    const handle: HTMLElement = this.renderer.createElement('span');
    handle.className = 'hi-col-handle';
    handle.tabIndex = 0;
    handle.setAttribute('role', 'separator');
    handle.setAttribute('aria-orientation', 'vertical');
    handle.setAttribute('aria-label', `Resize ${this.name(th) || 'column'}`);
    handle.title =
      'Drag or use the arrow keys to resize. Double click to reset.';
    th.appendChild(handle);

    const stop = (e: Event) => e.stopPropagation();
    this.unlisten.push(
      // a click on the handle must not sort the column
      this.renderer.listen(handle, 'click', stop),
      this.renderer.listen(handle, 'dblclick', (e: Event) => {
        stop(e);
        this.reset();
      }),
      this.renderer.listen(handle, 'keydown', (e: KeyboardEvent) => {
        const step =
          e.key === 'ArrowRight'
            ? KEY_STEP
            : e.key === 'ArrowLeft'
              ? -KEY_STEP
              : 0;
        if (step) {
          e.preventDefault();
          this.freeze();
          this.setWidth(th, this.widthOf(th) + step);
          this.save();
        }
      }),
      this.renderer.listen(handle, 'pointerdown', (e: PointerEvent) =>
        this.drag(e, th, handle),
      ),
    );
  }

  private drag(
    e: PointerEvent,
    th: HTMLTableCellElement,
    handle: HTMLElement,
  ): void {
    if (e.button !== 0) {
      return;
    }
    e.preventDefault();
    e.stopPropagation();
    this.freeze();
    const startX = e.clientX;
    const startWidth = this.widthOf(th);
    handle.classList.add('active');
    document.body.classList.add('hi-col-resizing');
    const move = (ev: PointerEvent) =>
      this.setWidth(th, startWidth + ev.clientX - startX);
    const up = () => {
      document.removeEventListener('pointermove', move);
      document.removeEventListener('pointerup', up);
      document.removeEventListener('pointercancel', up);
      handle.classList.remove('active');
      document.body.classList.remove('hi-col-resizing');
      this.save();
    };
    document.addEventListener('pointermove', move);
    document.addEventListener('pointerup', up);
    document.addEventListener('pointercancel', up);
  }

  // Pins every column at its current (or saved) width and switches to a fixed
  // layout, so dragging one column never reflows the others.
  private freeze(saved: Record<string, number> = {}): void {
    if (this.table.classList.contains('hi-columns-fixed')) {
      return;
    }
    const headers = this.headers();
    const widths = headers.map(
      (th) => saved[this.name(th)] || th.getBoundingClientRect().width,
    );
    headers.forEach(
      (th, i) =>
        (th.style.width = `${Math.max(MIN_COLUMN_WIDTH, widths[i])}px`),
    );
    this.table.classList.add('hi-columns-fixed');
    this.syncTableWidth();
  }

  // the pinned width once a column is fixed, else what the layout gave it
  private widthOf(th: HTMLTableCellElement): number {
    return parseFloat(th.style.width) || th.getBoundingClientRect().width;
  }

  private setWidth(th: HTMLTableCellElement, width: number): void {
    th.style.width = `${Math.round(Math.max(MIN_COLUMN_WIDTH, width))}px`;
    th.querySelector('.hi-col-handle')?.setAttribute(
      'aria-valuenow',
      String(Math.round(Math.max(MIN_COLUMN_WIDTH, width))),
    );
    this.syncTableWidth();
  }

  private syncTableWidth(): void {
    const total = this.headers().reduce((sum, th) => sum + this.widthOf(th), 0);
    this.table.style.width = `${Math.round(total)}px`;
  }

  private save(): void {
    if (!this.hiResizableColumns) {
      return;
    }
    const widths: Record<string, number> = {};
    this.headers().forEach((th) => {
      const name = this.name(th);
      if (name && th.style.width) {
        widths[name] = Math.round(parseFloat(th.style.width));
      }
    });
    writeWidths(this.hiResizableColumns, widths);
  }

  private reset(): void {
    this.headers().forEach((th) => (th.style.width = ''));
    this.table.style.width = '';
    this.table.classList.remove('hi-columns-fixed');
    if (this.hiResizableColumns) {
      writeWidths(this.hiResizableColumns, null);
    }
  }
}
