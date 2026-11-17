import 'zone.js';
import 'zone.js/dist/zone-testing';

import { Component } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from '@jest/globals';

import { ColumnWidths } from './column-widths';
import { ResizableColumnsDirective } from './resizable-columns.directive';

@Component({
  template: `
    <section>
      <table *ngIf="shown" hiResizableColumns="test">
        <thead>
          <tr>
            <th>Instance</th>
            <th>Zone</th>
            <th *ngIf="extra">Capacity</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>n1</td>
            <td>z1</td>
          </tr>
        </tbody>
      </table>
    </section>
  `,
})
class HostComponent {
  shown = true;
  extra = false;
}

describe('ResizableColumnsDirective', () => {
  beforeEach(() => {
    localStorage.clear();
    TestBed.configureTestingModule({
      declarations: [HostComponent, ResizableColumnsDirective],
    });
  });

  function render() {
    const fixture = TestBed.createComponent(HostComponent);
    fixture.detectChanges();
    const el: HTMLElement = fixture.nativeElement;
    const handles = () =>
      Array.from(el.querySelectorAll<HTMLElement>('th .hi-col-handle'));
    const key = (h: HTMLElement, k: string) =>
      h.dispatchEvent(new KeyboardEvent('keydown', { key: k }));
    return { fixture, el, handles, key };
  }

  it('wraps the table in its own scroll box and gives every header a handle', () => {
    const { el, handles } = render();
    expect(el.querySelector('section > .hi-table-scroll > table')).toBeTruthy();
    expect(handles().map((h) => h.getAttribute('aria-label'))).toEqual([
      'Resize Instance',
      'Resize Zone',
    ]);
  });

  it('resizes with the arrow keys, keeps the width and restores it next time', () => {
    const first = render();
    const [instance] = first.handles();
    first.key(instance, 'ArrowRight');
    first.key(instance, 'ArrowRight');
    const width = parseFloat(
      first.el.querySelector<HTMLElement>('th').style.width,
    );
    expect(width).toBeGreaterThan(48);
    expect(first.el.querySelector('table').classList).toContain(
      'hi-columns-fixed',
    );

    const second = render();
    expect(second.el.querySelector<HTMLElement>('th').style.width).toBe(
      `${width}px`,
    );
  });

  it('keeps the pinned table width in step when a column comes or goes', async () => {
    const { fixture, el, handles, key } = render();
    key(handles()[0], 'ArrowRight');
    const table = el.querySelector('table');
    const total = () => parseFloat(table.style.width);
    const pinned = total();

    fixture.componentInstance.extra = true;
    fixture.detectChanges();
    await new Promise((done) => setTimeout(done));
    const added = el.querySelectorAll<HTMLElement>('th')[2];
    expect(parseFloat(added.style.width)).toBe(48);
    expect(total()).toBe(pinned + 48);

    fixture.componentInstance.extra = false;
    fixture.detectChanges();
    await new Promise((done) => setTimeout(done));
    expect(total()).toBe(pinned);
  });

  it('never goes below the minimum width and resets on double click', () => {
    const { el, handles, key } = render();
    const [instance] = handles();
    for (let i = 0; i < 10; i++) {
      key(instance, 'ArrowLeft');
    }
    expect(el.querySelector<HTMLElement>('th').style.width).toBe('48px');

    instance.dispatchEvent(new MouseEvent('dblclick'));
    expect(el.querySelector<HTMLElement>('th').style.width).toBe('');
    expect(el.querySelector('table').classList).not.toContain(
      'hi-columns-fixed',
    );
    expect(localStorage.getItem('helix-columns:test')).toBeNull();
  });

  it('removes its scroll box when the table goes away', () => {
    const { fixture, el } = render();
    fixture.componentInstance.shown = false;
    fixture.detectChanges();
    expect(el.querySelector('.hi-table-scroll')).toBeNull();
  });
});

describe('ColumnWidths', () => {
  beforeEach(() => localStorage.clear());

  it('falls back until a column is resized, then remembers it next time', () => {
    const widths = new ColumnWidths('list');
    expect(widths.get('Name', 260)).toBe(260);
    widths.save({ column: { name: 'Name' }, newValue: 301.6 });
    widths.save({ column: { name: 'Zone' }, newValue: 90 });
    // the bound width does not change mid-session, which would refit the table
    expect(widths.get('Name', 260)).toBe(260);
    const next = new ColumnWidths('list');
    expect(next.get('Name', 260)).toBe(302);
    expect(next.get('Zone', 120)).toBe(90);
  });

  it('ignores storage it cannot parse', () => {
    localStorage.setItem('helix-columns:list', '{not json');
    expect(new ColumnWidths('list').get('Name', 260)).toBe(260);
  });
});
