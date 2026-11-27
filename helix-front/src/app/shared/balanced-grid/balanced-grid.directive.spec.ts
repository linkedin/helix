import { describe, expect, it } from '@jest/globals';
import { ElementRef } from '@angular/core';

import {
  BalancedGridDirective,
  balancedColumns,
} from './balanced-grid.directive';

describe('balancedColumns', () => {
  it('keeps rows even instead of leaving one card on its own', () => {
    expect(balancedColumns(7, 6)).toBe(4);
    expect(balancedColumns(7, 5)).toBe(4);
    expect(balancedColumns(7, 3)).toBe(3);
    expect(balancedColumns(5, 4)).toBe(3);
  });

  it('uses one row when everything fits, and one column when nothing does', () => {
    expect(balancedColumns(7, 9)).toBe(7);
    expect(balancedColumns(3, 3)).toBe(3);
    expect(balancedColumns(4, 0)).toBe(1);
    expect(balancedColumns(0, 4)).toBe(1);
  });
});

describe('BalancedGridDirective', () => {
  function grid(width: number, children: number, min: number | '' = 170) {
    const host = document.createElement('div');
    host.style.display = 'grid';
    host.style.columnGap = '12px';
    for (let i = 0; i < children; i++)
      host.appendChild(document.createElement('div'));
    Object.defineProperty(host, 'clientWidth', { value: width });
    const directive = new BalancedGridDirective(new ElementRef(host));
    directive.hiBalancedGrid = min;
    directive.layout();
    return host.style.gridTemplateColumns;
  }

  it('sets the column count from the width and the child count', () => {
    // 1050px fits 5 cards of 170px, so seven become 4 + 3
    expect(grid(1050, 7)).toBe('repeat(4, minmax(0, 1fr))');
    expect(grid(1400, 7)).toBe('repeat(7, minmax(0, 1fr))');
    expect(grid(360, 7)).toBe('repeat(2, minmax(0, 1fr))');
  });

  it('leaves the stylesheet columns alone until it can measure', () => {
    expect(grid(0, 7)).toBe('');
  });
});
