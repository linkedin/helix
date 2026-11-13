import { describe, expect, it } from '@jest/globals';

import { InputInlineComponent } from './input-inline.component';

function editing(value: string): InputInlineComponent {
  const c = new InputInlineComponent();
  c.writeValue(value);
  c.edit(value);
  return c;
}

describe('InputInlineComponent', () => {
  it('emits only on an explicit save of a changed value', () => {
    const c = editing('true');
    const emitted: string[] = [];
    c.change.subscribe((v) => emitted.push(v));

    c.save();
    expect(emitted).toEqual([]);

    const d = editing('true');
    d.change.subscribe((v) => emitted.push(v));
    d.value = 'false';
    d.save();
    expect(emitted).toEqual(['false']);
    expect(d.value).toBe('true');
  });

  it('discard restores the value and emits nothing', () => {
    const c = editing('1');
    const emitted: string[] = [];
    c.change.subscribe((v) => emitted.push(v));
    c.value = '2';
    c.cancel();
    expect(c.value).toBe('1');
    expect(c.editing).toBe(false);
    expect(emitted).toEqual([]);
  });

  it('allows a single field to be edited at a time', () => {
    const a = editing('a');
    a.value = 'changed';
    const b = editing('b');
    expect(a.editing).toBe(false);
    expect(a.value).toBe('a');
    expect(b.editing).toBe(true);
  });
});
