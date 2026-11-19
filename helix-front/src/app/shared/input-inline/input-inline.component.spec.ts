import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';

import { InlineEditGroup } from './inline-edit-group';
import { InputInlineComponent } from './input-inline.component';

describe('InputInlineComponent', () => {
  let group: InlineEditGroup;

  beforeEach(() => {
    jest.useFakeTimers();
    group = new InlineEditGroup();
  });

  afterEach(() => jest.useRealTimers());

  function editing(value: string, in_ = group): InputInlineComponent {
    const c = new InputInlineComponent(in_);
    c.writeValue(value);
    c.edit(value);
    return c;
  }

  function emitted(c: InputInlineComponent): string[] {
    const out: string[] = [];
    c.change.subscribe((v) => out.push(v));
    return out;
  }

  it('asks to save only a changed value, and stays open until answered', () => {
    const c = editing('true');
    const out = emitted(c);
    c.save();
    expect(out).toEqual([]);

    c.value = 'false';
    c.save();
    c.save();
    expect(out).toEqual(['false']);
    expect([c.editing, c.pending, c.value]).toEqual([true, true, 'false']);
  });

  it('keeps the typed value when the confirmation is declined', () => {
    const c = editing('1');
    const out = emitted(c);
    c.value = '2';
    c.save();
    c.declined();
    expect([c.editing, c.pending, c.value]).toEqual([true, false, '2']);
    c.save();
    expect(out).toEqual(['2', '2']);
  });

  it('shows the stored value after a confirmed save and restores the typed one on retry', () => {
    const c = editing('1');
    c.value = '2';
    c.save();
    c.confirmed();
    expect([c.editing, c.value]).toEqual([false, '1']);

    // the write was rejected, so the field still shows 1
    c.edit('1');
    expect(c.value).toBe('2');
  });

  it('drops the typed value once the field shows a new one', () => {
    const c = editing('1');
    c.value = '2';
    c.save();
    c.confirmed();
    c.writeValue('3');
    c.edit('3');
    expect(c.value).toBe('3');
  });

  it('discard restores the value and emits nothing', () => {
    const c = editing('1');
    const out = emitted(c);
    c.value = '2';
    c.cancel();
    expect([c.value, c.editing]).toEqual(['1', false]);
    expect(out).toEqual([]);
  });

  it('edits one field at a time within a table, but not across tables', () => {
    const a = editing('a');
    a.value = 'changed';
    const b = editing('b');
    expect([a.editing, a.value, b.editing]).toEqual([false, 'a', true]);

    const other = editing('c', new InlineEditGroup());
    expect([b.editing, other.editing]).toEqual([true, true]);
  });

  it('lets go of the table when destroyed mid-edit', () => {
    const a = editing('a');
    a.ngOnDestroy();
    const cancel = jest.spyOn(a, 'cancel');
    editing('b');
    expect(cancel).not.toHaveBeenCalled();
  });
});
