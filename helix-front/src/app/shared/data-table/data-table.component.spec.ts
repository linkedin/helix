import { describe, expect, it, jest } from '@jest/globals';
import { MatDialog } from '@angular/material/dialog';
import { of } from 'rxjs';

import { DataTableComponent } from './data-table.component';

describe('DataTableComponent', () => {
  function table(answer: boolean) {
    const open = jest.fn(() => ({ afterClosed: () => of(answer) }));
    const component = new DataTableComponent({ open } as unknown as MatDialog);
    const updates = [];
    component.update.subscribe((e) => updates.push(e));
    const editor = { confirmed: jest.fn(), declined: jest.fn() };
    return { component, open, updates, editor };
  }
  const row = { name: 'MAX_OFFLINE', value: '5' };
  const column = { name: 'Value' };

  it('writes only after the change is confirmed', () => {
    const { component, open, updates, editor } = table(true);
    component.onEdited(row, column, '6', editor);

    expect(open).toHaveBeenCalledTimes(1);
    expect((open.mock.calls[0] as any)[1].data.message).toBe(
      'Change "MAX_OFFLINE" from "5" to "6"?',
    );
    expect(updates).toEqual([{ row, column, value: '6' }]);
    expect(editor.confirmed).toHaveBeenCalled();
  });

  it('writes nothing when the confirmation is dismissed', () => {
    const { component, updates, editor } = table(false);
    component.onEdited(row, column, '6', editor);

    expect(updates).toEqual([]);
    expect(editor.declined).toHaveBeenCalled();
    expect(editor.confirmed).not.toHaveBeenCalled();
  });

  it('names the config key of a nested list or map row', () => {
    const { component, open } = table(true);
    component.keyName = 'PREFERENCE_LIST';
    component.onEdited({ value: 'a' }, column, 'b');
    component.keyName = 'CAPACITY';
    component.onEdited({ name: 'CPU', value: '4' }, column, '8');

    expect(open.mock.calls.map((c: any) => c[1].data.message)).toEqual([
      'Change "PREFERENCE_LIST" from "a" to "b"?',
      'Change "CAPACITY.CPU" from "4" to "8"?',
    ]);
  });

  it('asks nothing for an unchanged value', () => {
    const { component, open, updates } = table(true);
    component.onEdited(row, column, '5');

    expect(open).not.toHaveBeenCalled();
    expect(updates).toEqual([]);
  });
});
