import { Injectable } from '@angular/core';

// The one field being edited in a table. Each hi-data-table provides its own,
// so two tables on the screen never cancel each other's edit.
@Injectable()
export class InlineEditGroup {
  private active: { cancel(): void } | null = null;

  claim(editor: { cancel(): void }) {
    if (this.active && this.active !== editor) {
      this.active.cancel();
    }
    this.active = editor;
  }

  release(editor: { cancel(): void }) {
    if (this.active === editor) {
      this.active = null;
    }
  }
}
