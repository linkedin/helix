import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';

import { Settings } from '../../core/settings';
import { InputDialogComponent } from '../dialog/input-dialog/input-dialog.component';
import { ConfirmDialogComponent } from '../dialog/confirm-dialog/confirm-dialog.component';
import { InlineEditGroup } from '../input-inline/inline-edit-group';

// what the inline editor needs to hear back after the confirmation
interface Editor {
  confirmed(): void;
  declined(): void;
}

@Component({
  selector: 'hi-data-table',
  templateUrl: './data-table.component.html',
  styleUrls: ['./data-table.component.scss'],
  providers: [InlineEditGroup],
})
export class DataTableComponent implements OnInit {
  @Input() rows = [];
  @Input() columns = [];
  @Input() sorts = [];
  @Input() deletable = false;
  @Input() insertable = false;
  // the config key a nested list or map table belongs to, so the confirmation names it
  @Input() keyName?: string;

  @Output() update: EventEmitter<any> = new EventEmitter<any>();
  @Output() create: EventEmitter<any> = new EventEmitter<any>();
  @Output() delete: EventEmitter<any> = new EventEmitter<any>();

  rowHeight = Settings.tableRowHeight;

  constructor(protected dialog: MatDialog) {}

  ngOnInit() {}

  onEdited(row, column, value, editor?: Editor) {
    const prop = this.getPropName(column);

    // only emit when value changes, and only after the user confirms it
    if (row[prop] === value) {
      editor?.declined();
      return;
    }

    const inner = row.name !== undefined ? row.name : null;
    const name = this.keyName
      ? inner !== null
        ? `${this.keyName}.${inner}`
        : this.keyName
      : (inner ?? column.name);
    this.dialog
      .open(ConfirmDialogComponent, {
        data: {
          title: 'Save change?',
          message: `Change "${name}" from "${row[prop]}" to "${value}"?`,
          confirmButtonText: 'Save',
        },
        // the editor takes focus back itself; Save is disabled while it waits
        restoreFocus: false,
      })
      .afterClosed()
      .subscribe((result) => {
        if (result) {
          editor?.confirmed();
          this.update.emit({
            row,
            column,
            value,
          });
        } else {
          editor?.declined();
        }
      });
  }

  onCreate() {
    const data = {
      title: 'Create a new item',
      message: 'Please enter the following information to continue:',
      values: {},
    };

    for (const column of this.columns) {
      const prop = this.getPropName(column);
      data.values[prop] = {
        label: column.name,
      };
    }

    this.dialog
      .open(InputDialogComponent, {
        data,
      })
      .afterClosed()
      .subscribe((result) => {
        if (result) {
          this.create.emit(result);
        }
      });
  }

  onDelete(row) {
    this.dialog
      .open(ConfirmDialogComponent, {
        data: {
          title: 'Confirmation',
          message: 'Are you sure you want to delete this configuration?',
        },
      })
      .afterClosed()
      .subscribe((result) => {
        if (result) {
          this.delete.emit({
            row,
          });
        }
      });
  }

  getPropName(column): string {
    return column.prop ? column.prop : column.name.toLowerCase();
  }
}
