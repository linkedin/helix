import {
  Component,
  OnDestroy,
  OnInit,
  Input,
  Optional,
  Output,
  ViewChild,
  ElementRef,
  EventEmitter,
} from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';

import { InlineEditGroup } from './inline-edit-group';

@Component({
  selector: 'hi-input-inline',
  templateUrl: './input-inline.component.html',
  styleUrls: ['./input-inline.component.scss'],
})
export class InputInlineComponent
  implements ControlValueAccessor, OnInit, OnDestroy
{
  // the input only exists while editing, so the query cannot be static
  @ViewChild('inputControl') inputControl: ElementRef;

  @Output('update') change: EventEmitter<string> = new EventEmitter<string>();

  @Input() label = '';
  @Input() min = -9999;
  @Input() max = 99999999;
  @Input() minlength = 0;
  @Input() maxlength = 2555;
  @Input() type = 'text';
  @Input() required = false;
  @Input() pattern: string = null;
  @Input() errorLabel = 'Invalid input value';
  @Input() editLabel = 'Click to edit';
  @Input() disabled = false;

  editing = false;
  // a save is waiting for the parent's confirmation
  pending = false;

  private _value = '';

  private lastValue = '';

  // what was typed in the last confirmed save, kept until the field shows a
  // new value, so a rejected write can be retried without retyping
  private draft: { from: string; value: string } | null = null;

  // Required forControlValueAccessor interface
  public onChange: any = Function.prototype;
  public onTouched: any = Function.prototype;
  @Input()
  get value(): any {
    return this._value;
  }
  set value(v: any) {
    if (v !== this._value) {
      this._value = v;
      this.onChange(v);
    }
  }
  @Input() focus: Function = (_) => {};
  @Input() blur: Function = (_) => {};
  public registerOnChange(fn: (_: any) => {}): void {
    this.onChange = fn;
  }
  public registerOnTouched(fn: () => {}): void {
    this.onTouched = fn;
  }
  writeValue(value: any) {
    this._value = value;
  }

  private readonly group: InlineEditGroup;

  constructor(@Optional() group?: InlineEditGroup) {
    // outside a table each field is its own group
    this.group = group || new InlineEditGroup();
  }

  ngOnInit() {}

  ngOnDestroy() {
    this.group.release(this);
  }

  hasError() {
    const exp = new RegExp(this.pattern);

    if (!this.value) {
      return this.required;
    }

    if (this.pattern && !exp.test(this.value)) {
      return true;
    }

    return false;
  }

  isChanged() {
    return this.value !== this.lastValue;
  }

  edit(value) {
    if (this.disabled || this.editing) {
      return;
    }

    // only one field in a table is edited at a time, so a save is one key
    this.group.claim(this);
    this.lastValue = value;
    if (this.draft && this.draft.from === value) {
      this.value = this.draft.value;
    }
    this.draft = null;
    this.editing = true;
    this.focusInput();
  }

  // Asks the parent to save; the editor stays open with the typed value until
  // the parent reports the user's answer through confirmed() or declined().
  save() {
    if (this.pending || this.hasError() || !this.isChanged()) {
      return;
    }
    this.pending = true;
    this.change.emit(this.value);
  }

  // The write was sent. Show the stored value until the parent reloads, and
  // keep the typed one in case the write is rejected.
  confirmed() {
    this.draft = { from: this.lastValue, value: this.value };
    this.value = this.lastValue;
    this.close();
  }

  // The user backed out of the confirmation, so let them keep editing.
  declined() {
    this.pending = false;
    this.focusInput();
  }

  cancel() {
    this.value = this.lastValue;
    this.draft = null;
    this.close();
  }

  private close() {
    this.pending = false;
    this.editing = false;
    this.group.release(this);
  }

  private focusInput() {
    setTimeout((_) => {
      this.inputControl?.nativeElement.focus();
    });
  }
}
