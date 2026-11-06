import {
  Component,
  OnInit,
  Input,
  Output,
  ViewChild,
  ElementRef,
  EventEmitter,
} from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';

@Component({
  selector: 'hi-input-inline',
  templateUrl: './input-inline.component.html',
  styleUrls: ['./input-inline.component.scss'],
})
export class InputInlineComponent implements ControlValueAccessor, OnInit {
  @ViewChild('inputControl', { static: true }) inputControl: ElementRef;

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

  private _value = '';

  private lastValue = '';

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

  constructor() {}

  ngOnInit() {}

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

  // Only one field may be edited at a time, so a change is always a single,
  // reviewable key.
  private static active: InputInlineComponent = null;

  isChanged() {
    return this.value !== this.lastValue;
  }

  edit(value) {
    if (this.disabled || this.editing) {
      return;
    }

    if (InputInlineComponent.active && InputInlineComponent.active !== this) {
      InputInlineComponent.active.cancel();
    }
    InputInlineComponent.active = this;

    this.lastValue = value;
    this.editing = true;
    setTimeout((_) => {
      this.inputControl?.nativeElement.focus();
    });
  }

  save() {
    if (this.hasError()) {
      return;
    }

    this.editing = false;
    this.release();
    if (this.isChanged()) {
      this.change.emit(this.value);
      // the parent confirms and reloads; show the old value until it does
      this.value = this.lastValue;
    }
  }

  cancel() {
    this.value = this.lastValue;
    this.editing = false;
    this.release();
  }

  private release() {
    if (InputInlineComponent.active === this) {
      InputInlineComponent.active = null;
    }
  }
}
