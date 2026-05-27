#!/usr/bin/env node
/**
 * Patches ng2-ace-editor type declaration files to add the Ivy (ngtsc) metadata
 * that Angular 13's AOT compiler requires. The library ships old View Engine
 * declarations; ngcc processes the JS but leaves the .d.ts files without the
 * required static ɵmod / ɵinj / ɵcmp / ɵdir fields.
 */
'use strict';

const fs = require('fs');
const path = require('path');

const base = path.join(__dirname, '..', 'node_modules', 'ng2-ace-editor', 'src');

const files = {
  'module.d.ts': `\
import * as ɵngcc0 from '@angular/core';
import * as ɵngcc1 from './component';
import * as ɵngcc2 from './directive';
export declare class AceEditorModule {
    static ɵmod: ɵngcc0.ɵɵNgModuleDeclaration<AceEditorModule, [typeof ɵngcc1.AceEditorComponent, typeof ɵngcc2.AceEditorDirective], never, [typeof ɵngcc1.AceEditorComponent, typeof ɵngcc2.AceEditorDirective]>;
    static ɵinj: ɵngcc0.ɵɵInjectorDeclaration<AceEditorModule>;
}
`,
  'component.d.ts': `\
import { EventEmitter, ElementRef, OnInit, OnDestroy, NgZone } from "@angular/core";
import { ControlValueAccessor } from "@angular/forms";
import "brace";
import "brace/theme/monokai";
import * as ɵngcc0 from '@angular/core';
export declare class AceEditorComponent implements ControlValueAccessor, OnInit, OnDestroy {
    private zone;
    textChanged: EventEmitter<{}>;
    textChange: EventEmitter<{}>;
    style: any;
    _options: any;
    _readOnly: boolean;
    _theme: string;
    _mode: any;
    _autoUpdateContent: boolean;
    _editor: any;
    _durationBeforeCallback: number;
    _text: string;
    oldText: any;
    timeoutSaving: any;
    constructor(elementRef: ElementRef, zone: NgZone);
    ngOnInit(): void;
    ngOnDestroy(): void;
    init(): void;
    initEvents(): void;
    updateText(): void;
    options: any;
    setOptions(options: any): void;
    readOnly: any;
    setReadOnly(readOnly: any): void;
    theme: any;
    setTheme(theme: any): void;
    mode: any;
    setMode(mode: any): void;
    value: string;
    writeValue(value: any): void;
    private _onChange;
    registerOnChange(fn: any): void;
    private _onTouched;
    registerOnTouched(fn: any): void;
    text: string;
    setText(text: any): void;
    autoUpdateContent: any;
    setAutoUpdateContent(status: any): void;
    durationBeforeCallback: number;
    setDurationBeforeCallback(num: number): void;
    getEditor(): any;
    static ɵcmp: ɵngcc0.ɵɵComponentDeclaration<AceEditorComponent, 'ace-editor', never, { 'style': "style"; 'options': "options"; 'readOnly': "readOnly"; 'theme': "theme"; 'mode': "mode"; 'autoUpdateContent': "autoUpdateContent"; 'durationBeforeCallback': "durationBeforeCallback"; 'text': "text"; }, { 'textChanged': "textChanged"; 'textChange': "textChange"; }, never, never>;
    static ɵfac: ɵngcc0.ɵɵFactoryDeclaration<AceEditorComponent, never>;
}
`,
  'directive.d.ts': `\
import { EventEmitter, ElementRef, OnInit, OnDestroy, NgZone } from "@angular/core";
import "brace";
import "brace/theme/monokai";
import * as ɵngcc0 from '@angular/core';
export declare class AceEditorDirective implements OnInit, OnDestroy {
    private zone;
    textChanged: EventEmitter<{}>;
    textChange: EventEmitter<{}>;
    _options: any;
    _readOnly: boolean;
    _theme: string;
    _mode: any;
    _autoUpdateContent: boolean;
    _durationBeforeCallback: number;
    _text: string;
    editor: any;
    oldText: any;
    timeoutSaving: any;
    constructor(elementRef: ElementRef, zone: NgZone);
    ngOnInit(): void;
    ngOnDestroy(): void;
    init(): void;
    initEvents(): void;
    updateText(): void;
    options: any;
    readOnly: any;
    theme: any;
    mode: any;
    setMode(mode: any): void;
    text: string;
    setText(text: any): void;
    autoUpdateContent: any;
    durationBeforeCallback: number;
    setDurationBeforeCallback(num: number): void;
    readonly aceEditor: any;
    static ɵdir: ɵngcc0.ɵɵDirectiveDeclaration<AceEditorDirective, '[ace-editor]', never, { 'options': "options"; 'readOnly': "readOnly"; 'theme': "theme"; 'mode': "mode"; 'autoUpdateContent': "autoUpdateContent"; 'durationBeforeCallback': "durationBeforeCallback"; 'text': "text"; }, { 'textChanged': "textChanged"; 'textChange': "textChange"; }, never>;
    static ɵfac: ɵngcc0.ɵɵFactoryDeclaration<AceEditorDirective, never>;
}
`,
};

let patched = 0;
for (const [file, content] of Object.entries(files)) {
  const filePath = path.join(base, file);
  if (!fs.existsSync(filePath)) {
    console.warn(`[patch-ng2-ace-editor] Skipping ${file}: not found`);
    continue;
  }
  const current = fs.readFileSync(filePath, 'utf8');
  if (current.includes('ɵmod') || current.includes('ɵcmp') || current.includes('ɵdir')) {
    continue;
  }
  fs.writeFileSync(filePath, content, 'utf8');
  patched++;
  console.log(`[patch-ng2-ace-editor] Patched ${file}`);
}
if (patched > 0) {
  console.log(`[patch-ng2-ace-editor] Done (${patched} file(s) patched).`);
}
