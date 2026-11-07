import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

import {
  GuardrailViolation,
  isForceable,
  ruleTitle,
  splitMessage,
} from '../../../core/guardrail';

@Component({
  selector: 'hi-guardrail-dialog',
  templateUrl: './guardrail-dialog.component.html',
  styleUrls: ['./guardrail-dialog.component.scss'],
})
export class GuardrailDialogComponent {
  items: {
    ruleId: string;
    title: string;
    cause: string;
    advice: string;
    resource?: string;
    partition?: string;
  }[];
  forceable: boolean;
  confirming = false;

  constructor(
    @Inject(MAT_DIALOG_DATA) data: { violations: GuardrailViolation[] },
    private dialogRef: MatDialogRef<GuardrailDialogComponent>,
  ) {
    this.items = data.violations.map((v) => ({
      ruleId: v.ruleId,
      title: ruleTitle(v.ruleId),
      resource: v.resourceName,
      partition: v.partitionName,
      ...splitMessage(v.message),
    }));
    this.forceable = isForceable(data.violations);
  }

  close() {
    this.dialogRef.close(false);
  }

  force() {
    this.dialogRef.close(true);
  }
}
