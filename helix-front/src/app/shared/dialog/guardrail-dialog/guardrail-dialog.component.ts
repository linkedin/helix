import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

import { GuardrailViolation, isForceable } from '../../../core/guardrail';

@Component({
  selector: 'hi-guardrail-dialog',
  templateUrl: './guardrail-dialog.component.html',
  styleUrls: ['./guardrail-dialog.component.scss'],
})
export class GuardrailDialogComponent {
  violations: GuardrailViolation[];
  forceable: boolean;
  confirming = false;

  constructor(
    @Inject(MAT_DIALOG_DATA) data: { violations: GuardrailViolation[] },
    private dialogRef: MatDialogRef<GuardrailDialogComponent>,
  ) {
    this.violations = data.violations;
    this.forceable = isForceable(data.violations);
  }

  target(v: GuardrailViolation) {
    return [v.resourceName, v.partitionName].filter((s) => !!s).join(' / ');
  }

  close() {
    this.dialogRef.close(false);
  }

  force() {
    this.dialogRef.close(true);
  }
}
