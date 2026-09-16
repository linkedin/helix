import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { Instance, InstanceOperationState } from '../shared/instance.model';
import { HelperService } from '../../shared/helper.service';
import { InstanceService } from '../shared/instance.service';

@Component({
  selector: 'hi-instance-detail',
  templateUrl: './instance-detail.component.html',
  styleUrls: ['./instance-detail.component.scss'],
  providers: [InstanceService],
})
export class InstanceDetailComponent implements OnInit {
  readonly tabLinks = [
    { label: 'Resources', link: 'resources' },
    { label: 'Configuration', link: 'configs' },
    { label: 'History', link: 'history' },
  ];

  clusterName: string;
  instanceName: string;
  instance: Instance;
  isLoading = true;
  can = false;

  constructor(
    protected route: ActivatedRoute,
    protected router: Router,
    protected service: InstanceService,
    protected helperService: HelperService
  ) {}

  ngOnInit() {
    this.service.can().subscribe((data) => (this.can = data));
    this.clusterName = this.route.snapshot.params.cluster_name;
    this.instanceName = this.route.snapshot.params.instance_name;
    this.loadInstance();
  }

  removeInstance() {
    this.helperService
      .showConfirmation('Are you sure you want to remove this Instance?')
      .then((result) => {
        if (result) {
          this.service.remove(this.clusterName, this.instance.name).subscribe(
            /* happy path */ () => {
              this.helperService.showSnackBar(
                `Instance: ${this.instance.name} removed!`
              );
              this.router.navigate(['..'], { relativeTo: this.route });
            },
            /* error path */ (error) => this.helperService.showError(error),
            /* onComplete */ () => (this.isLoading = false)
          );
        }
      });
  }

  enableInstance() {
    this.setInstanceState('ENABLE');
  }

  disableInstance() {
    this.setInstanceState('DISABLE');
  }

  evacuateInstance() {
    this.setInstanceState('EVACUATE');
  }

  setInstanceState(targetState: InstanceOperationState) {
    const stateLabel =
      targetState === 'ENABLE'
        ? 'Enable'
        : targetState === 'DISABLE'
        ? 'Disable'
        : targetState === 'EVACUATE'
        ? 'Evacuate'
        : targetState;

    this.helperService
      .showInput(
        `${stateLabel} Instance`,
        `Please provide a reason for changing the state of ${this.instance.name} to ${targetState}:`,
        {
          reason: {
            label: 'Reason',
            type: 'input',
          },
        }
      )
      .then((result) => {
        if (result && result.reason && result.reason.value) {
          this.isLoading = true;
          this.service
            .setInstanceOperation(
              this.clusterName,
              this.instance.name,
              targetState,
              result.reason.value
            )
            .subscribe(
              () => {
                this.helperService.showSnackBar(
                  `Instance ${this.instance.name} state changed to ${targetState}`
                );
                this.loadInstance();
              },
              (error) => this.helperService.showError(error),
              () => (this.isLoading = false)
            );
        }
      });
  }

  protected loadInstance() {
    this.isLoading = true;
    this.service.get(this.clusterName, this.instanceName).subscribe(
      /* happy path */ (instance) => (this.instance = instance),
      /* error path */ (error) => this.helperService.showError(error),
      /* onComplete */ () => (this.isLoading = false)
    );
  }
}
