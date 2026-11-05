import {
  Component,
  ElementRef,
  HostListener,
  OnInit,
  ViewChild,
} from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Router } from '@angular/router';

import { ClusterService } from '../shared/cluster.service';
import { Cluster } from '../shared/cluster.model';
import { AlertDialogComponent } from '../../shared/dialog/alert-dialog/alert-dialog.component';
import { InputDialogComponent } from '../../shared/dialog/input-dialog/input-dialog.component';

@Component({
  selector: 'hi-cluster-list',
  templateUrl: './cluster-list.component.html',
  styleUrls: ['./cluster-list.component.scss'],
})
export class ClusterListComponent implements OnInit {
  clusters: Cluster[] = [];
  errorMessage = '';
  isLoading = true;
  // is the currrent user logged in? If true, then yes.
  can = false;
  service = '';
  filter = '';
  matchCase = false;

  @ViewChild('filterInput') filterInput?: ElementRef<HTMLInputElement>;

  get filteredClusters(): Cluster[] {
    const fold = (s: string) => (this.matchCase ? s : s.toLowerCase());
    const query = fold(this.filter.trim());
    return query
      ? this.clusters.filter((c) => fold(c.name).includes(query))
      : this.clusters;
  }

  @HostListener('document:keydown', ['$event'])
  focusFilterOnSlash(event: KeyboardEvent) {
    const input = this.filterInput?.nativeElement;
    if (
      event.key !== '/' ||
      event.ctrlKey ||
      event.metaKey ||
      event.altKey ||
      !input ||
      input.offsetParent === null ||
      isEditable(event.target)
    ) {
      return;
    }
    event.preventDefault();
    input.focus();
    input.select();
  }

  clearFilter(event: Event) {
    if (this.filter) {
      event.stopPropagation();
      this.filter = '';
    } else {
      this.filterInput?.nativeElement.blur();
    }
  }

  openFirstMatch() {
    const [first] = this.filteredClusters;
    if (this.filter && first) {
      this.router.navigate([this.service, first.name]);
    }
  }

  constructor(
    protected clusterService: ClusterService,
    protected dialog: MatDialog,
    protected snackBar: MatSnackBar,
    protected router: Router
  ) {}

  ngOnInit() {
    this.loadClusters();
    // check if the current user is logged in
    this.clusterService.can().subscribe((data) => (this.can = data));
    this.service = this.router.url.split('/')[1];
  }

  loadClusters() {
    this.clusterService.getAll().subscribe(
      /* happy path */ (clusters) => (this.clusters = clusters),
      /* error path */ (error) => this.showErrorMessage(error),
      /* onComplete */ () => (this.isLoading = false)
    );
    // check if the current user is logged in again
    this.clusterService.can().subscribe((data) => (this.can = data));
  }

  showErrorMessage(error: any) {
    this.errorMessage = error;
    this.isLoading = false;
    this.dialog.open(AlertDialogComponent, {
      data: {
        title: 'Error',
        message: this.errorMessage,
      },
    });
  }

  createCluster() {
    const dialogRef = this.dialog.open(InputDialogComponent, {
      data: {
        title: 'Create a cluster',
        message: 'Please enter the following information to continue:',
        values: {
          name: {
            label: 'new cluster name',
          },
        },
      },
    });

    dialogRef.afterClosed().subscribe((result) => {
      if (result && result.name && result.name.value) {
        this.isLoading = true;
        this.clusterService.create(result.name.value).subscribe((data) => {
          this.snackBar.open('Cluster created!', 'OK', {
            duration: 2000,
          });
          this.dialog.open(AlertDialogComponent, {
            width: '600px',
            data: {
              title: "What's Next",
              message:
                'New cluster is created yet not activated. When you are ready to activate this cluster,' +
                ' please select "Activate this Cluster" menu in the cluster operations to continue.',
            },
          });
          this.loadClusters();
        });
      }
    });
  }
}

function isEditable(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  return (
    !!el &&
    (el.isContentEditable ||
      ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName))
  );
}
