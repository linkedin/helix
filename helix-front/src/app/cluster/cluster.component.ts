import { Component, OnInit, ViewChild } from '@angular/core';
import { MediaChange, MediaObserver } from '@angular/flex-layout';

@Component({
  selector: 'hi-cluster',
  templateUrl: './cluster.component.html',
  styleUrls: ['./cluster.component.scss'],
})
export class ClusterComponent implements OnInit {
  @ViewChild('sidenav', { static: true }) sidenav;

  isNarrowView: boolean;
  hasCluster = false;

  constructor(protected media: MediaObserver) {}

  ngOnInit() {
    // auto adjust side nav only if not embed; below 1280px it overlays the page
    this.isNarrowView = this.media.isActive('lt-lg');

    this.media.asObservable().subscribe((change: MediaChange[]) => {
      this.isNarrowView = change.some((item) =>
        ['xs', 'sm', 'md'].includes(item.mqAlias),
      );
    });
  }

  toggleSidenav() {
    this.sidenav.opened ? this.sidenav.close() : this.sidenav.open();
  }
}
