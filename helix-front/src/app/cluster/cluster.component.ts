import {
  Component,
  ElementRef,
  HostBinding,
  NgZone,
  OnInit,
  ViewChild,
} from '@angular/core';
import { MediaChange, MediaObserver } from '@angular/flex-layout';
import { MatSidenavContainer } from '@angular/material/sidenav';

const WIDTH_KEY = 'helix-sidenav-width';
const MIN_WIDTH = 200;
const KEY_STEP = 24;

@Component({
  selector: 'hi-cluster',
  templateUrl: './cluster.component.html',
  styleUrls: ['./cluster.component.scss'],
})
export class ClusterComponent implements OnInit {
  @ViewChild('sidenav', { static: true }) sidenav;
  @ViewChild('sidenav', { static: true, read: ElementRef })
  sidenavEl: ElementRef<HTMLElement>;
  @ViewChild(MatSidenavContainer, { static: true })
  container: MatSidenavContainer;

  isNarrowView: boolean;
  hasCluster = false;
  // null keeps the default of a quarter of the window
  sidenavWidth: number | null = readWidth();

  @HostBinding('style.--hi-sidenav-width')
  get sidenavWidthCss(): string | null {
    return this.sidenavWidth ? `${this.sidenavWidth}px` : null;
  }

  constructor(
    protected media: MediaObserver,
    private zone: NgZone,
  ) {}

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

  startResize(event: PointerEvent) {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = this.sidenavEl.nativeElement.offsetWidth;
    document.body.classList.add('hi-col-resizing');
    const move = (e: PointerEvent) =>
      this.zone.run(() => this.setWidth(startWidth + e.clientX - startX));
    const up = () => {
      document.removeEventListener('pointermove', move);
      document.removeEventListener('pointerup', up);
      document.removeEventListener('pointercancel', up);
      document.body.classList.remove('hi-col-resizing');
      writeWidth(this.sidenavWidth);
      relayout();
    };
    document.addEventListener('pointermove', move);
    document.addEventListener('pointerup', up);
    document.addEventListener('pointercancel', up);
  }

  resizeByKey(event: KeyboardEvent) {
    const step =
      event.key === 'ArrowRight'
        ? KEY_STEP
        : event.key === 'ArrowLeft'
          ? -KEY_STEP
          : 0;
    if (step) {
      event.preventDefault();
      this.setWidth(this.sidenavEl.nativeElement.offsetWidth + step);
      writeWidth(this.sidenavWidth);
      relayout();
    }
  }

  resetWidth() {
    this.sidenavWidth = null;
    writeWidth(null);
    setTimeout(() => this.container.updateContentMargins());
    relayout();
  }

  private setWidth(width: number) {
    const max = Math.max(MIN_WIDTH, window.innerWidth / 2);
    this.sidenavWidth = Math.round(Math.min(max, Math.max(MIN_WIDTH, width)));
    // the content margin follows the drawer only when told to
    setTimeout(() => this.container.updateContentMargins());
  }
}

// tab bars and tables only re-measure on a window resize, so announce one
// once the content area has its new width
function relayout() {
  setTimeout(() => window.dispatchEvent(new Event('resize')));
}

function readWidth(): number | null {
  try {
    const width = Number(localStorage.getItem(WIDTH_KEY));
    return width >= MIN_WIDTH ? width : null;
  } catch (e) {
    return null;
  }
}

function writeWidth(width: number | null) {
  try {
    width
      ? localStorage.setItem(WIDTH_KEY, String(width))
      : localStorage.removeItem(WIDTH_KEY);
  } catch (e) {
    // an unsaved width is not worth an error
  }
}
