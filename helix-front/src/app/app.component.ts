import { Component, OnInit } from '@angular/core';
import {
  Router,
  ActivatedRoute,
  NavigationStart,
  NavigationEnd,
  NavigationCancel,
  NavigationError,
} from '@angular/router';
import { MatDialog } from '@angular/material/dialog';

// import { Angulartics2Piwik } from 'angulartics2/piwik';

import { tap } from 'rxjs/operators';

import { UserService } from './core/user.service';
import { InputDialogComponent } from './shared/dialog/input-dialog/input-dialog.component';
import { AlertDialogComponent } from './shared/dialog/alert-dialog/alert-dialog.component';
import { HelperService } from './shared/helper.service';

@Component({
  selector: 'hi-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
  providers: [UserService /*, Angulartics2Piwik */],
})
export class AppComponent implements OnInit {
  headerEnabled = true;
  footerEnabled = true;
  isLoading = true;
  currentUser: any;
  isLoggedIn = false;
  private expiryCheckHandle?: ReturnType<typeof setInterval>;

  constructor(
    // protected angulartics2Piwik: Angulartics2Piwik,
    protected route: ActivatedRoute,
    protected router: Router,
    protected dialog: MatDialog,
    protected service: UserService,
    protected helper: HelperService
  ) {
    router.events.subscribe((event) => {
      if (event instanceof NavigationStart) {
        this.isLoading = true;
      }
      if (event instanceof NavigationEnd) {
        this.isLoading = false;
      }
      if (event instanceof NavigationError) {
        this.isLoading = false;
      }
      if (event instanceof NavigationCancel) {
        this.isLoading = false;
      }
    });
    // angulartics2Piwik.startTracking();
  }

  ngOnInit() {
    this.currentUser = this.service.getCurrentUser().pipe(
      tap((user: any) => this.isLoggedIn = user && user !== 'Sign In')
    );

    this.route.queryParams.subscribe((params) => {
      if (params['embed'] == 'true') {
        this.headerEnabled = this.footerEnabled = false;
      }
    });

    this.watchTokenExpiry();
  }

  private hasIdentityToken(): boolean {
    return document.cookie.split(';').some(c => c.trim().startsWith('helixui_identity.token='));
  }

  private watchTokenExpiry() {
    if (this.expiryCheckHandle) clearInterval(this.expiryCheckHandle);
    if (!this.hasIdentityToken()) return;
    this.expiryCheckHandle = setInterval(() => {
      if (
        !this.hasIdentityToken() &&
        this.dialog.openDialogs.length === 0
      ) {
        clearInterval(this.expiryCheckHandle!);
        this.dialog
          .open(AlertDialogComponent, {
            data: {
              title: 'Session Expired',
              message:
                'Your session has expired. Please sign in again to continue.',
            },
          })
          .afterClosed()
          .subscribe(() => {
            fetch('/api/user/logout', { method: 'POST' }).finally(() =>
              window.location.reload()
            );
          });
      }
    }, 30000);
  }

  login() {
    this.dialog
      .open(InputDialogComponent, {
        data: {
          title: 'Sign In',
          message: 'Please enter your LDAP username and password to continue:',
          values: {
            username: {
              label: 'Username',
            },
            password: {
              label: 'Password',
              type: 'password',
            },
          },
        },
      })
      .afterClosed()
      .subscribe(
        (result) => {
          if (result && result.username.value && result.password.value) {
            this.service
              .login(result.username.value, result.password.value)
              .subscribe(
                (loginResponse) => {
                  if (!loginResponse) {
                    this.helper.showError(
                      `${loginResponse.status}: Either You are not part of helix-admin LDAP group or your password is incorrect.`
                    );
                  }

                  this.currentUser = this.service.getCurrentUser().pipe(
                    tap((user: any) => this.isLoggedIn = user && user !== 'Sign In')
                  );
                  this.watchTokenExpiry();
                },
                (error) => {
                  // since rest API simply throws 404 instead of empty config when config is not initialized yet
                  // frontend has to treat 404 as normal result
                  if (error != 'Not Found') {
                    this.helper.showError(error);
                  }
                  this.isLoading = false;
                }
              );
          }
        },
        (error) => {
          // since rest API simply throws 404 instead of empty config when config is not initialized yet
          // frontend has to treat 404 as normal result
          if (error != 'Not Found') {
            this.helper.showError(error);
          }
          this.isLoading = false;
        }
      );
  }

  logout() {
    if (this.expiryCheckHandle) clearInterval(this.expiryCheckHandle);
    this.service.logout().subscribe(
      () => {
        this.currentUser = this.service.getCurrentUser().pipe(
          tap((user: any) => this.isLoggedIn = user && user !== 'Sign In')
        );
        this.helper.showSnackBar('Signed out successfully.');
      },
      (error) => {
        this.helper.showError(error);
      }
    );
  }
}
