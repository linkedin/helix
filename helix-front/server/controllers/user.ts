import { Response, Router } from 'express';
import * as LdapClient from 'ldapjs';
import { fetch, Agent } from 'undici';
import { readFileSync } from 'fs';
import type { ConnectionOptions } from 'tls';

import {
  LDAP,
  IDENTITY_TOKEN_SOURCE,
  CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
  SSL,
} from '../config';
import { HelixRequest } from './d';
import { TOKEN_EXPIRATION_KEY, TOKEN_RESPONSE_KEY } from '../config';

// SSL material is read once at import. The identity-token endpoint only
// validates the server cert — no client mTLS — so we just trust SSL.cafiles[0].
const tokenConnectOpts: ConnectionOptions = { rejectUnauthorized: false };
if (SSL.cafiles.length > 0) {
  tokenConnectOpts.ca = readFileSync(SSL.cafiles[0], { encoding: 'utf-8' });
}
const tokenDispatcher = new Agent({ connect: tokenConnectOpts });

const TOKEN_TIMEOUT_MS = 10000;

function escapeLdapFilter(str: string): string {
  return str.replace(/([\\*()\x00])/g, (c) =>
    '\\' + c.charCodeAt(0).toString(16).padStart(2, '0')
  );
}

export class UserCtrl {
  constructor(router: Router) {
    // uncomment the following line to use customized login
    // router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    router.route('/user/logout').post(this.logout);
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
  }

  protected logout(req: HelixRequest, res: Response) {
    res.clearCookie('helixui_identity.token');
    req.session = null;
    res.json(true);
  }

  //
  // You may rewrite this function to support your own authorization logic.
  // Usually it would be helpful to integrate with 3rd party login.
  // For example:
  //
  /*
  protected authorize(req: HelixRequest, res: Response) {
    const { isAdmin, username, token } = get_auth_state();
    req.session.isAdmin = isAdmin;
    req.session.username = username;
    req.session.identityToken = token;
    res.redirect('/');
  }
  */

  protected current(req: HelixRequest, res: Response) {
    res.json(req.session.username || 'Sign In');
  }

  //
  // Returns true if the user is authenticated (has a valid session).
  // Per-user authorization is handled by helix-rest's DatavaultAuthValidator
  // via Identity-Token forwarding.
  //
  protected can(req: HelixRequest, res: Response) {
    try {
      return res.json(req.session.username ? true : false);
    } catch (err) {
      throw new Error(
        `Error from /can endpoint: ${err}`
      );
    }
  }

  protected login(req: HelixRequest, res: Response) {
    const credential = req.body;
    if (!credential.username || !credential.password) {
      res.status(401).json(false);
      return;
    }

    // check LDAP
    const ldap = LdapClient.createClient({ url: LDAP.uri });
    ldap.bind(
      credential.username + LDAP.principalSuffix,
      credential.password,
      (err) => {
        if (err) {
          console.error('LDAP bind failed:', err);
          ldap.unbind();
          res.status(401).json(false);
          return;
        }

        // LDAP login success
        const opts: LdapClient.SearchOptions = {
          filter:
            '(&(sAMAccountName=' +
            escapeLdapFilter(credential.username) +
            ')(objectcategory=person))',
          scope: 'sub',
        };

        req.session.username = credential.username;
        res.set('Username', credential.username);

        ldap.search(LDAP.base, opts, function (searchErr, result) {
          if (searchErr || !result) {
            console.error('LDAP search failed:', searchErr);
            ldap.unbind();
            res.json(true);
            return;
          }

          let isInAdminGroup = false;
          let searchComplete = false;
          result.on('searchEntry', function (entry) {
            if (!entry.object) return;
            // ldapjs returns memberOf as a string when the user is in
            // exactly one group, and as a string[] when in multiple.
            const raw = entry.object['memberOf'];
            const groups = Array.isArray(raw) ? raw : raw ? [raw] : [];
            for (const group of groups) {
              const groupName = group.split(',', 1)[0].split('=')[1];
              if (groupName == LDAP.adminGroup) {
                isInAdminGroup = true;
              }
            }
          });
          result.on('error', function (err) {
            if (searchComplete) return;
            searchComplete = true;
            console.error('LDAP search error:', err);
            ldap.unbind();
            res.json(true);
          });
          result.on('end', async function () {
            if (searchComplete) return;
            searchComplete = true;
            ldap.unbind();
            req.session.isAdmin = isInAdminGroup;

            //
            // Fetch an Identity-Token for ALL authenticated users
            // so that helix-rest can perform per-user authorization
            // via DatavaultAuthValidator.
            //
            if (!IDENTITY_TOKEN_SOURCE) {
              res.json(true);
              return;
            }

            const tokenBody = JSON.stringify({
              username: credential.username,
              password: credential.password,
              ...CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
            });

            const abort = new AbortController();
            const timeoutId = setTimeout(() => abort.abort(), TOKEN_TIMEOUT_MS);
            try {
              const tokenResponse = await fetch(IDENTITY_TOKEN_SOURCE, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: tokenBody,
                dispatcher: tokenDispatcher,
                signal: abort.signal,
              });

              const tokenText = await tokenResponse.text();
              const parsedBody = JSON.parse(tokenText);

              if (!tokenResponse.ok || parsedBody?.error) {
                res.json(true);
                return;
              }

              req.session.identityToken = parsedBody;

              const cookieName = 'helixui_identity.token';
              const cookieValue = parsedBody.value[TOKEN_RESPONSE_KEY];
              const cookieExpiresDate = new Date(parsedBody.value[TOKEN_EXPIRATION_KEY]);
              const cookieOptions = {
                expires: cookieExpiresDate,
              };
              res.cookie(cookieName, cookieValue, cookieOptions);
              res.json(true);
            } catch (error) {
              console.error('Identity token fetch failed:', error);
              res.json(true);
            } finally {
              clearTimeout(timeoutId);
            }
          });
        });
      }
    );
  }
}
