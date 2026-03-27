import { Response, Router } from 'express';
import * as LdapClient from 'ldapjs';
import * as request from 'request';
import { readFileSync } from 'fs';

import {
  LDAP,
  IDENTITY_TOKEN_SOURCE,
  CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
  SSL,
} from '../config';
import { HelixRequest, HelixRequestOptions } from './d';
import { TOKEN_EXPIRATION_KEY, TOKEN_RESPONSE_KEY } from '../config';

export class UserCtrl {
  constructor(router: Router) {
    // uncomment the following line to use customized login
    // router.route('/user/authorize').get(this.authorize);
    router.route('/user/login').post(this.login.bind(this));
    router.route('/user/current').get(this.current);
    router.route('/user/can').get(this.can);
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
          res.status(401).json(false);
        } else {
          // LDAP login success
          const opts = {
            filter:
              '(&(sAMAccountName=' +
              credential.username +
              ')(objectcategory=person))',
            scope: 'sub',
          };

          req.session.username = credential.username;
          res.set('Username', credential.username);

          ldap.search(LDAP.base, opts, function (err, result) {
            let isInAdminGroup = false;
            let searchComplete = false;
            result.on('searchEntry', function (entry) {
              if (entry.object && !err) {
                const groups = entry.object['memberOf'];
                for (const group of groups) {
                  const groupName = group.split(',', 1)[0].split('=')[1];
                  if (groupName == LDAP.adminGroup) {
                    isInAdminGroup = true;
                  }
                }
              }
            });
            result.on('end', function () {
              if (searchComplete) {
                return;
              }
              searchComplete = true;
              req.session.isAdmin = isInAdminGroup;

              //
              // Fetch an Identity-Token for ALL authenticated users
              // so that helix-rest can perform per-user authorization
              // via DatavaultAuthValidator.
              //
              if (IDENTITY_TOKEN_SOURCE) {
                const body = JSON.stringify({
                  username: credential.username,
                  password: credential.password,
                  ...CUSTOM_IDENTITY_TOKEN_REQUEST_BODY,
                });

                const options: HelixRequestOptions = {
                  url: IDENTITY_TOKEN_SOURCE,
                  json: '',
                  body,
                  headers: {
                    'Content-Type': 'application/json',
                  },
                  agentOptions: {
                    rejectUnauthorized: false,
                  },
                };

                if (SSL.cafiles.length > 0) {
                  options.agentOptions.ca = readFileSync(SSL.cafiles[0], {
                    encoding: 'utf-8',
                  });
                }

                request.post(options, function (error, _res, body) {
                  if (error || body?.error) {
                    res.json(true);
                  } else {
                    const parsedBody = JSON.parse(body);
                    req.session.identityToken = parsedBody;

                    const cookieName = 'helixui_identity.token';
                    const cookieValue =
                      parsedBody.value[TOKEN_RESPONSE_KEY];
                    const cookieExpiresDate = new Date(
                      parsedBody.value[TOKEN_EXPIRATION_KEY]
                    );
                    const cookieOptions = {
                      expires: cookieExpiresDate,
                    };
                    res.cookie(cookieName, cookieValue, cookieOptions);
                    res.json(true);
                  }
                });
              } else {
                res.json(true);
              }
            });
          });
        }
      }
    );
  }
}
