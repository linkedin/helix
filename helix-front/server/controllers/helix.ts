import { Request, Response, Router } from 'express';
import { fetch, Agent } from 'undici';
import { readFileSync } from 'fs';
import type { ConnectionOptions } from 'tls';

import { HELIX_ENDPOINTS, IDENTITY_TOKEN_SOURCE, SSL } from '../config';
import { HelixRequest } from './d';

// SSL material is read once at import time and shared across all proxied
// requests. A single undici Agent keeps the TLS connection pool warm —
// constructing one per request would defeat keepalive.
const proxyConnectOpts: ConnectionOptions = { rejectUnauthorized: false };
if (SSL.cafiles.length > 0) {
  proxyConnectOpts.ca = readFileSync(SSL.cafiles[0], { encoding: 'utf-8' });
}
if (SSL.keyfile) {
  proxyConnectOpts.key = readFileSync(SSL.keyfile);
}
if (SSL.certfile) {
  proxyConnectOpts.cert = readFileSync(SSL.certfile);
}
const proxyDispatcher = new Agent({ connect: proxyConnectOpts });

export class HelixCtrl {
  static readonly ROUTE_PREFIX = '/api/helix';

  constructor(router: Router) {
    router.route('/helix/list').get(this.list);
    router.route('/helix/*').all((req, res) => this.proxy(req as HelixRequest, res));
  }

  protected async proxy(req: HelixRequest, res: Response) {
    const url = req.originalUrl.replace(HelixCtrl.ROUTE_PREFIX, '');
    const helixKey = url.split('/')[1];

    const segments = helixKey.split('.');
    const group = segments[0];

    segments.shift();
    const name = segments.join('.');

    const user = req.session.username;
    const method = req.method.toUpperCase();

    if (
      IDENTITY_TOKEN_SOURCE &&
      method !== 'GET' &&
      !res.locals.cookie?.['helixui_identity.token']
    ) {
      res.status(401).send('Session expired. Please log in again.');
      return;
    }

    if (method !== 'GET' && !req.session.username) {
      res.status(401).send('Unauthorized, must be logged in to make write requests');
      return;
    }

    let apiPrefix: string | null = null;
    if (HELIX_ENDPOINTS[group]) {
      HELIX_ENDPOINTS[group].forEach((section) => {
        if (section[name]) {
          apiPrefix = section[name];
        }
      });
    }

    if (!apiPrefix) {
      res.status(404).send('Not found');
      return;
    }

    const realUrl = apiPrefix + url.replace(`/${helixKey}`, '');
    console.log(`helix-rest request url ${realUrl}`);

    const headers: Record<string, string> = {
      'Helix-User': user,
      Accept: 'application/json',
    };

    if (IDENTITY_TOKEN_SOURCE) {
      headers['Identity-Token'] = res.locals.cookie['helixui_identity.token'];
    }

    let body: string | undefined;
    if (method !== 'GET' && req?.body !== undefined) {
      body = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
      headers['Content-Type'] = headers['Content-Type'] || 'application/json';
    }

    try {
      const response = await fetch(realUrl, {
        method,
        headers,
        body,
        dispatcher: proxyDispatcher,
      });

      const text = await response.text();
      let payload: unknown = text;
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch {
          /* empty */
        }
      }

      const errorField =
        payload && typeof payload === 'object' && 'error' in payload
          ? (payload as { error: unknown }).error
          : undefined;

      if (!response.ok) {
        res.status(response.status).send(errorField ?? payload ?? `HTTP ${response.status}`);
        return;
      }
      if (errorField) {
        res.status(response.status).send(errorField);
        return;
      }
      res.status(response.status).send(payload);
    } catch (error) {
      console.error(error);
      res.status(500).send(error instanceof Error ? error.message : String(error));
    }
  }

  protected list(req: Request, res: Response) {
    try {
      res.json(HELIX_ENDPOINTS);
    } catch (err) {
      console.log('error from helix/list/');
      console.log(err);
    }
  }
}
