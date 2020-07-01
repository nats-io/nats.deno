/*
 * Copyright 2018-2020 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
import {
  DEFAULT_PORT,
  DEFAULT_URI,
  ServerInfo,
  ServersChanged,
} from "./types.ts";
import { shuffle } from "./util.ts";

/**
 * @hidden
 */
export class Server {
  url: URL;
  hostname: string;
  port: number;
  didConnect: boolean;
  reconnects: number;
  lastConnect: number;
  implicit: boolean;

  constructor(u: string, implicit = false) {
    // remove any of the standard protocols we are used to seeing
    u = u.replace("tls://", "");
    u = u.replace("ws://", "");
    u = u.replace("wss://", "");
    u = u.replace("nats://", "");

    // in web environments, URL may not be a living standard
    // that means that protocols other than HTTP/S are not
    // parsable correctly.
    if (!/^.*:\/\/.*/.test(u)) {
      u = `http://${u}`;
    }
    this.url = new URL(u);
    if (!this.url.port) {
      this.url.port = `${DEFAULT_PORT}`;
    }

    this.hostname = this.url.hostname;
    this.port = parseInt(this.url.port, 10);

    this.didConnect = false;
    this.reconnects = 0;
    this.lastConnect = 0;
    this.implicit = implicit;
  }

  toString(): string {
    return this.url.href || "";
  }

  hostport(): { hostname: string; port: number } {
    return this;
  }

  getCredentials(): string[] | undefined {
    let auth;
    if (this.url.username) {
      auth = [];
      auth.push(this.url.username);
    }
    if (this.url.password) {
      auth = auth || [];
      auth.push(this.url.password);
    }
    return auth;
  }
}

/**
 * @hidden
 */
export class Servers {
  private firstSelect: boolean = true;
  private readonly servers: Server[];
  private currentServer: Server;

  constructor(randomize: boolean, urls: string[] = [], firstServer?: string) {
    this.servers = [] as Server[];
    if (urls) {
      urls.forEach((element) => {
        this.servers.push(new Server(element));
      });
      if (randomize) {
        this.servers = shuffle(this.servers);
      }
    }

    if (firstServer) {
      let index = urls.indexOf(firstServer);
      if (index === -1) {
        this.addServer(firstServer, false);
      } else {
        let fs = this.servers[index];
        this.servers.splice(index, 1);
        this.servers.unshift(fs);
      }
    } else {
      if (this.servers.length === 0) {
        this.addServer(DEFAULT_URI, false);
      }
    }
    this.currentServer = this.servers[0];
  }

  getCurrentServer(): Server {
    return this.currentServer;
  }

  addServer(u: string, implicit = false): void {
    this.servers.push(new Server(u, implicit));
  }

  selectServer(): Server | undefined {
    // allow using select without breaking the order of the servers
    if (this.firstSelect) {
      this.firstSelect = false;
      return this.currentServer;
    }
    let t = this.servers.shift();
    if (t) {
      this.servers.push(t);
      this.currentServer = t;
    }
    return t;
  }

  removeCurrentServer(): void {
    this.removeServer(this.currentServer);
  }

  removeServer(server: Server | undefined): void {
    if (server) {
      let index = this.servers.indexOf(server);
      this.servers.splice(index, 1);
    }
  }

  length(): number {
    return this.servers.length;
  }

  next(): Server | undefined {
    return this.servers.length ? this.servers[0] : undefined;
  }

  getServers(): Server[] {
    return this.servers;
  }

  update(info: ServerInfo): ServersChanged | void {
    let added: string[] = [];
    let deleted: string[] = [];

    if (info.connect_urls && info.connect_urls.length > 0) {
      console.log(info.connect_urls)
      const discovered = new Map<string,Server>()

      info.connect_urls.forEach((hp) => {
        // protocol in node includes the ':'
        let protocol = this.currentServer.url.protocol;
        discovered.set(hp, new Server(hp, true))
      });

      // remove implicit servers that are no longer reported
      let toDelete: number[] = [];
      this.servers.forEach((s, index) => {
        let u = s.url.host;
        if (
          s.implicit && this.currentServer.url.host !== u && discovered.get(u) === undefined
        ) {
          // server was removed
          toDelete.push(index);
        }
        // remove this entry from reported
        discovered.delete(u);
      });

      // perform the deletion
      toDelete.reverse();
      toDelete.forEach((index) => {
        let removed = this.servers.splice(index, 1);
        deleted = deleted.concat(removed[0].url.host);
      });

      // remaining servers are new
      discovered.forEach((v, k, m) => {
        this.servers.push(v);
        added.push(k);
      })
    }
    return { added, deleted };
  }
}
