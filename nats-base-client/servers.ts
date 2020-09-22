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
  DEFAULT_HOSTPORT,
  ServerInfo,
  ServersChanged,
  Server,
} from "./types.ts";
import { shuffle } from "./util.ts";

/**
 * @hidden
 */
export class ServerImpl implements Server {
  src: string;
  listen: string;
  hostname: string;
  port: number;
  didConnect: boolean;
  reconnects: number;
  lastConnect: number;
  gossiped: boolean;

  constructor(u: string, gossiped = false) {
    this.src = u;
    // remove any protocol that may have been provided
    if (u.match(/^(.*:\/\/)(.*)/m)) {
      u = u.replace(/^(.*:\/\/)(.*)/gm, "$2");
    }
    // in web environments, URL may not be a living standard
    // that means that protocols other than HTTP/S are not
    // parsable correctly.
    let url = new URL(`http://${u}`);
    if (!url.port) {
      url.port = `${DEFAULT_PORT}`;
    }
    this.listen = url.host;
    this.hostname = url.hostname;
    this.port = parseInt(url.port, 10);

    this.didConnect = false;
    this.reconnects = 0;
    this.lastConnect = 0;
    this.gossiped = gossiped;
  }

  toString(): string {
    return this.listen;
  }
}

/**
 * @hidden
 */
export class Servers {
  private firstSelect: boolean = true;
  private readonly servers: ServerImpl[];
  private currentServer: ServerImpl;

  constructor(
    randomize: boolean,
    listens: string[] = [],
    firstServer?: string,
  ) {
    this.servers = [] as ServerImpl[];
    if (listens) {
      listens.forEach((hp) => {
        this.servers.push(new ServerImpl(hp));
      });
      if (randomize) {
        this.servers = shuffle(this.servers);
      }
    }

    if (firstServer) {
      let index = listens.indexOf(firstServer);
      if (index === -1) {
        this.servers.unshift(new ServerImpl(firstServer));
      } else {
        let fs = this.servers[index];
        this.servers.splice(index, 1);
        this.servers.unshift(fs);
      }
    } else {
      if (this.servers.length === 0) {
        this.addServer(DEFAULT_HOSTPORT, false);
      }
    }
    this.currentServer = this.servers[0];
  }

  getCurrentServer(): ServerImpl {
    return this.currentServer;
  }

  addServer(u: string, implicit = false): void {
    this.servers.push(new ServerImpl(u, implicit));
  }

  selectServer(): ServerImpl | undefined {
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

  removeServer(server: ServerImpl | undefined): void {
    if (server) {
      let index = this.servers.indexOf(server);
      this.servers.splice(index, 1);
    }
  }

  length(): number {
    return this.servers.length;
  }

  next(): ServerImpl | undefined {
    return this.servers.length ? this.servers[0] : undefined;
  }

  getServers(): ServerImpl[] {
    return this.servers;
  }

  update(info: ServerInfo): ServersChanged {
    const added: string[] = [];
    let deleted: string[] = [];

    const discovered = new Map<string, ServerImpl>();
    if (info.connect_urls && info.connect_urls.length > 0) {
      info.connect_urls.forEach((hp) => {
        discovered.set(hp, new ServerImpl(hp, true));
      });
    }
    // remove gossiped servers that are no longer reported
    let toDelete: number[] = [];
    this.servers.forEach((s, index) => {
      let u = s.listen;
      if (
        s.gossiped && this.currentServer.listen !== u &&
        discovered.get(u) === undefined
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
      deleted = deleted.concat(removed[0].listen);
    });

    // remaining servers are new
    discovered.forEach((v, k, m) => {
      this.servers.push(v);
      added.push(k);
    });

    return { added, deleted };
  }
}
