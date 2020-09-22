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
import { Servers } from "../nats-base-client/servers.ts";
import {
  assertEquals,
} from "https://deno.land/std@0.69.0/testing/asserts.ts";
import type { ServerInfo } from "../nats-base-client/internal_mod.ts";

Deno.test("servers - single", () => {
  const servers = new Servers(false, ["127.0.0.1:4222"]);
  assertEquals(servers.length(), 1);
  assertEquals(servers.getServers().length, 1);
  assertEquals(servers.getCurrentServer().listen, "127.0.0.1:4222");
  let ni = 0;
  servers.getServers().forEach((s) => {
    if (s.gossiped) {
      ni++;
    }
  });
  assertEquals(ni, 0);
});

Deno.test("servers - multiples", () => {
  const servers = new Servers(
    false,
    ["h:1", "h:2"],
  );
  assertEquals(servers.length(), 2);
  assertEquals(servers.getServers().length, 2);
  assertEquals(servers.getCurrentServer().listen, "h:1");
  let ni = 0;
  servers.getServers().forEach((s) => {
    if (s.gossiped) {
      ni++;
    }
  });
  assertEquals(ni, 0);
});

function servInfo(): ServerInfo {
  return {
    max_payload: 1,
    client_id: 1,
    proto: 1,
    version: "1",
  } as ServerInfo;
}

Deno.test("servers - add/delete", () => {
  const servers = new Servers(false, ["127.0.0.1:4222"]);
  assertEquals(servers.length(), 1);
  let ce = servers.update(Object.assign(servInfo(), { connect_urls: ["h:1"] }));
  assertEquals(ce.added.length, 1);
  assertEquals(ce.deleted.length, 0);
  assertEquals(servers.length(), 2);
  let gossiped = servers.getServers().filter((s) => {
    return s.gossiped;
  });
  assertEquals(gossiped.length, 1);

  ce = servers.update(Object.assign(servInfo(), { connect_urls: [] }));
  assertEquals(ce.added.length, 0);
  assertEquals(ce.deleted.length, 1);
  assertEquals(servers.length(), 1);

  gossiped = servers.getServers().filter((s) => {
    return s.gossiped;
  });
  assertEquals(gossiped.length, 0);
});

Deno.test("servers - url parse fn", () => {
  const fn = (s: string): string => {
    return `x://${s}`;
  };
  const s = new Servers(
    false,
    ["127.0.0.1:4222"],
    { urlParseFn: fn },
  );
  s.update(Object.assign(servInfo(), { connect_urls: ["h:1", "j:2/path"] }));

  const servers = s.getServers();
  assertEquals(servers[0].src, "x://127.0.0.1:4222");
  assertEquals(servers[1].src, "x://h:1");
  assertEquals(servers[2].src, "x://j:2/path");
});
