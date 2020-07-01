import { Server, Servers } from "../nats-base-client/servers.ts";
import {
  assertEquals,
  assert,
} from "https://deno.land/std/testing/asserts.ts";
import { ServerInfo } from "../nats-base-client/types.ts";

Deno.test("servers - single", () => {
  const servers = new Servers(false, [], "localhost:4222");
  assertEquals(servers.length(), 1);
  assertEquals(servers.getServers().length, 1);
  assertEquals(servers.getCurrentServer().listen, "localhost:4222");
  let ni = 0;
  servers.getServers().forEach((s) => {
    if (s.gossiped) {
      ni++;
    }
  });
  assertEquals(ni, 0);
});

Deno.test("servers - multiples", () => {
  const servers = new Servers(false, ["h:1", "h:2"], "localhost:4222");
  assertEquals(servers.length(), 3);
  assertEquals(servers.getServers().length, 3);
  assertEquals(servers.getCurrentServer().listen, "localhost:4222");
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
  const servers = new Servers(false, [], "localhost:4222");
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
