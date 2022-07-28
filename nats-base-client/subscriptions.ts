/*
 * Copyright 2020-2021 The NATS Authors
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
 */
import type { SubscriptionImpl } from "./subscription.ts";
import type { NatsError } from "./error.ts";
import type { Msg } from "./types.ts";

export class Subscriptions {
  mux: SubscriptionImpl | null;
  subs: Map<number, SubscriptionImpl>;
  sidCounter: number;

  constructor() {
    this.sidCounter = 0;
    this.mux = null;
    this.subs = new Map<number, SubscriptionImpl>();
  }

  size(): number {
    return this.subs.size;
  }

  add(s: SubscriptionImpl): SubscriptionImpl {
    this.sidCounter++;
    s.sid = this.sidCounter;
    this.subs.set(s.sid, s as SubscriptionImpl);
    return s;
  }

  setMux(s: SubscriptionImpl | null): SubscriptionImpl | null {
    this.mux = s;
    return s;
  }

  getMux(): SubscriptionImpl | null {
    return this.mux;
  }

  get(sid: number): SubscriptionImpl | undefined {
    return this.subs.get(sid);
  }

  resub(s: SubscriptionImpl): SubscriptionImpl {
    this.sidCounter++;
    this.subs.delete(s.sid);
    s.sid = this.sidCounter;
    this.subs.set(s.sid, s);
    return s;
  }

  all(): (SubscriptionImpl)[] {
    return Array.from(this.subs.values());
  }

  cancel(s: SubscriptionImpl): void {
    if (s) {
      s.close();
      this.subs.delete(s.sid);
    }
  }

  handleError(err?: NatsError): boolean {
    if (err && err.permissionContext) {
      const ctx = err.permissionContext;
      const subs = this.all();
      let sub;
      if (ctx.operation === "subscription") {
        sub = subs.find((s) => {
          return s.subject === ctx.subject;
        });
      }
      if (ctx.operation === "publish") {
        // we have a no mux subscription
        sub = subs.find((s) => {
          return s.requestSubject === ctx.subject;
        });
      }
      if (sub) {
        sub.callback(err, {} as Msg);
        sub.close();
        return sub !== this.mux;
      }
    }
    return false;
  }

  close() {
    this.subs.forEach((sub) => {
      sub.close();
    });
  }
}
