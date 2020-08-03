/*
 * Copyright 2020 The NATS Authors
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
import { Empty, Msg } from "./types.ts";
import { MsgHdrsImpl } from "./headers.ts";
import { Publisher } from "./protocol.ts";

export class MsgImpl implements Msg {
  publisher: Publisher;
  subject!: string;
  sid!: number;
  reply?: string;
  data!: Uint8Array;
  headers?: MsgHdrsImpl;

  constructor(publisher: Publisher) {
    this.publisher = publisher;
  }

  // eslint-ignore-next-line @typescript-eslint/no-explicit-any
  respond(data: Uint8Array = Empty, headers?: MsgHdrsImpl): boolean {
    if (this.reply) {
      this.publisher.publish(this.reply, data, { headers: headers });
      return true;
    }
    return false;
  }
}
