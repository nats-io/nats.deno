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
import { Msg, Payload } from "./types.ts";
import { ErrorCode, NatsError } from "./error.ts";
import { MsgImpl } from "./msg.ts";
import { CR_LF_LEN } from "./util.ts";
import { DataBuffer } from "./databuffer.ts";
import { MsgHdrsImpl } from "./headers.ts";
import { Publisher } from "./protocol.ts";

export class MsgBuffer {
  msg: Msg;
  length: number;
  headerLen: number;
  buf?: Uint8Array | null;
  payload: string;
  err: NatsError | null = null;
  status: number = 0;

  constructor(
    publisher: Publisher,
    chunks: RegExpExecArray,
    payload: "string" | "json" | "binary" = "string",
  ) {
    this.msg = new MsgImpl(publisher);
    this.msg.subject = chunks[1];
    this.msg.sid = parseInt(chunks[2], 10);
    this.msg.reply = chunks[4];
    this.length =
      (chunks.length === 7
        ? parseInt(chunks[6], 10)
        : parseInt(chunks[5], 10)) + CR_LF_LEN;
    this.headerLen = (chunks.length === 7 ? parseInt(chunks[5], 10) : 0);

    this.payload = payload;
  }

  fill(data: Uint8Array) {
    if (!this.buf) {
      this.buf = data;
    } else {
      this.buf = DataBuffer.concat(this.buf, data);
    }
    this.length -= data.length;

    if (this.length === 0) {
      const headers = this.headerLen
        ? this.buf.slice(0, this.headerLen)
        : undefined;
      if (headers) {
        this.msg.headers = MsgHdrsImpl.decode(headers);
      }
      this.msg.data = this.buf.slice(this.headerLen, this.buf.length - 2);

      switch (this.payload) {
        case Payload.JSON:
          this.msg.data = new TextDecoder("utf-8").decode(this.msg.data);
          try {
            this.msg.data = JSON.parse(this.msg.data);
          } catch (err) {
            this.err = NatsError.errorForCode(ErrorCode.BAD_JSON, err);
          }
          break;
        case Payload.STRING:
          this.msg.data = new TextDecoder("utf-8").decode(this.msg.data);
          break;
        case Payload.BINARY:
          break;
      }
      this.buf = null;
    }
  }
}
