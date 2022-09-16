/*
 * Copyright 2021-2022 The NATS Authors
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

import {
  ApiPagedRequest,
  Empty,
  JetStreamOptions,
  KvStatus,
  Lister,
  MsgDeleteRequest,
  MsgRequest,
  NatsConnection,
  ObjectStoreStatus,
  PurgeBySeq,
  PurgeOpts,
  PurgeResponse,
  PurgeTrimOpts,
  StoredMsg,
  StreamAPI,
  StreamConfig,
  StreamInfo,
  StreamInfoRequestOptions,
  StreamListResponse,
  StreamMsgResponse,
  StreamUpdateConfig,
  SuccessResponse,
} from "./types.ts";
import { BaseApiClient } from "./jsbaseclient_api.ts";
import { ListerFieldFilter, ListerImpl } from "./jslister.ts";
import { validateStreamName } from "./jsutil.ts";
import { headers, MsgHdrs, MsgHdrsImpl } from "./headers.ts";
import { kvPrefix, KvStatusImpl } from "./kv.ts";
import { ObjectStoreStatusImpl, osPrefix } from "./objectstore.ts";

export class StreamAPIImpl extends BaseApiClient implements StreamAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(cfg = {} as Partial<StreamConfig>): Promise<StreamInfo> {
    validateStreamName(cfg.name);
    const r = await this._request(
      `${this.prefix}.STREAM.CREATE.${cfg.name}`,
      cfg,
    );
    const si = r as StreamInfo;
    this._fixInfo(si);
    return si;
  }

  async delete(stream: string): Promise<boolean> {
    validateStreamName(stream);
    const r = await this._request(`${this.prefix}.STREAM.DELETE.${stream}`);
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async update(
    name: string,
    cfg = {} as Partial<StreamUpdateConfig>,
  ): Promise<StreamInfo> {
    if (typeof name === "object") {
      const sc = name as StreamConfig;
      name = sc.name;
      cfg = sc;
      console.trace(
        `\u001B[33m >> streams.update(config: StreamConfig) api changed to streams.update(name: string, config: StreamUpdateConfig) - this shim will be removed - update your code.  \u001B[0m`,
      );
    }
    validateStreamName(name);
    const old = await this.info(name);
    const update = Object.assign(old.config, cfg);

    const r = await this._request(
      `${this.prefix}.STREAM.UPDATE.${name}`,
      update,
    );
    const si = r as StreamInfo;
    this._fixInfo(si);
    return si;
  }

  async info(
    name: string,
    data?: Partial<StreamInfoRequestOptions>,
  ): Promise<StreamInfo> {
    validateStreamName(name);
    const subj = `${this.prefix}.STREAM.INFO.${name}`;
    const r = await this._request(subj, data);
    let si = r as StreamInfo;
    const { total } = si;

    // check how many subjects we got in the first request
    let have = si.state.subjects
      ? Object.getOwnPropertyNames(si.state.subjects).length
      : 1;

    // if the response is paged, we have a large list of subjects
    // handle the paging and return a StreamInfo with all of it
    if (total && total > have) {
      const infos: StreamInfo[] = [si];
      const paged = data || {} as unknown as ApiPagedRequest;
      while (total > have) {
        paged.offset = have;
        const r = await this._request(subj, paged) as StreamInfo;
        infos.push(r);
        have += Object.getOwnPropertyNames(r.state.subjects).length;
      }
      // collect all the subjects
      let subjects = {};
      for (let i = 0; i < infos.length; i++) {
        si = infos[i];
        if (si.state.subjects) {
          subjects = Object.assign(subjects, si.state.subjects);
        }
      }
      // don't give the impression we paged
      si.offset = 0;
      si.total = 0;
      si.limit = 0;
      si.state.subjects = subjects;
    }
    this._fixInfo(si);
    return si;
  }

  list(): Lister<StreamInfo> {
    const filter: ListerFieldFilter<StreamInfo> = (
      v: unknown,
    ): StreamInfo[] => {
      const slr = v as StreamListResponse;
      slr.streams.forEach((si) => {
        this._fixInfo(si);
      });
      return slr.streams;
    };
    const subj = `${this.prefix}.STREAM.LIST`;
    return new ListerImpl<StreamInfo>(subj, filter, this);
  }

  // FIXME: init of sealed, deny_delete, deny_purge shouldn't be necessary
  //  https://github.com/nats-io/nats-server/issues/2633
  _fixInfo(si: StreamInfo) {
    si.config.sealed = si.config.sealed || false;
    si.config.deny_delete = si.config.deny_delete || false;
    si.config.deny_purge = si.config.deny_purge || false;
    si.config.allow_rollup_hdrs = si.config.allow_rollup_hdrs || false;
  }

  async purge(name: string, opts?: PurgeOpts): Promise<PurgeResponse> {
    if (opts) {
      const { keep, seq } = opts as PurgeBySeq & PurgeTrimOpts;
      if (typeof keep === "number" && typeof seq === "number") {
        throw new Error("can specify one of keep or seq");
      }
    }
    validateStreamName(name);
    const v = await this._request(`${this.prefix}.STREAM.PURGE.${name}`, opts);
    return v as PurgeResponse;
  }

  async deleteMessage(
    stream: string,
    seq: number,
    erase = true,
  ): Promise<boolean> {
    validateStreamName(stream);
    const dr = { seq } as MsgDeleteRequest;
    if (!erase) {
      dr.no_erase = true;
    }
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.DELETE.${stream}`,
      dr,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async getMessage(stream: string, query: MsgRequest): Promise<StoredMsg> {
    validateStreamName(stream);
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.GET.${stream}`,
      query,
    );
    const sm = r as StreamMsgResponse;
    return new StoredMsgImpl(sm);
  }

  find(subject: string): Promise<string> {
    return this.findStream(subject);
  }

  listKvs(): Lister<KvStatus> {
    const filter: ListerFieldFilter<KvStatus> = (
      v: unknown,
    ): KvStatus[] => {
      const slr = v as StreamListResponse;
      const kvStreams = slr.streams.filter((v) => {
        return v.config.name.startsWith(kvPrefix);
      });
      kvStreams.forEach((si) => {
        this._fixInfo(si);
      });
      let cluster = "";
      if (kvStreams.length) {
        cluster = this.nc.info?.cluster ?? "";
      }
      const status = kvStreams.map((si) => {
        return new KvStatusImpl(si, cluster);
      });
      return status;
    };
    const subj = `${this.prefix}.STREAM.LIST`;
    return new ListerImpl<KvStatus>(subj, filter, this);
  }

  listObjectStores(): Lister<ObjectStoreStatus> {
    const filter: ListerFieldFilter<ObjectStoreStatus> = (
      v: unknown,
    ): ObjectStoreStatus[] => {
      const slr = v as StreamListResponse;
      const objStreams = slr.streams.filter((v) => {
        return v.config.name.startsWith(osPrefix);
      });
      objStreams.forEach((si) => {
        this._fixInfo(si);
      });
      const status = objStreams.map((si) => {
        return new ObjectStoreStatusImpl(si);
      });
      return status;
    };
    const subj = `${this.prefix}.STREAM.LIST`;
    return new ListerImpl<ObjectStoreStatus>(subj, filter, this);
  }
}

export class StoredMsgImpl implements StoredMsg {
  subject: string;
  seq: number;
  data: Uint8Array;
  time: Date;
  header: MsgHdrs;

  constructor(smr: StreamMsgResponse) {
    this.subject = smr.message.subject;
    this.seq = smr.message.seq;
    this.time = new Date(Date.parse(smr.message.time));
    this.data = smr.message.data ? this._parse(smr.message.data) : Empty;
    if (smr.message.hdrs) {
      const hd = this._parse(smr.message.hdrs);
      this.header = MsgHdrsImpl.decode(hd);
    } else {
      this.header = headers();
    }
  }

  _parse(s: string): Uint8Array {
    const bs = atob(s);
    const len = bs.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = bs.charCodeAt(i);
    }
    return bytes;
  }
}
