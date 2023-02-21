/*
 * Copyright 2021-2023 The NATS Authors
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
  ExternalStream,
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
  StreamNames,
  StreamSource,
  StreamUpdateConfig,
  SuccessResponse,
} from "./types.ts";
import { BaseApiClient } from "./jsbaseclient_api.ts";
import { ListerFieldFilter, ListerImpl } from "./jslister.ts";
import { validateStreamName } from "./jsutil.ts";
import { headers, MsgHdrs, MsgHdrsImpl } from "./headers.ts";
import { kvPrefix, KvStatusImpl } from "./kv.ts";
import { ObjectStoreStatusImpl, osPrefix } from "./objectstore.ts";
import { Codec, JSONCodec } from "./codec.ts";
import { TD } from "./encoders.ts";
import { Feature } from "./semver.ts";
import { NatsConnectionImpl } from "./nats.ts";

export function convertStreamSourceDomain(s?: StreamSource) {
  if (s === undefined) {
    return undefined;
  }
  const { domain } = s;
  if (domain === undefined) {
    return s;
  }
  const copy = Object.assign({}, s) as StreamSource;
  delete copy.domain;

  if (domain === "") {
    return copy;
  }
  if (copy.external) {
    throw new Error("domain and external are both set");
  }
  copy.external = { api: `$JS.${domain}.API` } as ExternalStream;
  return copy;
}

export class StreamAPIImpl extends BaseApiClient implements StreamAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(cfg = {} as Partial<StreamConfig>): Promise<StreamInfo> {
    const nci = this.nc as NatsConnectionImpl;
    if (cfg.metadata) {
      const { min, ok } = nci.features.get(Feature.JS_STREAM_CONSUMER_METADATA);
      if (!ok) {
        throw new Error(`stream 'metadata' requires server ${min}`);
      }
    }
    validateStreamName(cfg.name);
    cfg.mirror = convertStreamSourceDomain(cfg.mirror);
    //@ts-ignore: the sources are either set or not - so no item should be undefined in the list
    cfg.sources = cfg.sources?.map(convertStreamSourceDomain);
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
    const nci = this.nc as NatsConnectionImpl;
    if (cfg.metadata) {
      const { min, ok } = nci.features.get(Feature.JS_STREAM_CONSUMER_METADATA);
      if (!ok) {
        throw new Error(`stream 'metadata' requires server ${min}`);
      }
    }
    validateStreamName(name);
    const old = await this.info(name);
    const update = Object.assign(old.config, cfg);
    update.mirror = convertStreamSourceDomain(update.mirror);
    //@ts-ignore: the sources are either set or not - so no item should be undefined in the list
    update.sources = update.sources?.map(convertStreamSourceDomain);

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
    let { total, limit } = si;

    // check how many subjects we got in the first request
    let have = si.state.subjects
      ? Object.getOwnPropertyNames(si.state.subjects).length
      : 1;

    // if the response is paged, we have a large list of subjects
    // handle the paging and return a StreamInfo with all of it
    if (total && total > have) {
      const infos: StreamInfo[] = [si];
      const paged = data || {} as unknown as ApiPagedRequest;
      let i = 0;
      // total could change, so it is possible to have collected
      // more that the total
      while (total > have) {
        i++;
        paged.offset = limit * i;
        const r = await this._request(subj, paged) as StreamInfo;
        // update it in case it changed
        total = r.total;
        infos.push(r);
        const count = Object.getOwnPropertyNames(r.state.subjects).length;
        have += count;
        // if request returns less than limit it is done
        if (count < limit) {
          // done
          break;
        }
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

  list(subject = ""): Lister<StreamInfo> {
    const payload = subject?.length ? { subject } : {};
    const listerFilter: ListerFieldFilter<StreamInfo> = (
      v: unknown,
    ): StreamInfo[] => {
      const slr = v as StreamListResponse;
      slr.streams.forEach((si) => {
        this._fixInfo(si);
      });
      return slr.streams;
    };
    const subj = `${this.prefix}.STREAM.LIST`;
    return new ListerImpl<StreamInfo>(subj, listerFilter, this, payload);
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

  names(subject = ""): Lister<string> {
    const payload = subject?.length ? { subject } : {};
    const listerFilter: ListerFieldFilter<string> = (
      v: unknown,
    ): string[] => {
      const slr = v as StreamNames;
      return slr.streams;
    };
    const subj = `${this.prefix}.STREAM.NAMES`;
    return new ListerImpl<string>(subj, listerFilter, this, payload);
  }
}

export class StoredMsgImpl implements StoredMsg {
  _header?: MsgHdrs;
  smr: StreamMsgResponse;
  static jc?: Codec<unknown>;

  constructor(smr: StreamMsgResponse) {
    this.smr = smr;
  }

  get subject(): string {
    return this.smr.message.subject;
  }

  get seq(): number {
    return this.smr.message.seq;
  }

  get timestamp(): string {
    return this.smr.message.time;
  }

  get time(): Date {
    return new Date(Date.parse(this.timestamp));
  }

  get data(): Uint8Array {
    return this.smr.message.data ? this._parse(this.smr.message.data) : Empty;
  }

  get header(): MsgHdrs {
    if (!this._header) {
      if (this.smr.message.hdrs) {
        const hd = this._parse(this.smr.message.hdrs);
        this._header = MsgHdrsImpl.decode(hd);
      } else {
        this._header = headers();
      }
    }
    return this._header;
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

  json<T = unknown>(): T {
    if (!StoredMsgImpl.jc) {
      StoredMsgImpl.jc = JSONCodec();
    }
    return StoredMsgImpl.jc.decode(this.data) as T;
  }

  string(): string {
    return TD.decode(this.data);
  }
}
