import { BaseApiClient } from "./jsbaseclient_api.ts";
import {
  DirectMsg,
  DirectMsgRequest,
  DirectStreamAPI,
  JetStreamOptions,
  Msg,
  NatsConnection,
  RepublishedHeaders,
  StoredMsg,
} from "./types.ts";
import { checkJsError, validateStreamName } from "./jsutil.ts";
import { MsgHdrs } from "./headers.ts";

export class DirectStreamAPIImpl extends BaseApiClient
  implements DirectStreamAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async getMessage(
    stream: string,
    query: DirectMsgRequest,
  ): Promise<StoredMsg> {
    validateStreamName(stream);
    const r = await this.nc.request(
      `$JS.API.DIRECT.GET.${stream}`,
      this.jc.encode(query),
    );

    // response is not a JS.API response
    const err = checkJsError(r);
    if (err) {
      return Promise.reject(err);
    }
    const dm = new DirectMsgImpl(r);
    return Promise.resolve(dm);
  }
}

export class DirectMsgImpl implements DirectMsg {
  data: Uint8Array;
  header: MsgHdrs;

  constructor(m: Msg) {
    if (!m.headers) {
      throw new Error("headers expected");
    }
    this.data = m.data;
    this.header = m.headers;
  }

  get subject(): string {
    return this.header.get(RepublishedHeaders.JsSubject);
  }

  get seq(): number {
    const v = this.header.get(RepublishedHeaders.JsSequence);
    return typeof v === "string" ? parseInt(v) : 0;
  }

  get time(): Date {
    return new Date(this.header.get(RepublishedHeaders.JsTimeStamp));
  }

  get stream(): string {
    return this.header.get(RepublishedHeaders.JsStream);
  }

  get lastSequence(): number {
    const v = this.header.get(RepublishedHeaders.JsLastSequence);
    return typeof v === "string" ? parseInt(v) : 0;
  }
}
