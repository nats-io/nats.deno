import { ErrorCode, NatsError } from "./error.ts";

export interface Codec {
  encode(d: any): Uint8Array;
  decode(a: Uint8Array): any;
}

export function StringCodec(): Codec {
  const te = new TextEncoder();
  const td = new TextDecoder();
  return {
    encode(d: any): Uint8Array {
      return te.encode(d);
    },
    decode(a: Uint8Array): any {
      return td.decode(a);
    },
  };
}

export function JSONCodec(): Codec {
  const te = new TextEncoder();
  const td = new TextDecoder();
  return {
    encode(d: any): Uint8Array {
      try {
        if (d === undefined) {
          d = null;
        }
        return te.encode(JSON.stringify(d));
      } catch (err) {
        throw NatsError.errorForCode(ErrorCode.BAD_JSON, err);
      }
    },
    //@ts-ignore
    decode(a: Uint8Array): any {
      try {
        return JSON.parse(td.decode(a));
      } catch (err) {
        throw NatsError.errorForCode(ErrorCode.BAD_JSON, err);
      }
    },
  };
}
