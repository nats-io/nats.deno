import { ErrorCode, NatsError } from "./error.ts";

export interface Codec<T> {
  encode(d: T): Uint8Array;
  decode(a: Uint8Array): T;
}

export function StringCodec(): Codec<string> {
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

export function JSONCodec(): Codec<any> {
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
