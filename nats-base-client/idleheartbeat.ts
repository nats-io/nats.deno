import { Deferred, deferred } from "./util.ts";

export class IdleHeartbeat {
  interval: number;
  maxOut: number;
  timer?: number;
  last!: number;
  missed: number;
  done: Deferred<number>;
  count: number;

  constructor(interval: number, maxOut: number) {
    this.interval = interval;
    this.maxOut = maxOut;
    this.done = deferred();
    this.last = Date.now();
    this.missed = 0;
    this.count = 0;

    this._schedule();
  }

  cancel() {
    if (this.timer) {
      clearInterval(this.timer);
    }
    this.done.resolve(0);
  }

  ok() {
    this.last = Date.now();
    this.missed = 0;
  }

  _schedule() {
    // @ts-ignore: node is not a number - we treat this opaquely
    this.timer = setInterval(() => {
      this.count++;
      if (Date.now() - this.interval > this.last) {
        this.missed++;
      }
      if (this.missed >= this.maxOut) {
        clearInterval(this.timer);
        this.done.resolve(this.missed);
      }
    }, this.interval);
  }
}
