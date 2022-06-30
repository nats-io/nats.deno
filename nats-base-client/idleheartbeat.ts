/**
 * Called with the number of missed heartbeats.
 * If the function returns true, the monitor will
 * cancel
 */
export type IdleHeartbeatFn = (n: number) => boolean;

export class IdleHeartbeat {
  interval: number;
  maxOut: number;
  timer?: number;
  last!: number;
  missed: number;
  count: number;
  callback: IdleHeartbeatFn;

  /**
   * Constructor
   * @param interval in millis to check
   * @param cb a callback to report when heartbeats are missed
   * @param maxOut acts as a multiplier to the interval, so error will trigger on the number of intervals missed
   */
  constructor(interval: number, cb: IdleHeartbeatFn, maxOut = 2) {
    this.interval = interval;
    this.maxOut = maxOut;
    this.last = Date.now();
    this.missed = 0;
    this.count = 0;
    this.callback = cb;

    this._schedule();
  }

  /**
   * cancel monitoring
   */
  cancel() {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  /**
   * work signals that there was work performed
   */
  work() {
    this.last = Date.now();
    this.missed = 0;
  }

  /**
   * internal api to change the interval and maxOut useful only in testing
   * @param interval
   * @param maxOut
   */
  _change(interval: number, maxOut: number) {
    this.interval = interval;
    this.maxOut = maxOut;
    clearInterval(this.timer);
    this._schedule();
  }

  /**
   * internal api called to start monitoring
   */
  _schedule() {
    // @ts-ignore: node is not a number - we treat this opaquely
    this.timer = setInterval(() => {
      this.count++;
      if (Date.now() - this.interval > this.last) {
        this.missed++;
      }
      if (this.missed >= this.maxOut) {
        try {
          if (this.callback(this.missed) === true) {
            this.cancel();
          }
        } catch (err) {
          console.log(err);
        }
      }
    }, this.interval);
  }
}
