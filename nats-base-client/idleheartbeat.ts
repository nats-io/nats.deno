/*
 * Copyright 2022 The NATS Authors
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

/**
 * Called with the number of missed heartbeats.
 * If the function returns true, the monitor will cancel monitoring.
 */
export type IdleHeartbeatFn = (n: number) => boolean;

/**
 * IdleHeartbeatOptions
 */
export type IdleHeartbeatOptions = {
  /**
   * @field maxOut - optional maximum number of missed heartbeats before notifying (default is 2)
   */
  maxOut: number;
  /**
   * @field cancelAfter - optional timer to auto cancel monitoring in millis
   */
  cancelAfter: number;
};

export class IdleHeartbeat {
  interval: number;
  maxOut: number;
  cancelAfter: number;
  timer?: number;
  autoCancelTimer?: number;
  last!: number;
  missed: number;
  count: number;
  callback: IdleHeartbeatFn;

  /**
   * Constructor
   * @param interval in millis to check
   * @param cb a callback to report when heartbeats are missed
   * @param opts monitor options @see IdleHeartbeatOptions
   */
  constructor(
    interval: number,
    cb: IdleHeartbeatFn,
    opts: Partial<IdleHeartbeatOptions> = { maxOut: 2 },
  ) {
    this.interval = interval;
    this.maxOut = opts?.maxOut || 2;
    this.cancelAfter = opts?.cancelAfter || 0;
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
    if (this.autoCancelTimer) {
      clearTimeout(this.autoCancelTimer);
    }
    if (this.timer) {
      clearInterval(this.timer);
    }
    this.timer = 0;
    this.autoCancelTimer = 0;
  }

  /**
   * work signals that there was work performed
   */
  work() {
    this.last = Date.now();
    this.missed = 0;
  }

  /**
   * internal api to change the interval, cancelAfter and maxOut
   * @param interval
   * @param cancelAfter
   * @param maxOut
   */
  _change(interval: number, cancelAfter = 0, maxOut = 2) {
    this.interval = interval;
    this.maxOut = maxOut;
    this.cancelAfter = cancelAfter;
    this.restart();
  }

  /**
   * cancels and restarts the monitoring
   */
  restart() {
    this.cancel();
    this._schedule();
  }

  /**
   * internal api called to start monitoring
   */
  _schedule() {
    if (this.cancelAfter > 0) {
      // @ts-ignore: in node is not a number - we treat this opaquely
      this.autoCancelTimer = setTimeout(() => {
        this.cancel();
      }, this.cancelAfter);
    }
    // @ts-ignore: in node is not a number - we treat this opaquely
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
