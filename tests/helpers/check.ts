/*
 * Copyright 2020-2022 The NATS Authors
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

export function check(
  fn: () => unknown,
  timeout = 5000,
  opts?: { interval?: number; name?: string },
): Promise<unknown> {
  opts = opts || {};
  opts = Object.assign(opts, { interval: 50 });
  let toHandle: number;
  const to = new Promise((_, reject) => {
    toHandle = setTimeout(() => {
      const m = opts?.name ? `${opts.name} timeout` : "timeout";
      reject(new Error(m));
    }, timeout);
  });

  const task = new Promise((done) => {
    const i = setInterval(async () => {
      try {
        const v = await fn();
        if (v) {
          clearTimeout(toHandle);
          clearInterval(i);
          done(v);
        }
      } catch (_) {
        // ignore
      }
    }, opts?.interval);
  });

  return Promise.race([to, task]);
}
