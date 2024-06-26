/*
 * Copyright 2023 The NATS Authors
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

export function humanizeBytes(bytes: number, si = false): string {
  const base = si ? 1000 : 1024;
  const pre = si
    ? ["k", "M", "G", "T", "P", "E"]
    : ["K", "M", "G", "T", "P", "E"];
  const post = si ? "iB" : "B";

  if (bytes < base) {
    return `${bytes.toFixed(2)} ${post}`;
  }
  const exp = parseInt(Math.log(bytes) / Math.log(base) + "");
  const index = parseInt((exp - 1) + "");
  return `${(bytes / Math.pow(base, exp)).toFixed(2)} ${pre[index]}${post}`;
}

export type DataRequest = {
  // generate data that is this size
  size: number;
  // send using multiple messages - note server will reject if too big
  max_chunk?: number;
};
