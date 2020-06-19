export interface Lock<T> extends Promise<T> {
  resolve: (value?: T | PromiseLike<T>) => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  reject: (reason?: any) => void;
  cancel: () => void;
  lock: () => void;
  unlock: () => void;
}
/** Creates a lock that resolves when it's lock count reaches 0.
 * If a timeout is provided, the lock rejects if it has not unlocked
 * by the specified number of milliseconds (default 1000).
 */
export function Lock<T>(ms: number = 1000, count: number = 1): Lock<T> {
  let methods;
  const promise = new Promise((resolve, reject) => {
    let timer: number;

    let cancel = (): void => {
      if (timer) {
        clearTimeout(timer);
      }
    };

    let lock = (): void => {
      count++;
    };

    let unlock = (): void => {
      count--;
      if (count === 0) {
        cancel();
        resolve();
      }
    };

    methods = { resolve, reject, lock, unlock, cancel };
    if (ms) {
      timer = setTimeout(() => {
        reject(new Error("timeout"));
      }, ms);
    }
  });
  return Object.assign(promise, methods) as Lock<T>;
}
