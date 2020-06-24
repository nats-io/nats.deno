export function check(
  fn: Function,
  interval: number = 50,
  timeout: number = 1000,
): Promise<any> {
  let toHandle: number;
  const to = new Promise((_, reject) => {
    toHandle = setTimeout(() => {
      reject(new Error("timeout"));
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
    });
  });

  return Promise.race([to, task]);
}
