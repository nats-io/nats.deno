import { Lock } from "./helpers/mod.ts";

class EventTest extends EventTarget {
  child: ChildEmitter;
  constructor() {
    super();
    this.child = new ChildEmitter();
  }

  emit(name: string): void {
    const evt = new CustomEvent(name, { detail: { test: true, fatal: false } });
    this.dispatchEvent(evt);
  }
}

class ChildEmitter extends EventTarget {
  constructor() {
    super();
  }

  emit(name: string): void {
    const evt = new CustomEvent(
      name,
      { detail: { test: "hey" }, bubbles: true },
    );
    this.dispatchEvent(evt);
  }
}

// Deno.test("events - test evt", async () => {
//   const lock = Lock();
//   const et = new EventTest()
//
//   et.addEventListener("foo", ((e: CustomEvent<void>) => {
//     //@ts-ignore
//     console.log(e.detail);
//     lock.unlock();
//   }) as EventListener)
//
//   et.child.emit("foo")
//   await lock;
// })
