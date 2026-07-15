import type { Clock } from "../../src/http/deadline.ts";

export class FakeClock implements Clock {
  readonly sleeps: number[] = [];
  #monotonicNow: number;
  #wallNow: number;
  #randomValue: number;

  constructor(options: { monotonicNow?: number; wallNow?: number; random?: number } = {}) {
    this.#monotonicNow = options.monotonicNow ?? 0;
    this.#wallNow = options.wallNow ?? Date.parse("2026-07-15T00:00:00.000Z");
    this.#randomValue = options.random ?? 0;
  }

  now(): number {
    return this.#monotonicNow;
  }

  wallNow(): number {
    return this.#wallNow;
  }

  random(): number {
    return this.#randomValue;
  }

  async sleep(milliseconds: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw signal.reason;
    this.sleeps.push(milliseconds);
    this.#monotonicNow += milliseconds;
  }

  advance(milliseconds: number): void {
    this.#monotonicNow += milliseconds;
  }

  setWallNow(milliseconds: number): void {
    this.#wallNow = milliseconds;
  }

  setRandom(value: number): void {
    this.#randomValue = value;
  }
}
