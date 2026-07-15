import assert from "node:assert/strict";
import test from "node:test";

import { Deadline } from "../src/http/deadline.ts";
import {
  RetryRejectedError,
  defaultRetryDelay,
  parseRetryAfter,
  waitForRetry,
} from "../src/http/retry.ts";
import { FakeClock } from "./helpers/fake-clock.ts";

test("a late logical request receives the smaller request or tool budget", () => {
  const clock = new FakeClock();
  const toolDeadline = Deadline.after(clock, 30_000);
  clock.advance(15_000);

  const requestDeadline = toolDeadline.child(20_000);

  assert.equal(requestDeadline.remaining(), 15_000);
  clock.advance(15_001);
  assert.equal(requestDeadline.remaining(), 0);
  assert.equal(requestDeadline.expired(), true);
});

test("deadline elapsed time ignores wall-clock jumps", () => {
  const clock = new FakeClock();
  const deadline = Deadline.after(clock, 20_000);

  clock.setWallNow(Date.parse("2036-07-15T00:00:00.000Z"));
  assert.equal(deadline.remaining(), 20_000);

  clock.advance(500);
  assert.equal(deadline.remaining(), 19_500);
});

test("default retry jitter stays inside the two frozen ranges", () => {
  assert.equal(defaultRetryDelay(1, 0), 250);
  assert.equal(defaultRetryDelay(1, 0.999_999), 350);
  assert.equal(defaultRetryDelay(2, 0), 500);
  assert.equal(defaultRetryDelay(2, 0.999_999), 600);
  assert.throws(() => defaultRetryDelay(3, 0));
});

test("Retry-After supports delta seconds and HTTP dates", () => {
  const wallNow = Date.parse("2026-07-15T00:00:00.000Z");
  assert.equal(parseRetryAfter("2", wallNow), 2_000);
  assert.equal(parseRetryAfter("Wed, 15 Jul 2026 00:00:03 GMT", wallNow), 3_000);
  assert.equal(parseRetryAfter("Wed, 14 Jul 2026 00:00:00 GMT", wallNow), 0);
  assert.equal(parseRetryAfter("not a date", wallNow), null);
});

test("an invalid Retry-After fails closed without sleeping", async () => {
  const clock = new FakeClock();
  const toolDeadline = Deadline.after(clock, 30_000);
  const requestDeadline = toolDeadline.child(20_000);

  await assert.rejects(
    waitForRetry({
      retryNumber: 1,
      retryAfter: "not a date",
      clock,
      requestDeadline,
      toolDeadline,
    }),
    (error: unknown) =>
      error instanceof RetryRejectedError && error.reason === "invalid-retry-after",
  );
  assert.deepEqual(clock.sleeps, []);
});

test("Retry-After beyond the remaining budget fails immediately", async () => {
  const clock = new FakeClock();
  const toolDeadline = Deadline.after(clock, 30_000);
  const requestDeadline = toolDeadline.child(20_000);
  clock.advance(19_500);

  await assert.rejects(
    waitForRetry({
      retryNumber: 1,
      retryAfter: "1",
      clock,
      requestDeadline,
      toolDeadline,
    }),
    (error: unknown) =>
      error instanceof RetryRejectedError && error.reason === "insufficient-budget",
  );
  assert.deepEqual(clock.sleeps, []);
});

test("a permitted retry sleeps for the selected delay", async () => {
  const clock = new FakeClock({ random: 0.5 });
  const toolDeadline = Deadline.after(clock, 30_000);
  const requestDeadline = toolDeadline.child(20_000);

  await waitForRetry({ retryNumber: 1, clock, requestDeadline, toolDeadline });

  assert.deepEqual(clock.sleeps, [300]);
  assert.equal(requestDeadline.remaining(), 19_700);
});
