import assert from "node:assert/strict";
import test from "node:test";

import { Deadline } from "../src/http/deadline.ts";
import { JsonHttpClient } from "../src/http/json-http-client.ts";
import {
  RetryRejectedError,
  defaultRetryDelay,
  parseRetryAfter,
  waitForRetry,
} from "../src/http/retry.ts";
import { Semaphore } from "../src/http/semaphore.ts";
import { SingleFlight } from "../src/http/single-flight.ts";
import { ToolFailure } from "../src/errors.ts";
import { FakeClock } from "./helpers/fake-clock.ts";
import { deferred, jsonResponse, streamingResponse } from "./helpers/fake-fetch.ts";

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

test("single-flight cancellation is isolated between sibling subscribers", async () => {
  const flight = new SingleFlight<string, string>();
  const operation = deferred<string>();
  let underlyingSignal: AbortSignal | undefined;
  let starts = 0;
  const firstController = new AbortController();
  const secondController = new AbortController();

  const start = (signal: AbortSignal) => {
    starts += 1;
    underlyingSignal = signal;
    return operation.promise;
  };
  const first = flight.run("same", firstController.signal, start);
  const second = flight.run("same", secondController.signal, start);

  firstController.abort(new Error("first caller cancelled"));
  await assert.rejects(first, /first caller cancelled/);
  assert.equal(underlyingSignal?.aborted, false);

  operation.resolve("complete");
  assert.equal(await second, "complete");
  assert.equal(starts, 1);
});

test("single-flight aborts the underlying operation after every subscriber cancels", async () => {
  const flight = new SingleFlight<string, string>();
  const firstController = new AbortController();
  const secondController = new AbortController();
  let underlyingSignal: AbortSignal | undefined;

  const start = (signal: AbortSignal) => {
    underlyingSignal = signal;
    return new Promise<string>((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
  };

  const first = flight.run("same", firstController.signal, start);
  const second = flight.run("same", secondController.signal, start);
  firstController.abort(new Error("first"));
  secondController.abort(new Error("second"));

  await Promise.allSettled([first, second]);
  assert.equal(underlyingSignal?.aborted, true);
});

test("the semaphore grants only one upstream slot at a time", async () => {
  const semaphore = new Semaphore(1);
  const firstRelease = await semaphore.acquire();
  let secondGranted = false;
  const second = semaphore.acquire().then((release) => {
    secondGranted = true;
    return release;
  });

  await Promise.resolve();
  assert.equal(secondGranted, false);
  firstRelease();
  const secondRelease = await second;
  assert.equal(secondGranted, true);
  secondRelease();
});

test("JsonHttpClient serializes distinct upstream requests globally", async () => {
  const clock = new FakeClock();
  const releases: Array<() => void> = [];
  let active = 0;
  let maximumActive = 0;
  const fetchImpl: typeof fetch = async () => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    await new Promise<void>((resolve) => releases.push(resolve));
    active -= 1;
    return jsonResponse({ ok: true });
  };
  const client = new JsonHttpClient({ fetchImpl, clock });
  const validate = (input: unknown) => input as { ok: boolean };

  const first = client.request("https://example.test/one", {
    cacheKey: "one",
    toolDeadline: Deadline.after(clock, 30_000),
    validate,
  });
  const second = client.request("https://example.test/two", {
    cacheKey: "two",
    toolDeadline: Deadline.after(clock, 30_000),
    validate,
  });

  while (releases.length < 1) await Promise.resolve();
  assert.equal(releases.length, 1);
  releases[0]!();
  await first;
  while (releases.length < 2) await Promise.resolve();
  releases[1]!();
  await second;

  assert.equal(maximumActive, 1);
});

test("queue time consumes the waiting caller's tool budget", async () => {
  const clock = new FakeClock();
  const firstClock = new FakeClock();
  const releaseFirst = deferred<void>();
  let fetches = 0;
  const fetchImpl: typeof fetch = async () => {
    fetches += 1;
    if (fetches === 1) await releaseFirst.promise;
    return jsonResponse({ ok: true });
  };
  const client = new JsonHttpClient({ fetchImpl, clock });
  const validate = (input: unknown) => input as { ok: boolean };
  const first = client.request("https://example.test/one", {
    cacheKey: "one",
    toolDeadline: Deadline.after(firstClock, 30_000),
    validate,
  });
  const waiting = client.request("https://example.test/two", {
    cacheKey: "two",
    toolDeadline: Deadline.after(clock, 30_000),
    validate,
  });

  while (fetches < 1) await Promise.resolve();
  clock.advance(30_001);
  releaseFirst.resolve();
  await first;

  await assert.rejects(
    waiting,
    (error: unknown) => error instanceof ToolFailure && error.code === "UPSTREAM_TIMEOUT",
  );
  assert.equal(fetches, 1);
});

test("transient network and HTTP failures retry at most twice", async () => {
  const clock = new FakeClock({ random: 0.5 });
  let attempts = 0;
  const fetchImpl: typeof fetch = async () => {
    attempts += 1;
    if (attempts === 1) throw new TypeError("temporary network error");
    if (attempts === 2) return jsonResponse({}, { status: 503 });
    return jsonResponse({ ok: true });
  };
  const client = new JsonHttpClient({ fetchImpl, clock });

  const result = await client.request("https://example.test/retry", {
    cacheKey: "retry",
    toolDeadline: Deadline.after(clock, 30_000),
    validate: (input) => input as { ok: boolean },
  });

  assert.deepEqual(result.data, { ok: true });
  assert.equal(attempts, 3);
  assert.deepEqual(clock.sleeps, [300, 550]);
});

test("Retry-After beyond budget does not sleep or retry", async () => {
  const clock = new FakeClock();
  let attempts = 0;
  const client = new JsonHttpClient({
    clock,
    fetchImpl: async () => {
      attempts += 1;
      return jsonResponse({}, { status: 429, headers: { "retry-after": "30" } });
    },
  });

  await assert.rejects(
    client.request("https://example.test/rate", {
      cacheKey: "rate",
      toolDeadline: Deadline.after(clock, 30_000),
      validate: (input) => input,
    }),
    (error: unknown) =>
      error instanceof ToolFailure && error.code === "UPSTREAM_RATE_LIMITED",
  );
  assert.equal(attempts, 1);
  assert.deepEqual(clock.sleeps, []);
});

test("ordinary 4xx, malformed JSON, and invalid envelopes never retry", async (t) => {
  await t.test("ordinary 4xx", async () => {
    let attempts = 0;
    const client = new JsonHttpClient({
      fetchImpl: async () => {
        attempts += 1;
        return jsonResponse({}, { status: 400 });
      },
    });
    await assert.rejects(
      client.request("https://example.test/400", {
        cacheKey: "400",
        toolDeadline: Deadline.after(new FakeClock(), 30_000),
        validate: (input) => input,
      }),
      (error: unknown) =>
        error instanceof ToolFailure && error.code === "UPSTREAM_UNAVAILABLE",
    );
    assert.equal(attempts, 1);
  });

  await t.test("malformed JSON", async () => {
    let attempts = 0;
    const client = new JsonHttpClient({
      fetchImpl: async () => {
        attempts += 1;
        return new Response("not json");
      },
    });
    await assert.rejects(
      client.request("https://example.test/json", {
        cacheKey: "json",
        toolDeadline: Deadline.after(new FakeClock(), 30_000),
        validate: (input) => input,
      }),
      (error: unknown) =>
        error instanceof ToolFailure && error.code === "UPSTREAM_INVALID_RESPONSE",
    );
    assert.equal(attempts, 1);
  });

  await t.test("invalid envelope", async () => {
    let attempts = 0;
    const client = new JsonHttpClient({
      fetchImpl: async () => {
        attempts += 1;
        return jsonResponse({ wrong: true });
      },
    });
    await assert.rejects(
      client.request("https://example.test/schema", {
        cacheKey: "schema",
        toolDeadline: Deadline.after(new FakeClock(), 30_000),
        validate: () => {
          throw new Error("schema mismatch");
        },
      }),
      (error: unknown) =>
        error instanceof ToolFailure && error.code === "UPSTREAM_INVALID_RESPONSE",
    );
    assert.equal(attempts, 1);
  });
});

test("MediaWiki maxlag is recognized without sending a maxlag parameter", async () => {
  const clock = new FakeClock();
  const requestedUrls: string[] = [];
  const client = new JsonHttpClient({
    clock,
    fetchImpl: async (input) => {
      requestedUrls.push(String(input));
      return requestedUrls.length === 1
        ? jsonResponse({ error: { code: "maxlag" } })
        : jsonResponse({ ok: true });
    },
  });

  await client.request("https://example.test/api?action=query", {
    cacheKey: "maxlag",
    toolDeadline: Deadline.after(clock, 30_000),
    validate: (input) => input as { ok: boolean },
  });

  assert.equal(requestedUrls.length, 2);
  assert.ok(requestedUrls.every((url) => !url.includes("maxlag=")));
});

test("the decompressed response stream stops above 5 MiB", async () => {
  const client = new JsonHttpClient({
    fetchImpl: async () =>
      streamingResponse([new Uint8Array(3 * 1024 * 1024), new Uint8Array(3 * 1024 * 1024)]),
  });

  await assert.rejects(
    client.request("https://example.test/large", {
      cacheKey: "large",
      toolDeadline: Deadline.after(new FakeClock(), 30_000),
      validate: (input) => input,
    }),
    (error: unknown) =>
      error instanceof ToolFailure && error.code === "RESPONSE_TOO_LARGE",
  );
});

test("caller cancellation is never retried", async () => {
  const controller = new AbortController();
  let attempts = 0;
  const client = new JsonHttpClient({
    fetchImpl: async (_input, init) => {
      attempts += 1;
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), {
          once: true,
        });
      });
    },
  });
  const request = client.request("https://example.test/cancel", {
    cacheKey: "cancel",
    signal: controller.signal,
    toolDeadline: Deadline.after(new FakeClock(), 30_000),
    validate: (input) => input,
  });

  while (attempts < 1) await Promise.resolve();
  controller.abort(new Error("caller cancelled"));

  await assert.rejects(request, /caller cancelled/);
  assert.equal(attempts, 1);
});

test("only envelope-valid JSON is cached and normalization failures do not evict it", async () => {
  const clock = new FakeClock();
  let fetches = 0;
  const client = new JsonHttpClient({
    clock,
    fetchImpl: async () => {
      fetches += 1;
      return jsonResponse({ rows: [{ id: 1 }] });
    },
  });
  const validate = (input: unknown) => {
    const value = input as { rows?: unknown[] };
    if (!Array.isArray(value.rows)) throw new Error("invalid envelope");
    return { rows: value.rows };
  };

  const first = await client.request("https://example.test/cache", {
    cacheKey: "cache",
    toolDeadline: Deadline.after(clock, 30_000),
    validate,
  });
  assert.throws(() => {
    void first;
    throw new Error("downstream normalization failed");
  });
  const second = await client.request("https://example.test/cache", {
    cacheKey: "cache",
    toolDeadline: Deadline.after(clock, 30_000),
    validate,
  });

  assert.equal(fetches, 1);
  assert.equal(second.fromCache, true);
  assert.equal(second.fetchedAt, first.fetchedAt);
});

test("an envelope rejected by its validator is not cached", async () => {
  const clock = new FakeClock();
  let fetches = 0;
  const client = new JsonHttpClient({
    clock,
    fetchImpl: async () => {
      fetches += 1;
      return jsonResponse({ wrong: true });
    },
  });
  const options = {
    cacheKey: "invalid-cache",
    toolDeadline: Deadline.after(clock, 30_000),
    validate: () => {
      throw new Error("invalid envelope");
    },
  };

  await assert.rejects(client.request("https://example.test/invalid-cache", options));
  await assert.rejects(
    client.request("https://example.test/invalid-cache", {
      ...options,
      toolDeadline: Deadline.after(clock, 30_000),
    }),
  );
  assert.equal(fetches, 2);
});
