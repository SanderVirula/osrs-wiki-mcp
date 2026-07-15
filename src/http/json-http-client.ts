import { ToolFailure } from "../errors.ts";
import { ByteLruCache } from "./cache.ts";
import { Deadline, systemClock, type Clock } from "./deadline.ts";
import { RetryRejectedError, waitForRetry } from "./retry.ts";
import { Semaphore } from "./semaphore.ts";
import { SingleFlight } from "./single-flight.ts";

const REQUEST_BUDGET_MS = 20_000;
const MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
const USER_AGENT = "osrs-wiki-mcp/1.0 (+https://github.com/SanderVirula/osrs-wiki-mcp)";
const RETRYABLE_STATUS = new Set([429, 502, 503, 504]);

interface CachedJsonEnvelope {
  data: unknown;
  fetchedAt: string;
  byteLength: number;
}

export interface JsonEnvelope<T> {
  data: T;
  fetchedAt: string;
  byteLength: number;
  fromCache: boolean;
}

export interface JsonRequestOptions<T> {
  cacheKey: string;
  toolDeadline: Deadline;
  validate(value: unknown): T;
  signal?: AbortSignal;
}

export interface JsonHttpClientOptions {
  fetchImpl?: typeof fetch;
  clock?: Clock;
  maximumResponseBytes?: number;
  userAgent?: string;
  semaphore?: Semaphore;
  cache?: ByteLruCache<string, CachedJsonEnvelope>;
  singleFlight?: SingleFlight<string, CachedJsonEnvelope>;
}

export class JsonHttpClient {
  readonly #fetch: typeof fetch;
  readonly #clock: Clock;
  readonly #maximumResponseBytes: number;
  readonly #userAgent: string;
  readonly #semaphore: Semaphore;
  readonly #cache: ByteLruCache<string, CachedJsonEnvelope>;
  readonly #singleFlight: SingleFlight<string, CachedJsonEnvelope>;

  constructor({
    fetchImpl = fetch,
    clock = systemClock,
    maximumResponseBytes = MAX_RESPONSE_BYTES,
    userAgent = USER_AGENT,
    semaphore = new Semaphore(1),
    cache = new ByteLruCache<string, CachedJsonEnvelope>({ clock }),
    singleFlight = new SingleFlight<string, CachedJsonEnvelope>(),
  }: JsonHttpClientOptions = {}) {
    this.#fetch = fetchImpl;
    this.#clock = clock;
    this.#maximumResponseBytes = maximumResponseBytes;
    this.#userAgent = userAgent;
    this.#semaphore = semaphore;
    this.#cache = cache;
    this.#singleFlight = singleFlight;
  }

  async request<T>(url: string | URL, options: JsonRequestOptions<T>): Promise<JsonEnvelope<T>> {
    const cached = this.#cache.get(options.cacheKey);
    if (cached) return this.#validatedEnvelope(cached, options.validate, true);

    const networkEnvelope = await this.#singleFlight.run(
      options.cacheKey,
      options.signal,
      async (underlyingSignal) => {
        const repeatedCacheHit = this.#cache.get(options.cacheKey);
        if (repeatedCacheHit) return repeatedCacheHit;

        const envelope = await this.#requestNetwork(
          String(url),
          options.toolDeadline,
          underlyingSignal,
          options.validate,
        );
        this.#cache.set(options.cacheKey, envelope);
        return envelope;
      },
    );

    return this.#validatedEnvelope(networkEnvelope, options.validate, false);
  }

  #validatedEnvelope<T>(
    envelope: CachedJsonEnvelope,
    validate: (value: unknown) => T,
    fromCache: boolean,
  ): JsonEnvelope<T> {
    let data: T;
    try {
      data = validate(envelope.data);
    } catch (error) {
      throw new ToolFailure(
        "UPSTREAM_INVALID_RESPONSE",
        "The Wiki returned data in an unexpected format.",
        { cause: error },
      );
    }
    return { data, fetchedAt: envelope.fetchedAt, byteLength: envelope.byteLength, fromCache };
  }

  async #requestNetwork<T>(
    url: string,
    toolDeadline: Deadline,
    signal: AbortSignal,
    validate: (value: unknown) => T,
  ): Promise<CachedJsonEnvelope> {
    const requestDeadline = toolDeadline.child(REQUEST_BUDGET_MS);
    const release = await this.#semaphore.acquire(signal);
    try {
      if (toolDeadline.expired() || requestDeadline.expired()) throw timeoutFailure();

      for (let attempt = 0; attempt < 3; attempt += 1) {
        if (signal.aborted) throw abortReason(signal);
        if (toolDeadline.expired() || requestDeadline.expired()) throw timeoutFailure();

        let response: Response;
        try {
          response = await this.#fetch(url, {
            headers: {
              accept: "application/json",
              "user-agent": this.#userAgent,
            },
            signal,
          });
        } catch (error) {
          if (signal.aborted) throw abortReason(signal);
          if (error instanceof TypeError && attempt < 2) {
            await this.#wait(
              retryNumberForAttempt(attempt),
              undefined,
              requestDeadline,
              toolDeadline,
              signal,
            );
            continue;
          }
          throw new ToolFailure("UPSTREAM_UNAVAILABLE", "The Wiki request failed.", {
            cause: error,
          });
        }

        if (!response.ok) {
          if (RETRYABLE_STATUS.has(response.status)) {
            await cancelBody(response);
            if (attempt < 2) {
              try {
                await this.#wait(
                  retryNumberForAttempt(attempt),
                  response.headers.get("retry-after") ?? undefined,
                  requestDeadline,
                  toolDeadline,
                  signal,
                );
              } catch (error) {
                if (
                  response.status === 429 &&
                  (error instanceof RetryRejectedError ||
                    (error instanceof ToolFailure && error.code === "UPSTREAM_TIMEOUT"))
                ) {
                  throw new ToolFailure(
                    "UPSTREAM_RATE_LIMITED",
                    "The Wiki asked the client to retry after the remaining time budget.",
                    { cause: error },
                  );
                }
                throw error;
              }
              continue;
            }
            throw statusFailure(response.status);
          }
          await cancelBody(response);
          throw new ToolFailure(
            "UPSTREAM_UNAVAILABLE",
            `The Wiki request failed with HTTP ${response.status}.`,
          );
        }

        const body = await readJsonBody(response, this.#maximumResponseBytes);
        if (isMaxlag(body.data)) {
          if (attempt < 2) {
            await this.#wait(
              retryNumberForAttempt(attempt),
              undefined,
              requestDeadline,
              toolDeadline,
              signal,
            );
            continue;
          }
          throw new ToolFailure(
            "UPSTREAM_UNAVAILABLE",
            "The Wiki remained overloaded after bounded retries.",
          );
        }

        try {
          validate(body.data);
        } catch (error) {
          throw new ToolFailure(
            "UPSTREAM_INVALID_RESPONSE",
            "The Wiki returned data in an unexpected format.",
            { cause: error },
          );
        }

        return {
          data: body.data,
          byteLength: body.byteLength,
          fetchedAt: new Date(this.#clock.wallNow()).toISOString(),
        };
      }

      throw new ToolFailure("UPSTREAM_UNAVAILABLE", "The Wiki request failed.");
    } finally {
      release();
    }
  }

  async #wait(
    retryNumber: 1 | 2,
    retryAfter: string | undefined,
    requestDeadline: Deadline,
    toolDeadline: Deadline,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      await waitForRetry({
        retryNumber,
        ...(retryAfter === undefined ? {} : { retryAfter }),
        clock: this.#clock,
        requestDeadline,
        toolDeadline,
        signal,
      });
    } catch (error) {
      if (signal.aborted) throw abortReason(signal);
      if (error instanceof RetryRejectedError && error.reason === "insufficient-budget") {
        throw timeoutFailure(error);
      }
      throw error;
    }
  }
}

async function readJsonBody(
  response: Response,
  maximumResponseBytes: number,
): Promise<{ data: unknown; byteLength: number }> {
  const declaredLength = Number.parseInt(response.headers.get("content-length") ?? "", 10);
  if (Number.isFinite(declaredLength) && declaredLength > maximumResponseBytes) {
    await cancelBody(response);
    throw new ToolFailure("RESPONSE_TOO_LARGE", "The Wiki response exceeded 5 MiB.");
  }

  const reader = response.body?.getReader();
  const chunks: Uint8Array[] = [];
  let byteLength = 0;

  if (reader) {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      byteLength += value.byteLength;
      if (byteLength > maximumResponseBytes) {
        try {
          await reader.cancel();
        } catch {
          // The size failure is authoritative even if stream cancellation fails.
        }
        throw new ToolFailure("RESPONSE_TOO_LARGE", "The Wiki response exceeded 5 MiB.");
      }
      chunks.push(value);
    }
  }

  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (error) {
    throw new ToolFailure("UPSTREAM_INVALID_RESPONSE", "The Wiki returned invalid text.", {
      cause: error,
    });
  }

  try {
    return { data: JSON.parse(text) as unknown, byteLength };
  } catch (error) {
    throw new ToolFailure("UPSTREAM_INVALID_RESPONSE", "The Wiki returned invalid JSON.", {
      cause: error,
    });
  }
}

function isMaxlag(value: unknown): boolean {
  if (typeof value !== "object" || value === null) return false;
  const error = (value as { error?: unknown }).error;
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === "maxlag"
  );
}

function statusFailure(status: number): ToolFailure {
  return status === 429
    ? new ToolFailure("UPSTREAM_RATE_LIMITED", "The Wiki rate limit persisted after retries.")
    : new ToolFailure("UPSTREAM_UNAVAILABLE", `The Wiki returned HTTP ${status} after retries.`);
}

function timeoutFailure(cause?: unknown): ToolFailure {
  return new ToolFailure("UPSTREAM_TIMEOUT", "The Wiki request exceeded its time budget.", {
    cause,
  });
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("The operation was aborted.", "AbortError");
}

async function cancelBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Discard failures must not replace the actual HTTP error.
  }
}

function retryNumberForAttempt(attempt: number): 1 | 2 {
  if (attempt === 0) return 1;
  if (attempt === 1) return 2;
  throw new RangeError("No retry is available after the third attempt.");
}
