interface BatchQueueOptions {
  /**
   * Maximum number of items in a batch before flushing.
   * @default 1000
   */
  batchSize?: number;
  /**
   * Maximum time in milliseconds to wait before flushing a non-empty batch.
   * @default 5000
   */
  timeout?: number;
}

interface BatchQueue<T> {
  /**
   * Add one or more items to the queue. If the batch size is reached, it flushes and awaits the result.
   */
  enqueue(item: T | T[]): Promise<void>;
  /**
   * Manually flush the current batch and await the result.
   */
  flush(): Promise<void>;
  /**
   * Number of items currently in the batch.
   */
  readonly length: number;
}

/**
 * A simple batching queue with backpressure.
 */
export function batchQueue<T, R>(
  fn: (items: T[]) => Promise<R> | R,
  options: BatchQueueOptions = {},
): BatchQueue<T> {
  const batchSize = options.batchSize ?? 1000;
  const timeout = options.timeout ?? 5000;

  const batch: T[] = [];
  let lastFlushAt = Date.now();
  let flushPromise: Promise<void> | null = null;
  let flushError: Error | null = null;
  let timeoutId: Timer | null = null;

  const flush = async (waitForCurrent = true) => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }

    // Backpressure: wait for previous flush to finish (max 1 active flush)
    if (flushPromise) {
      await flushPromise;
    }
    if (flushError) throw flushError;

    const count = batch.length;
    if (count === 0) {
      return;
    }

    const items = batch.slice(0, count);
    batch.splice(0, count);
    lastFlushAt = Date.now();

    flushPromise = (async () => {
      try {
        await fn(items);
      } catch (err) {
        flushError = err instanceof Error ? err : new Error(String(err));
      } finally {
        flushPromise = null;
      }
    })();

    if (waitForCurrent) {
      await flushPromise;
      if (flushError) throw flushError;
    }
  };

  const scheduleTimeout = () => {
    if (timeoutId || batch.length === 0) return;

    const elapsed = Date.now() - lastFlushAt;
    const remaining = Math.max(0, timeout - elapsed);

    timeoutId = setTimeout(() => {
      timeoutId = null;
      flush().catch((err) => {
        flushError = err instanceof Error ? err : new Error(String(err));
      });
    }, remaining);
  };

  return {
    async enqueue(item: T | T[]) {
      if (flushError) throw flushError;

      if (Array.isArray(item)) {
        batch.push(...item);
      } else {
        batch.push(item);
      }

      if (batch.length >= batchSize) {
        await flush(false);
      } else {
        scheduleTimeout();
      }
    },
    flush: () => flush(true),
    get length() {
      return batch.length;
    },
  };
}
