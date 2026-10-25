import { DatabaseSync } from 'node:sqlite';
import type { SQLInputValue } from 'node:sqlite';

import {
  createMessagesListIncludeResourceScopeTest,
  createObservationalMemoryTest,
  createRunFencingTests,
} from '@internal/storage-test-utils';
import type { MemoryStorage } from '@mastra/core/storage';
import { beforeAll, describe, expect, it, vi } from 'vitest';

import { CloudflareDOStorage } from '../index';

vi.setConfig({ testTimeout: 30000, hookTimeout: 30000 });

/**
 * Stands in for a Durable Object's `ctx.storage.sql` with a real SQLite database. Like workerd's
 * SqlStorage, `exec` runs synchronously and binds only strings, numbers, null and bytes, and
 * transaction statements are refused (workerd only allows `transactionSync()`).
 */
function createSqlStorage() {
  const db = new DatabaseSync(':memory:');
  return {
    exec(query: string, ...bindings: SQLInputValue[]) {
      if (/^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b/i.test(query)) {
        throw new Error('Durable Object SqlStorage does not allow transaction statements; use transactionSync()');
      }
      const rows = db.prepare(query).all(...bindings) as Record<string, unknown>[];
      return { toArray: () => rows };
    },
  };
}

describe('CloudflareDOStorage', () => {
  const storage = new CloudflareDOStorage({ sql: createSqlStorage() as never, tablePrefix: 'test_' });
  let memory: MemoryStorage;

  beforeAll(async () => {
    await storage.init();
    memory = (await storage.getStore('memory'))!;
  });

  createRunFencingTests({ storage });

  // Memory turns observational memory off for a store that doesn't declare support, and the shared
  // suite below skips such a store, so neither would notice the Durable Objects store losing it.
  it('keeps observational memory for an agent whose memory lives in a Durable Object', async () => {
    expect(memory.supportsObservationalMemory).toBe(true);
    const record = await memory.initializeObservationalMemory({
      threadId: 'thread-in-durable-object',
      resourceId: 'user-in-durable-object',
      scope: 'thread',
      config: {},
    });
    expect((await memory.getObservationalMemory('thread-in-durable-object', 'user-in-durable-object'))?.id).toBe(
      record.id,
    );
  });

  createObservationalMemoryTest({ storage });

  createMessagesListIncludeResourceScopeTest({ getMemoryStorage: () => memory });
});
