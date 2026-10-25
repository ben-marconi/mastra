import {
  assertActiveObservationsApplied,
  isAppendOnlySince,
  isBufferedChunkCoveredByCursor,
  maxObservationCursor,
  planReflectionGenerationText,
  withObservationalMemoryLifecycleColumns,
} from '@internal/core/memory';
import type { UpdateActiveObservationsResult, UpdateBufferedObservationsResult } from '@internal/core/memory';
import { MessageList } from '@mastra/core/agent';
import type { MastraMessageContentV2 } from '@mastra/core/agent';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import type { MastraMessageV1, MastraDBMessage, StorageThreadType } from '@mastra/core/memory';
import {
  createStorageErrorId,
  ensureDate,
  isRunFenceConflictError,
  MemoryStorage,
  OBSERVATIONAL_MEMORY_TABLE_SCHEMA,
  resolveRunFence,
  RUN_FENCING_TABLE_SCHEMAS,
  serializeDate,
  TABLE_MEMORY_RUN_FENCES,
  TABLE_MESSAGES,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_THREADS,
  TABLE_RESOURCES,
  TABLE_SCHEMAS,
  normalizePerPage,
  calculatePagination,
  validateStorageMetadataFilter,
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
} from '@mastra/core/storage';
import type {
  BufferedObservationChunk,
  CreateObservationalMemoryInput,
  CreateReflectionGenerationInput,
  ObservationalMemoryHistoryOptions,
  ObservationalMemoryRecord,
  RunFence,
  StorageResourceType,
  StorageListMessagesByResourceIdInput,
  StorageListMessagesInput,
  StorageListMessagesOutput,
  StorageMetadataFilter,
  StorageMetadataFilterValue,
  StorageListThreadsInput,
  StorageListThreadsOutput,
  TABLE_NAMES,
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  SwapBufferedReflectionToActiveInput,
  SwapBufferedToActiveInput,
  SwapBufferedToActiveResult,
  TableRetentionPolicy,
  UpdateActiveObservationsInput,
  UpdateBufferedObservationsInput,
  UpdateBufferedReflectionInput,
  UpdateObservationalMemoryConfigInput,
} from '@mastra/core/storage';

import { DODB } from '../../db';
import type { DODomainConfig } from '../../db';
import { assertRunFenceSync, DB_NOW_MS, executeFenced, runFenceGuard } from '../../db/run-fencing';
import type { RunFenceCheck } from '../../db/run-fencing';
import { createSqlBuilder } from '../../sql-builder';
import type { SqlParam } from '../../sql-builder';
import { deserializeValue, isArrayOfRecords } from '../utils';

const OM_TABLE = TABLE_OBSERVATIONAL_MEMORY as TABLE_NAMES;
/** Canonical head order: newest generation, then earliest createdAt, then lowest id. */
const OM_HEAD_ORDER = `"generationCount" DESC, "createdAt" ASC, id ASC`;
/** Lifecycle writes aimed at a retired record follow `supersededBy` to the head at most this many times. */
const OM_MAX_HEAD_HOPS = 3;

function addSqliteMetadataFilter(
  conditions: string[],
  params: unknown[],
  metadataFilter: StorageMetadataFilter | undefined,
): void {
  if (!metadataFilter) return;

  for (const [key, value] of Object.entries(metadataFilter)) {
    const path = `$.metadata.${key}`;
    conditions.push(`CASE WHEN json_valid(content) THEN json_type(content, ?) IS NOT NULL ELSE 0 END`);
    params.push(path);
    addSqliteMetadataValuePredicate(conditions, params, path, value);
  }
}

function addSqliteMetadataValuePredicate(
  conditions: string[],
  params: unknown[],
  path: string,
  value: StorageMetadataFilterValue,
): void {
  if (value === null) {
    conditions.push(`CASE WHEN json_valid(content) THEN json_type(content, ?) = 'null' ELSE 0 END`);
    params.push(path);
    return;
  }

  if (typeof value === 'string') {
    conditions.push(
      `CASE WHEN json_valid(content) THEN json_type(content, ?) = 'text' AND json_extract(content, ?) = ? ELSE 0 END`,
    );
    params.push(path, path, value);
    return;
  }

  if (typeof value === 'number') {
    conditions.push(
      `CASE WHEN json_valid(content) THEN json_type(content, ?) IN ('integer', 'real') AND json_extract(content, ?) = ? ELSE 0 END`,
    );
    params.push(path, path, value);
    return;
  }

  conditions.push(
    `CASE WHEN json_valid(content) THEN json_type(content, ?) = ? AND json_extract(content, ?) = ? ELSE 0 END`,
  );
  params.push(path, value ? 'true' : 'false', path, value ? 1 : 0);
}

export class MemoryStorageDO extends MemoryStorage {
  /**
   * Tables `prune()` can clean up. Run fences anchor on `retiredAt` (epoch ms), which stays unset until the
   * execution holding the fence settles, so a running execution's fence is
   * never pruned.
   */
  static override readonly retentionTables: RetentionTablesDescriptor = {
    runFences: { table: TABLE_MEMORY_RUN_FENCES, column: 'retiredAt', indexed: false, anchorType: 'epoch-ms' },
  };

  override readonly supportsPartialThreadUpdate = true;
  readonly supportsObservationalMemory = true;
  readonly supportsObservationalMemoryHistorySearch = true;
  #db: DODB;

  constructor(config: DODomainConfig) {
    super();
    this.#db = new DODB(config);
  }

  async init(): Promise<void> {
    await this.#db.createTable({ tableName: TABLE_THREADS, schema: TABLE_SCHEMAS[TABLE_THREADS] });
    await this.#db.createTable({ tableName: TABLE_MESSAGES, schema: TABLE_SCHEMAS[TABLE_MESSAGES] });
    await this.#db.createTable({ tableName: TABLE_RESOURCES, schema: TABLE_SCHEMAS[TABLE_RESOURCES] });
    // Add resourceId column for backwards compatibility
    await this.#db.alterTable({
      tableName: TABLE_MESSAGES,
      schema: TABLE_SCHEMAS[TABLE_MESSAGES],
      ifNotExists: ['resourceId'],
    });
    await this.#db.createTable({
      tableName: TABLE_MEMORY_RUN_FENCES as TABLE_NAMES,
      schema: RUN_FENCING_TABLE_SCHEMAS[TABLE_MEMORY_RUN_FENCES],
    });

    const omSchema = withObservationalMemoryLifecycleColumns(
      OBSERVATIONAL_MEMORY_TABLE_SCHEMA[TABLE_OBSERVATIONAL_MEMORY],
    )!;
    await this.#db.createTable({ tableName: OM_TABLE, schema: omSchema });
    // Adds the columns a newer @mastra/core defines to a table created under an older one.
    await this.#db.alterTable({
      tableName: OM_TABLE,
      schema: omSchema,
      ifNotExists: Object.keys(omSchema),
    });
    const { sql: indexSql } = createSqlBuilder()
      .createIndex(`${this.#omTable}_lookup_key_idx`, this.#omTable, 'lookupKey')
      .build();
    await this.#db.executeQuery({ sql: indexSql });
  }

  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    const targets = resolveRetentionTargets({
      policies,
      descriptor: MemoryStorageDO.retentionTables,
      order: ['runFences'],
    });
    return executeRetentionPrune({
      domain: 'memory',
      targets,
      options,
      cutoffFor: (target, now) => retentionCutoffMs(target.policy, now),
      deleteBatch: (_target, cutoff, limit) =>
        this.#db.pruneRunRows({
          tableName: TABLE_MEMORY_RUN_FENCES as TABLE_NAMES,
          column: 'retiredAt',
          cutoff: Number(cutoff),
          limit,
        }),
    });
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.#db.clearTable({ tableName: TABLE_MESSAGES });
    await this.#db.clearTable({ tableName: TABLE_THREADS });
    await this.#db.clearTable({ tableName: TABLE_RESOURCES });
    await this.#db.clearTable({ tableName: TABLE_MEMORY_RUN_FENCES as TABLE_NAMES });
    await this.#db.clearTable({ tableName: OM_TABLE });
  }

  override supportsRunFencing(): boolean {
    return true;
  }

  get #fencesTable(): string {
    return this.#db.getTableName(TABLE_MEMORY_RUN_FENCES as TABLE_NAMES);
  }

  override async raiseRunFence(fence: RunFence): Promise<boolean> {
    const table = this.#fencesTable;
    try {
      // One statement, so no other raise lands between judging the current fence and replacing it.
      // Re-raising the current fence writes the same row back and still counts as raised.
      // Every write un-retires the fence, so a reused run's fence isn't pruned mid-run.
      const rows = (await this.#db.executeQuery({
        sql: `INSERT INTO ${table} (runId, generation, ownerId) VALUES (?, ?, ?)
          ON CONFLICT(runId) DO UPDATE SET generation = excluded.generation, ownerId = excluded.ownerId, retiredAt = NULL
          WHERE excluded.generation > ${table}.generation
            OR (excluded.generation = ${table}.generation AND excluded.ownerId = ${table}.ownerId)
          RETURNING runId`,
        params: [fence.runId, fence.generation, fence.ownerId],
      })) as Record<string, unknown>[];
      return rows.length > 0;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'RAISE_RUN_FENCE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { runId: fence.runId },
        },
        error,
      );
    }
  }

  override async retireRunFence(fence: RunFence): Promise<boolean> {
    try {
      const rows = (await this.#db.executeQuery({
        sql: `UPDATE ${this.#fencesTable} SET retiredAt = ${DB_NOW_MS}
          WHERE runId = ? AND generation = ? AND ownerId = ?
          RETURNING runId`,
        params: [fence.runId, fence.generation, fence.ownerId],
      })) as Record<string, unknown>[];
      return rows.length > 0;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'RETIRE_RUN_FENCE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { runId: fence.runId },
        },
        error,
      );
    }
  }

  /** The check a write must pass: its own fence, otherwise the one in scope. */
  #runFenceCheck(fence: RunFence | undefined, operation: string): RunFenceCheck | undefined {
    const resolved = resolveRunFence(this, fence);
    return resolved && { claimsTable: this.#fencesTable, fence: resolved, operation };
  }

  async getResourceById({ resourceId }: { resourceId: string }): Promise<StorageResourceType | null> {
    const resource = await this.#db.load<StorageResourceType>({
      tableName: TABLE_RESOURCES,
      keys: { id: resourceId },
    });

    if (!resource) return null;

    try {
      return {
        ...resource,
        createdAt: ensureDate(resource.createdAt) as Date,
        updatedAt: ensureDate(resource.updatedAt) as Date,
        metadata:
          typeof resource.metadata === 'string'
            ? (JSON.parse(resource.metadata || '{}') as Record<string, unknown>)
            : resource.metadata,
      };
    } catch (error) {
      const mastraError = new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'GET_RESOURCE_BY_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Error processing resource ${resourceId}: ${error instanceof Error ? error.message : String(error)}`,
          details: { resourceId },
        },
        error,
      );
      this.logger?.error(mastraError.toString());
      this.logger?.trackException(mastraError);
      return null;
    }
  }

  async saveResource({ resource }: { resource: StorageResourceType }): Promise<StorageResourceType> {
    return this.#insertResource(resource, undefined);
  }

  async #insertResource(resource: StorageResourceType, check: RunFenceCheck | undefined): Promise<StorageResourceType> {
    const fullTableName = this.#db.getTableName(TABLE_RESOURCES);

    // Prepare the record for SQL insertion
    const resourceToSave = {
      id: resource.id,
      workingMemory: resource.workingMemory,
      metadata: resource.metadata ? JSON.stringify(resource.metadata) : null,
      createdAt: resource.createdAt,
      updatedAt: resource.updatedAt,
    };

    // Process record for SQL insertion
    const processedRecord = await this.#db.processRecord(resourceToSave);

    const columns = Object.keys(processedRecord);
    const values = Object.values(processedRecord);

    // Specify which columns to update on conflict (all except id)
    const updateMap: Record<string, string> = {
      workingMemory: 'excluded.workingMemory',
      metadata: 'excluded.metadata',
      createdAt: 'excluded.createdAt',
      updatedAt: 'excluded.updatedAt',
    };

    // Use the new insert method with ON CONFLICT
    const query = createSqlBuilder().insert(
      fullTableName,
      columns,
      values as (string | number | boolean | null | undefined)[],
      ['id'],
      updateMap,
      runFenceGuard(check),
    );

    try {
      await executeFenced(this.#db, check, query.build());
      return resource;
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'SAVE_RESOURCE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to save resource to ${fullTableName}: ${error instanceof Error ? error.message : String(error)}`,
          details: { resourceId: resource.id },
        },
        error,
      );
    }
  }

  async updateResource({
    resourceId,
    workingMemory,
    metadata,
    fence,
  }: {
    resourceId: string;
    workingMemory?: string;
    metadata?: Record<string, unknown>;
    fence?: RunFence;
  }): Promise<StorageResourceType> {
    const check = this.#runFenceCheck(fence, 'updateResource');
    const existingResource = await this.getResourceById({ resourceId });

    if (!existingResource) {
      // Create new resource if it doesn't exist
      const newResource: StorageResourceType = {
        id: resourceId,
        workingMemory,
        metadata: metadata || {},
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      return this.#insertResource(newResource, check);
    }

    const updatedAt = new Date();
    const updatedResource = {
      ...existingResource,
      workingMemory: workingMemory !== undefined ? workingMemory : existingResource.workingMemory,
      metadata: {
        ...existingResource.metadata,
        ...metadata,
      },
      updatedAt,
    };

    const fullTableName = this.#db.getTableName(TABLE_RESOURCES);

    const columns = ['workingMemory', 'metadata', 'updatedAt'];
    const values = [updatedResource.workingMemory, JSON.stringify(updatedResource.metadata), updatedAt.toISOString()];

    const query = createSqlBuilder().update(fullTableName, columns, values).where('id = ?', resourceId);
    const guard = runFenceGuard(check);
    if (guard) query.andWhere(guard.sql, ...guard.params);

    try {
      await executeFenced(this.#db, check, query.build());
      return updatedResource;
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'UPDATE_RESOURCE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to update resource ${resourceId}: ${error instanceof Error ? error.message : String(error)}`,
          details: { resourceId },
        },
        error,
      );
    }
  }

  async getThreadById({
    threadId,
    resourceId,
  }: {
    threadId: string;
    resourceId?: string;
  }): Promise<StorageThreadType | null> {
    const thread = await this.#db.load<StorageThreadType>({
      tableName: TABLE_THREADS,
      keys: { id: threadId },
    });

    if (!thread || (resourceId !== undefined && thread.resourceId !== resourceId)) return null;

    try {
      return {
        ...thread,
        createdAt: ensureDate(thread.createdAt) as Date,
        updatedAt: ensureDate(thread.updatedAt) as Date,
        metadata:
          typeof thread.metadata === 'string'
            ? (JSON.parse(thread.metadata || '{}') as Record<string, unknown>)
            : thread.metadata || {},
      };
    } catch (error) {
      const mastraError = new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'GET_THREAD_BY_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Error processing thread ${threadId}: ${error instanceof Error ? error.message : String(error)}`,
          details: { threadId },
        },
        error,
      );
      this.logger?.error(mastraError.toString());
      this.logger?.trackException(mastraError);
      return null;
    }
  }

  public async listThreads(args: StorageListThreadsInput): Promise<StorageListThreadsOutput> {
    const { page = 0, perPage: perPageInput, orderBy, filter } = args;

    try {
      this.validatePaginationInput(page, perPageInput ?? 100);
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_THREADS', 'INVALID_PAGE'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { page, ...(perPageInput !== undefined && { perPage: perPageInput }) },
        },
        error instanceof Error ? error : new Error('Invalid pagination parameters'),
      );
    }

    const perPage = normalizePerPage(perPageInput, 100);

    try {
      this.validateMetadataKeys(filter?.metadata);
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_THREADS', 'INVALID_METADATA_KEY'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { metadataKeys: filter?.metadata ? Object.keys(filter.metadata).join(', ') : '' },
        },
        error instanceof Error ? error : new Error('Invalid metadata key'),
      );
    }

    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);
    const { field, direction } = this.parseOrderBy(orderBy);
    const fullTableName = this.#db.getTableName(TABLE_THREADS);

    const mapRowToStorageThreadType = (row: any): StorageThreadType => ({
      ...row,
      createdAt: ensureDate(row.createdAt) as Date,
      updatedAt: ensureDate(row.updatedAt) as Date,
      metadata:
        typeof row.metadata === 'string'
          ? (JSON.parse(row.metadata || '{}') as Record<string, unknown>)
          : row.metadata || {},
    });

    try {
      let countQuery = createSqlBuilder().count().from(fullTableName);
      let selectQuery = createSqlBuilder().select('*').from(fullTableName);

      if (filter?.resourceId) {
        countQuery = countQuery.whereAnd('resourceId = ?', filter.resourceId);
        selectQuery = selectQuery.whereAnd('resourceId = ?', filter.resourceId);
      }

      if (filter?.metadata && Object.keys(filter.metadata).length > 0) {
        for (const [key, value] of Object.entries(filter.metadata)) {
          if (value !== null && typeof value === 'object') {
            throw new MastraError(
              {
                id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_THREADS', 'INVALID_METADATA_VALUE'),
                domain: ErrorDomain.STORAGE,
                category: ErrorCategory.USER,
                text: `Metadata filter value for key "${key}" must be a scalar type (string, number, boolean, or null), got ${Array.isArray(value) ? 'array' : 'object'}`,
                details: { key, valueType: Array.isArray(value) ? 'array' : 'object' },
              },
              new Error('Invalid metadata filter value type'),
            );
          }

          if (value === null) {
            const condition = `json_extract(metadata, '$.${key}') IS NULL`;
            countQuery = countQuery.whereAnd(condition);
            selectQuery = selectQuery.whereAnd(condition);
          } else {
            const condition = `json_extract(metadata, '$.${key}') = ?`;
            const filterValue = value as string | number | boolean;
            countQuery = countQuery.whereAnd(condition, filterValue);
            selectQuery = selectQuery.whereAnd(condition, filterValue);
          }
        }
      }

      const countResult = (await this.#db.executeQuery(countQuery.build())) as {
        count: number;
      }[];
      const total = Number(countResult?.[0]?.count ?? 0);

      if (total === 0) {
        return {
          threads: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      const limitValue = perPageInput === false ? total : perPage;
      selectQuery = selectQuery.orderBy(field, direction).limit(limitValue).offset(offset);

      const results = (await this.#db.executeQuery(selectQuery.build())) as Record<string, unknown>[];
      const threads = results.map(mapRowToStorageThreadType);

      return {
        threads,
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError && error.category === ErrorCategory.USER) {
        throw error;
      }
      const mastraError = new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_THREADS', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Error listing threads: ${error instanceof Error ? error.message : String(error)}`,
          details: {
            ...(filter?.resourceId && { resourceId: filter.resourceId }),
            hasMetadataFilter: !!filter?.metadata,
          },
        },
        error,
      );
      this.logger?.error(mastraError.toString());
      this.logger?.trackException(mastraError);
      return {
        threads: [],
        total: 0,
        page,
        perPage: perPageForResponse,
        hasMore: false,
      };
    }
  }

  async saveThread({ thread, fence }: { thread: StorageThreadType; fence?: RunFence }): Promise<StorageThreadType> {
    const fullTableName = this.#db.getTableName(TABLE_THREADS);
    const check = this.#runFenceCheck(fence, 'saveThread');

    // Prepare the record for SQL insertion
    const threadToSave = {
      id: thread.id,
      resourceId: thread.resourceId,
      title: thread.title,
      metadata: thread.metadata ? JSON.stringify(thread.metadata) : null,
      createdAt: thread.createdAt.toISOString(),
      updatedAt: thread.updatedAt.toISOString(),
    };

    // Process record for SQL insertion
    const processedRecord = await this.#db.processRecord(threadToSave);

    const columns = Object.keys(processedRecord);
    const values = Object.values(processedRecord);

    // Specify which columns to update on conflict (all except id)
    const updateMap: Record<string, string> = {
      resourceId: 'excluded.resourceId',
      title: 'excluded.title',
      metadata: 'excluded.metadata',
      createdAt: 'excluded.createdAt',
      updatedAt: 'excluded.updatedAt',
    };

    // Use the new insert method with ON CONFLICT
    const query = createSqlBuilder().insert(
      fullTableName,
      columns,
      values as (string | number | boolean | null | undefined)[],
      ['id'],
      updateMap,
      runFenceGuard(check),
    );

    try {
      await executeFenced(this.#db, check, query.build());
      return thread;
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'SAVE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to save thread to ${fullTableName}: ${error instanceof Error ? error.message : String(error)}`,
          details: { threadId: thread.id },
        },
        error,
      );
    }
  }

  async updateThread({
    id,
    title,
    metadata,
    fence,
  }: {
    id: string;
    title?: string;
    metadata?: Record<string, unknown>;
    fence?: RunFence;
  }): Promise<StorageThreadType> {
    const check = this.#runFenceCheck(fence, 'updateThread');
    const thread = await this.getThreadById({ threadId: id });
    try {
      if (!thread) {
        throw new Error(`Thread ${id} not found`);
      }
      const fullTableName = this.#db.getTableName(TABLE_THREADS);

      const mergedMetadata = {
        ...thread.metadata,
        ...metadata,
      };

      const updatedTitle = title !== undefined ? title : thread.title;
      const updatedAt = new Date();

      // Only write the columns the caller supplied. Writing a title read moments
      // ago would clobber one generated in between, and the same applies to
      // metadata on a title-only update.
      const columns: string[] = [];
      const values: (string | null)[] = [];

      if (title !== undefined) {
        columns.push('title');
        values.push(title);
      }
      if (metadata !== undefined) {
        columns.push('metadata');
        values.push(JSON.stringify(mergedMetadata));
      }
      columns.push('updatedAt');
      values.push(updatedAt.toISOString());

      const query = createSqlBuilder().update(fullTableName, columns, values).where('id = ?', id);
      const guard = runFenceGuard(check);
      if (guard) query.andWhere(guard.sql, ...guard.params);

      await executeFenced(this.#db, check, query.build());

      return {
        ...thread,
        title: updatedTitle,
        metadata: mergedMetadata,
        updatedAt,
      };
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'UPDATE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to update thread ${id}: ${error instanceof Error ? error.message : String(error)}`,
          details: { threadId: id },
        },
        error,
      );
    }
  }

  async deleteThread({ threadId }: { threadId: string }): Promise<void> {
    const fullTableName = this.#db.getTableName(TABLE_THREADS);

    try {
      // Delete associated messages first to avoid orphaned data
      const messagesTableName = this.#db.getTableName(TABLE_MESSAGES);
      const deleteMessagesQuery = createSqlBuilder().delete(messagesTableName).where('thread_id = ?', threadId);

      const { sql: messagesSql, params: messagesParams } = deleteMessagesQuery.build();
      await this.#db.executeQuery({ sql: messagesSql, params: messagesParams });

      // Then delete the thread
      const deleteThreadQuery = createSqlBuilder().delete(fullTableName).where('id = ?', threadId);

      const { sql: threadSql, params: threadParams } = deleteThreadQuery.build();
      await this.#db.executeQuery({ sql: threadSql, params: threadParams });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'DELETE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to delete thread ${threadId}: ${error instanceof Error ? error.message : String(error)}`,
          details: { threadId },
        },
        error,
      );
    }
  }

  async saveMessages(args: {
    messages: MastraDBMessage[];
    fence?: RunFence;
  }): Promise<{ messages: MastraDBMessage[] }> {
    const { messages } = args;
    if (messages.length === 0) return { messages: [] };
    const check = this.#runFenceCheck(args.fence, 'saveMessages');

    try {
      const now = new Date();

      // Validate all messages before insert
      for (const [i, message] of messages.entries()) {
        if (!message.id) throw new Error(`Message at index ${i} missing id`);
        if (!message.threadId) {
          throw new Error(`Message at index ${i} missing threadId`);
        }
        if (!message.content) {
          throw new Error(`Message at index ${i} missing content`);
        }
        if (!message.role) {
          throw new Error(`Message at index ${i} missing role`);
        }
        if (!message.resourceId) {
          throw new Error(`Message at index ${i} missing resourceId`);
        }
      }

      // Batch validate thread existence to avoid N+1 queries
      const uniqueThreadIds = [...new Set(messages.map(m => m.threadId!))];
      const threads = await Promise.all(uniqueThreadIds.map(id => this.getThreadById({ threadId: id })));
      const missingThreadId = uniqueThreadIds.find((id, i) => !threads[i]);
      if (missingThreadId) {
        throw new Error(`Thread ${missingThreadId} not found`);
      }

      // Prepare all messages for insertion (set timestamps, thread_id, etc.)
      const messagesToInsert = messages.map(message => {
        const createdAt = message.createdAt ? new Date(message.createdAt) : now;
        return {
          id: message.id,
          thread_id: message.threadId!,
          content: typeof message.content === 'string' ? message.content : JSON.stringify(message.content),
          createdAt: createdAt.toISOString(),
          role: message.role,
          type: message.type || 'v2',
          resourceId: message.resourceId,
        };
      });

      // Insert messages, then update every affected thread's updatedAt. In order: a fenced save that
      // lost its run must stop at its first refused statement, before anything after it lands.
      await this.#db.batchUpsert({
        tableName: TABLE_MESSAGES,
        records: messagesToInsert,
        check,
      });
      const guard = runFenceGuard(check);
      for (const tid of uniqueThreadIds) {
        await executeFenced(this.#db, check, {
          sql: `UPDATE ${this.#db.getTableName(TABLE_THREADS)} SET updatedAt = ? WHERE id = ?${guard ? ` AND ${guard.sql}` : ''}`,
          params: [now.toISOString(), tid, ...(guard?.params ?? [])],
        });
      }

      this.logger.debug(`Saved ${messages.length} messages`);
      const list = new MessageList().add(messages, 'memory');
      return { messages: list.get.all.db() };
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'SAVE_MESSAGES', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to save messages: ${error instanceof Error ? error.message : String(error)}`,
        },
        error,
      );
    }
  }

  /**
   * Fetches the messages named by `include` together with their surrounding context.
   *
   * @param include - Message ids to pin, each with an optional before/after window.
   * @param resourceId - When set, restricts both the pinned messages and their context
   * to that resource so an id from another resource returns nothing.
   */
  private async _getIncludedMessages(include: StorageListMessagesInput['include'], resourceId?: string) {
    if (!include || include.length === 0) return null;

    const unionQueries: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;
    const tableName = this.#db.getTableName(TABLE_MESSAGES);
    const resourceCondition = resourceId ? ` AND resourceId = ?` : '';

    for (const inc of include) {
      const { id, withPreviousMessages = 0, withNextMessages = 0 } = inc;
      // Query by message ID directly - get the threadId from the message itself via subquery

      unionQueries.push(`
                SELECT * FROM (
                  WITH target_thread AS (
                    SELECT thread_id FROM ${tableName} WHERE id = ?${resourceCondition}
                  ),
                  ordered_messages AS (
                    SELECT
                      *,
                      ROW_NUMBER() OVER (ORDER BY createdAt ASC) AS row_num
                    FROM ${tableName}
                    WHERE thread_id = (SELECT thread_id FROM target_thread)${resourceCondition}
                  )
                  SELECT
                    m.id,
                    m.content,
                    m.role,
                    m.type,
                    m.createdAt,
                    m.thread_id AS threadId,
                    m.resourceId
                  FROM ordered_messages m
                  WHERE m.id = ?
                  OR EXISTS (
                    SELECT 1 FROM ordered_messages target
                    WHERE target.id = ?
                    AND (
                      (m.row_num <= target.row_num + ? AND m.row_num > target.row_num)
                      OR
                      (m.row_num >= target.row_num - ? AND m.row_num < target.row_num)
                    )
                  )
                ) AS query_${paramIdx}
            `);

      params.push(id);
      if (resourceId) params.push(resourceId, resourceId);
      params.push(id, id, withNextMessages, withPreviousMessages);
      paramIdx++;
    }

    const finalQuery = unionQueries.join(' UNION ALL ') + ' ORDER BY createdAt ASC';
    const messages = await this.#db.executeQuery({
      sql: finalQuery,
      params: params as (string | number | boolean | null | undefined)[],
    });

    if (!Array.isArray(messages)) {
      return [];
    }

    // Parse message content
    const processedMessages = messages.map((message: Record<string, unknown>) => {
      const processedMsg: Record<string, unknown> = {};

      for (const [key, value] of Object.entries(message)) {
        if (key === `type` && value === `v2`) continue;
        processedMsg[key] = deserializeValue(value);
      }

      return processedMsg;
    });

    return processedMessages;
  }

  public async listMessagesById({ messageIds }: { messageIds: string[] }): Promise<{ messages: MastraDBMessage[] }> {
    if (messageIds.length === 0) return { messages: [] };
    const fullTableName = this.#db.getTableName(TABLE_MESSAGES);
    const messages: unknown[] = [];

    try {
      const query = createSqlBuilder()
        .select(['id', 'content', 'role', 'type', 'createdAt', 'thread_id AS threadId', 'resourceId'])
        .from(fullTableName)
        .where(`id in (${messageIds.map(() => '?').join(',')})`, ...messageIds);

      query.orderBy('createdAt', 'DESC');

      const { sql, params } = query.build();

      const result = await this.#db.executeQuery({ sql, params });

      if (Array.isArray(result)) messages.push(...result);

      // Parse message content
      const processedMessages = messages.map(message => {
        const processedMsg: Record<string, unknown> = {};

        for (const [key, value] of Object.entries(message as Record<string, unknown>)) {
          if (key === `type` && value === `v2`) continue;
          processedMsg[key] = deserializeValue(value);
        }

        return processedMsg;
      });
      this.logger.debug(`Retrieved ${messages.length} messages`);
      const list = new MessageList().add(processedMessages as MastraMessageV1[] | MastraDBMessage[], 'memory');
      return { messages: list.get.all.db() };
    } catch (error) {
      const mastraError = new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES_BY_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to retrieve messages by ID: ${error instanceof Error ? error.message : String(error)}`,
          details: { messageIds: JSON.stringify(messageIds) },
        },
        error,
      );
      this.logger?.error?.(mastraError.toString());
      this.logger?.trackException?.(mastraError);
      throw mastraError;
    }
  }

  public async listMessages(args: StorageListMessagesInput): Promise<StorageListMessagesOutput> {
    const { threadId, resourceId, include, filter, perPage: perPageInput, page = 0, orderBy } = args;

    // Normalize threadId to array
    const threadIds = Array.isArray(threadId) ? threadId : [threadId];

    if (threadIds.length === 0 || threadIds.some(id => !id.trim())) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES', 'INVALID_THREAD_ID'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId: Array.isArray(threadId) ? threadId.join(',') : threadId },
        },
        new Error('threadId must be a non-empty string or array of non-empty strings'),
      );
    }

    if (page < 0) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES', 'INVALID_PAGE'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { page },
        },
        new Error('page must be >= 0'),
      );
    }

    const perPage = normalizePerPage(perPageInput, 40);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);
    const metadataFilter = validateStorageMetadataFilter(filter?.metadata);

    try {
      const fullTableName = this.#db.getTableName(TABLE_MESSAGES);

      // Step 1: Get paginated messages from the thread(s) first (without excluding included ones)
      const placeholders = threadIds.map(() => '?').join(', ');
      let query = `
        SELECT id, content, role, type, createdAt, thread_id AS threadId, resourceId
        FROM ${fullTableName}
        WHERE thread_id IN (${placeholders})
      `;
      const queryParams: unknown[] = [...threadIds];

      if (resourceId) {
        query += ` AND resourceId = ?`;
        queryParams.push(resourceId);
      }

      const dateRange = filter?.dateRange;
      if (dateRange?.start) {
        const startDate =
          dateRange.start instanceof Date ? serializeDate(dateRange.start) : serializeDate(new Date(dateRange.start));
        const startOp = dateRange.startExclusive ? '>' : '>=';
        query += ` AND createdAt ${startOp} ?`;
        queryParams.push(startDate);
      }

      if (dateRange?.end) {
        const endDate =
          dateRange.end instanceof Date ? serializeDate(dateRange.end) : serializeDate(new Date(dateRange.end));
        const endOp = dateRange.endExclusive ? '<' : '<=';
        query += ` AND createdAt ${endOp} ?`;
        queryParams.push(endDate);
      }

      const metadataConditions: string[] = [];
      addSqliteMetadataFilter(metadataConditions, queryParams, metadataFilter);
      if (metadataConditions.length > 0) {
        query += ` AND ${metadataConditions.join(' AND ')}`;
      }

      // Build ORDER BY clause
      const { field, direction } = this.parseOrderBy(orderBy, 'ASC');

      query += ` ORDER BY "${field}" ${direction}`;

      // Apply pagination
      if (perPage !== Number.MAX_SAFE_INTEGER) {
        query += ` LIMIT ? OFFSET ?`;
        queryParams.push(perPage, offset);
      }

      const results = await this.#db.executeQuery({
        sql: query,
        params: queryParams as (string | number | boolean | null | undefined)[],
      });

      // Parse message content
      const paginatedMessages = (isArrayOfRecords(results) ? results : []).map((message: Record<string, unknown>) => {
        const processedMsg: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(message)) {
          if (key === `type` && value === `v2`) continue;
          processedMsg[key] = deserializeValue(value);
        }
        return processedMsg;
      });

      const paginatedCount = paginatedMessages.length;

      // Get total count
      let countQuery = `SELECT count() as count FROM ${fullTableName} WHERE thread_id = ?`;
      const countParams: unknown[] = [threadId];

      if (resourceId) {
        countQuery += ` AND resourceId = ?`;
        countParams.push(resourceId);
      }

      if (dateRange?.start) {
        const startDate =
          dateRange.start instanceof Date ? serializeDate(dateRange.start) : serializeDate(new Date(dateRange.start));
        const startOp = dateRange.startExclusive ? '>' : '>=';
        countQuery += ` AND createdAt ${startOp} ?`;
        countParams.push(startDate);
      }

      if (dateRange?.end) {
        const endDate =
          dateRange.end instanceof Date ? serializeDate(dateRange.end) : serializeDate(new Date(dateRange.end));
        const endOp = dateRange.endExclusive ? '<' : '<=';
        countQuery += ` AND createdAt ${endOp} ?`;
        countParams.push(endDate);
      }

      const countMetadataConditions: string[] = [];
      addSqliteMetadataFilter(countMetadataConditions, countParams, metadataFilter);
      if (countMetadataConditions.length > 0) {
        countQuery += ` AND ${countMetadataConditions.join(' AND ')}`;
      }

      const countResult = (await this.#db.executeQuery({
        sql: countQuery,
        params: countParams as (string | number | boolean | null | undefined)[],
      })) as {
        count: number;
      }[];
      const total = Number(countResult[0]?.count ?? 0);

      // Only return early if there are no messages AND no includes to process
      if (total === 0 && paginatedCount === 0 && (!include || include.length === 0)) {
        return {
          messages: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      // Step 2: Add included messages with context (if any), excluding duplicates
      const messageIds = new Set(paginatedMessages.map((m: Record<string, unknown>) => m.id as string));
      let includeMessages: MastraDBMessage[] = [];

      if (include && include.length > 0) {
        // Use the existing _getIncludedMessages helper, but adapt it for listMessages format
        const includeResult = (await this._getIncludedMessages(include, resourceId)) as MastraDBMessage[];
        if (Array.isArray(includeResult)) {
          includeMessages = includeResult;

          // Deduplicate: only add messages that aren't already in the paginated results
          for (const includeMsg of includeMessages) {
            if (!messageIds.has(includeMsg.id)) {
              paginatedMessages.push(includeMsg as unknown as Record<string, unknown>);
              messageIds.add(includeMsg.id);
            }
          }
        }
      }

      // Use MessageList for proper deduplication and format conversion to V2
      const list = new MessageList().add(paginatedMessages as MastraMessageV1[] | MastraDBMessage[], 'memory');
      let finalMessages = list.get.all.db();

      // Sort all messages (paginated + included) for final output
      finalMessages = this.#sortMessages(finalMessages, field, direction);

      // Calculate hasMore based on pagination window
      // If all thread messages have been returned (through pagination or include), hasMore = false
      // Otherwise, check if there are more pages in the pagination window
      const returnedThreadMessageIds = new Set(finalMessages.filter(m => m.threadId === threadId).map(m => m.id));
      const hasMore =
        perPageInput !== false &&
        (metadataFilter || returnedThreadMessageIds.size < total) &&
        offset + paginatedCount < total;

      return {
        messages: finalMessages,
        total,
        page,
        perPage: perPageForResponse,
        hasMore,
      };
    } catch (error: unknown) {
      const mastraError = new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to list messages for thread ${Array.isArray(threadId) ? threadId.join(',') : threadId}: ${
            error instanceof Error ? error.message : String(error)
          }`,
          details: {
            threadId: Array.isArray(threadId) ? threadId.join(',') : threadId,
            resourceId: resourceId ?? '',
          },
        },
        error,
      );
      this.logger?.error?.(mastraError.toString());
      this.logger?.trackException?.(mastraError);
      return {
        messages: [],
        total: 0,
        page,
        perPage: perPageForResponse,
        hasMore: false,
      };
    }
  }

  #sortMessages(messages: MastraDBMessage[], field: string, direction: string): MastraDBMessage[] {
    return messages.sort((a, b) => {
      const isDateField = field === 'createdAt' || field === 'updatedAt';
      const aValue = isDateField
        ? new Date((a as Record<string, unknown>)[field] as string | Date).getTime()
        : (a as Record<string, unknown>)[field];
      const bValue = isDateField
        ? new Date((b as Record<string, unknown>)[field] as string | Date).getTime()
        : (b as Record<string, unknown>)[field];

      // Handle tiebreaker for stable sorting
      if (aValue === bValue) {
        return a.id.localeCompare(b.id);
      }

      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return direction === 'ASC' ? aValue - bValue : bValue - aValue;
      }
      // Fallback to string comparison for non-numeric fields
      return direction === 'ASC'
        ? String(aValue).localeCompare(String(bValue))
        : String(bValue).localeCompare(String(aValue));
    });
  }

  public async listMessagesByResourceId(
    args: StorageListMessagesByResourceIdInput,
  ): Promise<StorageListMessagesOutput> {
    const { resourceId, include, filter, perPage: perPageInput, page = 0, orderBy } = args;

    if (!resourceId || typeof resourceId !== 'string' || resourceId.trim().length === 0) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES_BY_RESOURCE_ID', 'INVALID_QUERY'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { resourceId: resourceId ?? '' },
        },
        new Error('resourceId is required'),
      );
    }

    if (page < 0) {
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES_BY_RESOURCE_ID', 'INVALID_PAGE'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { page },
        },
        new Error('page must be >= 0'),
      );
    }

    const perPage = normalizePerPage(perPageInput, 40);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);
    const metadataFilter = validateStorageMetadataFilter(filter?.metadata);
    const toMessages = (rows: unknown) =>
      (isArrayOfRecords(rows) ? rows : []).map(row => {
        const message: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(row)) {
          if (key === `type` && value === `v2`) continue;
          message[key] = deserializeValue(value);
        }
        return message as unknown as MastraDBMessage;
      });

    try {
      const { field, direction } = this.parseOrderBy(orderBy, 'ASC');
      const fullTableName = this.#db.getTableName(TABLE_MESSAGES);

      const conditions = [`resourceId = ?`];
      const params: unknown[] = [resourceId];

      if (filter?.dateRange?.start) {
        const startOp = filter.dateRange.startExclusive ? '>' : '>=';
        conditions.push(`createdAt ${startOp} ?`);
        params.push(serializeDate(new Date(filter.dateRange.start)));
      }

      if (filter?.dateRange?.end) {
        const endOp = filter.dateRange.endExclusive ? '<' : '<=';
        conditions.push(`createdAt ${endOp} ?`);
        params.push(serializeDate(new Date(filter.dateRange.end)));
      }

      addSqliteMetadataFilter(conditions, params, metadataFilter);
      const whereClause = `WHERE ${conditions.join(' AND ')}`;

      // When perPage is 0 with no includes, there's nothing to return.
      if (perPage === 0 && (!include || include.length === 0)) {
        return { messages: [], total: 0, page, perPage: perPageForResponse, hasMore: false };
      }

      // Fast path: when perPage is 0 and include is provided, skip COUNT and data queries.
      if (perPage === 0 && include && include.length > 0) {
        const includeMessages = toMessages(await this._getIncludedMessages(include, resourceId));
        const list = new MessageList().add(includeMessages, 'memory');
        return {
          messages: this.#sortMessages(list.get.all.db(), field, direction),
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      const countResult = (await this.#db.executeQuery({
        sql: `SELECT count() as count FROM ${fullTableName} ${whereClause}`,
        params: params as SqlParam[],
      })) as { count: number }[];
      const total = Number(countResult[0]?.count ?? 0);

      const limitValue = perPageInput === false ? total : perPage;
      const messages = toMessages(
        await this.#db.executeQuery({
          sql: `SELECT id, content, role, type, createdAt, thread_id AS threadId, resourceId FROM ${fullTableName} ${whereClause} ORDER BY "${field}" ${direction} LIMIT ? OFFSET ?`,
          params: [...(params as SqlParam[]), limitValue, offset],
        }),
      );

      // Only return early if there are no messages AND no includes to process
      if (total === 0 && messages.length === 0 && (!include || include.length === 0)) {
        return { messages: [], total: 0, page, perPage: perPageForResponse, hasMore: false };
      }

      // Add included messages with context (if any), excluding duplicates
      const messageIds = new Set(messages.map(m => m.id));
      if (include && include.length > 0) {
        for (const includeMsg of toMessages(await this._getIncludedMessages(include, resourceId))) {
          if (!messageIds.has(includeMsg.id)) {
            messages.push(includeMsg);
            messageIds.add(includeMsg.id);
          }
        }
      }

      // Use MessageList for proper deduplication and format conversion to V2
      const list = new MessageList().add(messages, 'memory');

      return {
        messages: this.#sortMessages(list.get.all.db(), field, direction),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput !== false && offset + perPage < total,
      };
    } catch (error) {
      // Re-throw USER errors (validation errors) directly so callers get proper 400 responses
      if (error instanceof MastraError && error.category === ErrorCategory.USER) {
        throw error;
      }
      const mastraError = new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'LIST_MESSAGES_BY_RESOURCE_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { resourceId },
        },
        error,
      );
      this.logger?.error?.(mastraError.toString());
      this.logger?.trackException?.(mastraError);
      throw mastraError;
    }
  }

  async updateMessages(args: {
    messages: (Partial<Omit<MastraDBMessage, 'createdAt'>> & {
      id: string;
      content?: {
        metadata?: MastraMessageContentV2['metadata'];
        content?: MastraMessageContentV2['content'];
      };
    })[];
    fence?: RunFence;
  }): Promise<MastraDBMessage[]> {
    const { messages } = args;
    this.logger.debug('Updating messages', { count: messages.length });

    if (!messages.length) {
      return [];
    }
    const check = this.#runFenceCheck(args.fence, 'updateMessages');
    const guard = runFenceGuard(check);

    const messageIds = messages.map(m => m.id);
    const fullTableName = this.#db.getTableName(TABLE_MESSAGES);
    const threadsTableName = this.#db.getTableName(TABLE_THREADS);

    try {
      // Get existing messages
      const placeholders = messageIds.map(() => '?').join(',');
      const selectQuery = `SELECT id, content, role, type, createdAt, thread_id AS threadId, resourceId FROM ${fullTableName} WHERE id IN (${placeholders})`;
      const existingMessages = (await this.#db.executeQuery({ sql: selectQuery, params: messageIds })) as any[];

      if (existingMessages.length === 0) {
        return [];
      }

      // Parse content from string to object for merging
      const parsedExistingMessages = existingMessages.map(msg => {
        let parsedContent = msg.content;
        if (typeof msg.content === 'string') {
          try {
            parsedContent = JSON.parse(msg.content);
          } catch {
            // Keep as string if parsing fails
          }
        }
        return { ...msg, content: parsedContent };
      });

      // Create a map of existing messages by ID for quick lookup
      const existingMessagesMap = new Map(parsedExistingMessages.map(msg => [msg.id, msg]));

      // Merge updates with existing messages
      const updatedMessages: any[] = [];
      const now = new Date().toISOString();

      for (const update of messages) {
        const existing = existingMessagesMap.get(update.id);
        if (!existing) continue;

        // Deep merge the content
        let mergedContent = existing.content;
        if (update.content) {
          if (typeof mergedContent === 'object' && mergedContent !== null) {
            mergedContent = {
              ...mergedContent,
              ...update.content,
              metadata: {
                ...mergedContent.metadata,
                ...update.content.metadata,
              },
            };
          } else {
            mergedContent = update.content;
          }
        }

        updatedMessages.push({
          ...existing,
          ...update,
          content: mergedContent,
        });
      }

      // Update each message
      for (const msg of updatedMessages) {
        const contentStr = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content);
        const updateQuery = createSqlBuilder()
          .update(fullTableName, ['content', 'role', 'type'], [contentStr, msg.role as string, msg.type as string])
          .where('id = ?', msg.id);
        if (guard) updateQuery.andWhere(guard.sql, ...guard.params);

        await executeFenced(this.#db, check, updateQuery.build());
      }

      // Update thread's updatedAt timestamp
      const threadIds = [...new Set(updatedMessages.map(m => m.threadId))];
      for (const tid of threadIds) {
        await executeFenced(this.#db, check, {
          sql: `UPDATE ${threadsTableName} SET updatedAt = ? WHERE id = ?${guard ? ` AND ${guard.sql}` : ''}`,
          params: [now, tid, ...(guard?.params ?? [])],
        });
      }

      // Return updated messages in the expected format
      const list = new MessageList().add(updatedMessages, 'memory');
      return list.get.all.db();
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'UPDATE_MESSAGES', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to update messages: ${error instanceof Error ? error.message : String(error)}`,
          details: { messageIds: JSON.stringify(messageIds) },
        },
        error,
      );
    }
  }

  async deleteMessages(messageIds: string[], options?: { fence?: RunFence }): Promise<void> {
    if (messageIds.length === 0) return;

    const fullTableName = this.#db.getTableName(TABLE_MESSAGES);
    const check = this.#runFenceCheck(options?.fence, 'deleteMessages');

    try {
      const placeholders = messageIds.map(() => '?').join(',');
      const query = createSqlBuilder()
        .delete(fullTableName)
        .where(`id IN (${placeholders})`, ...messageIds);
      const guard = runFenceGuard(check);
      if (guard) query.andWhere(guard.sql, ...guard.params);
      await executeFenced(this.#db, check, query.build());
      this.logger.debug(`Deleted ${messageIds.length} messages`);
    } catch (error) {
      if (isRunFenceConflictError(error)) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('CLOUDFLARE_DO', 'DELETE_MESSAGES', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text: `Failed to delete messages: ${error instanceof Error ? error.message : String(error)}`,
          details: { messageIds: JSON.stringify(messageIds) },
        },
        error,
      );
    }
  }

  // ============================================
  // Observational Memory Methods
  // ============================================

  get #omTable(): string {
    return this.#db.getTableName(OM_TABLE);
  }

  #getOMKey(threadId: string | null, resourceId: string): string {
    return threadId ? `thread:${threadId}` : `resource:${resourceId}`;
  }

  #parseOMRow(row: Record<string, any>): ObservationalMemoryRecord {
    return {
      id: row.id,
      scope: row.scope,
      threadId: row.threadId || null,
      resourceId: row.resourceId,
      createdAt: new Date(row.createdAt),
      updatedAt: new Date(row.updatedAt),
      lastObservedAt: row.lastObservedAt ? new Date(row.lastObservedAt) : undefined,
      originType: row.originType || 'initial',
      generationCount: Number(row.generationCount || 0),
      activeObservations: row.activeObservations || '',
      bufferedObservationChunks: row.bufferedObservationChunks ? JSON.parse(row.bufferedObservationChunks) : undefined,
      // Deprecated fields (for backward compatibility)
      bufferedObservations: row.activeObservationsPendingUpdate || undefined,
      bufferedObservationTokens: row.bufferedObservationTokens ? Number(row.bufferedObservationTokens) : undefined,
      bufferedMessageIds: undefined, // Use bufferedObservationChunks instead
      bufferedReflection: row.bufferedReflection || undefined,
      bufferedReflectionTokens: row.bufferedReflectionTokens ? Number(row.bufferedReflectionTokens) : undefined,
      bufferedReflectionInputTokens: row.bufferedReflectionInputTokens
        ? Number(row.bufferedReflectionInputTokens)
        : undefined,
      reflectedObservationLineCount: row.reflectedObservationLineCount
        ? Number(row.reflectedObservationLineCount)
        : undefined,
      totalTokensObserved: Number(row.totalTokensObserved || 0),
      observationTokenCount: Number(row.observationTokenCount || 0),
      pendingMessageTokens: Number(row.pendingMessageTokens || 0),
      isReflecting: Boolean(row.isReflecting),
      isObserving: Boolean(row.isObserving),
      isBufferingObservation: Boolean(row.isBufferingObservation),
      isBufferingReflection: Boolean(row.isBufferingReflection),
      lastBufferedAtTokens: Number(row.lastBufferedAtTokens || 0),
      lastBufferedAtTime: row.lastBufferedAtTime ? new Date(row.lastBufferedAtTime) : null,
      config: row.config ? JSON.parse(row.config) : {},
      metadata: row.metadata ? JSON.parse(row.metadata) : undefined,
      observedMessageIds: row.observedMessageIds ? JSON.parse(row.observedMessageIds) : undefined,
      observedTimezone: row.observedTimezone || undefined,
      supersededBy: row.supersededBy ?? null,
    };
  }

  /** An error this domain raised on purpose passes through; anything else is wrapped. */
  #omError(operation: string, error: unknown, details: Record<string, string | number | boolean | null>): MastraError {
    if (error instanceof MastraError) return error;
    return new MastraError(
      {
        id: createStorageErrorId('CLOUDFLARE_DO', operation, 'FAILED'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.THIRD_PARTY,
        details,
      },
      error,
    );
  }

  #omNotFound(operation: string, id: string, details: Record<string, string | number | boolean | null> = {}) {
    return new MastraError({
      id: createStorageErrorId('CLOUDFLARE_DO', operation, 'NOT_FOUND'),
      text: `Observational memory record not found: ${id}`,
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.THIRD_PARTY,
      details: { id, ...details },
    });
  }

  /**
   * Runs an observational memory lifecycle write as one synchronous section: `write` reads the
   * rows it needs and writes them with no `await` in between, so it must not be async. A Durable
   * Object runs one event at a time and `sql.exec()` is synchronous, so no other write lands
   * between the section's read and its write, and other adapters' optimistic retries are not
   * needed here.
   *
   * With `fencedOperation`, a write made inside a run fence scope checks the run's claim in the
   * same section, so a superseded run never writes after a takeover.
   */
  #omWrite<T>(fencedOperation: string | undefined, write: () => T): T {
    const check = fencedOperation ? this.#runFenceCheck(undefined, fencedOperation) : undefined;
    if (check) assertRunFenceSync(this.#db, check);
    return write();
  }

  #readOM(id: string): ObservationalMemoryRecord | null {
    const [row] = this.#db.querySync({ sql: `SELECT * FROM ${this.#omTable} WHERE id = ?`, params: [id] });
    return row ? this.#parseOMRow(row) : null;
  }

  #readOMHead(lookupKey: string): ObservationalMemoryRecord | null {
    const [row] = this.#db.querySync({
      sql: `SELECT * FROM ${this.#omTable} WHERE "lookupKey" = ? ORDER BY ${OM_HEAD_ORDER} LIMIT 1`,
      params: [lookupKey],
    });
    return row ? this.#parseOMRow(row) : null;
  }

  /** The live record a lifecycle write aimed at `record` lands on: the record itself while live, otherwise the head. */
  #resolveLiveOM(record: ObservationalMemoryRecord): ObservationalMemoryRecord {
    let current = record;
    for (let hop = 0; current.supersededBy; hop++) {
      const head =
        hop < OM_MAX_HEAD_HOPS ? this.#readOMHead(this.#getOMKey(current.threadId, current.resourceId)) : null;
      if (!head || head.id === current.id) {
        throw new MastraError({
          id: createStorageErrorId('CLOUDFLARE_DO', 'RESOLVE_OBSERVATIONAL_MEMORY_HEAD', 'FAILED'),
          text: `Observational memory record ${record.id} is superseded but no live head was found`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: record.id },
        });
      }
      current = head;
    }
    return current;
  }

  #insertOM(record: ObservationalMemoryRecord, lastReflectionAt: Date | null = null): void {
    const iso = (value: Date | string | null | undefined) => (value ? new Date(value).toISOString() : null);
    const row: Record<string, SqlParam> = {
      id: record.id,
      lookupKey: this.#getOMKey(record.threadId, record.resourceId),
      scope: record.scope,
      resourceId: record.resourceId,
      threadId: record.threadId || null,
      activeObservations: record.activeObservations || '',
      activeObservationsPendingUpdate: null,
      originType: record.originType || 'initial',
      config: JSON.stringify(record.config ?? {}),
      generationCount: record.generationCount || 0,
      lastObservedAt: iso(record.lastObservedAt),
      lastReflectionAt: iso(lastReflectionAt),
      pendingMessageTokens: record.pendingMessageTokens || 0,
      totalTokensObserved: record.totalTokensObserved || 0,
      observationTokenCount: record.observationTokenCount || 0,
      observedMessageIds: record.observedMessageIds ? JSON.stringify(record.observedMessageIds) : null,
      bufferedObservationChunks: record.bufferedObservationChunks
        ? JSON.stringify(record.bufferedObservationChunks)
        : null,
      bufferedReflection: record.bufferedReflection || null,
      bufferedReflectionTokens: record.bufferedReflectionTokens ?? null,
      bufferedReflectionInputTokens: record.bufferedReflectionInputTokens ?? null,
      reflectedObservationLineCount: record.reflectedObservationLineCount ?? null,
      // SqlStorage binds no booleans.
      isObserving: record.isObserving ? 1 : 0,
      isReflecting: record.isReflecting ? 1 : 0,
      isBufferingObservation: record.isBufferingObservation ? 1 : 0,
      isBufferingReflection: record.isBufferingReflection ? 1 : 0,
      lastBufferedAtTokens: record.lastBufferedAtTokens || 0,
      lastBufferedAtTime: iso(record.lastBufferedAtTime),
      observedTimezone: record.observedTimezone || null,
      metadata: record.metadata ? JSON.stringify(record.metadata) : null,
      supersededBy: record.supersededBy ?? null,
      createdAt: iso(record.createdAt),
      updatedAt: iso(record.updatedAt),
    };
    const columns = Object.keys(row);
    this.#db.querySync({
      sql: `INSERT INTO ${this.#omTable} (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`,
      params: Object.values(row),
    });
  }

  /**
   * Create the next generation from the stored (live) record and retire it. Buffered chunks move
   * to the new generation (in order); the cursor, buffering markers, flags, counters, config, and
   * metadata carry over; buffered reflection state does not (it is cleared on the retired record).
   *
   * Runs inside an `#omWrite` section, which has no rollback, so the insert goes first: it is the
   * statement a constraint can refuse, and a refused insert leaves the stored record live.
   */
  #rollOverOM(
    stored: ObservationalMemoryRecord,
    observations: string,
    tokenCount: number,
    newRecordId: string | undefined,
  ): ObservationalMemoryRecord {
    const now = new Date();
    const chunks = Array.isArray(stored.bufferedObservationChunks) ? stored.bufferedObservationChunks : [];
    const record: ObservationalMemoryRecord = {
      id: newRecordId ?? crypto.randomUUID(),
      scope: stored.scope,
      threadId: stored.threadId,
      resourceId: stored.resourceId,
      createdAt: now,
      updatedAt: now,
      // Carried verbatim: inventing a cursor would mark unobserved messages (and moved chunks) as observed.
      lastObservedAt: stored.lastObservedAt,
      originType: 'reflection',
      generationCount: stored.generationCount + 1,
      activeObservations: observations,
      bufferedObservationChunks: chunks.length > 0 ? chunks : undefined,
      totalTokensObserved: stored.totalTokensObserved,
      observationTokenCount: tokenCount,
      pendingMessageTokens: stored.pendingMessageTokens,
      isReflecting: false,
      isObserving: false,
      isBufferingObservation: stored.isBufferingObservation,
      isBufferingReflection: false,
      lastBufferedAtTokens: stored.lastBufferedAtTokens,
      lastBufferedAtTime: stored.lastBufferedAtTime,
      config: stored.config,
      metadata: stored.metadata,
      observedTimezone: stored.observedTimezone,
      supersededBy: null,
    };

    this.#insertOM(record, now);
    // Retire the stored record: chunks moved, buffered reflection consumed, liveness marker set (never cleared).
    this.#db.querySync({
      sql: `UPDATE ${this.#omTable} SET "supersededBy" = ?, "bufferedObservationChunks" = NULL,
        "bufferedReflection" = NULL, "bufferedReflectionTokens" = NULL,
        "bufferedReflectionInputTokens" = NULL, "reflectedObservationLineCount" = NULL, "updatedAt" = ?
        WHERE id = ?`,
      params: [record.id, now.toISOString(), stored.id],
    });
    return record;
  }

  async getObservationalMemory(threadId: string | null, resourceId: string): Promise<ObservationalMemoryRecord | null> {
    try {
      return this.#readOMHead(this.#getOMKey(threadId, resourceId));
    } catch (error) {
      throw this.#omError('GET_OBSERVATIONAL_MEMORY', error, { threadId, resourceId });
    }
  }

  async getObservationalMemoryHistory(
    threadId: string | null,
    resourceId: string,
    limit: number = 10,
    options?: ObservationalMemoryHistoryOptions,
  ): Promise<ObservationalMemoryRecord[]> {
    try {
      const conditions = [`"lookupKey" = ?`];
      const params: SqlParam[] = [this.#getOMKey(threadId, resourceId)];

      if (options?.recordId !== undefined) {
        conditions.push(`id = ?`);
        params.push(options.recordId);
      }
      if (options?.from) {
        conditions.push(`"createdAt" >= ?`);
        params.push(options.from.toISOString());
      }
      if (options?.to) {
        conditions.push(`"createdAt" <= ?`);
        params.push(options.to.toISOString());
      }
      if (options?.groupId !== undefined) {
        conditions.push(`(instr("activeObservations", ?) > 0 OR EXISTS (
          SELECT 1 FROM json_each("bufferedObservationChunks") AS chunk
          WHERE instr(json_extract(chunk.value, '$.observations'), ?) > 0
        ))`);
        const prefix = `<observation-group id="${options.groupId}"`;
        params.push(prefix, prefix);
      }
      if (options?.beforeGeneration !== undefined) {
        conditions.push(`"generationCount" < ?`);
        params.push(options.beforeGeneration);
      }
      if (options?.afterGeneration !== undefined) {
        conditions.push(`"generationCount" > ?`);
        params.push(options.afterGeneration);
      }
      const direction = options?.sortDirection === 'ASC' ? 'ASC' : 'DESC';
      params.push(limit);
      let sql = `SELECT * FROM ${this.#omTable} WHERE ${conditions.join(' AND ')} ORDER BY "generationCount" ${direction}, "createdAt" ASC, id ASC LIMIT ?`;
      if (options?.offset != null) {
        params.push(options.offset);
        sql += ` OFFSET ?`;
      }

      const rows = await this.#db.executeQuery({ sql, params });
      return (isArrayOfRecords(rows) ? rows : []).map(row => this.#parseOMRow(row));
    } catch (error) {
      throw this.#omError('GET_OBSERVATIONAL_MEMORY_HISTORY', error, { threadId, resourceId, limit });
    }
  }

  async initializeObservationalMemory(input: CreateObservationalMemoryInput): Promise<ObservationalMemoryRecord> {
    try {
      return this.#omWrite(undefined, () => {
        const existing = this.#readOMHead(this.#getOMKey(input.threadId, input.resourceId));
        if (existing) return existing;

        const now = new Date();
        const record: ObservationalMemoryRecord = {
          // Never reused: a write addressed to a cleared record must not land on its successor.
          id: crypto.randomUUID(),
          scope: input.scope,
          threadId: input.threadId,
          resourceId: input.resourceId,
          createdAt: now,
          updatedAt: now,
          lastObservedAt: undefined,
          originType: 'initial',
          generationCount: 0,
          activeObservations: '',
          totalTokensObserved: 0,
          observationTokenCount: 0,
          pendingMessageTokens: 0,
          isReflecting: false,
          isObserving: false,
          isBufferingObservation: false,
          isBufferingReflection: false,
          lastBufferedAtTokens: 0,
          lastBufferedAtTime: null,
          config: input.config,
          observedTimezone: input.observedTimezone,
          supersededBy: null,
        };
        this.#insertOM(record);
        return record;
      });
    } catch (error) {
      throw this.#omError('INITIALIZE_OBSERVATIONAL_MEMORY', error, {
        threadId: input.threadId,
        resourceId: input.resourceId,
      });
    }
  }

  async insertObservationalMemoryRecord(record: ObservationalMemoryRecord): Promise<void> {
    try {
      this.#insertOM(record);
    } catch (error) {
      throw this.#omError('INSERT_OBSERVATIONAL_MEMORY_RECORD', error, {
        id: record.id,
        threadId: record.threadId,
        resourceId: record.resourceId,
      });
    }
  }

  async updateActiveObservations(input: UpdateActiveObservationsInput): Promise<void> {
    assertActiveObservationsApplied(await this.commitActiveObservations(input), input.id);
  }

  async commitActiveObservations(input: UpdateActiveObservationsInput): Promise<UpdateActiveObservationsResult> {
    try {
      return this.#omWrite('commitActiveObservations', (): UpdateActiveObservationsResult => {
        const stored = this.#readOM(input.id);
        if (!stored) throw this.#omNotFound('UPDATE_ACTIVE_OBSERVATIONS', input.id);
        if (stored.supersededBy) return { applied: false, reason: 'retired' };
        if (
          input.expectedActiveObservations !== undefined &&
          input.expectedActiveObservations !== stored.activeObservations
        ) {
          return { applied: false, reason: 'conflict' };
        }

        // The cursor never moves backward.
        const lastObservedAt = maxObservationCursor(stored.lastObservedAt, input.lastObservedAt)!;
        this.#db.querySync({
          sql: `UPDATE ${this.#omTable} SET
            "activeObservations" = ?,
            "lastObservedAt" = ?,
            "pendingMessageTokens" = 0,
            "observationTokenCount" = ?,
            "totalTokensObserved" = "totalTokensObserved" + ?,
            "observedMessageIds" = ?,
            "updatedAt" = ?
          WHERE id = ?`,
          params: [
            input.observations,
            lastObservedAt.toISOString(),
            input.tokenCount,
            input.tokenCount,
            input.observedMessageIds ? JSON.stringify(input.observedMessageIds) : null,
            new Date().toISOString(),
            input.id,
          ],
        });
        return { applied: true };
      });
    } catch (error) {
      throw this.#omError('UPDATE_ACTIVE_OBSERVATIONS', error, { id: input.id });
    }
  }

  async createReflectionGeneration(input: CreateReflectionGenerationInput): Promise<ObservationalMemoryRecord> {
    try {
      return this.#omWrite('createReflectionGeneration', () => {
        const { currentRecord } = input;
        const stored = this.#readOM(currentRecord.id);
        if (!stored) return currentRecord;
        // A retired snapshot creates nothing; the caller adopts the head.
        if (stored.supersededBy) return this.#resolveLiveOM(stored);
        const plan = planReflectionGenerationText({
          storedObservations: stored.activeObservations,
          storedObservationTokenCount: stored.observationTokenCount,
          snapshotObservations: currentRecord.activeObservations,
          snapshotObservationTokenCount: currentRecord.observationTokenCount,
          reflection: input.reflection,
          tokenCount: input.tokenCount,
        });
        // The text was rewritten (not only appended to) since the snapshot: the reflection is stale.
        if (!plan) return stored;
        return this.#rollOverOM(stored, plan.observations, plan.tokenCount, input.newRecordId);
      });
    } catch (error) {
      throw this.#omError('CREATE_REFLECTION_GENERATION', error, { currentRecordId: input.currentRecord.id });
    }
  }

  /** Sets a coordination flag. Flags are not fenced, so a superseded owner can still clear its own. */
  async #setOMFlag(operation: string, column: string, id: string, value: boolean): Promise<void> {
    try {
      const updated = this.#db.querySync({
        sql: `UPDATE ${this.#omTable} SET "${column}" = ?, "updatedAt" = ? WHERE id = ? RETURNING id`,
        params: [value ? 1 : 0, new Date().toISOString(), id],
      });
      if (updated.length === 0) throw this.#omNotFound(operation, id, { [column]: value });
    } catch (error) {
      throw this.#omError(operation, error, { id, [column]: value });
    }
  }

  async setReflectingFlag(id: string, isReflecting: boolean): Promise<void> {
    await this.#setOMFlag('SET_REFLECTING_FLAG', 'isReflecting', id, isReflecting);
  }

  async setObservingFlag(id: string, isObserving: boolean): Promise<void> {
    await this.#setOMFlag('SET_OBSERVING_FLAG', 'isObserving', id, isObserving);
  }

  async setBufferingReflectionFlag(id: string, isBuffering: boolean): Promise<void> {
    await this.#setOMFlag('SET_BUFFERING_REFLECTION_FLAG', 'isBufferingReflection', id, isBuffering);
  }

  async setBufferingObservationFlag(id: string, isBuffering: boolean, lastBufferedAtTokens?: number): Promise<void> {
    try {
      this.#omWrite(undefined, () => {
        const stored = this.#readOM(id);
        if (!stored) {
          throw this.#omNotFound('SET_BUFFERING_OBSERVATION_FLAG', id, {
            isBuffering,
            lastBufferedAtTokens: lastBufferedAtTokens ?? null,
          });
        }
        // A retired id is redirected to the head.
        const target = this.#resolveLiveOM(stored);
        const now = new Date().toISOString();
        this.#db.querySync(
          lastBufferedAtTokens !== undefined
            ? {
                sql: `UPDATE ${this.#omTable} SET "isBufferingObservation" = ?, "lastBufferedAtTokens" = ?, "updatedAt" = ? WHERE id = ?`,
                params: [isBuffering ? 1 : 0, lastBufferedAtTokens, now, target.id],
              }
            : {
                sql: `UPDATE ${this.#omTable} SET "isBufferingObservation" = ?, "updatedAt" = ? WHERE id = ?`,
                params: [isBuffering ? 1 : 0, now, target.id],
              },
        );
      });
    } catch (error) {
      throw this.#omError('SET_BUFFERING_OBSERVATION_FLAG', error, {
        id,
        isBuffering,
        lastBufferedAtTokens: lastBufferedAtTokens ?? null,
      });
    }
  }

  async clearObservationalMemory(threadId: string | null, resourceId: string): Promise<void> {
    try {
      await this.#db.executeQuery({
        sql: `DELETE FROM ${this.#omTable} WHERE "lookupKey" = ?`,
        params: [this.#getOMKey(threadId, resourceId)],
      });
    } catch (error) {
      throw this.#omError('CLEAR_OBSERVATIONAL_MEMORY', error, { threadId, resourceId });
    }
  }

  async setPendingMessageTokens(id: string, tokenCount: number): Promise<void> {
    try {
      this.#omWrite(undefined, () => {
        const stored = this.#readOM(id);
        if (!stored) throw this.#omNotFound('SET_PENDING_MESSAGE_TOKENS', id, { tokenCount });
        // A retired id is redirected to the head.
        const target = this.#resolveLiveOM(stored);
        this.#db.querySync({
          sql: `UPDATE ${this.#omTable} SET "pendingMessageTokens" = ?, "updatedAt" = ? WHERE id = ?`,
          params: [tokenCount, new Date().toISOString(), target.id],
        });
      });
    } catch (error) {
      throw this.#omError('SET_PENDING_MESSAGE_TOKENS', error, { id, tokenCount });
    }
  }

  async updateObservationalMemoryConfig(input: UpdateObservationalMemoryConfigInput): Promise<void> {
    try {
      this.#omWrite(undefined, () => {
        const [row] = this.#db.querySync({
          sql: `SELECT config FROM ${this.#omTable} WHERE id = ?`,
          params: [input.id],
        });
        if (!row) throw this.#omNotFound('UPDATE_OM_CONFIG', input.id);
        const existing: Record<string, unknown> = row.config ? JSON.parse(String(row.config)) : {};
        this.#db.querySync({
          sql: `UPDATE ${this.#omTable} SET config = ?, "updatedAt" = ? WHERE id = ?`,
          params: [JSON.stringify(this.deepMergeConfig(existing, input.config)), new Date().toISOString(), input.id],
        });
      });
    } catch (error) {
      throw this.#omError('UPDATE_OM_CONFIG', error, { id: input.id });
    }
  }

  // ============================================
  // Async Buffering Methods
  // ============================================

  async updateBufferedObservations(input: UpdateBufferedObservationsInput): Promise<void> {
    await this.appendBufferedObservations(input);
  }

  async appendBufferedObservations(input: UpdateBufferedObservationsInput): Promise<UpdateBufferedObservationsResult> {
    try {
      return this.#omWrite('appendBufferedObservations', (): UpdateBufferedObservationsResult => {
        const stored = this.#readOM(input.id);
        if (!stored) throw this.#omNotFound('UPDATE_BUFFERED_OBSERVATIONS', input.id);
        // A retired id is redirected to the head.
        const target = this.#resolveLiveOM(stored);
        const existingChunks = Array.isArray(target.bufferedObservationChunks) ? target.bufferedObservationChunks : [];

        // Skip a retried append (same cycle) and a chunk the cursor already wholly covers.
        if (
          existingChunks.some(existing => existing.cycleId === input.chunk.cycleId) ||
          isBufferedChunkCoveredByCursor(input.chunk.lastObservedAt, target.lastObservedAt)
        ) {
          return { persisted: false, recordId: target.id };
        }

        const newChunk: BufferedObservationChunk = {
          id: `ombuf-${crypto.randomUUID()}`,
          cycleId: input.chunk.cycleId,
          observations: input.chunk.observations,
          tokenCount: input.chunk.tokenCount,
          messageIds: input.chunk.messageIds,
          messageTokens: input.chunk.messageTokens,
          lastObservedAt: input.chunk.lastObservedAt,
          createdAt: new Date(),
          suggestedContinuation: input.chunk.suggestedContinuation,
          currentTask: input.chunk.currentTask,
          threadTitle: input.chunk.threadTitle,
          extractedValues: input.chunk.extractedValues,
          extractionFailures: input.chunk.extractionFailures,
        };

        // lastBufferedAtTime never moves backward.
        const lastBufferedAtTime = maxObservationCursor(target.lastBufferedAtTime, input.lastBufferedAtTime);
        this.#db.querySync({
          sql: `UPDATE ${this.#omTable} SET "bufferedObservationChunks" = ?, "lastBufferedAtTime" = ?, "updatedAt" = ? WHERE id = ?`,
          params: [
            JSON.stringify([...existingChunks, newChunk]),
            lastBufferedAtTime ? lastBufferedAtTime.toISOString() : null,
            new Date().toISOString(),
            target.id,
          ],
        });
        return { persisted: true, recordId: target.id };
      });
    } catch (error) {
      throw this.#omError('UPDATE_BUFFERED_OBSERVATIONS', error, { id: input.id });
    }
  }

  async swapBufferedToActive(input: SwapBufferedToActiveInput): Promise<SwapBufferedToActiveResult> {
    try {
      return this.#omWrite('swapBufferedToActive', (): SwapBufferedToActiveResult => {
        const stored = this.#readOM(input.id);
        if (!stored) throw this.#omNotFound('SWAP_BUFFERED_TO_ACTIVE', input.id);

        const emptyResult: SwapBufferedToActiveResult = {
          chunksActivated: 0,
          messageTokensActivated: 0,
          observationTokensActivated: 0,
          messagesActivated: 0,
          activatedCycleIds: [],
          activatedMessageIds: [],
        };
        // A retired record is frozen: activation reports it and writes nothing.
        if (stored.supersededBy) return { ...emptyResult, retired: true };

        // Activation always works on the stored list, so a chunk appended after the caller
        // read the record is never dropped. Caller-provided chunks only override token weights.
        const persistedChunks = Array.isArray(stored.bufferedObservationChunks) ? stored.bufferedObservationChunks : [];
        const refreshedWeights = new Map(
          (Array.isArray(input.bufferedChunks) ? input.bufferedChunks : []).map(c => [c.id, c.messageTokens]),
        );
        const chunks = persistedChunks.map(c =>
          refreshedWeights.has(c.id) ? { ...c, messageTokens: refreshedWeights.get(c.id)! } : c,
        );

        if (chunks.length === 0) {
          return emptyResult;
        }

        // Calculate target message tokens to activate based on new formula:
        // retentionFloor = threshold * (1 - ratio) represents tokens to keep as raw messages
        // targetMessageTokens = max(0, currentPending - retentionFloor) represents tokens to activate
        const retentionFloor = input.messageTokensThreshold * (1 - input.activationRatio);
        const targetMessageTokens = Math.max(0, input.currentPendingTokens - retentionFloor);

        // Find the closest chunk boundary to the target, biased over (prefer removing
        // slightly more than the target so remaining context lands at or below retentionFloor).
        // Track both best-over and best-under boundaries so we can fall back to under
        // if the over boundary would overshoot by too much.
        let cumulativeMessageTokens = 0;
        let bestOverBoundary = 0;
        let bestOverTokens = 0;
        let bestUnderBoundary = 0;
        let bestUnderTokens = 0;

        for (let i = 0; i < chunks.length; i++) {
          cumulativeMessageTokens += chunks[i]!.messageTokens ?? 0;
          const boundary = i + 1;

          if (cumulativeMessageTokens >= targetMessageTokens) {
            // Over or equal — track the closest (lowest) over boundary
            if (bestOverBoundary === 0 || cumulativeMessageTokens < bestOverTokens) {
              bestOverBoundary = boundary;
              bestOverTokens = cumulativeMessageTokens;
            }
          } else {
            // Under — track the closest (highest) under boundary
            if (cumulativeMessageTokens > bestUnderTokens) {
              bestUnderBoundary = boundary;
              bestUnderTokens = cumulativeMessageTokens;
            }
          }
        }

        // Safeguard: if the over boundary would eat into more than 95% of the
        // retention floor, fall back to the best under boundary instead.
        // This prevents edge cases where a large chunk overshoots dramatically.
        // When forceMaxActivation is set (above blockAfter), still prefer the over
        // boundary, but never if it would leave fewer than the smaller of 1000
        // tokens or the retention floor remaining.
        const maxOvershoot = retentionFloor * 0.95;
        const overshoot = bestOverTokens - targetMessageTokens;
        const remainingAfterOver = input.currentPendingTokens - bestOverTokens;
        const remainingAfterUnder = input.currentPendingTokens - bestUnderTokens;
        // When activationRatio ≈ 1.0, retentionFloor is 0 and minRemaining becomes 0 — intentional for "activate everything" configs.
        const minRemaining = Math.min(1000, retentionFloor);

        let chunksToActivate: number;
        if (input.forceMaxActivation && bestOverBoundary > 0 && remainingAfterOver >= minRemaining) {
          chunksToActivate = bestOverBoundary;
        } else if (bestOverBoundary > 0 && overshoot <= maxOvershoot && remainingAfterOver >= minRemaining) {
          chunksToActivate = bestOverBoundary;
        } else if (bestUnderBoundary > 0 && remainingAfterUnder >= minRemaining) {
          chunksToActivate = bestUnderBoundary;
        } else if (bestOverBoundary > 0) {
          // All boundaries are over and exceed the safeguard — still activate
          // the closest over boundary (better than nothing)
          chunksToActivate = bestOverBoundary;
        } else {
          chunksToActivate = 1;
        }

        // Split chunks
        const activatedChunks = chunks.slice(0, chunksToActivate);
        const remainingChunks = persistedChunks.slice(chunksToActivate);

        // Combine activated observations
        const activatedContent = activatedChunks.map(c => c.observations).join('\n\n');
        const activatedTokens = activatedChunks.reduce((sum, c) => sum + c.tokenCount, 0);
        const activatedMessageTokens = activatedChunks.reduce((sum, c) => sum + (c.messageTokens ?? 0), 0);
        const activatedMessageCount = activatedChunks.reduce((sum, c) => sum + c.messageIds.length, 0);
        const activatedCycleIds = activatedChunks.map(c => c.cycleId).filter((id): id is string => !!id);
        const activatedMessageIds = activatedChunks.flatMap(c => c.messageIds ?? []);

        // Derive lastObservedAt from the latest activated chunk, or use provided value
        const latestChunk = activatedChunks[activatedChunks.length - 1];
        const lastObservedAt =
          input.lastObservedAt ?? (latestChunk?.lastObservedAt ? new Date(latestChunk.lastObservedAt) : new Date());
        // The stored cursor never moves backward (a sync observation may already be past this chunk).
        const storedLastObservedAt = maxObservationCursor(stored.lastObservedAt, lastObservedAt)!;

        // Calculate new values
        const existingActive = stored.activeObservations || '';
        const boundary = `\n\n--- message boundary (${lastObservedAt.toISOString()}) ---\n\n`;
        const newActive = existingActive ? `${existingActive}${boundary}${activatedContent}` : activatedContent;
        const newTokenCount = stored.observationTokenCount + activatedTokens;
        // NOTE: We intentionally do NOT add message IDs to observedMessageIds during buffered activation.
        // Buffered chunks represent observations of messages as they were at buffering time.
        // With streaming, messages grow after buffering, so we rely on lastObservedAt for filtering.
        // New content after lastObservedAt will be picked up in subsequent observations.

        // Decrement pending message tokens (clamped to zero)
        const newPending = Math.max(0, stored.pendingMessageTokens - activatedMessageTokens);

        this.#db.querySync({
          sql: `UPDATE ${this.#omTable} SET
            "activeObservations" = ?,
            "observationTokenCount" = ?,
            "pendingMessageTokens" = ?,
            "bufferedObservationChunks" = ?,
            "lastObservedAt" = ?,
            "updatedAt" = ?
          WHERE id = ?`,
          params: [
            newActive,
            newTokenCount,
            newPending,
            remainingChunks.length > 0 ? JSON.stringify(remainingChunks) : null,
            storedLastObservedAt.toISOString(),
            new Date().toISOString(),
            input.id,
          ],
        });

        // Use hints from the most recent activated chunk only — stale hints from older chunks are discarded
        return {
          chunksActivated: activatedChunks.length,
          messageTokensActivated: activatedMessageTokens,
          observationTokensActivated: activatedTokens,
          messagesActivated: activatedMessageCount,
          activatedCycleIds,
          activatedMessageIds,
          observations: activatedContent,
          perChunk: activatedChunks.map(c => ({
            cycleId: c.cycleId ?? '',
            messageTokens: c.messageTokens ?? 0,
            observationTokens: c.tokenCount,
            messageCount: c.messageIds.length,
            observations: c.observations,
          })),
          suggestedContinuation: latestChunk?.suggestedContinuation ?? undefined,
          currentTask: latestChunk?.currentTask ?? undefined,
        };
      });
    } catch (error) {
      throw this.#omError('SWAP_BUFFERED_TO_ACTIVE', error, { id: input.id });
    }
  }

  async updateBufferedReflection(input: UpdateBufferedReflectionInput): Promise<void> {
    try {
      this.#omWrite('updateBufferedReflection', () => {
        const updated = this.#db.querySync({
          sql: `UPDATE ${this.#omTable} SET
            "bufferedReflection" = CASE
              WHEN "bufferedReflection" IS NOT NULL AND "bufferedReflection" != ''
              THEN "bufferedReflection" || char(10) || char(10) || ?
              ELSE ?
            END,
            "bufferedReflectionTokens" = COALESCE("bufferedReflectionTokens", 0) + ?,
            "bufferedReflectionInputTokens" = COALESCE("bufferedReflectionInputTokens", 0) + ?,
            "reflectedObservationLineCount" = ?,
            "updatedAt" = ?
          WHERE id = ? RETURNING id`,
          params: [
            input.reflection,
            input.reflection,
            input.tokenCount,
            input.inputTokenCount,
            input.reflectedObservationLineCount,
            new Date().toISOString(),
            input.id,
          ],
        });
        if (updated.length === 0) throw this.#omNotFound('UPDATE_BUFFERED_REFLECTION', input.id);
      });
    } catch (error) {
      throw this.#omError('UPDATE_BUFFERED_REFLECTION', error, { id: input.id });
    }
  }

  async swapBufferedReflectionToActive(input: SwapBufferedReflectionToActiveInput): Promise<ObservationalMemoryRecord> {
    try {
      return this.#omWrite('swapBufferedReflectionToActive', () => {
        const { currentRecord } = input;
        const stored = this.#readOM(currentRecord.id);
        if (!stored) return currentRecord;
        // A retired snapshot creates nothing; the caller adopts the head.
        if (stored.supersededBy) return this.#resolveLiveOM(stored);

        const bufferedReflection = stored.bufferedReflection || '';
        if (!bufferedReflection) {
          throw new MastraError({
            id: createStorageErrorId('CLOUDFLARE_DO', 'SWAP_BUFFERED_REFLECTION_TO_ACTIVE', 'NO_CONTENT'),
            text: 'No buffered reflection to swap',
            domain: ErrorDomain.STORAGE,
            category: ErrorCategory.USER,
            details: { id: currentRecord.id },
          });
        }
        // Only appends may have happened since the caller's snapshot; a rewrite invalidates the
        // reflected line count.
        if (!isAppendOnlySince(stored.activeObservations, currentRecord.activeObservations)) {
          return stored;
        }

        // Split current activeObservations by the recorded boundary.
        // Lines 0..reflectedLineCount were reflected on → replaced by bufferedReflection.
        // Lines after reflectedLineCount were added after reflection started → kept as-is.
        const reflectedLineCount = stored.reflectedObservationLineCount ?? 0;
        const unreflectedContent = (stored.activeObservations || '')
          .split('\n')
          .slice(reflectedLineCount)
          .join('\n')
          .trim();
        const newObservations = unreflectedContent
          ? `${bufferedReflection}\n\n${unreflectedContent}`
          : bufferedReflection;
        // tokenCount is computed by the processor from its snapshot; add tokens appended since.
        const tokenCount =
          input.tokenCount +
          Math.max(0, (stored.observationTokenCount ?? 0) - (currentRecord.observationTokenCount ?? 0));

        // The rollover also clears the buffered reflection on the retired record.
        return this.#rollOverOM(stored, newObservations, tokenCount, input.newRecordId);
      });
    } catch (error) {
      throw this.#omError('SWAP_BUFFERED_REFLECTION_TO_ACTIVE', error, { currentRecordId: input.currentRecord.id });
    }
  }
}
