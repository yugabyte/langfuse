import { parseClickhouseUTCDateTimeFormat } from "./clickhouse";
import { FilterState } from "../../types";
import { TraceRecordReadType } from "./definitions";
import { tracesTableUiColumnDefinitions } from "../tableMappings/mapTracesTable";
import { UiColumnMappings } from "../../tableDefinitions";
import {
  convertDateToClickhouseDateTime,
  PreferredClickhouseService,
} from "../clickhouse/client";
import { convertClickhouseToDomain } from "./traces_converters";
import { env } from "../../env";
import { ClickHouseClientConfigOptions } from "@clickhouse/client";
import { recordDistribution } from "../instrumentation";
import type { AnalyticsTraceEvent } from "../analytics-integrations/types";
import { measureAndReturn } from "../clickhouse/measureAndReturn";
import { DEFAULT_RENDERING_PROPS, RenderingProps } from "../utils/rendering";
import { logger } from "../logger";
import { traceException } from "../instrumentation";
import { prisma as metadataPrisma, tracingPrisma as prisma } from "../../db";
import { Prisma } from "@prisma/client";

const toClickhouseDateTimeString = (value: Date | null | undefined) =>
  value ? value.toISOString().replace("T", " ").replace("Z", "") : undefined;

const serializeErrorForLogs = (error: unknown) => {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return { message: String(error) };
};

const stringifyErrorForMessage = (error: unknown) =>
  JSON.stringify(serializeErrorForLogs(error));

const toClickhouseMetadataRecord = (value: unknown): Record<string, string> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([k, v]) => [
      k,
      typeof v === "string" ? v : JSON.stringify(v),
    ]),
  );
};

const parseDateInput = (value: unknown) => {
  if (value instanceof Date) return value;
  if (typeof value === "number") return new Date(value);
  if (typeof value === "string" && /^\d+$/.test(value)) {
    return new Date(Number(value));
  }
  return parseClickhouseUTCDateTimeFormat(String(value));
};

type PgTraceRow = {
  id: string;
  name: string | null;
  user_id: string | null;
  metadata: Record<string, unknown> | null;
  release: string | null;
  version: string | null;
  project_id: string;
  environment: string | null;
  public: boolean;
  bookmarked: boolean;
  tags: string[] | null;
  input: string | null;
  output: string | null;
  session_id: string | null;
  is_deleted: boolean;
  timestamp: Date;
  created_at: Date;
  updated_at: Date;
  event_ts: Date;
};

const toTraceRecordReadType = (trace: PgTraceRow): TraceRecordReadType => ({
  id: trace.id,
  name: trace.name,
  user_id: trace.user_id,
  metadata: toClickhouseMetadataRecord(trace.metadata),
  release: trace.release,
  version: trace.version,
  project_id: trace.project_id,
  environment: trace.environment ?? "default",
  public: trace.public,
  bookmarked: trace.bookmarked,
  tags: trace.tags ?? [],
  input: trace.input,
  output: trace.output,
  session_id: trace.session_id,
  is_deleted: trace.is_deleted ? 1 : 0,
  timestamp: toClickhouseDateTimeString(trace.timestamp) ?? "",
  created_at: toClickhouseDateTimeString(trace.created_at) ?? "",
  updated_at: toClickhouseDateTimeString(trace.updated_at) ?? "",
  event_ts: toClickhouseDateTimeString(trace.event_ts) ?? "",
});

/**
 * Checks if trace exists in clickhouse.
 * Additionally, give back the timestamp of the trace as metadata.
 *
 * @param {string} projectId - Project ID for the trace
 * @param {string} traceId - ID of the trace to check
 * @param {Date} timestamp - Timestamp for time-based filtering, uses event payload or job timestamp
 * @param {FilterState} filter - Filter for the trace
 * @param {Date} maxTimeStamp - Upper bound on timestamp
 * @param {Date} exactTimestamp - Exact match for the trace
 * @returns {Promise<boolean>} - True if trace exists
 *
 * Notes:
 * • Filters within ±2 day window
 * • Used for validating trace references before eval job creation
 */
export const checkTraceExistsAndGetTimestamp = async ({
  projectId,
  traceId,
  timestamp,
  filter,
  maxTimeStamp,
  exactTimestamp,
}: {
  projectId: string;
  traceId: string;
  timestamp: Date;
  filter: FilterState;
  maxTimeStamp: Date | undefined;
  exactTimestamp?: Date;
}): Promise<{ exists: boolean; timestamp?: Date }> => {
  if (filter.length > 0) {
    logger.warn(
      "checkTraceExistsAndGetTimestamp ignores non-time filter predicates in PostgreSQL mode",
      { projectId, traceId },
    );
  }

  const timestampWhere: { gte?: Date; lte?: Date; lt?: Date } = {};
  if (timestamp) {
    timestampWhere.gte = new Date(
      timestamp.getTime() - 2 * 24 * 60 * 60 * 1000,
    );
  }
  if (maxTimeStamp) {
    timestampWhere.lte = maxTimeStamp;
  }
  if (exactTimestamp) {
    const exactStart = new Date(
      Date.UTC(
        exactTimestamp.getUTCFullYear(),
        exactTimestamp.getUTCMonth(),
        exactTimestamp.getUTCDate(),
        0,
        0,
        0,
        0,
      ),
    );
    const exactEnd = new Date(
      Date.UTC(
        exactTimestamp.getUTCFullYear(),
        exactTimestamp.getUTCMonth(),
        exactTimestamp.getUTCDate() + 1,
        0,
        0,
        0,
        0,
      ),
    );
    timestampWhere.gte = exactStart;
    timestampWhere.lt = exactEnd;
  }

  const conditions: Prisma.Sql[] = [
    Prisma.sql`id = ${traceId}`,
    Prisma.sql`project_id = ${projectId}`,
  ];
  if (timestampWhere.gte) {
    conditions.push(Prisma.sql`timestamp >= ${timestampWhere.gte}`);
  }
  if (timestampWhere.lte) {
    conditions.push(Prisma.sql`timestamp <= ${timestampWhere.lte}`);
  }
  if (timestampWhere.lt) {
    conditions.push(Prisma.sql`timestamp < ${timestampWhere.lt}`);
  }

  const rows = await prisma.$queryRaw<Array<{ timestamp: Date }>>(Prisma.sql`
    SELECT timestamp
    FROM traces
    WHERE ${Prisma.join(conditions, " AND ")}
    ORDER BY updated_at DESC
    LIMIT 1
  `);
  const row = rows[0];

  return { exists: !!row, timestamp: row?.timestamp };
};

/**
 * Accepts a trace in a Clickhouse-ready format.
 * id, project_id, and timestamp must always be provided.
 */
export const upsertTrace = async (trace: Partial<TraceRecordReadType>) => {
  if (!["id", "project_id", "timestamp"].every((key) => key in trace)) {
    throw new Error("Identifier fields must be provided to upsert Trace.");
  }

  const timestamp = parseDateInput(trace.timestamp);

  const createdAt = trace.created_at
    ? parseDateInput(trace.created_at)
    : timestamp;
  const updatedAt = trace.updated_at
    ? parseDateInput(trace.updated_at)
    : timestamp;
  await prisma.$executeRaw`
    DELETE FROM traces
    WHERE project_id = ${trace.project_id as string}
      AND id = ${trace.id as string}
  `;
  await prisma.$executeRaw`
    INSERT INTO traces (
      id, project_id, "timestamp", name, user_id, session_id, environment,
      public, bookmarked, tags, input, output, metadata, release, version,
      created_at, updated_at, event_ts, is_deleted
    ) VALUES (
      ${trace.id as string},
      ${trace.project_id as string},
      ${timestamp},
      ${trace.name ?? null},
      ${trace.user_id ?? null},
      ${trace.session_id ?? null},
      ${trace.environment ?? "default"},
      ${trace.public ?? false},
      ${trace.bookmarked ?? false},
      ${trace.tags ?? []},
      ${typeof trace.input === "string" ? trace.input : trace.input == null ? null : JSON.stringify(trace.input)},
      ${typeof trace.output === "string" ? trace.output : trace.output == null ? null : JSON.stringify(trace.output)},
      ${(trace.metadata ?? {}) as any},
      ${trace.release ?? null},
      ${trace.version ?? null},
      ${createdAt},
      ${updatedAt},
      ${updatedAt},
      ${false}
    )
  `;
};

export const getTracesByIds = async (
  traceIds: string[],
  projectId: string,
  timestamp?: Date,
  _clickhouseConfigs?: ClickHouseClientConfigOptions | undefined,
) => {
  const conditions: Prisma.Sql[] = [
    Prisma.sql`project_id = ${projectId}`,
    Prisma.sql`id IN (${Prisma.join(traceIds)})`,
  ];
  if (timestamp) conditions.push(Prisma.sql`timestamp >= ${timestamp}`);
  const records = await prisma.$queryRaw<PgTraceRow[]>(Prisma.sql`
    SELECT *
    FROM traces
    WHERE ${Prisma.join(conditions, " AND ")}
    ORDER BY updated_at DESC
  `);

  return records.map((record) =>
    convertClickhouseToDomain(
      toTraceRecordReadType(record),
      DEFAULT_RENDERING_PROPS,
    ),
  );
};

export const getTracesBySessionId = async (
  projectId: string,
  sessionIds: string[],
  timestamp?: Date,
) => {
  if (sessionIds.length === 0) return [];

  const records = await measureAndReturn({
    operationName: "getTracesBySessionId",
    projectId,
    input: {
      params: {
        sessionIds,
        projectId,
        timestamp: timestamp
          ? convertDateToClickhouseDateTime(timestamp)
          : null,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "list",
        projectId,
        operation_name: "getTracesBySessionId",
      },
      timestamp,
    },
    fn: async () => {
      const rows = await prisma.$queryRaw<PgTraceRow[]>(Prisma.sql`
        SELECT DISTINCT ON (id, project_id) *
        FROM traces
        WHERE session_id IN (${Prisma.join(sessionIds)})
          AND project_id = ${projectId}
          ${timestamp ? Prisma.sql`AND timestamp >= ${timestamp}` : Prisma.empty}
        ORDER BY id, project_id, event_ts DESC
      `);
      return rows.map((row) => toTraceRecordReadType(row));
    },
  });

  const traces = records.map((record) =>
    convertClickhouseToDomain(record, DEFAULT_RENDERING_PROPS),
  );

  traces.forEach((trace) => {
    recordDistribution(
      "langfuse.traces_by_session_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
    );
  });

  return traces;
};

export const hasAnyTrace = async (projectId: string) => {
  // Check PostgreSQL flag first — once set, it's never reverted
  try {
    const project = await metadataPrisma.project.findUnique({
      where: { id: projectId },
      select: { hasTraces: true },
    });
    if (project?.hasTraces) {
      return true;
    }
  } catch (error) {
    traceException(error);
    logger.error(
      `Failed to read hasTraces flag from PostgreSQL; projectId=${projectId}; error=${stringifyErrorForMessage(error)}`,
    );
  }

  let result = false;
  try {
    result =
      (
        await prisma.$queryRaw<Array<{ id: string }>>(Prisma.sql`
          SELECT id FROM traces WHERE project_id = ${projectId} LIMIT 1
        `)
      ).length > 0;
  } catch (error) {
    traceException(error);
    logger.error(
      `Failed to read traces presence from tracing PostgreSQL; projectId=${projectId}; error=${stringifyErrorForMessage(error)}`,
    );
    throw error;
  }

  // Persist positive result in PostgreSQL — once a project has traces, it stays true
  // Only update if not already set to avoid unnecessary writes
  if (result) {
    try {
      await metadataPrisma.project.updateMany({
        where: { id: projectId, hasTraces: false },
        data: { hasTraces: true },
      });
    } catch (error) {
      traceException(error);
      logger.error(
        `Failed to persist hasTraces flag to PostgreSQL; projectId=${projectId}; error=${stringifyErrorForMessage(error)}`,
      );
    }
  }

  return result;
};

export const getTraceCountsByProjectInCreationInterval = async ({
  start,
  end,
}: {
  start: Date;
  end: Date;
}) => {
  return measureAndReturn({
    operationName: "getTraceCountsByProjectInCreationInterval",
    projectId: "__CROSS_PROJECT__",
    input: {
      params: {
        start: convertDateToClickhouseDateTime(start),
        end: convertDateToClickhouseDateTime(end),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        operation_name: "getTraceCountsByProjectInCreationInterval",
      },
      timestamp: start,
    },
    fn: async () => {
      const rows = await prisma.$queryRaw<
        Array<{ project_id: string; count: bigint }>
      >(Prisma.sql`
        SELECT project_id, count(*)::bigint as count
        FROM traces
        WHERE created_at >= ${start}
          AND created_at < ${end}
        GROUP BY project_id
      `);

      return rows.map((row) => ({
        projectId: row.project_id,
        count: Number(row.count),
      }));
    },
  });
};

export const getTraceCountOfProjectsSinceCreationDate = async ({
  projectIds,
  start,
}: {
  projectIds: string[];
  start: Date;
}) => {
  if (projectIds.length === 0) return 0;

  return measureAndReturn({
    operationName: "getTraceCountOfProjectsSinceCreationDate",
    projectId: "__CROSS_PROJECT__",
    input: {
      params: {
        projectIds,
        start: convertDateToClickhouseDateTime(start),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        operation_name: "getTraceCountOfProjectsSinceCreationDate",
      },
      timestamp: start,
    },
    fn: async () => {
      const rows = await prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
        SELECT count(*)::bigint as count
        FROM traces
        WHERE project_id IN (${Prisma.join(projectIds)})
          AND created_at >= ${start}
      `);
      return Number(rows[0]?.count ?? 0n);
    },
  });
};

/**
 * Retrieves a trace record by its ID and associated project ID, with optional filtering by timestamp range.
 * If no timestamp filters are provided, runs two queries in parallel:
 * 1. One with a 7-day fromTimestamp filter (typically faster)
 * 2. One without any timestamp filters (complete but slower)
 * Returns the first non-empty result.
 */
export const getTraceById = async ({
  traceId,
  projectId,
  timestamp,
  fromTimestamp,
  renderingProps = DEFAULT_RENDERING_PROPS,
  clickhouseFeatureTag: _clickhouseFeatureTag = "tracing",
  preferredClickhouseService: _preferredClickhouseService,
  excludeInputOutput = false,
}: {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  fromTimestamp?: Date;
  renderingProps?: RenderingProps;
  clickhouseFeatureTag?: string;
  preferredClickhouseService?: PreferredClickhouseService;
  /** When true, sets input/output columns to empty in the query to reduce database load */
  excludeInputOutput?: boolean;
}) => {
  const timestampWhere: { gte?: Date; lt?: Date } = {};
  if (timestamp) {
    timestampWhere.gte = new Date(
      Date.UTC(
        timestamp.getUTCFullYear(),
        timestamp.getUTCMonth(),
        timestamp.getUTCDate(),
        0,
        0,
        0,
        0,
      ),
    );
    timestampWhere.lt = new Date(
      Date.UTC(
        timestamp.getUTCFullYear(),
        timestamp.getUTCMonth(),
        timestamp.getUTCDate() + 1,
        0,
        0,
        0,
        0,
      ),
    );
  }
  if (fromTimestamp) {
    timestampWhere.gte = fromTimestamp;
  }

  const conditions: Prisma.Sql[] = [
    Prisma.sql`id = ${traceId}`,
    Prisma.sql`project_id = ${projectId}`,
  ];
  if (timestampWhere.gte)
    conditions.push(Prisma.sql`timestamp >= ${timestampWhere.gte}`);
  if (timestampWhere.lt)
    conditions.push(Prisma.sql`timestamp < ${timestampWhere.lt}`);
  const records = await prisma.$queryRaw<PgTraceRow[]>(Prisma.sql`
    SELECT *
    FROM traces
    WHERE ${Prisma.join(conditions, " AND ")}
    ORDER BY updated_at DESC
    LIMIT 1
  `);
  const record = records[0];

  if (!record) return undefined;

  const mapped = toTraceRecordReadType(record);
  if (excludeInputOutput) {
    mapped.input = "";
    mapped.output = "";
  }

  const res = [convertClickhouseToDomain(mapped, renderingProps)];

  res.forEach((trace) => {
    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - trace.timestamp.getTime(),
      {
        table: "traces",
      },
    );
  });

  return res.shift();
};

export const getTracesGroupedByName = async (
  projectId: string,
  _tableDefinitions: UiColumnMappings = tracesTableUiColumnDefinitions,
  timestampFilter?: FilterState,
) => {
  return measureAndReturn({
    operationName: "getTracesGroupedByName",
    projectId,
    input: {
      params: { projectId },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTracesGroupedByName",
      },
    },
    fn: async () => {
      const timestampFilters = (timestampFilter ?? []).filter(
        (f) => f.column === "timestamp" && f.type === "datetime",
      ) as Array<{ operator: string; value: Date }>;
      const conditions: Prisma.Sql[] = [
        Prisma.sql`project_id = ${projectId}`,
        Prisma.sql`name IS NOT NULL`,
      ];
      for (const tf of timestampFilters) {
        if (tf.operator === ">=")
          conditions.push(Prisma.sql`timestamp >= ${tf.value}`);
        if (tf.operator === ">")
          conditions.push(Prisma.sql`timestamp > ${tf.value}`);
        if (tf.operator === "<=")
          conditions.push(Prisma.sql`timestamp <= ${tf.value}`);
        if (tf.operator === "<")
          conditions.push(Prisma.sql`timestamp < ${tf.value}`);
      }
      const rows = await prisma.$queryRaw<
        Array<{ name: string; count: bigint }>
      >(
        Prisma.sql`
          SELECT name, COUNT(*)::bigint AS count
          FROM traces
          WHERE ${Prisma.join(conditions, " AND ")}
          GROUP BY name
          ORDER BY COUNT(*) DESC
          LIMIT 1000
        `,
      );
      return rows.map((r) => ({ name: r.name, count: String(r.count) }));
    },
  });
};

export const getTracesGroupedBySessionId = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit?: number,
  offset?: number,
  _columns?: UiColumnMappings,
) => {
  return measureAndReturn({
    operationName: "getTracesGroupedBySessionId",
    projectId,
    input: {
      params: { projectId, limit, offset },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTracesGroupedBySessionId",
      },
    },
    fn: async () => {
      const timestampFilters = (filter ?? []).filter(
        (f) => f.column === "timestamp" && f.type === "datetime",
      ) as Array<{ operator: string; value: Date }>;
      const conditions: Prisma.Sql[] = [
        Prisma.sql`project_id = ${projectId}`,
        Prisma.sql`session_id IS NOT NULL`,
        Prisma.sql`session_id != ''`,
      ];
      for (const tf of timestampFilters) {
        if (tf.operator === ">=")
          conditions.push(Prisma.sql`timestamp >= ${tf.value}`);
        if (tf.operator === ">")
          conditions.push(Prisma.sql`timestamp > ${tf.value}`);
        if (tf.operator === "<=")
          conditions.push(Prisma.sql`timestamp <= ${tf.value}`);
        if (tf.operator === "<")
          conditions.push(Prisma.sql`timestamp < ${tf.value}`);
      }
      if (searchQuery) {
        conditions.push(Prisma.sql`session_id ILIKE ${`%${searchQuery}%`}`);
      }
      const rows = await prisma.$queryRaw<
        Array<{ session_id: string; count: bigint }>
      >(Prisma.sql`
        SELECT session_id, COUNT(*)::bigint AS count
        FROM traces
        WHERE ${Prisma.join(conditions, " AND ")}
        GROUP BY session_id
        ORDER BY count DESC
        ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
        ${offset !== undefined ? Prisma.sql`OFFSET ${offset}` : Prisma.empty}
      `);
      return rows.map((r) => ({
        session_id: r.session_id,
        count: String(r.count),
      }));
    },
  });
};

export const getTracesGroupedByUsers = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit?: number,
  offset?: number,
  _columns?: UiColumnMappings,
) => {
  return measureAndReturn({
    operationName: "getTracesGroupedByUsers",
    projectId,
    input: {
      params: { projectId, limit, offset },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTracesGroupedByUsers",
      },
    },
    fn: async () => {
      const timestampFilters = (filter ?? []).filter(
        (f) => f.column === "timestamp" && f.type === "datetime",
      ) as Array<{ operator: string; value: Date }>;
      const conditions: Prisma.Sql[] = [
        Prisma.sql`project_id = ${projectId}`,
        Prisma.sql`user_id IS NOT NULL`,
        Prisma.sql`user_id != ''`,
      ];
      for (const tf of timestampFilters) {
        if (tf.operator === ">=")
          conditions.push(Prisma.sql`timestamp >= ${tf.value}`);
        if (tf.operator === ">")
          conditions.push(Prisma.sql`timestamp > ${tf.value}`);
        if (tf.operator === "<=")
          conditions.push(Prisma.sql`timestamp <= ${tf.value}`);
        if (tf.operator === "<")
          conditions.push(Prisma.sql`timestamp < ${tf.value}`);
      }
      if (searchQuery) {
        conditions.push(Prisma.sql`user_id ILIKE ${`%${searchQuery}%`}`);
      }
      const rows = await prisma.$queryRaw<
        Array<{ user: string; count: bigint }>
      >(
        Prisma.sql`
          SELECT user_id AS user, COUNT(*)::bigint AS count
          FROM traces
          WHERE ${Prisma.join(conditions, " AND ")}
          GROUP BY user_id
          ORDER BY count DESC
          ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
          ${offset !== undefined ? Prisma.sql`OFFSET ${offset}` : Prisma.empty}
        `,
      );
      return rows.map((r) => ({ user: r.user, count: String(r.count) }));
    },
  });
};

export type GroupedTracesQueryProp = {
  projectId: string;
  filter: FilterState;
  columns?: UiColumnMappings;
};

export const getTracesGroupedByTags = async (props: GroupedTracesQueryProp) => {
  const { projectId, filter } = props;

  return measureAndReturn({
    operationName: "getTracesGroupedByTags",
    projectId,
    input: {
      params: { projectId },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTracesGroupedByTags",
      },
    },
    fn: async () => {
      const timestampFilters = (filter ?? []).filter(
        (f) => f.column === "timestamp" && f.type === "datetime",
      ) as Array<{ operator: string; value: Date }>;
      const conditions: Prisma.Sql[] = [
        Prisma.sql`t.project_id = ${projectId}`,
      ];
      for (const tf of timestampFilters) {
        if (tf.operator === ">=")
          conditions.push(Prisma.sql`t.timestamp >= ${tf.value}`);
        if (tf.operator === ">")
          conditions.push(Prisma.sql`t.timestamp > ${tf.value}`);
        if (tf.operator === "<=")
          conditions.push(Prisma.sql`t.timestamp <= ${tf.value}`);
        if (tf.operator === "<")
          conditions.push(Prisma.sql`t.timestamp < ${tf.value}`);
      }
      const rows = await prisma.$queryRaw<Array<{ value: string }>>(Prisma.sql`
        SELECT DISTINCT tag.value
        FROM traces t
        CROSS JOIN LATERAL UNNEST(t.tags) AS tag(value)
        WHERE ${Prisma.join(conditions, " AND ")}
        LIMIT 1000
      `);
      return rows;
    },
  });
};

export const getTracesIdentifierForSession = async (
  projectId: string,
  sessionId: string,
) => {
  const rows = await measureAndReturn({
    operationName: "getTracesIdentifierForSession",
    projectId,
    input: {
      params: {
        projectId,
        sessionId,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "list",
        projectId,
        operation_name: "getTracesIdentifierForSession",
      },
    },
    fn: async () => {
      return prisma.$queryRaw<
        Array<{
          id: string;
          user_id: string;
          name: string;
          timestamp: Date;
          environment: string;
        }>
      >(Prisma.sql`
        SELECT DISTINCT ON (id, project_id)
          id,
          user_id,
          name,
          timestamp,
          environment
        FROM traces
        WHERE project_id = ${projectId}
          AND session_id = ${sessionId}
        ORDER BY id, project_id, timestamp ASC
      `);
    },
  });

  return rows.map((row) => ({
    id: row.id,
    userId: row.user_id,
    name: row.name,
    timestamp:
      row.timestamp instanceof Date
        ? row.timestamp
        : parseClickhouseUTCDateTimeFormat(row.timestamp),
    environment: row.environment,
  }));
};

export const deleteTraces = async (projectId: string, traceIds: string[]) => {
  await measureAndReturn({
    operationName: "deleteTraces",
    projectId,
    input: {
      params: {
        projectId,
        traceIds,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "delete",
        projectId,
      },
    },
    fn: async () => {
      const preflight = await prisma.$queryRaw<
        Array<{ min_ts: Date | null; max_ts: Date | null; cnt: bigint }>
      >(Prisma.sql`
        SELECT
          (min(timestamp) - INTERVAL '1 hour') as min_ts,
          (max(timestamp) + INTERVAL '1 hour') as max_ts,
          count(*)::bigint as cnt
        FROM traces
        WHERE project_id = ${projectId}
          AND id IN (${Prisma.join(traceIds)})
      `);

      const count = Number(preflight[0]?.cnt ?? 0n);
      if (count === 0) {
        logger.info(
          `deleteTraces: no rows found for project ${projectId}, skipping DELETE`,
        );
        return;
      }

      await prisma.$executeRaw`
        DELETE FROM traces
        WHERE project_id = ${projectId}
          AND id IN (${Prisma.join(traceIds)})
          AND timestamp >= ${preflight[0].min_ts}
          AND timestamp <= ${preflight[0].max_ts}
      `;
    },
  });
};

export const hasAnyTraceOlderThan = async (
  projectId: string,
  beforeDate: Date,
) => {
  const rows = await prisma.$queryRaw<Array<{ one: number }>>(Prisma.sql`
    SELECT 1 as one
    FROM traces
    WHERE project_id = ${projectId}
      AND timestamp < ${beforeDate}
    LIMIT 1
  `);

  return rows.length > 0;
};

export const deleteTracesOlderThanDays = async (
  projectId: string,
  beforeDate: Date,
): Promise<boolean> => {
  const hasData = await hasAnyTraceOlderThan(projectId, beforeDate);
  if (!hasData) {
    return false;
  }

  await measureAndReturn({
    operationName: "deleteTracesOlderThanDays",
    projectId,
    input: {
      params: {
        projectId,
        cutoffDate: convertDateToClickhouseDateTime(beforeDate),
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "delete",
        projectId,
      },
    },
    fn: async () => {
      await prisma.$executeRaw`
        DELETE FROM traces
        WHERE project_id = ${projectId}
          AND timestamp < ${beforeDate}
      `;
    },
  });

  return true;
};

export const deleteTracesByProjectId = async (
  projectId: string,
): Promise<boolean> => {
  const hasData = await hasAnyTrace(projectId);
  if (!hasData) {
    return false;
  }

  await measureAndReturn({
    operationName: "deleteTracesByProjectId",
    projectId,
    input: {
      params: {
        projectId,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "delete",
        projectId,
      },
    },
    fn: async () => {
      await prisma.$executeRaw`
        DELETE FROM traces
        WHERE project_id = ${projectId}
      `;
    },
  });

  return true;
};

export const hasAnyUser = async (projectId: string) => {
  return measureAndReturn({
    operationName: "hasAnyUser",
    projectId,
    input: {
      projectId,
      tags: {
        feature: "tracing",
        type: "user",
        kind: "hasAny",
        projectId,
        operation_name: "hasAnyUser",
      },
    },
    fn: async (input) => {
      const rows = await prisma.$queryRaw<Array<{ one: number }>>(Prisma.sql`
        SELECT 1 as one
        FROM traces
        WHERE project_id = ${input.projectId}
          AND user_id IS NOT NULL
          AND user_id != ''
        LIMIT 1
      `);

      return rows.length > 0;
    },
  });
};

export const getTotalUserCount = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
): Promise<{ totalCount: bigint }[]> => {
  return measureAndReturn({
    operationName: "getTotalUserCount",
    projectId,
    input: {
      params: {},
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getTotalUserCount",
      },
    },
    fn: async () => {
      const timestampConditions = filter
        .filter(
          (f) =>
            f.type === "datetime" &&
            (f.column === "timestamp" || f.column === "Timestamp"),
        )
        .map((f) => {
          if (f.operator === ">=") return Prisma.sql`t.timestamp >= ${f.value}`;
          if (f.operator === ">") return Prisma.sql`t.timestamp > ${f.value}`;
          if (f.operator === "<=") return Prisma.sql`t.timestamp <= ${f.value}`;
          return Prisma.sql`t.timestamp < ${f.value}`;
        });

      const userSearch = searchQuery?.trim()
        ? Prisma.sql`AND t.user_id ILIKE ${`%${searchQuery.trim()}%`}`
        : Prisma.empty;

      return prisma.$queryRaw<Array<{ totalCount: bigint }>>(Prisma.sql`
        SELECT COUNT(DISTINCT t.user_id)::bigint AS "totalCount"
        FROM traces t
        WHERE t.project_id = ${projectId}
          AND t.user_id IS NOT NULL
          AND t.user_id != ''
          ${timestampConditions.length ? Prisma.sql`AND ${Prisma.join(timestampConditions, " AND ")}` : Prisma.empty}
          ${userSearch}
      `);
    },
  });
};

export const getUserMetrics = async (
  projectId: string,
  userIds: string[],
  filter: FilterState,
) => {
  if (userIds.length === 0) return [];

  return measureAndReturn({
    operationName: "getUserMetrics",
    projectId,
    input: {
      params: {},
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "analytic",
        projectId,
        operation_name: "getUserMetrics",
      },
    },
    fn: async () => {
      const traceTimestampConditions = filter
        .filter(
          (f) =>
            f.type === "datetime" &&
            (f.column === "timestamp" || f.column === "Timestamp"),
        )
        .map((f) => {
          if (f.operator === ">=") return Prisma.sql`t.timestamp >= ${f.value}`;
          if (f.operator === ">") return Prisma.sql`t.timestamp > ${f.value}`;
          if (f.operator === "<=") return Prisma.sql`t.timestamp <= ${f.value}`;
          return Prisma.sql`t.timestamp < ${f.value}`;
        });

      const fromTimestamp = filter.find(
        (f) =>
          f.type === "datetime" &&
          (f.column === "timestamp" || f.column === "Timestamp") &&
          (f.operator === ">=" || f.operator === ">"),
      )?.value;
      const observationLowerBound =
        fromTimestamp instanceof Date
          ? new Date(fromTimestamp.getTime() - 2 * 24 * 60 * 60 * 1000)
          : null;

      const rows = await prisma.$queryRaw<
        Array<{
          user_id: string;
          environment: string | null;
          max_timestamp: Date;
          min_timestamp: Date;
          input_usage: string;
          output_usage: string;
          total_usage: string;
          obs_count: bigint;
          trace_count: bigint;
          sum_total_cost: string;
        }>
      >(Prisma.sql`
        SELECT
          t.user_id,
          MAX(t.environment) AS environment,
          MAX(t.timestamp) AS max_timestamp,
          MIN(t.timestamp) AS min_timestamp,
          COALESCE(SUM((o.usage_details->>'input')::numeric), 0)::text AS input_usage,
          COALESCE(SUM((o.usage_details->>'output')::numeric), 0)::text AS output_usage,
          COALESCE(SUM((o.usage_details->>'total')::numeric), 0)::text AS total_usage,
          COUNT(DISTINCT o.id)::bigint AS obs_count,
          COUNT(DISTINCT t.id)::bigint AS trace_count,
          COALESCE(
            SUM(
              COALESCE(
                o.total_cost,
                NULLIF(o.cost_details->>'total', '')::numeric
              )
            ),
            0
          )::text AS sum_total_cost
        FROM traces t
        LEFT JOIN observations o
          ON o.project_id = t.project_id
         AND o.trace_id = t.id
         ${observationLowerBound ? Prisma.sql`AND o.start_time >= ${observationLowerBound}` : Prisma.empty}
        WHERE t.project_id = ${projectId}
          AND t.user_id IN (${Prisma.join(userIds)})
          ${traceTimestampConditions.length ? Prisma.sql`AND ${Prisma.join(traceTimestampConditions, " AND ")}` : Prisma.empty}
        GROUP BY t.user_id
      `);

      return rows.map((row) => ({
        userId: row.user_id,
        environment: row.environment ?? "default",
        maxTimestamp: row.max_timestamp,
        minTimestamp: row.min_timestamp,
        inputUsage: Number(row.input_usage),
        outputUsage: Number(row.output_usage),
        totalUsage: Number(row.total_usage),
        observationCount: Number(row.obs_count),
        traceCount: Number(row.trace_count),
        totalCost: Number(row.sum_total_cost),
      }));
    },
  });
};

export const getTracesForBlobStorageExport = function (
  projectId: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const iterator = (async function* () {
    const rows = await prisma.$queryRaw<Record<string, unknown>[]>(Prisma.sql`
      SELECT
        id,
        timestamp,
        name,
        environment,
        project_id,
        metadata,
        user_id,
        session_id,
        release,
        version,
        public as public,
        bookmarked as bookmarked,
        tags,
        input as input,
        output as output
      FROM traces
      WHERE project_id = ${projectId}
        AND timestamp >= ${minTimestamp}
        AND timestamp <= ${maxTimestamp}
      ORDER BY timestamp ASC
    `);
    for (const row of rows) {
      yield row;
    }
  })();

  return iterator;
};

export const getTracesForAnalyticsIntegrations = async function* (
  projectId: string,
  projectName: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const records = await prisma.$queryRaw<Record<string, unknown>[]>(Prisma.sql`
    WITH observations_agg AS (
      SELECT
        o.project_id,
        o.trace_id,
        COALESCE(
          SUM(
            COALESCE(
              o.total_cost,
              NULLIF(o.cost_details->>'total', '')::numeric
            )
          ),
          0
        ) as total_cost,
        count(*)::bigint as observation_count,
        (EXTRACT(EPOCH FROM (MAX(COALESCE(o.end_time, o.start_time)) - MIN(o.start_time))) * 1000) as latency_milliseconds
      FROM observations o
      WHERE o.project_id = ${projectId}
        AND o.start_time >= ${new Date(minTimestamp.getTime() - 2 * 24 * 60 * 60 * 1000)}
      GROUP BY o.project_id, o.trace_id
    )
    SELECT
      t.id as id,
      t.timestamp as timestamp,
      t.name as name,
      t.session_id as session_id,
      t.user_id as user_id,
      t.release as release,
      t.version as version,
      t.tags as tags,
      t.environment as environment,
      t.metadata->>'$posthog_session_id' as posthog_session_id,
      t.metadata->>'$mixpanel_session_id' as mixpanel_session_id,
      o.total_cost as total_cost,
      (o.latency_milliseconds / 1000) as latency,
      o.observation_count as observation_count
    FROM traces t
    LEFT JOIN observations_agg o ON t.id = o.trace_id AND t.project_id = o.project_id
    WHERE t.project_id = ${projectId}
      AND t.timestamp >= ${minTimestamp}
      AND t.timestamp <= ${maxTimestamp}
  `);

  const baseUrl = env.NEXTAUTH_URL?.replace("/api/auth", "");

  for (const record of records) {
    yield {
      timestamp: record.timestamp,
      langfuse_id: record.id,
      langfuse_trace_name: record.name,
      langfuse_url: `${baseUrl}/project/${projectId}/traces/${encodeURIComponent(record.id as string)}`,
      langfuse_user_url: record.user_id
        ? `${baseUrl}/project/${projectId}/users/${encodeURIComponent(record.user_id as string)}`
        : undefined,
      langfuse_cost_usd: record.total_cost,
      langfuse_count_observations: record.observation_count,
      langfuse_session_id: record.session_id,
      langfuse_project_id: projectId,
      langfuse_project_name: projectName,
      langfuse_user_id: record.user_id || null,
      langfuse_latency: record.latency,
      langfuse_release: record.release,
      langfuse_version: record.version,
      langfuse_tags: record.tags,
      langfuse_environment: record.environment,
      langfuse_event_version: "1.0.0",
      posthog_session_id: record.posthog_session_id ?? null,
      mixpanel_session_id: record.mixpanel_session_id ?? null,
    } satisfies AnalyticsTraceEvent;
  }
};

/**
 * This query is used only for legacy support of redirects without a projectId.
 * We don't have an index on the traceId so it will be a full table scan.
 * We expect at most 10s of calls per day, so this is acceptable.
 */
export const getTracesByIdsForAnyProject = async (traceIds: string[]) => {
  if (traceIds.length === 0) return [];
  return measureAndReturn({
    operationName: "getTracesByIdsForAnyProject",
    projectId: "__CROSS_PROJECT__",
    input: {
      params: {
        traceIds,
      },
      tags: {
        feature: "tracing",
        type: "trace",
        kind: "list",
        operation_name: "getTracesByIdsForAnyProject",
      },
    },
    fn: async () => {
      const records = await prisma.$queryRaw<
        Array<{ id: string; project_id: string }>
      >(Prisma.sql`
        SELECT DISTINCT ON (id, project_id) id, project_id
        FROM traces
        WHERE id IN (${Prisma.join(traceIds)})
        ORDER BY id, project_id, event_ts DESC
      `);

      return records.map((record) => ({
        id: record.id,
        projectId: record.project_id,
      }));
    },
  });
};

export async function getAgentGraphData(params: {
  projectId: string;
  traceId: string;
  chMinStartTime: string;
  chMaxStartTime: string;
}): Promise<
  Array<{
    id: string;
    parent_observation_id: string | null;
    type: string;
    name: string;
    start_time: string;
    end_time: string | null;
    node: string | null;
    step: number | null;
  }>
> {
  const { projectId, traceId, chMinStartTime, chMaxStartTime } = params;
  const minStartTime = parseClickhouseUTCDateTimeFormat(chMinStartTime);
  const maxStartTime = parseClickhouseUTCDateTimeFormat(chMaxStartTime);

  return prisma.$queryRaw<
    Array<{
      id: string;
      parent_observation_id: string | null;
      type: string;
      name: string;
      start_time: string;
      end_time: string | null;
      node: string | null;
      step: number | null;
    }>
  >(Prisma.sql`
    SELECT
      id,
      parent_observation_id,
      type::text as type,
      name,
      start_time::text as start_time,
      end_time::text as end_time,
      metadata->>'langgraph_node' AS node,
      NULLIF(metadata->>'langgraph_step','')::int AS step
    FROM observations
    WHERE project_id = ${projectId}
      AND trace_id = ${traceId}
      AND start_time >= ${minStartTime}
      AND start_time <= ${maxStartTime}
  `);
}

/**
 * Get trace counts grouped by project and day within a date range.
 *
 * Returns one row per project per day with the count of traces created on that day.
 * Uses half-open interval [startDate, endDate) for filtering.
 *
 * @param startDate - Start of date range (inclusive)
 * @param endDate - End of date range (exclusive)
 * @returns Array of { count, projectId, date } objects
 *
 * @example
 * // Get trace counts for March 1-2, 2024
 * const counts = await getTraceCountsByProjectAndDay({
 *   startDate: new Date('2024-03-01T00:00:00Z'),
 *   endDate: new Date('2024-03-03T00:00:00Z')
 * });
 * // Returns: [
 * //   { count: 1500, projectId: 'proj-123', date: '2024-03-01' },
 * //   { count: 1200, projectId: 'proj-123', date: '2024-03-02' },
 * //   { count: 2300, projectId: 'proj-456', date: '2024-03-01' },
 * //   ...
 * // ]
 *
 * Note: Uses non-deduplicating reads for faster and cheaper queries.
 * queries against clickhouse. Generous 4x overcompensation before blocking allows
 * for usage aggregation to be meaningful.
 *
 */
export const getTraceCountsByProjectAndDay = async ({
  startDate,
  endDate,
}: {
  startDate: Date;
  endDate: Date;
}) => {
  const rows = await prisma.$queryRaw<
    Array<{ count: bigint; project_id: string; date: string }>
  >(Prisma.sql`
    SELECT
      count(*)::bigint as count,
      project_id,
      DATE(timestamp)::text as date
    FROM traces
    WHERE timestamp >= ${startDate}
      AND timestamp < ${endDate}
    GROUP BY project_id, DATE(timestamp)
  `);

  return rows.map((row) => ({
    count: Number(row.count),
    projectId: row.project_id,
    date: row.date,
  }));
};
