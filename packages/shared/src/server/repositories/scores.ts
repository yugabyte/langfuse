import {
  ScoreDataTypeType,
  ScoreDomain,
  ScoreSourceType,
  AGGREGATABLE_SCORE_TYPES,
  AggregatableScoreDataType,
} from "../../domain/scores";
import { queryClickhouse, queryClickhouseStream } from "./clickhouse";
import { FilterList, orderByToClickhouseSql } from "../queries";
import { FilterCondition, FilterState, TimeFilter } from "../../types";
import {
  createFilterFromFilterState,
  getProjectIdDefaultFilter,
} from "../queries/clickhouse-sql/factory";
import { OrderByState } from "../../interfaces/orderBy";
import {
  dashboardColumnDefinitions,
  scoresTableUiColumnDefinitions,
  scoresTableUiColumnDefinitionsFromEvents,
} from "../tableMappings";
import {
  convertScoreAggregation,
  convertClickhouseScoreToDomain,
  ScoreAggregation,
} from "./scores_converters";
import {
  convertDateToClickhouseDateTime,
  PreferredClickhouseService,
} from "../clickhouse/client";
import { ScoreRecordReadType } from "./definitions";
import { env } from "../../env";
import { _handleGetScoreById, _handleGetScoresByIds } from "./scores-utils";
import { parseMetadataCHRecordToDomain } from "../utils/metadata_conversion";
import type { AnalyticsScoreEvent } from "../analytics-integrations/types";
import { ClickHouseClientConfigOptions } from "@clickhouse/client";
import { recordDistribution } from "../instrumentation";
import { prisma as metadataPrisma, tracingPrisma as prisma } from "../../db";
import { measureAndReturn } from "../clickhouse/measureAndReturn";
import { eventsTraceMetadata } from "../queries/clickhouse-sql/query-fragments";
import { Prisma } from "@prisma/client";
import { logger } from "../logger";

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

const toClickhouseDateTimeString = (value: Date | null | undefined) =>
  value ? value.toISOString().replace("T", " ").replace("Z", "") : undefined;

const toClickhouseMetadataRecord = (value: unknown): Record<string, string> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([k, v]) => [
      k,
      typeof v === "string" ? v : JSON.stringify(v),
    ]),
  );
};

const jsonbHasAnyKeys = (value: unknown) =>
  !!value &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  Object.keys(value as Record<string, unknown>).length > 0;

const toScoreRecordReadType = (score: {
  id: string;
  timestamp: Date;
  project_id: string;
  name: string | null;
  value: number | null;
  source: string;
  author_user_id: string | null;
  comment: string | null;
  trace_id: string | null;
  observation_id: string | null;
  config_id: string | null;
  string_value: string | null;
  queue_id: string | null;
  created_at: Date;
  updated_at: Date;
  data_type: string;
  metadata: Record<string, unknown> | null;
  session_id: string | null;
  dataset_run_id: string | null;
  environment: string | null;
  long_string_value: string | null;
  execution_trace_id: string | null;
  event_ts: Date;
  is_deleted: boolean;
}): ScoreRecordReadType => ({
  id: score.id,
  timestamp: toClickhouseDateTimeString(score.timestamp) ?? "",
  project_id: score.project_id,
  name: score.name ?? "",
  value: score.value ?? 0,
  source: score.source,
  author_user_id: score.author_user_id,
  comment: score.comment,
  trace_id: score.trace_id,
  observation_id: score.observation_id,
  config_id: score.config_id,
  string_value: score.string_value,
  queue_id: score.queue_id,
  created_at: toClickhouseDateTimeString(score.created_at) ?? "",
  updated_at: toClickhouseDateTimeString(score.updated_at) ?? "",
  data_type: score.data_type,
  metadata: toClickhouseMetadataRecord(score.metadata),
  session_id: score.session_id,
  dataset_run_id: score.dataset_run_id,
  environment: score.environment ?? "default",
  long_string_value: score.long_string_value ?? "",
  execution_trace_id: score.execution_trace_id,
  event_ts: toClickhouseDateTimeString(score.event_ts) ?? "",
  is_deleted: score.is_deleted ? 1 : 0,
});

const parseDateInput = (value: unknown) => {
  if (value instanceof Date) return value;
  if (typeof value === "number") return new Date(value);
  if (typeof value === "string" && /^\d+$/.test(value)) {
    return new Date(Number(value));
  }
  return new Date(String(value));
};

const normalizeTimestampColumn = (column: string) => {
  const c = column.toLowerCase().replace(/\s+/g, "");
  return c === "timestamp";
};

const SCORE_DATA_TYPE_VALUES = new Set([
  "NUMERIC",
  "BOOLEAN",
  "CATEGORICAL",
] as const);
const toScoreDataTypeEnum = (value: unknown) => {
  const normalized = String(value ?? "NUMERIC").toUpperCase();
  return SCORE_DATA_TYPE_VALUES.has(normalized as any) ? normalized : "NUMERIC";
};

export const searchExistingAnnotationScore = async (
  projectId: string,
  observationId: string | null,
  traceId: string | null,
  sessionId: string | null,
  name: string | undefined,
  configId: string | undefined,
  dataType: ScoreDataTypeType,
) => {
  if (!name && !configId) {
    throw new Error("Either name or configId (or both) must be provided.");
  }

  const orConditions: Prisma.Sql[] = [];
  if (name) orConditions.push(Prisma.sql`name = ${name}`);
  if (configId) orConditions.push(Prisma.sql`config_id = ${configId}`);
  const row = (
    await prisma.$queryRaw<any[]>(Prisma.sql`
      SELECT *
      FROM scores
      WHERE project_id = ${projectId}
        AND source = 'ANNOTATION'
        AND data_type::text = ${dataType}
        ${traceId ? Prisma.sql`AND trace_id = ${traceId}` : Prisma.empty}
        ${observationId ? Prisma.sql`AND observation_id = ${observationId}` : Prisma.empty}
        AND (${Prisma.join(orConditions, " OR ")})
      ORDER BY updated_at DESC
      LIMIT 1
    `)
  )[0];
  return row
    ? convertClickhouseScoreToDomain(toScoreRecordReadType(row))
    : undefined;
};

export const getScoreById = async ({
  projectId,
  scoreId,
  source,
}: {
  projectId: string;
  scoreId: string;
  source?: ScoreSourceType;
}): Promise<ScoreDomain | undefined> => {
  return _handleGetScoreById({
    projectId,
    scoreId,
    source,
    scoreScope: "all",
  });
};

export const getScoresByIds = async (
  projectId: string,
  scoreId: string[],
  source?: ScoreSourceType,
): Promise<ScoreDomain[]> => {
  const rows = await prisma.$queryRaw<any[]>(Prisma.sql`
    SELECT *
    FROM scores
    WHERE project_id = ${projectId}
      AND id IN (${Prisma.join(scoreId)})
      ${source ? Prisma.sql`AND source = ${source}` : Prisma.empty}
      AND data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    ORDER BY updated_at DESC
  `);
  return rows.map((row) =>
    convertClickhouseScoreToDomain(toScoreRecordReadType(row)),
  );
};

/**
 * Accepts a score in a Clickhouse-ready format.
 * id, project_id, name, and timestamp must always be provided.
 */
export const upsertScore = async (score: Partial<ScoreRecordReadType>) => {
  if (!["id", "project_id", "name", "timestamp"].every((key) => key in score)) {
    throw new Error("Identifier fields must be provided to upsert Score.");
  }
  const timestamp = parseDateInput(score.timestamp);
  const dataType = toScoreDataTypeEnum(score.data_type);

  const createdAt = score.created_at
    ? parseDateInput(score.created_at)
    : timestamp;
  const updatedAt = score.updated_at
    ? parseDateInput(score.updated_at)
    : timestamp;
  await prisma.$executeRaw`
    DELETE FROM scores
    WHERE project_id = ${score.project_id as string}
      AND id = ${score.id as string}
  `;
  await prisma.$executeRaw`
    INSERT INTO scores (
      id, project_id, trace_id, observation_id, config_id, environment, name, source,
      value, string_value, data_type, comment, author_user_id, queue_id, metadata,
      timestamp, created_at, updated_at, event_ts, is_deleted
    ) VALUES (
      ${score.id as string},
      ${score.project_id as string},
      ${score.trace_id ?? score.id ?? ""},
      ${score.observation_id ?? null},
      ${score.config_id ?? null},
      ${score.environment ?? "default"},
      ${score.name ?? ""},
      ${(score.source ?? "API") as any},
      ${score.value ?? null},
      ${score.string_value ?? null},
      ${dataType}::score_data_type,
      ${score.comment ?? null},
      ${score.author_user_id ?? null},
      ${score.queue_id ?? null},
      ${(score.metadata ?? {}) as any},
      ${timestamp},
      ${createdAt},
      ${updatedAt},
      ${updatedAt},
      ${false}
    )
  `;
};

export type GetScoresForTracesProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  traceIds: string[];
  timestamp?: Date;
  limit?: number;
  offset?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
  preferredClickhouseService?: PreferredClickhouseService;
};

type GetScoresForSessionsProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  sessionIds: string[];
  limit?: number;
  offset?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
};

type GetScoresForDatasetRunsProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  runIds: string[];
  limit?: number;
  offset?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
};

export const getScoresForSessions = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForSessionsProps<ExcludeMetadata, IncludeHasMetadata>,
) => {
  const {
    projectId,
    sessionIds,
    limit,
    offset,
    clickhouseConfigs: _clickhouseConfigs,
    excludeMetadata = false,
    includeHasMetadata = false,
  } = props;

  const rowsRaw = await prisma.$queryRaw<
    Array<Record<string, unknown>>
  >(Prisma.sql`
    WITH ranked AS (
      SELECT
        s.*,
        CASE WHEN jsonb_object_length(COALESCE(s.metadata, '{}'::jsonb)) > 0 THEN 1 ELSE 0 END AS has_metadata,
        ROW_NUMBER() OVER (PARTITION BY s.id, s.project_id ORDER BY s.event_ts DESC) AS rn
      FROM scores s
      WHERE s.project_id = ${projectId}
        AND s.session_id IN (${Prisma.join(sessionIds)})
        AND s.data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    )
    SELECT *
    FROM ranked
    WHERE rn = 1
    ORDER BY event_ts DESC
    ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
    ${offset !== undefined ? Prisma.sql`OFFSET ${offset}` : Prisma.empty}
  `);

  const includeMetadataPayload = excludeMetadata ? false : true;
  return rowsRaw.map((row) => {
    const mapped = toScoreRecordReadType(row as any);
    const score = convertClickhouseScoreToDomain(
      {
        ...mapped,
        metadata: excludeMetadata ? {} : mapped.metadata,
      },
      includeMetadataPayload,
    );
    if (includeHasMetadata) {
      Object.assign(score, {
        hasMetadata: !!(row.has_metadata as number | undefined),
      });
    }
    return score;
  });
};

export const getScoresForDatasetRuns = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForDatasetRunsProps<ExcludeMetadata, IncludeHasMetadata>,
) => {
  const {
    projectId,
    runIds,
    limit,
    offset,
    clickhouseConfigs: _clickhouseConfigs,
    excludeMetadata = false,
    includeHasMetadata = false,
  } = props;

  const rowsRaw = await prisma.$queryRaw<
    Array<Record<string, unknown>>
  >(Prisma.sql`
    WITH ranked AS (
      SELECT
        s.*,
        CASE WHEN jsonb_object_length(COALESCE(s.metadata, '{}'::jsonb)) > 0 THEN 1 ELSE 0 END AS has_metadata,
        ROW_NUMBER() OVER (PARTITION BY s.id, s.project_id ORDER BY s.event_ts DESC) AS rn
      FROM scores s
      WHERE s.project_id = ${projectId}
        AND s.dataset_run_id IN (${Prisma.join(runIds)})
        AND s.data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    )
    SELECT *
    FROM ranked
    WHERE rn = 1
    ORDER BY event_ts DESC
    ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
    ${offset !== undefined ? Prisma.sql`OFFSET ${offset}` : Prisma.empty}
  `);

  const includeMetadataPayload = excludeMetadata ? false : true;
  return rowsRaw.map((row) => {
    const mapped = toScoreRecordReadType(row as any);
    const score = convertClickhouseScoreToDomain<
      ExcludeMetadata,
      AggregatableScoreDataType
    >(
      {
        ...mapped,
        metadata: excludeMetadata ? {} : mapped.metadata,
      },
      includeMetadataPayload,
    );
    if (includeHasMetadata) {
      Object.assign(score, {
        hasMetadata: !!(row.has_metadata as number | undefined),
      });
    }
    return score;
  });
};

export const getTraceScoresForDatasetRuns = async (
  projectId: string,
  datasetRunIds: string[],
): Promise<Array<{ dataset_run_id: string } & any>> => {
  if (datasetRunIds.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<Record<string, unknown> & { has_metadata: 0 | 1; run_id: string }>
  >(Prisma.sql`
    WITH ranked AS (
      SELECT
        s.*,
        CASE WHEN jsonb_object_length(COALESCE(s.metadata, '{}'::jsonb)) > 0 THEN 1 ELSE 0 END AS has_metadata,
        dri.dataset_run_id as run_id,
        ROW_NUMBER() OVER (
          PARTITION BY s.id, s.project_id, dri.dataset_run_id
          ORDER BY s.event_ts DESC
        ) AS rn
      FROM dataset_run_items dri
      JOIN scores s ON dri.trace_id = s.trace_id AND dri.project_id = s.project_id
      WHERE dri.project_id = ${projectId}
        AND dri.dataset_run_id IN (${Prisma.join(datasetRunIds)})
        AND s.project_id = ${projectId}
        AND s.data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    )
    SELECT *
    FROM ranked
    WHERE rn = 1
    ORDER BY event_ts DESC
  `);

  const includeMetadataPayload = false;
  return rows.map((row) => ({
    ...convertClickhouseScoreToDomain(
      { ...toScoreRecordReadType(row as any), metadata: {} },
      includeMetadataPayload,
    ),
    datasetRunId: row.run_id as string,
    hasMetadata: !!row.has_metadata,
  }));
};

const getScoresForTracesInternal = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
  DataTypes extends readonly ScoreDataTypeType[],
>(
  props: GetScoresForTracesProps<ExcludeMetadata, IncludeHasMetadata> & {
    dataTypes?: DataTypes;
  },
) => {
  const {
    projectId,
    traceIds,
    timestamp,
    dataTypes,
    limit,
    offset,
    clickhouseConfigs: _clickhouseConfigs,
    excludeMetadata = false,
    includeHasMetadata = false,
    preferredClickhouseService: _preferredClickhouseService,
  } = props;

  const tsLowerBound = timestamp
    ? new Date(timestamp.getTime() - 2 * 24 * 60 * 60 * 1000)
    : null;
  const pagination = Prisma.sql`
    ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
    ${offset !== undefined ? Prisma.sql`OFFSET ${offset}` : Prisma.empty}
  `;
  const rowsRaw = await prisma.$queryRaw<any[]>(Prisma.sql`
    SELECT *
    FROM scores
    WHERE project_id = ${projectId}
      AND trace_id IN (${Prisma.join(traceIds)})
      ${dataTypes ? Prisma.sql`AND data_type::text IN (${Prisma.join(dataTypes as unknown as string[])})` : Prisma.empty}
      ${tsLowerBound ? Prisma.sql`AND timestamp >= ${tsLowerBound}` : Prisma.empty}
    ORDER BY updated_at DESC
    ${pagination}
  `);
  const rows = rowsRaw.map((row) => ({
    ...toScoreRecordReadType(row),
    metadata: excludeMetadata
      ? ({} as ExcludeMetadata extends true
          ? never
          : ScoreRecordReadType["metadata"])
      : (toClickhouseMetadataRecord(
          row.metadata,
        ) as ExcludeMetadata extends true
          ? never
          : ScoreRecordReadType["metadata"]),
    has_metadata: (jsonbHasAnyKeys(row.metadata)
      ? 1
      : 0) as unknown as IncludeHasMetadata extends true ? 0 | 1 : never,
  }));

  const includeMetadataPayload = excludeMetadata ? false : true;
  return rows.map((row) => {
    const score = convertClickhouseScoreToDomain(
      {
        ...row,
        metadata: excludeMetadata ? {} : row.metadata,
      },
      includeMetadataPayload,
    );

    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - score.timestamp.getTime(),
      {
        table: "scores",
      },
    );

    if (includeHasMetadata) {
      Object.assign(score, { hasMetadata: !!row.has_metadata });
    }

    return score;
  });
};

// Used in multiple places, including the public API, hence the non-default exclusion of metadata via excludeMetadata flag
export const getScoresForTraces = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForTracesProps<ExcludeMetadata, IncludeHasMetadata>,
) => {
  return getScoresForTracesInternal({
    ...props,
    dataTypes: AGGREGATABLE_SCORE_TYPES,
  });
};

export const getScoresAndCorrectionsForTraces = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForTracesProps<ExcludeMetadata, IncludeHasMetadata>,
) => {
  return getScoresForTracesInternal({
    ...props,
  });
};

export type GetScoresForObservationsProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  observationIds: string[];
  limit?: number;
  offset?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
};

// Currently only used from the observations table, hence the exclusion of metadata without excludeMetadata flag
export const getScoresForObservations = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForObservationsProps<ExcludeMetadata, IncludeHasMetadata>,
) => {
  const {
    projectId,
    observationIds,
    limit,
    offset,
    clickhouseConfigs: _clickhouseConfigs,
    excludeMetadata = false,
    includeHasMetadata = false,
  } = props;

  const rowsRaw = await prisma.$queryRaw<
    Array<
      {
        has_metadata: IncludeHasMetadata extends true ? 0 | 1 : never;
      } & Record<string, unknown>
    >
  >(Prisma.sql`
    SELECT
      s.*,
      ${
        includeHasMetadata
          ? Prisma.sql`CASE WHEN jsonb_object_length(COALESCE(s.metadata, '{}'::jsonb)) > 0 THEN 1 ELSE 0 END`
          : Prisma.sql`0`
      } AS has_metadata
    FROM scores s
    WHERE s.project_id = ${projectId}
      AND s.observation_id IN (${Prisma.join(observationIds)})
      AND s.data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    ORDER BY s.event_ts DESC
    ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
    ${offset !== undefined ? Prisma.sql`OFFSET ${offset}` : Prisma.empty}
  `);

  const rows = rowsRaw.map((row) => ({
    ...toScoreRecordReadType(row as any),
    metadata: (excludeMetadata
      ? {}
      : toClickhouseMetadataRecord(
          (row as any).metadata,
        )) as ExcludeMetadata extends true
      ? never
      : ScoreRecordReadType["metadata"],
    has_metadata: (row.has_metadata ?? 0) as IncludeHasMetadata extends true
      ? 0 | 1
      : never,
  }));

  const includeMetadataPayload = excludeMetadata ? false : true;
  return rows.map((row) => ({
    ...convertClickhouseScoreToDomain(
      {
        ...row,
        metadata: excludeMetadata ? {} : row.metadata,
      },
      includeMetadataPayload,
    ),
    hasMetadata: (includeHasMetadata
      ? !!row.has_metadata
      : undefined) as IncludeHasMetadata extends true ? boolean : never,
  }));
};

export const getScoresGroupedByNameSourceType = async ({
  projectId,
  filter: _filter,
  fromTimestamp,
  toTimestamp,
}: {
  projectId: string;
  filter: FilterCondition[];
  fromTimestamp?: Date;
  toTimestamp?: Date;
}) => {
  const rows = await prisma.$queryRaw<
    {
      name: string;
      source: string;
      data_type: string;
    }[]
  >(Prisma.sql`
    SELECT
      s.name as name,
      s.source as source,
      s.data_type::text as data_type
    FROM scores s
    WHERE s.project_id = ${projectId}
      ${fromTimestamp ? Prisma.sql`AND s.timestamp >= ${fromTimestamp}` : Prisma.empty}
      ${toTimestamp ? Prisma.sql`AND s.timestamp <= ${toTimestamp}` : Prisma.empty}
      AND s.data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    GROUP BY s.name, s.source, s.data_type
    ORDER BY COUNT(*) DESC
    LIMIT 1000
  `);

  return rows.map((row) => ({
    name: row.name,
    source: row.source as ScoreSourceType,
    dataType: row.data_type as AggregatableScoreDataType,
  }));
};

export const getNumericScoresGroupedByName = async (
  projectId: string,
  timestampFilter?: FilterState,
) => {
  try {
    const timeConditions = (timestampFilter ?? [])
      .filter(
        (f) => f.type === "datetime" && normalizeTimestampColumn(f.column),
      )
      .map((f) => {
        if (f.operator === ">=") return Prisma.sql`s.timestamp >= ${f.value}`;
        if (f.operator === ">") return Prisma.sql`s.timestamp > ${f.value}`;
        if (f.operator === "<=") return Prisma.sql`s.timestamp <= ${f.value}`;
        return Prisma.sql`s.timestamp < ${f.value}`;
      });
    return prisma.$queryRaw<Array<{ name: string }>>(Prisma.sql`
      SELECT s.name as name
      FROM scores s
      WHERE s.project_id = ${projectId}
        AND s.data_type::text IN ('NUMERIC','BOOLEAN')
        ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
      GROUP BY s.name
      ORDER BY COUNT(*) DESC
      LIMIT 1000
    `);
  } catch (error) {
    logger.error(
      `getNumericScoresGroupedByName failed; projectId=${projectId}; timestampFilterCount=${timestampFilter?.length ?? 0}; error=${stringifyErrorForMessage(error)}`,
    );
    throw error;
  }
};

export const getCategoricalScoresGroupedByName = async (
  projectId: string,
  timestampFilter?: FilterState,
) => {
  try {
    const timeConditions = (timestampFilter ?? [])
      .filter(
        (f) => f.type === "datetime" && normalizeTimestampColumn(f.column),
      )
      .map((f) => {
        if (f.operator === ">=") return Prisma.sql`s.timestamp >= ${f.value}`;
        if (f.operator === ">") return Prisma.sql`s.timestamp > ${f.value}`;
        if (f.operator === "<=") return Prisma.sql`s.timestamp <= ${f.value}`;
        return Prisma.sql`s.timestamp < ${f.value}`;
      });

    const rows = await prisma.$queryRaw<
      {
        label: string;
        values: string[];
      }[]
    >(Prisma.sql`
      SELECT
        s.name AS label,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.string_value), NULL) AS values
      FROM scores s
      WHERE s.project_id = ${projectId}
        AND s.data_type::text = 'CATEGORICAL'
        ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
      GROUP BY s.name
      ORDER BY COUNT(*) DESC
      LIMIT 1000
    `);

    // Get score names from ClickHouse results to query score configs
    const scoreNames = rows.map((row) => row.label);

    // Query score_configs table for categorical configurations
    const scoreConfigs =
      scoreNames.length > 0
        ? await metadataPrisma.scoreConfig.findMany({
            where: {
              projectId: projectId,
              name: {
                in: scoreNames,
              },
              dataType: "CATEGORICAL",
              isArchived: false,
            },
            select: {
              name: true,
              categories: true,
            },
          })
        : [];

    // Create a map of score configs for easy lookup
    const configMap = new Map(
      scoreConfigs.map((config) => [config.name, config.categories]),
    );

    // Enhance the results with all possible category values from score configs
    return rows.map((row) => {
      const configCategories = configMap.get(row.label);

      if (configCategories && Array.isArray(configCategories)) {
        // Extract all possible category labels from the score config
        const allPossibleValues = (
          configCategories as Array<{ label: string; value: number }>
        ).map((category) => category.label);

        // Merge actual values from ClickHouse with all possible values from config
        // Use Set to ensure uniqueness
        const mergedValues = Array.from(
          new Set([...row.values, ...allPossibleValues]),
        );

        return {
          ...row,
          values: mergedValues,
        };
      }

      // If no config found, return original values
      return row;
    });
  } catch (error) {
    logger.error(
      `getCategoricalScoresGroupedByName failed; projectId=${projectId}; timestampFilterCount=${timestampFilter?.length ?? 0}; error=${stringifyErrorForMessage(error)}`,
    );
    throw error;
  }
};

export const getScoresUiCount = async (props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
}) => {
  const rows = await getScoresUiGeneric<{ count: string }>({
    select: "count",
    excludeMetadata: true,
    tags: { kind: "count" },
    ...props,
  });

  return Number(rows[0].count);
};

export type ScoreUiTableRow = ScoreDomain & {
  traceName: string | null;
  traceUserId: string | null;
  traceTags: Array<string> | null;
};

export async function getScoresUiTable<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadataFlag?: IncludeHasMetadata;
}) {
  const {
    excludeMetadata = false,
    includeHasMetadataFlag = false,
    clickhouseConfigs,
    ...rest
  } = props;

  const rows = await getScoresUiGeneric<{
    id: string;
    project_id: string;
    environment: string;
    name: string;
    value: number;
    string_value: string | null;
    timestamp: string;
    source: string;
    data_type: string;
    comment: string | null;
    trace_id: string | null;
    session_id: string | null;
    dataset_run_id: string | null;
    metadata: ExcludeMetadata extends true ? never : Record<string, string>;
    observation_id: string | null;
    author_user_id: string | null;
    user_id: string | null;
    trace_name: string | null;
    trace_tags: Array<string> | null;
    job_configuration_id: string | null;
    author_user_image: string | null;
    author_user_name: string | null;
    config_id: string | null;
    queue_id: string | null;
    execution_trace_id: string | null;
    is_deleted: number;
    event_ts: string;
    created_at: string;
    updated_at: string;
    // has_metadata is 0 or 1 from ClickHouse, later converted to a boolean
    has_metadata: IncludeHasMetadata extends true ? 0 | 1 : never;
  }>({
    select: "rows",
    tags: { kind: "analytic" },
    excludeMetadata,
    includeHasMetadataFlag,
    clickhouseConfigs,
    ...rest,
  });

  const includeMetadataPayload = excludeMetadata ? false : true;
  return rows.map((row) => {
    const score = convertClickhouseScoreToDomain(
      {
        ...row,
        metadata: excludeMetadata ? {} : row.metadata,
        // Long string value is never required for scores UI table, so we always return an empty string
        long_string_value: "",
      },
      includeMetadataPayload,
    );
    return {
      ...score,
      traceUserId: row.user_id,
      traceName: row.trace_name,
      traceTags: row.trace_tags,
      hasMetadata: (includeHasMetadataFlag
        ? !!row.has_metadata
        : undefined) as IncludeHasMetadata extends true ? boolean : never,
    };
  });
}

const getScoresUiGeneric = async <T>(props: {
  select: "count" | "rows";
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  tags?: Record<string, string>;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: boolean;
  includeHasMetadataFlag?: boolean;
}): Promise<T[]> => {
  const {
    projectId,
    filter,
    orderBy,
    limit,
    offset,
    clickhouseConfigs,
    excludeMetadata = false,
    includeHasMetadataFlag = false,
  } = props;

  const select =
    props.select === "count"
      ? "count(*) as count"
      : `
        s.id,
        s.project_id,
        s.environment,
        s.name,
        s.value,
        s.string_value,
        s.timestamp,
        s.source,
        s.data_type,
        s.comment,
        ${excludeMetadata ? "" : "s.metadata,"}
        s.trace_id,
        s.session_id,
        s.observation_id,
        s.author_user_id,
        t.user_id,
        t.name,
        t.tags,
        s.created_at,
        s.updated_at,
        s.source,
        s.config_id,
        s.queue_id,
        s.execution_trace_id,
        s.is_deleted,
        s.event_ts,
        t.user_id,
        t.name as trace_name,
        t.tags as trace_tags
        ${includeHasMetadataFlag ? ",length(mapKeys(s.metadata)) > 0 AS has_metadata" : ""}
      `;

  const { scoresFilter } = getProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });
  scoresFilter.push(
    ...createFilterFromFilterState(filter, scoresTableUiColumnDefinitions),
  );
  const scoresFilterRes = scoresFilter.apply();

  // Only join traces for rows or if there is a trace filter on counts
  const performTracesJoin =
    props.select === "rows" ||
    scoresFilter.some((f) => f.clickhouseTable === "traces");

  const query = `
      SELECT
          ${select}
      FROM scores s
      ${performTracesJoin ? "LEFT JOIN traces t ON s.trace_id = t.id AND t.project_id = s.project_id" : ""}
      WHERE s.project_id = {projectId: String}
      AND s.data_type IN ({dataTypes: Array(String)})
      ${scoresFilterRes?.query ? `AND ${scoresFilterRes.query}` : ""}
      ${orderByToClickhouseSql(orderBy ?? null, scoresTableUiColumnDefinitions)}
      ${limit !== undefined && offset !== undefined ? `limit {limit: Int32} offset {offset: Int32}` : ""}
    `;

  return measureAndReturn({
    operationName: "getScoresUiGeneric",
    projectId,
    input: {
      params: {
        projectId: projectId,
        dataTypes: AGGREGATABLE_SCORE_TYPES,
        ...(scoresFilterRes ? scoresFilterRes.params : {}),
        limit: limit,
        offset: offset,
      },
      tags: {
        ...(props.tags ?? {}),
        feature: "tracing",
        type: "score",
        projectId,
        select: props.select,
        operation_name: "getScoresUiGeneric",
      },
    },
    fn: async (input) => {
      return queryClickhouse<T>({
        query,
        params: input.params,
        tags: input.tags,
        clickhouseConfigs,
      });
    },
  });
};

/**
 * Trace column mapping for building WHERE filters inside the flat events CTE.
 * References actual events_core columns (trace_name, user_id, tags) with the
 * "e" prefix used by EventsQueryBuilder.
 */
const scoresTraceFilterEventsMapping = [
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    clickhouseTableName: "traces",
    clickhouseSelect: "trace_name",
    queryPrefix: "e",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    clickhouseTableName: "traces",
    clickhouseSelect: "user_id",
    queryPrefix: "e",
  },
  {
    uiTableName: "Trace Tags",
    uiTableId: "trace_tags",
    clickhouseTableName: "traces",
    clickhouseSelect: "tags",
    queryPrefix: "e",
  },
];

/**
 * v4 variant: scores query using a flat events CTE instead of the physical
 * traces table. Trace-level filters and sort use a "traces" CTE built by
 * EventsQueryBuilder, joined as alias "e".
 * Does NOT select trace metadata (that comes via metricsFromEvents).
 */
const getScoresUiGenericFromEvents = async <T>(props: {
  select: "count" | "rows";
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  tags?: Record<string, string>;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
  excludeMetadata?: boolean;
  includeHasMetadataFlag?: boolean;
}): Promise<T[]> => {
  const {
    projectId,
    filter,
    orderBy,
    limit,
    offset,
    clickhouseConfigs,
    excludeMetadata = false,
    includeHasMetadataFlag = false,
  } = props;

  // tracesPrefix value is unused here — only scoresFilter is destructured,
  // and trace-level filtering is handled via the CTE below.
  const { scoresFilter } = getProjectIdDefaultFilter(projectId, {
    tracesPrefix: "t",
  });
  scoresFilter.push(
    ...createFilterFromFilterState(
      filter,
      scoresTableUiColumnDefinitionsFromEvents,
    ),
  );

  const scoreOnlyFilters = scoresFilter.filter(
    (f) => f.clickhouseTable !== "traces",
  );
  const scoreOnlyFilterRes = scoreOnlyFilters.apply();

  // Trace-level filter entries from the frontend filter state
  const traceFilterState = filter.filter((filterEntry) =>
    scoresTraceFilterEventsMapping.some(
      (col) =>
        col.uiTableName === filterEntry.column ||
        col.uiTableId === filterEntry.column,
    ),
  );

  const orderByColumn = orderBy
    ? scoresTableUiColumnDefinitionsFromEvents.find(
        (c) =>
          (c.uiTableName === orderBy.column ||
            c.uiTableId === orderBy.column) &&
          c.clickhouseTableName === "traces",
      )
    : null;

  const needsTracesCTE = traceFilterState.length > 0 || !!orderByColumn;

  // Build traces CTE using flat EventsQueryBuilder when needed
  let tracesCTEClause = "";
  const tracesCTEParams: Record<string, unknown> = {};

  if (needsTracesCTE) {
    const tracesEventsBuilder = eventsTraceMetadata(projectId);

    if (traceFilterState.length > 0) {
      const cteTraceFilters = new FilterList(
        createFilterFromFilterState(
          traceFilterState,
          scoresTraceFilterEventsMapping,
        ),
      );
      const cteTraceFilterRes = cteTraceFilters.apply();
      if (cteTraceFilterRes.query) {
        tracesEventsBuilder.where(cteTraceFilterRes);
      }
    }

    const { query: cteQuery, params: cteParams } =
      tracesEventsBuilder.buildWithParams();
    tracesCTEClause = `WITH traces AS (${cteQuery})`;
    Object.assign(tracesCTEParams, cteParams);
  }

  // Inner join when trace filters are active (exclude scores without matching traces)
  // Left join when only sorting (keep all scores)
  const eventsJoin = needsTracesCTE
    ? traceFilterState.length > 0
      ? `ANY JOIN traces e ON s.trace_id = e.id`
      : `LEFT ANY JOIN traces e ON s.trace_id = e.id`
    : "";

  const select =
    props.select === "count"
      ? "count(*) as count"
      : `
        s.id,
        s.project_id,
        s.environment,
        s.name,
        s.value,
        s.string_value,
        s.timestamp,
        s.source,
        s.data_type,
        s.comment,
        ${excludeMetadata ? "" : "s.metadata,"}
        s.trace_id,
        s.session_id,
        s.observation_id,
        s.author_user_id,
        s.created_at,
        s.updated_at,
        s.config_id,
        s.queue_id,
        s.execution_trace_id,
        s.is_deleted,
        s.event_ts
        ${includeHasMetadataFlag ? ",length(mapKeys(s.metadata)) > 0 AS has_metadata" : ""}
      `;

  const query = `
      ${tracesCTEClause}
      SELECT
          ${select}
      FROM scores s
      ${eventsJoin}
      WHERE s.project_id = {projectId: String}
      AND s.data_type IN ({dataTypes: Array(String)})
      ${scoreOnlyFilterRes?.query ? `AND ${scoreOnlyFilterRes.query}` : ""}
      ${orderByToClickhouseSql(orderBy ?? null, scoresTableUiColumnDefinitionsFromEvents)}
      ${limit !== undefined && offset !== undefined ? `limit {limit: Int32} offset {offset: Int32}` : ""}
    `;

  return measureAndReturn({
    operationName: "getScoresUiGenericFromEvents",
    projectId,
    input: {
      params: {
        projectId,
        dataTypes: AGGREGATABLE_SCORE_TYPES,
        ...(scoreOnlyFilterRes ? scoreOnlyFilterRes.params : {}),
        ...tracesCTEParams,
        limit,
        offset,
      },
      tags: {
        ...(props.tags ?? {}),
        feature: "tracing",
        type: "score",
        projectId,
        select: props.select,
        operation_name: "getScoresUiGenericFromEvents",
      },
    },
    fn: async (input) => {
      return queryClickhouse<T>({
        query,
        params: input.params,
        tags: input.tags,
        clickhouseConfigs,
      });
    },
  });
};

export const getScoresUiCountFromEvents = async (props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
}) => {
  const rows = await getScoresUiGenericFromEvents<{ count: string }>({
    select: "count",
    excludeMetadata: true,
    tags: { kind: "count" },
    ...props,
  });

  return Number(rows[0].count);
};

export type ScoreUiTableRowFromEvents = Omit<ScoreDomain, "metadata"> & {
  hasMetadata: boolean;
};

export async function getScoresUiTableFromEvents(props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions;
}) {
  const { clickhouseConfigs, ...rest } = props;

  const rows = await getScoresUiGenericFromEvents<{
    id: string;
    project_id: string;
    environment: string;
    name: string;
    value: number;
    string_value: string | null;
    timestamp: string;
    source: string;
    data_type: string;
    comment: string | null;
    trace_id: string | null;
    session_id: string | null;
    dataset_run_id: string | null;
    observation_id: string | null;
    author_user_id: string | null;
    config_id: string | null;
    queue_id: string | null;
    execution_trace_id: string | null;
    is_deleted: number;
    event_ts: string;
    created_at: string;
    updated_at: string;
    has_metadata: 0 | 1;
  }>({
    select: "rows",
    tags: { kind: "analytic" },
    excludeMetadata: true,
    includeHasMetadataFlag: true,
    clickhouseConfigs,
    ...rest,
  });

  return rows.map((row) => {
    const score = convertClickhouseScoreToDomain(
      {
        ...row,
        metadata: {},
        long_string_value: "",
      },
      false,
    );
    return {
      ...score,
      hasMetadata: !!row.has_metadata,
    };
  });
}

export const getScoreNames = async (
  projectId: string,
  timestampFilter: FilterState,
) => {
  const chFilter = new FilterList(
    createFilterFromFilterState(
      timestampFilter,
      scoresTableUiColumnDefinitions,
    ),
  );
  const timestampFilterRes = chFilter.apply();

  // We mainly use queries like this to retrieve filter options.
  // This endpoint is filter-options focused; minor count approximation is acceptable.
  const query = `
      select
        name,
        count(*) as count
      from scores s
      WHERE s.project_id = {projectId: String}
      ${timestampFilterRes?.query ? `AND ${timestampFilterRes.query}` : ""}
      AND s.data_type IN ({dataTypes: Array(String)})
      GROUP BY name
      ORDER BY count() desc
      LIMIT 1000;
    `;

  const rows = await queryClickhouse<{
    name: string;
    count: string;
  }>({
    query: query,
    params: {
      projectId: projectId,
      ...(timestampFilterRes ? timestampFilterRes.params : {}),
      dataTypes: AGGREGATABLE_SCORE_TYPES,
    },
    tags: {
      feature: "tracing",
      type: "score",
      kind: "list",
      projectId,
    },
  });

  return rows.map((row) => ({
    name: row.name,
    count: Number(row.count),
  }));
};

export const getScoreStringValues = async (
  projectId: string,
  timestampFilter: FilterState,
) => {
  const chFilter = new FilterList(
    createFilterFromFilterState(
      timestampFilter,
      scoresTableUiColumnDefinitions,
    ),
  );
  const timestampFilterRes = chFilter.apply();

  const query = `
      select
        string_value,
        count(*) as count
      from scores s
      WHERE s.project_id = {projectId: String}
      AND string_value IS NOT NULL
      AND string_value != ''
      ${timestampFilterRes?.query ? `AND ${timestampFilterRes.query}` : ""}
      GROUP BY string_value
      ORDER BY count() desc
      LIMIT 1000;
    `;

  const rows = await queryClickhouse<{
    string_value: string;
    count: string;
  }>({
    query: query,
    params: {
      projectId: projectId,
      ...(timestampFilterRes ? timestampFilterRes.params : {}),
    },
    tags: {
      feature: "tracing",
      type: "score",
      kind: "list",
      projectId,
    },
  });

  return rows.map((row) => ({
    value: row.string_value,
    count: Number(row.count),
  }));
};

export const deleteScores = async (projectId: string, scoreIds: string[]) => {
  if (scoreIds.length === 0) return;
  await prisma.$executeRaw`
    DELETE FROM scores
    WHERE project_id = ${projectId}
      AND id IN (${Prisma.join(scoreIds)})
  `;
};

export const deleteScoresByTraceIds = async (
  projectId: string,
  traceIds: string[],
) => {
  if (traceIds.length === 0) return;
  await prisma.$executeRaw`
    DELETE FROM scores
    WHERE project_id = ${projectId}
      AND trace_id IN (${Prisma.join(traceIds)})
  `;
};

export const deleteScoresByProjectId = async (
  projectId: string,
): Promise<boolean> => {
  const hasData = await hasAnyScore(projectId);
  if (!hasData) {
    return false;
  }

  await prisma.$executeRaw`
    DELETE FROM scores
    WHERE project_id = ${projectId}
  `;

  return true;
};

export const hasAnyScoreOlderThan = async (
  projectId: string,
  beforeDate: Date,
) => {
  const rows = await prisma.$queryRaw<Array<{ one: number }>>(Prisma.sql`
    SELECT 1 as one
    FROM scores
    WHERE project_id = ${projectId}
      AND timestamp < ${beforeDate}
    LIMIT 1
  `);

  return rows.length > 0;
};

export const deleteScoresOlderThanDays = async (
  projectId: string,
  beforeDate: Date,
): Promise<boolean> => {
  const hasData = await hasAnyScoreOlderThan(projectId, beforeDate);
  if (!hasData) {
    return false;
  }

  await prisma.$executeRaw`
    DELETE FROM scores
    WHERE project_id = ${projectId}
      AND timestamp < ${beforeDate}
  `;

  return true;
};

export const getNumericScoreHistogram = async (
  projectId: string,
  filter: FilterState,
  limit: number,
) => {
  const chFilter = new FilterList(
    createFilterFromFilterState(filter, dashboardColumnDefinitions),
  );
  const chFilterRes = chFilter.apply();

  const traceFilter = chFilter.find((f) => f.clickhouseTable === "traces");

  const query = `
    select s.value
    from scores s
    ${traceFilter ? `LEFT JOIN traces t ON s.trace_id = t.id AND t.project_id = s.project_id` : ""}
    WHERE s.project_id = {projectId: String}
    ${traceFilter ? `AND t.project_id = {projectId: String}` : ""}
    ${chFilterRes?.query ? `AND ${chFilterRes.query}` : ""}
    ORDER BY s.event_ts DESC
    LIMIT 1 BY s.id, s.project_id
    ${limit !== undefined ? `limit {limit: Int32}` : ""}
  `;

  return measureAndReturn({
    operationName: "getNumericScoreHistogram",
    projectId,
    input: {
      params: {
        projectId,
        limit,
        ...(chFilterRes ? chFilterRes.params : {}),
      },
      tags: {
        feature: "tracing",
        type: "score",
        kind: "analytic",
        projectId,
        operation_name: "getNumericScoreHistogram",
      },
    },
    fn: async (input) => {
      return queryClickhouse<{ value: number }>({
        query,
        params: input.params,
        tags: input.tags,
      });
    },
  });
};

export const getAggregatedScoresForPrompts = async (
  projectId: string,
  promptIds: string[],
  fetchScoreRelation: "observation" | "trace",
) => {
  const query = `
    SELECT
      prompt_id,
      s.id,
      s.name,
      s.string_value,
      s.value,
      s.source,
      s.data_type,
      s.comment,
      s.timestamp,
      length(mapKeys(s.metadata)) > 0 AS has_metadata
    FROM scores s LEFT JOIN observations o
      ON o.trace_id = s.trace_id
      AND o.project_id = s.project_id
      ${fetchScoreRelation === "observation" ? "AND o.id = s.observation_id" : ""}
    WHERE o.project_id = {projectId: String}
    AND s.project_id = {projectId: String}
    AND o.prompt_id IN ({promptIds: Array(String)})
    AND o.type = 'GENERATION'
    AND s.name IS NOT NULL
    ${fetchScoreRelation === "trace" ? "AND s.observation_id IS NULL" : ""}
    AND s.data_type IN ({dataTypes: Array(String)})
  `;

  const rows = await queryClickhouse<
    ScoreAggregation & {
      prompt_id: string;
      // has_metadata is 0 or 1 from ClickHouse, later converted to a boolean
      has_metadata: 0 | 1;
    }
  >({
    query,
    params: {
      projectId,
      promptIds,
      dataTypes: AGGREGATABLE_SCORE_TYPES,
    },
    tags: {
      feature: "tracing",
      type: "score",
      kind: "analytic",
      projectId,
    },
  });

  return rows.map((row) => ({
    ...convertScoreAggregation<AggregatableScoreDataType>(row),
    promptId: row.prompt_id,
    hasMetadata: !!row.has_metadata,
  }));
};

export const getScoreCountsByProjectInCreationInterval = async ({
  start,
  end,
}: {
  start: Date;
  end: Date;
}) => {
  const rows = await prisma.$queryRaw<
    Array<{ project_id: string; count: bigint }>
  >(
    Prisma.sql`
      SELECT project_id, count(*)::bigint as count
      FROM scores
      WHERE created_at >= ${start}
        AND created_at < ${end}
        AND data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
      GROUP BY project_id
    `,
  );

  return rows.map((row) => ({
    projectId: row.project_id,
    count: Number(row.count),
  }));
};

export const getScoreCountOfProjectsSinceCreationDate = async ({
  projectIds,
  start,
}: {
  projectIds: string[];
  start: Date;
}) => {
  if (projectIds.length === 0) return 0;

  const rows = await prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
    SELECT count(*)::bigint as count
    FROM scores
    WHERE project_id IN (${Prisma.join(projectIds)})
      AND created_at >= ${start}
  `);

  return Number(rows[0]?.count ?? 0n);
};

export const getDistinctScoreNames = async (p: {
  projectId: string;
  cutoffCreatedAt: Date;
  filter: FilterState;
  isTimestampFilter: (filter: FilterCondition) => filter is TimeFilter;
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined;
}) => {
  const {
    projectId,
    cutoffCreatedAt,
    filter,
    isTimestampFilter,
    clickhouseConfigs,
  } = p;
  const scoreTimestampFilter = filter?.find(isTimestampFilter);

  const query = `    SELECT DISTINCT
      name
    FROM scores s
    WHERE s.project_id = {projectId: String}
    AND s.created_at <= {cutoffCreatedAt: DateTime64(3)}
    ${scoreTimestampFilter ? `AND s.timestamp >= {filterTimestamp: DateTime64(3)}` : ""}
    AND s.data_type IN ({dataTypes: Array(String)})
  `;

  const rows = await queryClickhouse<{ name: string }>({
    query,
    params: {
      projectId,
      cutoffCreatedAt: convertDateToClickhouseDateTime(cutoffCreatedAt),
      dataTypes: AGGREGATABLE_SCORE_TYPES,
      ...(scoreTimestampFilter
        ? {
            filterTimestamp: convertDateToClickhouseDateTime(
              scoreTimestampFilter.value,
            ),
          }
        : {}),
    },
    tags: {
      feature: "tracing",
      type: "score",
      kind: "list",
      projectId,
    },
    clickhouseConfigs,
  });

  return rows.map((row) => row.name);
};

export const getScoresForBlobStorageExport = function (
  projectId: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const query = `
    SELECT
      id,
      timestamp,
      project_id,
      environment,
      trace_id,
      observation_id,
      name,
      value,
      source,
      comment,
      data_type,
      string_value
    FROM scores
    WHERE project_id = {projectId: String}
    AND timestamp >= {minTimestamp: DateTime64(3)}
    AND timestamp <= {maxTimestamp: DateTime64(3)}
    AND data_type IN ({dataTypes: Array(String)})
  `;

  const records = queryClickhouseStream<Record<string, unknown>>({
    query,
    params: {
      projectId,
      minTimestamp: convertDateToClickhouseDateTime(minTimestamp),
      maxTimestamp: convertDateToClickhouseDateTime(maxTimestamp),
      dataTypes: AGGREGATABLE_SCORE_TYPES,
    },
    tags: {
      feature: "blobstorage",
      type: "score",
      kind: "analytic",
      projectId,
    },
    clickhouseConfigs: {
      request_timeout: env.LANGFUSE_CLICKHOUSE_DATA_EXPORT_REQUEST_TIMEOUT_MS,
    },
  });

  return records;
};

export const getScoresForAnalyticsIntegrations = async function* (
  projectId: string,
  projectName: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  // Subtract 7d from minTimestamp to account for shift in query
  const traceTable = "traces";

  const query = `    SELECT
      s.id as id,
      s.timestamp as timestamp,
      s.name as name,
      s.value as value,
      s.string_value as string_value,
      s.data_type as data_type,
      s.comment as comment,
      s.environment as environment,
      s.trace_id as score_trace_id,
      s.session_id as score_session_id,
      s.dataset_run_id as score_dataset_run_id,
      t.id as trace_id,
      t.name as trace_name,
      t.session_id as trace_session_id,
      t.user_id as trace_user_id,
      t.release as trace_release,
      t.tags as trace_tags,
      s.metadata as metadata,
      t.metadata['$posthog_session_id'] as posthog_session_id,
      t.metadata['$mixpanel_session_id'] as mixpanel_session_id
    FROM scores s
    LEFT JOIN ${traceTable} t ON s.trace_id = t.id AND s.project_id = t.project_id
    WHERE s.project_id = {projectId: String}
    AND s.timestamp >= {minTimestamp: DateTime64(3)}
    AND s.timestamp <= {maxTimestamp: DateTime64(3)}
    AND s.data_type IN ({dataTypes: Array(String)})
    AND (
      s.trace_id IS NOT NULL
      OR s.session_id IS NOT NULL
      OR s.dataset_run_id IS NOT NULL
    )
    AND (
      t.project_id = '' -- use the default value for the string type to filter for absence
      OR (
        t.project_id = {projectId: String}
        AND t.timestamp >= {minTimestamp: DateTime64(3)} - INTERVAL 7 DAY
        AND t.timestamp <= {maxTimestamp: DateTime64(3)}
      )
    )
  `;

  const records = queryClickhouseStream<Record<string, unknown>>({
    query,
    params: {
      projectId,
      minTimestamp: convertDateToClickhouseDateTime(minTimestamp),
      maxTimestamp: convertDateToClickhouseDateTime(maxTimestamp),
      dataTypes: AGGREGATABLE_SCORE_TYPES,
    },
    tags: {
      feature: "posthog",
      type: "score",
      kind: "analytic",
      projectId,
    },
    clickhouseConfigs: {
      request_timeout: env.LANGFUSE_CLICKHOUSE_DATA_EXPORT_REQUEST_TIMEOUT_MS,
      clickhouse_settings: {
        join_algorithm: "grace_hash",
        grace_hash_join_initial_buckets: "32",
      },
    },
  });

  const baseUrl = env.NEXTAUTH_URL?.replace("/api/auth", "");
  for await (const record of records) {
    // Determine the effective session_id based on score attachment
    const effectiveSessionId =
      record.score_session_id || record.trace_session_id;

    // Determine the effective trace_id (could be null for session-only or dataset-run-only scores)
    const effectiveTraceId = record.score_trace_id || null;

    yield {
      timestamp: record.timestamp,
      langfuse_score_name: record.name,
      langfuse_score_value: record.value,
      langfuse_score_comment: record.comment,
      langfuse_score_metadata: record.metadata,
      langfuse_score_string_value: record.string_value,
      langfuse_score_data_type: record.data_type,
      langfuse_trace_name: record.trace_name,
      langfuse_trace_id: effectiveTraceId,
      langfuse_user_url: record.trace_user_id
        ? `${baseUrl}/project/${projectId}/users/${encodeURIComponent(record.trace_user_id as string)}`
        : undefined,
      langfuse_id: record.id,
      langfuse_session_id: effectiveSessionId,
      langfuse_project_id: projectId,
      langfuse_project_name: projectName,
      langfuse_user_id: record.trace_user_id || null,
      langfuse_release: record.trace_release,
      langfuse_tags: record.trace_tags,
      langfuse_environment: record.environment,
      langfuse_event_version: "1.0.0",
      langfuse_score_entity_type: record.score_trace_id
        ? "trace"
        : record.score_session_id
          ? "session"
          : record.score_dataset_run_id
            ? "dataset_run"
            : "unknown",
      langfuse_dataset_run_id: record.score_dataset_run_id,
      posthog_session_id: record.posthog_session_id ?? null,
      mixpanel_session_id: record.mixpanel_session_id ?? null,
    } satisfies AnalyticsScoreEvent;
  }
};

export const hasAnyScore = async (projectId: string) => {
  const rows = await prisma.$queryRaw<Array<{ one: number }>>(Prisma.sql`
    SELECT 1 as one
    FROM scores
    WHERE project_id = ${projectId}
    LIMIT 1
  `);

  return rows.length > 0;
};

export const getScoreMetadataById = async (
  projectId: string,
  id: string,
  source?: ScoreSourceType,
) => {
  const rows = await prisma.$queryRaw<
    Array<{ metadata: Record<string, unknown> | null }>
  >(
    Prisma.sql`
      SELECT DISTINCT ON (s.id, s.project_id)
        s.metadata
      FROM scores s
      WHERE s.project_id = ${projectId}
        AND s.id = ${id}
        ${source ? Prisma.sql`AND s.source = ${source}` : Prisma.empty}
      ORDER BY s.id, s.project_id, s.event_ts DESC
      LIMIT 1
    `,
  );

  return rows
    .map((row) =>
      parseMetadataCHRecordToDomain(toClickhouseMetadataRecord(row.metadata)),
    )
    .shift();
};

/**
 * Get score counts grouped by project and day within a date range.
 *
 * Returns one row per project per day with the count of scores created on that day.
 * Uses half-open interval [startDate, endDate) for filtering based on timestamp.
 *
 * @param startDate - Start of date range (inclusive)
 * @param endDate - End of date range (exclusive)
 * @returns Array of { count, projectId, date } objects
 *
 * @example
 * // Get score counts for March 1-2, 2024
 * const counts = await getScoreCountsByProjectAndDay({
 *   startDate: new Date('2024-03-01T00:00:00Z'),
 *   endDate: new Date('2024-03-03T00:00:00Z')
 * });
 *
 * Note: Uses non-deduplicating reads for faster and cheaper queries.
 * queries against clickhouse. Generous 4x overcompensation before blocking allows
 * for usage aggregation to be meaningful.
 *
 */
export const getScoreCountsByProjectAndDay = async ({
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
    FROM scores
    WHERE timestamp >= ${startDate}
      AND timestamp < ${endDate}
      AND data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
    GROUP BY project_id, DATE(timestamp)
  `);

  return rows.map((row) => ({
    count: Number(row.count),
    projectId: row.project_id,
    date: row.date,
  }));
};
