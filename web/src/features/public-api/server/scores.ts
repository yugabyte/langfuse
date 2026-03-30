import {
  removeObjectKeys,
  ScoreDataTypeEnum,
  type ScoreDataTypeType,
  type ScoreDomain,
  type FilterState,
  type ScoreSourceType,
} from "@langfuse/shared";
import { Prisma, prisma as tracingPrisma } from "@langfuse/shared/src/db";
import { logger } from "@langfuse/shared/src/server";

/**
 * Converts a ScoreDomain object to API format.
 * For CORRECTION scores, moves longStringValue to stringValue for API compatibility.
 * For other score types, removes longStringValue.
 */
export const convertScoreToPublicApi = <T extends ScoreDomain>(
  score: T,
): Omit<T, "longStringValue"> & { stringValue?: string | null } => {
  if (score.dataType === ScoreDataTypeEnum.CORRECTION) {
    const { longStringValue, ...rest } = score;
    return {
      ...rest,
      stringValue: longStringValue,
    };
  }

  return removeObjectKeys(score, ["longStringValue"]);
};

export type ScoreQueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  name?: string;
  source?: string;
  fromTimestamp?: string;
  toTimestamp?: string;
  value?: number;
  scoreId?: string;
  configId?: string;
  sessionId?: string;
  datasetRunId?: string;
  queueId?: string;
  traceTags?: string | string[];
  operator?: string;
  scoreIds?: string[];
  observationId?: string[];
  dataType?: string;
  environment?: string | string[];
  fields?: string[] | null;
  advancedFilters?: FilterState;
};

type PublicScoreRow = {
  id: string;
  project_id: string;
  timestamp: Date;
  environment: string | null;
  name: string;
  value: number;
  string_value: string | null;
  long_string_value: string | null;
  author_user_id: string | null;
  created_at: Date;
  updated_at: Date;
  source: string;
  comment: string | null;
  metadata: unknown;
  data_type: string;
  config_id: string | null;
  queue_id: string | null;
  execution_trace_id: string | null;
  trace_id: string | null;
  observation_id: string | null;
  session_id: string | null;
  dataset_run_id: string | null;
  user_id?: string | null;
  tags?: string[] | null;
  trace_environment?: string | null;
  trace_session_id?: string | null;
  event_ts?: Date | null;
  is_deleted?: boolean | null;
};

const normalizeStringArray = (value?: string | string[]) => {
  if (!value) return [];
  const values = Array.isArray(value) ? value : [value];
  return values
    .flatMap((entry) => entry.split(","))
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
};

/**
 * Determines if trace join is needed based on fields parameter and trace filters
 */
const determineTraceJoinRequirement = (
  fields: string[] | null | undefined,
  tracesFilterLength: number,
) => {
  const requestedFields = fields ?? ["score", "trace"]; // Default includes both
  const includeTrace = requestedFields.includes("trace");
  const needsTraceJoin = includeTrace || tracesFilterLength > 0;

  return { includeTrace, needsTraceJoin };
};

const buildScoreFilters = ({
  props,
  scoreScope,
  scoreDataTypes,
}: {
  props: ScoreQueryType;
  scoreScope: "traces_only" | "all";
  scoreDataTypes?: readonly ScoreDataTypeType[];
}) => {
  const clauses: Prisma.Sql[] = [
    Prisma.sql`s.project_id = ${props.projectId}`,
    Prisma.sql`s.is_deleted = false`,
  ];

  if (scoreScope === "traces_only") {
    clauses.push(Prisma.sql`s.trace_id IS NOT NULL`);
    clauses.push(Prisma.sql`s.session_id IS NULL`);
    clauses.push(Prisma.sql`s.dataset_run_id IS NULL`);
  }

  if (props.traceId) clauses.push(Prisma.sql`s.trace_id = ${props.traceId}`);
  if (props.name) clauses.push(Prisma.sql`s.name = ${props.name}`);
  if (props.source) clauses.push(Prisma.sql`s.source = ${props.source}`);
  if (props.fromTimestamp)
    clauses.push(Prisma.sql`s.timestamp >= ${new Date(props.fromTimestamp)}`);
  if (props.toTimestamp)
    clauses.push(Prisma.sql`s.timestamp < ${new Date(props.toTimestamp)}`);
  if (props.configId) clauses.push(Prisma.sql`s.config_id = ${props.configId}`);
  if (props.sessionId)
    clauses.push(Prisma.sql`s.session_id = ${props.sessionId}`);
  if (props.datasetRunId)
    clauses.push(Prisma.sql`s.dataset_run_id = ${props.datasetRunId}`);
  if (props.queueId) clauses.push(Prisma.sql`s.queue_id = ${props.queueId}`);
  if (props.scoreId) clauses.push(Prisma.sql`s.id = ${props.scoreId}`);
  if (props.dataType)
    clauses.push(Prisma.sql`s.data_type::text = ${props.dataType}`);

  if (props.scoreIds && props.scoreIds.length > 0) {
    clauses.push(Prisma.sql`s.id IN (${Prisma.join(props.scoreIds)})`);
  }
  if (props.observationId && props.observationId.length > 0) {
    clauses.push(
      Prisma.sql`s.observation_id IN (${Prisma.join(props.observationId)})`,
    );
  }

  const environments = normalizeStringArray(props.environment);
  if (environments.length === 1) {
    clauses.push(Prisma.sql`s.environment = ${environments[0]}`);
  } else if (environments.length > 1) {
    clauses.push(Prisma.sql`s.environment IN (${Prisma.join(environments)})`);
  }

  if (props.value !== undefined) {
    const op = props.operator ?? "=";
    if (op === ">") clauses.push(Prisma.sql`s.value > ${props.value}`);
    else if (op === ">=") clauses.push(Prisma.sql`s.value >= ${props.value}`);
    else if (op === "<") clauses.push(Prisma.sql`s.value < ${props.value}`);
    else if (op === "<=") clauses.push(Prisma.sql`s.value <= ${props.value}`);
    else if (op === "!=" || op === "<>")
      clauses.push(Prisma.sql`s.value <> ${props.value}`);
    else clauses.push(Prisma.sql`s.value = ${props.value}`);
  }

  if (scoreDataTypes && scoreDataTypes.length > 0) {
    clauses.push(
      Prisma.sql`s.data_type::text IN (${Prisma.join(scoreDataTypes as unknown as string[])})`,
    );
  }

  return clauses;
};

const buildTraceFilters = (props: ScoreQueryType) => {
  const clauses: Prisma.Sql[] = [];
  if (props.userId) clauses.push(Prisma.sql`t.user_id = ${props.userId}`);

  const traceTags = normalizeStringArray(props.traceTags);
  if (traceTags.length > 0) clauses.push(Prisma.sql`t.tags && ${traceTags}`);

  if (props.environment && clauses.length > 0) {
    const envValues = normalizeStringArray(props.environment);
    if (envValues.length === 1) {
      clauses.push(Prisma.sql`t.environment = ${envValues[0]}`);
    } else if (envValues.length > 1) {
      clauses.push(Prisma.sql`t.environment IN (${Prisma.join(envValues)})`);
    }
  }

  return clauses;
};

const toScoreDomain = (row: PublicScoreRow): ScoreDomain => {
  const dataType = row.data_type as ScoreDataTypeType;
  const metadata = (row.metadata ?? {}) as Record<string, unknown>;
  const base = {
    id: row.id,
    projectId: row.project_id,
    timestamp: row.timestamp,
    environment: row.environment ?? "default",
    name: row.name,
    value: Number(row.value ?? 0),
    source: row.source as ScoreSourceType,
    authorUserId: row.author_user_id,
    comment: row.comment,
    metadata,
    configId: row.config_id,
    queueId: row.queue_id,
    executionTraceId: row.execution_trace_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    traceId: row.trace_id,
    sessionId: row.session_id,
    datasetRunId: row.dataset_run_id,
    observationId: row.observation_id,
    longStringValue: row.long_string_value ?? "",
  };

  if (dataType === "NUMERIC") {
    return { ...base, dataType, stringValue: null } as ScoreDomain;
  }
  if (dataType === "CORRECTION") {
    return { ...base, dataType, stringValue: null } as ScoreDomain;
  }
  return {
    ...base,
    dataType,
    stringValue: row.string_value ?? "",
  } as ScoreDomain;
};

export const _handleGenerateScoresForPublicApi = async ({
  props,
  scoreScope,
  scoreDataTypes,
}: {
  props: ScoreQueryType;
  scoreScope: "traces_only" | "all";
  scoreDataTypes?: readonly ScoreDataTypeType[];
}) => {
  if (props.advancedFilters?.length) {
    logger.warn(
      "Public scores advancedFilters are currently ignored in PostgreSQL mode",
      { projectId: props.projectId },
    );
  }

  const scoreFilters = buildScoreFilters({ props, scoreScope, scoreDataTypes });
  const traceFilters = buildTraceFilters(props);
  const { includeTrace, needsTraceJoin } = determineTraceJoinRequirement(
    props.fields,
    traceFilters.length,
  );
  const scoreWhereSql = Prisma.join(scoreFilters, " AND ");
  const traceWhereSql =
    traceFilters.length > 0
      ? Prisma.sql`AND ${Prisma.join(traceFilters, " AND ")}`
      : Prisma.empty;
  const offset = (props.page - 1) * props.limit;

  const rows = await tracingPrisma.$queryRaw<PublicScoreRow[]>(Prisma.sql`
    WITH all_scores AS (
      SELECT
        s.id,
        s.project_id,
        s.timestamp,
        s.environment,
        s.name,
        s.value,
        s.string_value,
        s.long_string_value,
        s.author_user_id,
        s.created_at,
        s.updated_at,
        s.source,
        s.comment,
        s.metadata,
        s.data_type::text AS data_type,
        s.config_id,
        s.queue_id,
        s.execution_trace_id,
        s.trace_id,
        s.observation_id,
        s.session_id,
        s.dataset_run_id,
        s.event_ts,
        s.is_deleted
      FROM clickhouse.scores s
      UNION ALL
      SELECT
        s.id,
        s.project_id,
        s.timestamp,
        NULL::text AS environment,
        s.name,
        s.value,
        s.string_value,
        NULL::text AS long_string_value,
        s.author_user_id,
        s.created_at,
        s.updated_at,
        s.source::text AS source,
        s.comment,
        NULL::jsonb AS metadata,
        s.data_type::text AS data_type,
        s.config_id,
        s.queue_id,
        NULL::text AS execution_trace_id,
        s.trace_id,
        s.observation_id,
        NULL::text AS session_id,
        NULL::text AS dataset_run_id,
        s.updated_at AS event_ts,
        false AS is_deleted
      FROM public.scores s
    ),
    latest_scores AS (
      SELECT DISTINCT ON (s.id, s.project_id)
        s.id,
        s.project_id,
        s.timestamp,
        s.environment,
        s.name,
        s.value,
        s.string_value,
        s.long_string_value,
        s.author_user_id,
        s.created_at,
        s.updated_at,
        s.source,
        s.comment,
        s.metadata,
        s.data_type::text AS data_type,
        s.config_id,
        s.queue_id,
        s.execution_trace_id,
        s.trace_id,
        s.observation_id,
        s.session_id,
        s.dataset_run_id
      FROM all_scores s
      WHERE ${scoreWhereSql}
      ORDER BY s.id, s.project_id, s.event_ts DESC
    )
    SELECT
      ls.*,
      ${needsTraceJoin ? Prisma.sql`t.user_id, t.tags, t.environment as trace_environment, t.session_id as trace_session_id` : Prisma.sql`NULL::text as user_id, NULL::text[] as tags, NULL::text as trace_environment, NULL::text as trace_session_id`}
    FROM latest_scores ls
    ${
      needsTraceJoin
        ? Prisma.sql`
      LEFT JOIN LATERAL (
        SELECT DISTINCT ON (t.id, t.project_id)
          t.id,
          t.project_id,
          t.user_id,
          t.tags,
          t.environment,
          t.session_id
        FROM clickhouse.traces t
        WHERE t.id = ls.trace_id
          AND t.project_id = ls.project_id
          AND t.is_deleted = false
          ${traceWhereSql}
        ORDER BY t.id, t.project_id, t.event_ts DESC
      ) t ON TRUE
    `
        : Prisma.empty
    }
    ${needsTraceJoin ? Prisma.sql`WHERE ls.trace_id IS NULL OR t.id IS NOT NULL` : Prisma.empty}
    ORDER BY ls.timestamp DESC, ls.id ASC
    LIMIT ${props.limit}
    OFFSET ${offset}
  `);

  return rows.map((row) => {
    const apiScore = convertScoreToPublicApi(toScoreDomain(row));
    return {
      ...apiScore,
      trace:
        includeTrace && row.trace_id
          ? {
              userId: row.user_id ?? null,
              tags: row.tags ?? [],
              environment: row.trace_environment ?? null,
              sessionId: row.trace_session_id ?? null,
            }
          : null,
    };
  });
};

export const _handleGetScoresCountForPublicApi = async ({
  props,
  scoreScope,
  scoreDataTypes,
}: {
  props: ScoreQueryType;
  scoreScope: "traces_only" | "all";
  scoreDataTypes?: readonly ScoreDataTypeType[];
}) => {
  if (props.advancedFilters?.length) {
    console.log("props.advancedFilters", props.advancedFilters);
    logger.warn(
      "Public scores advancedFilters are currently ignored in PostgreSQL mode",
      { projectId: props.projectId },
    );
  }

  const scoreFilters = buildScoreFilters({ props, scoreScope, scoreDataTypes });
  const traceFilters = buildTraceFilters(props);
  const { needsTraceJoin } = determineTraceJoinRequirement(
    props.fields,
    traceFilters.length,
  );
  const scoreWhereSql = Prisma.join(scoreFilters, " AND ");
  const traceWhereSql =
    traceFilters.length > 0
      ? Prisma.sql`AND ${Prisma.join(traceFilters, " AND ")}`
      : Prisma.empty;

  const rows = await tracingPrisma.$queryRaw<
    Array<{ count: bigint }>
  >(Prisma.sql`
    WITH all_scores AS (
      SELECT
        s.id,
        s.project_id,
        s.trace_id,
        s.session_id,
        s.dataset_run_id,
        s.name,
        s.source,
        s.timestamp,
        s.config_id,
        s.queue_id,
        s.environment,
        s.value,
        s.data_type::text AS data_type,
        s.observation_id,
        s.event_ts,
        s.is_deleted
      FROM clickhouse.scores s
      UNION ALL
      SELECT
        s.id,
        s.project_id,
        s.trace_id,
        NULL::text AS session_id,
        NULL::text AS dataset_run_id,
        s.name,
        s.source::text AS source,
        s.timestamp,
        s.config_id,
        s.queue_id,
        NULL::text AS environment,
        s.value,
        s.data_type::text AS data_type,
        s.observation_id,
        s.updated_at AS event_ts,
        false AS is_deleted
      FROM public.scores s
    ),
    latest_scores AS (
      SELECT DISTINCT ON (s.id, s.project_id)
        s.id,
        s.project_id,
        s.trace_id
      FROM all_scores s
      WHERE ${scoreWhereSql}
      ORDER BY s.id, s.project_id, s.event_ts DESC
    )
    SELECT count(*)::bigint AS count
    FROM latest_scores ls
    ${
      needsTraceJoin
        ? Prisma.sql`
      LEFT JOIN LATERAL (
        SELECT DISTINCT ON (t.id, t.project_id)
          t.id,
          t.project_id
        FROM clickhouse.traces t
        WHERE t.id = ls.trace_id
          AND t.project_id = ls.project_id
          AND t.is_deleted = false
          ${traceWhereSql}
        ORDER BY t.id, t.project_id, t.event_ts DESC
      ) t ON TRUE
    `
        : Prisma.empty
    }
    ${needsTraceJoin ? Prisma.sql`WHERE ls.trace_id IS NULL OR t.id IS NOT NULL` : Prisma.empty}
  `);

  return Number(rows[0]?.count ?? 0n);
};

export const _handleGetScoreByIdForPublicApi = async ({
  projectId,
  scoreId,
  source,
  scoreScope,
  scoreDataTypes,
}: {
  projectId: string;
  scoreId: string;
  source?: ScoreSourceType;
  scoreScope: "traces_only" | "all";
  scoreDataTypes?: readonly ScoreDataTypeType[];
}) => {
  const scoreFilters = buildScoreFilters({
    props: { projectId, scoreId, page: 1, limit: 1, source },
    scoreScope,
    scoreDataTypes,
  });
  const scoreWhereSql = Prisma.join(scoreFilters, " AND ");

  const rows = await tracingPrisma.$queryRaw<PublicScoreRow[]>(Prisma.sql`
    WITH all_scores AS (
      SELECT
        s.id,
        s.project_id,
        s.timestamp,
        s.environment,
        s.name,
        s.value,
        s.string_value,
        s.long_string_value,
        s.author_user_id,
        s.created_at,
        s.updated_at,
        s.source,
        s.comment,
        s.metadata,
        s.data_type::text AS data_type,
        s.config_id,
        s.queue_id,
        s.execution_trace_id,
        s.trace_id,
        s.observation_id,
        s.session_id,
        s.dataset_run_id,
        s.event_ts,
        s.is_deleted
      FROM clickhouse.scores s
      UNION ALL
      SELECT
        s.id,
        s.project_id,
        s.timestamp,
        NULL::text AS environment,
        s.name,
        s.value,
        s.string_value,
        NULL::text AS long_string_value,
        s.author_user_id,
        s.created_at,
        s.updated_at,
        s.source::text AS source,
        s.comment,
        NULL::jsonb AS metadata,
        s.data_type::text AS data_type,
        s.config_id,
        s.queue_id,
        NULL::text AS execution_trace_id,
        s.trace_id,
        s.observation_id,
        NULL::text AS session_id,
        NULL::text AS dataset_run_id,
        s.updated_at AS event_ts,
        false AS is_deleted
      FROM public.scores s
    )
    SELECT DISTINCT ON (s.id, s.project_id)
      s.id,
      s.project_id,
      s.timestamp,
      s.environment,
      s.name,
      s.value,
      s.string_value,
      s.long_string_value,
      s.author_user_id,
      s.created_at,
      s.updated_at,
      s.source,
      s.comment,
      s.metadata,
      s.data_type::text AS data_type,
      s.config_id,
      s.queue_id,
      s.execution_trace_id,
      s.trace_id,
      s.observation_id,
      s.session_id,
      s.dataset_run_id
    FROM all_scores s
    WHERE ${scoreWhereSql}
    ORDER BY s.id, s.project_id, s.event_ts DESC
    LIMIT 1
  `);

  const row = rows[0];
  if (!row) return undefined;
  return toScoreDomain(row);
};
