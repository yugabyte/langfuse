import { logger, convertObservation } from "@langfuse/shared/src/server";
import { Prisma, prisma as tracingPrisma } from "@langfuse/shared/src/db";
import type { FilterState, Observation } from "@langfuse/shared";
import type { ObservationRecordReadType } from "@langfuse/shared/src/server";

type ObservationQueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  level?: string;
  name?: string;
  type?: string;
  parentObservationId?: string;
  fromStartTime?: string;
  toStartTime?: string;
  version?: string;
  environment?: string | string[];
};

type ObservationCountRow = { count: bigint };

type PublicObservationRow = {
  id: string;
  trace_id: string | null;
  project_id: string;
  type: string;
  parent_observation_id: string | null;
  environment: string | null;
  start_time: Date;
  end_time: Date | null;
  name: string | null;
  metadata: unknown;
  level: string | null;
  status_message: string | null;
  version: string | null;
  input: unknown;
  output: unknown;
  provided_model_name: string | null;
  internal_model_id: string | null;
  model_parameters: unknown;
  provided_usage_details: unknown;
  usage_details: unknown;
  provided_cost_details: unknown;
  cost_details: unknown;
  total_cost: number | null;
  completion_start_time: Date | null;
  prompt_id: string | null;
  prompt_name: string | null;
  prompt_version: number | null;
  created_at: Date;
  updated_at: Date;
  event_ts: Date;
  is_deleted: number;
};

const normalizeStringArray = (value?: string | string[]) => {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
};

const toStringifiedJson = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const toNumericRecord = (value: unknown): Record<string, number> => {
  if (!value || typeof value !== "object") return {};
  const entries = Object.entries(value as Record<string, unknown>);
  return Object.fromEntries(
    entries
      .map(([key, val]) => [key, Number(val)] as const)
      .filter(([, val]) => !Number.isNaN(val)),
  );
};

const toMetadataRecord = (value: unknown): Record<string, string> => {
  if (!value || typeof value !== "object") return {};
  const entries = Object.entries(value as Record<string, unknown>);
  return Object.fromEntries(
    entries.map(([key, val]) => {
      if (val === null || val === undefined) return [key, "null"];
      return [
        key,
        typeof val === "string" ? val : JSON.stringify(val),
      ] as const;
    }),
  );
};

const toClickhouseDateTime = (value: Date | null): string | null => {
  if (!value) return null;
  return value.toISOString().replace("T", " ").replace("Z", "");
};

const toObservationRecordReadType = (
  row: PublicObservationRow,
): ObservationRecordReadType => ({
  ...row,
  start_time: toClickhouseDateTime(row.start_time) ?? "1970-01-01 00:00:00.000",
  end_time: toClickhouseDateTime(row.end_time),
  completion_start_time: toClickhouseDateTime(row.completion_start_time),
  created_at: toClickhouseDateTime(row.created_at) ?? "1970-01-01 00:00:00.000",
  updated_at: toClickhouseDateTime(row.updated_at) ?? "1970-01-01 00:00:00.000",
  event_ts: toClickhouseDateTime(row.event_ts) ?? "1970-01-01 00:00:00.000",
  environment: row.environment ?? "default",
  metadata: toMetadataRecord(row.metadata),
  input: toStringifiedJson(row.input),
  output: toStringifiedJson(row.output),
  model_parameters: toStringifiedJson(row.model_parameters),
  provided_usage_details: toNumericRecord(row.provided_usage_details),
  usage_details: toNumericRecord(row.usage_details),
  provided_cost_details: toNumericRecord(row.provided_cost_details),
  cost_details: toNumericRecord(row.cost_details),
});

const buildObservationFilters = (props: ObservationQueryType): Prisma.Sql[] => {
  const clauses: Prisma.Sql[] = [
    Prisma.sql`o.project_id = ${props.projectId}`,
    Prisma.sql`o.is_deleted = false`,
  ];

  if (props.traceId) clauses.push(Prisma.sql`o.trace_id = ${props.traceId}`);
  if (props.level) clauses.push(Prisma.sql`o.level::text = ${props.level}`);
  if (props.name) clauses.push(Prisma.sql`o.name = ${props.name}`);
  if (props.type) clauses.push(Prisma.sql`o.type::text = ${props.type}`);
  if (props.parentObservationId)
    clauses.push(
      Prisma.sql`o.parent_observation_id = ${props.parentObservationId}`,
    );
  if (props.version) clauses.push(Prisma.sql`o.version = ${props.version}`);
  if (props.fromStartTime)
    clauses.push(Prisma.sql`o.start_time >= ${new Date(props.fromStartTime)}`);
  if (props.toStartTime)
    clauses.push(Prisma.sql`o.start_time <= ${new Date(props.toStartTime)}`);

  const environments = normalizeStringArray(props.environment);
  if (environments.length === 1) {
    clauses.push(Prisma.sql`o.environment = ${environments[0]}`);
  } else if (environments.length > 1) {
    clauses.push(Prisma.sql`o.environment IN (${Prisma.join(environments)})`);
  }

  return clauses;
};

const buildTraceFilters = (props: ObservationQueryType): Prisma.Sql[] => {
  const clauses: Prisma.Sql[] = [];
  if (props.userId) clauses.push(Prisma.sql`lt.user_id = ${props.userId}`);
  return clauses;
};

export const getObservationsCountForPublicApiPostgres = async ({
  props,
  advancedFilters,
}: {
  props: ObservationQueryType;
  advancedFilters?: FilterState;
}): Promise<number> => {
  if (advancedFilters?.length) {
    logger.warn(
      "Public observations advancedFilters are currently ignored in PostgreSQL mode",
      { projectId: props.projectId },
    );
  }

  const whereObservationSql = Prisma.join(
    buildObservationFilters(props),
    " AND ",
  );
  const traceFilters = buildTraceFilters(props);
  const traceFilterSql =
    traceFilters.length > 0
      ? Prisma.sql`AND ${Prisma.join(traceFilters, " AND ")}`
      : Prisma.empty;

  const rows = await tracingPrisma.$queryRaw<ObservationCountRow[]>(Prisma.sql`
    WITH latest_traces AS (
      SELECT DISTINCT ON (t.id, t.project_id)
        t.id,
        t.project_id,
        t.user_id
      FROM clickhouse.traces t
      WHERE t.project_id = ${props.projectId}
        AND t.is_deleted = false
      ORDER BY t.id, t.project_id, t.event_ts DESC
    ),
    latest_observations AS (
      SELECT DISTINCT ON (o.id, o.project_id) o.id, o.project_id
      FROM clickhouse.observations o
      LEFT JOIN latest_traces lt
        ON lt.id = o.trace_id
        AND lt.project_id = o.project_id
      WHERE ${whereObservationSql}
      ${traceFilterSql}
      ORDER BY o.id, o.project_id, o.event_ts DESC
    )
    SELECT count(*)::bigint AS count
    FROM latest_observations
  `);

  return Number(rows[0]?.count ?? 0n);
};

export const generateObservationsForPublicApiPostgres = async ({
  props,
  advancedFilters,
}: {
  props: ObservationQueryType;
  advancedFilters?: FilterState;
}): Promise<Observation[]> => {
  if (advancedFilters?.length) {
    logger.warn(
      "Public observations advancedFilters are currently ignored in PostgreSQL mode",
      { projectId: props.projectId },
    );
  }

  const whereObservationSql = Prisma.join(
    buildObservationFilters(props),
    " AND ",
  );
  const traceFilters = buildTraceFilters(props);
  const traceFilterSql =
    traceFilters.length > 0
      ? Prisma.sql`AND ${Prisma.join(traceFilters, " AND ")}`
      : Prisma.empty;
  const offset = (props.page - 1) * props.limit;

  const rows = await tracingPrisma.$queryRaw<PublicObservationRow[]>(Prisma.sql`
    WITH latest_traces AS (
      SELECT DISTINCT ON (t.id, t.project_id)
        t.id,
        t.project_id,
        t.user_id
      FROM clickhouse.traces t
      WHERE t.project_id = ${props.projectId}
        AND t.is_deleted = false
      ORDER BY t.id, t.project_id, t.event_ts DESC
    ),
    latest_observations AS (
      SELECT DISTINCT ON (o.id, o.project_id)
        o.id,
        o.trace_id,
        o.project_id,
        o.type,
        o.parent_observation_id,
        o.environment,
        o.start_time,
        o.end_time,
        o.name,
        o.metadata,
        o.level,
        o.status_message,
        o.version,
        o.input,
        o.output,
        o.provided_model_name,
        o.internal_model_id,
        o.model_parameters,
        o.provided_usage_details,
        o.usage_details,
        o.provided_cost_details,
        o.cost_details,
        o.total_cost,
        o.completion_start_time,
        o.prompt_id,
        o.prompt_name,
        o.prompt_version,
        o.created_at,
        o.updated_at,
        o.event_ts,
        CASE WHEN o.is_deleted THEN 1 ELSE 0 END AS is_deleted
      FROM clickhouse.observations o
      LEFT JOIN latest_traces lt
        ON lt.id = o.trace_id
        AND lt.project_id = o.project_id
      WHERE ${whereObservationSql}
      ${traceFilterSql}
      ORDER BY o.id, o.project_id, o.event_ts DESC
    )
    SELECT *
    FROM latest_observations lo
    ORDER BY lo.start_time DESC, lo.id ASC
    LIMIT ${props.limit}
    OFFSET ${offset}
  `);

  return rows.map((row: PublicObservationRow) =>
    convertObservation(toObservationRecordReadType(row)),
  );
};
