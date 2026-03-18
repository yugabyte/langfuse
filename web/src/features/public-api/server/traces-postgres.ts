import { logger } from "@langfuse/shared/src/server";
import { tracingPrisma } from "@langfuse/shared/src/db";
import { Prisma } from "@prisma/client";
import type { FilterState } from "@langfuse/shared";
import type { OrderByState } from "@langfuse/shared";
import type { TraceFieldGroup } from "@/src/features/public-api/types/traces";

type TraceQueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  name?: string;
  type?: string;
  sessionId?: string;
  version?: string;
  release?: string;
  tags?: string | string[];
  environment?: string | string[];
  fromTimestamp?: string;
  toTimestamp?: string;
  fields?: TraceFieldGroup[];
  useEventsTable?: boolean | null;
};

type PublicTraceRow = {
  id: string;
  project_id: string;
  timestamp: Date;
  name: string | null;
  user_id: string | null;
  metadata: unknown;
  release: string | null;
  version: string | null;
  environment: string | null;
  public: boolean;
  bookmarked: boolean;
  tags: string[] | null;
  input: string | null;
  output: string | null;
  session_id: string | null;
  created_at: Date;
  updated_at: Date;
  observations: string[] | null;
  scores: string[] | null;
  total_cost: number | null;
  latency_seconds: number | null;
};

const normalizeStringArray = (value?: string | string[]) => {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
};

const parseMaybeJson = (value: unknown) => {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

const getSortColumn = (column?: string | null) => {
  const map: Record<string, string> = {
    id: "lt.id",
    timestamp: "lt.timestamp",
    name: "lt.name",
    userId: "lt.user_id",
    release: "lt.release",
    version: "lt.version",
    public: "lt.public",
    bookmarked: "lt.bookmarked",
    sessionId: "lt.session_id",
  };
  return map[column ?? ""] ?? "lt.timestamp";
};

const getSortDirection = (order?: string | null) =>
  order?.toUpperCase() === "ASC" ? "ASC" : "DESC";

const buildBaseFilters = (props: TraceQueryType) => {
  const clauses: Prisma.Sql[] = [
    Prisma.sql`t.project_id = ${props.projectId}`,
    Prisma.sql`t.is_deleted = false`,
  ];

  if (props.userId) clauses.push(Prisma.sql`t.user_id = ${props.userId}`);
  if (props.name) clauses.push(Prisma.sql`t.name = ${props.name}`);
  if (props.sessionId)
    clauses.push(Prisma.sql`t.session_id = ${props.sessionId}`);
  if (props.version) clauses.push(Prisma.sql`t.version = ${props.version}`);
  if (props.release) clauses.push(Prisma.sql`t.release = ${props.release}`);
  if (props.fromTimestamp)
    clauses.push(Prisma.sql`t.timestamp >= ${new Date(props.fromTimestamp)}`);
  if (props.toTimestamp)
    clauses.push(Prisma.sql`t.timestamp <= ${new Date(props.toTimestamp)}`);

  const tags = normalizeStringArray(props.tags);
  if (tags.length > 0) {
    clauses.push(Prisma.sql`t.tags && ${tags}`);
  }

  const environments = normalizeStringArray(props.environment);
  if (environments.length === 1) {
    clauses.push(Prisma.sql`t.environment = ${environments[0]}`);
  } else if (environments.length > 1) {
    clauses.push(Prisma.sql`t.environment IN (${Prisma.join(environments)})`);
  }

  return clauses;
};

export const getTracesCountForPublicApiPostgres = async ({
  props,
  advancedFilters,
}: {
  props: TraceQueryType;
  advancedFilters?: FilterState;
}) => {
  if (advancedFilters?.length) {
    logger.warn(
      "Public traces advancedFilters are currently ignored in PostgreSQL mode",
      { projectId: props.projectId },
    );
  }

  const whereSql = Prisma.join(buildBaseFilters(props), " AND ");
  const rows = await tracingPrisma.$queryRaw<
    Array<{ count: bigint }>
  >(Prisma.sql`
    WITH latest_traces AS (
      SELECT DISTINCT ON (t.id, t.project_id) t.id, t.project_id
      FROM traces t
      WHERE ${whereSql}
      ORDER BY t.id, t.project_id, t.event_ts DESC
    )
    SELECT count(*)::bigint as count
    FROM latest_traces
  `);

  return Number(rows[0]?.count ?? 0n);
};

export const generateTracesForPublicApiPostgres = async ({
  props,
  advancedFilters,
  orderBy,
}: {
  props: TraceQueryType;
  advancedFilters?: FilterState;
  orderBy: OrderByState;
}) => {
  if (advancedFilters?.length) {
    logger.warn(
      "Public traces advancedFilters are currently ignored in PostgreSQL mode",
      { projectId: props.projectId },
    );
  }

  const whereSql = Prisma.join(buildBaseFilters(props), " AND ");
  const sortColumn = getSortColumn(orderBy?.[0]?.column);
  const sortDirection = getSortDirection(orderBy?.[0]?.order);
  const orderSql = Prisma.raw(
    `${sortColumn} ${sortDirection}, lt.id ASC, lt.project_id ASC`,
  );
  const offset = (props.page - 1) * props.limit;

  const rows = await tracingPrisma.$queryRaw<PublicTraceRow[]>(Prisma.sql`
    WITH latest_traces AS (
      SELECT DISTINCT ON (t.id, t.project_id)
        t.id,
        t.project_id,
        t.timestamp,
        t.name,
        t.user_id,
        t.metadata,
        t.release,
        t.version,
        t.environment,
        t.public,
        t.bookmarked,
        t.tags,
        t.input,
        t.output,
        t.session_id,
        t.created_at,
        t.updated_at
      FROM traces t
      WHERE ${whereSql}
      ORDER BY t.id, t.project_id, t.event_ts DESC
    ),
    observation_stats AS (
      SELECT
        o.trace_id,
        array_agg(o.id) as observations,
        SUM(o.total_cost)::double precision as total_cost,
        EXTRACT(EPOCH FROM (MAX(COALESCE(o.end_time, o.start_time)) - MIN(o.start_time)))::double precision as latency_seconds
      FROM observations o
      INNER JOIN latest_traces lt
        ON lt.id = o.trace_id
        AND lt.project_id = o.project_id
      GROUP BY o.trace_id
    ),
    score_stats AS (
      SELECT
        s.trace_id,
        array_agg(s.id) as scores
      FROM scores s
      INNER JOIN latest_traces lt
        ON lt.id = s.trace_id
        AND lt.project_id = s.project_id
      WHERE s.session_id IS NULL
        AND s.dataset_run_id IS NULL
      GROUP BY s.trace_id
    )
    SELECT
      lt.*,
      os.observations,
      ss.scores,
      os.total_cost,
      os.latency_seconds
    FROM latest_traces lt
    LEFT JOIN observation_stats os ON os.trace_id = lt.id
    LEFT JOIN score_stats ss ON ss.trace_id = lt.id
    ORDER BY ${orderSql}
    LIMIT ${props.limit}
    OFFSET ${offset}
  `);

  return rows.map((row) => ({
    id: row.id,
    projectId: row.project_id,
    name: row.name,
    timestamp: row.timestamp,
    environment: row.environment ?? "default",
    tags: row.tags ?? [],
    bookmarked: row.bookmarked,
    release: row.release,
    version: row.version,
    userId: row.user_id,
    sessionId: row.session_id,
    public: row.public,
    input: parseMaybeJson(row.input),
    output: parseMaybeJson(row.output),
    metadata: row.metadata ?? {},
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    observations: row.observations ?? [],
    scores: row.scores ?? [],
    totalCost: row.total_cost ?? 0,
    latency: row.latency_seconds ?? 0,
    htmlPath: `/project/${row.project_id}/traces/${row.id}`,
  }));
};
