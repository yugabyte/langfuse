import { Prisma, prisma as tracingPrisma } from "@langfuse/shared/src/db";

type QueryType = {
  page: number;
  limit: number;
  projectId: string;
  userId?: string;
  tags?: string | string[];
  traceName?: string;
  traceEnvironment?: string | string[];
  observationEnvironment?: string | string[];
  fromTimestamp?: string;
  toTimestamp?: string;
};

const normalizeStringArray = (value?: string | string[]) => {
  if (!value) return [];
  const values = Array.isArray(value) ? value : [value];
  return values
    .flatMap((entry) => entry.split(","))
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
};

const buildTraceFilters = (props: QueryType) => {
  const clauses: Prisma.Sql[] = [
    Prisma.sql`t.project_id = ${props.projectId}`,
    Prisma.sql`t.is_deleted = false`,
  ];

  if (props.userId) clauses.push(Prisma.sql`t.user_id = ${props.userId}`);
  if (props.traceName) clauses.push(Prisma.sql`t.name = ${props.traceName}`);
  if (props.fromTimestamp)
    clauses.push(Prisma.sql`t.timestamp >= ${new Date(props.fromTimestamp)}`);
  if (props.toTimestamp)
    clauses.push(Prisma.sql`t.timestamp < ${new Date(props.toTimestamp)}`);

  const tags = normalizeStringArray(props.tags);
  if (tags.length > 0) clauses.push(Prisma.sql`t.tags && ${tags}`);

  const traceEnvironments = normalizeStringArray(props.traceEnvironment);
  if (traceEnvironments.length === 1) {
    clauses.push(Prisma.sql`t.environment = ${traceEnvironments[0]}`);
  } else if (traceEnvironments.length > 1) {
    clauses.push(
      Prisma.sql`t.environment IN (${Prisma.join(traceEnvironments)})`,
    );
  }

  return clauses;
};

const buildObservationFilters = (props: QueryType) => {
  const clauses: Prisma.Sql[] = [
    Prisma.sql`o.project_id = ${props.projectId}`,
    Prisma.sql`o.is_deleted = false`,
  ];

  if (props.fromTimestamp)
    clauses.push(Prisma.sql`o.start_time >= ${new Date(props.fromTimestamp)}`);
  if (props.toTimestamp)
    clauses.push(Prisma.sql`o.start_time < ${new Date(props.toTimestamp)}`);

  const observationEnvironments = normalizeStringArray(
    props.observationEnvironment,
  );
  if (observationEnvironments.length === 1) {
    clauses.push(Prisma.sql`o.environment = ${observationEnvironments[0]}`);
  } else if (observationEnvironments.length > 1) {
    clauses.push(
      Prisma.sql`o.environment IN (${Prisma.join(observationEnvironments)})`,
    );
  }

  return clauses;
};

type DailyMetricsRow = {
  date: string;
  count_traces: number;
  count_observations: number;
  total_cost: number;
  usage: unknown;
};

type DailyMetricsCountRow = {
  count: bigint;
};

export const generateDailyMetrics = async (props: QueryType) => {
  const traceWhereSql = Prisma.join(buildTraceFilters(props), " AND ");
  const observationWhereSql = Prisma.join(
    buildObservationFilters(props),
    " AND ",
  );
  const limit = props.limit;
  const offset = (props.page - 1) * props.limit;

  const rows = await tracingPrisma.$queryRaw<DailyMetricsRow[]>(Prisma.sql`
    WITH latest_traces AS (
      SELECT DISTINCT ON (t.id, t.project_id)
        t.id,
        t.project_id,
        t.timestamp
      FROM clickhouse.traces t
      WHERE ${traceWhereSql}
      ORDER BY t.id, t.project_id, t.event_ts DESC
    ),
    latest_observations AS (
      SELECT DISTINCT ON (o.id, o.project_id)
        o.id,
        o.project_id,
        o.trace_id,
        o.start_time,
        o.provided_model_name,
        o.usage_details,
        o.total_cost
      FROM clickhouse.observations o
      WHERE ${observationWhereSql}
      ORDER BY o.id, o.project_id, o.event_ts DESC
    ),
    trace_usage AS (
      SELECT
        to_char(date_trunc('day', lt.timestamp), 'YYYY-MM-DD') AS date,
        count(*)::int AS count_traces
      FROM latest_traces lt
      GROUP BY 1
    ),
    model_usage AS (
      SELECT
        to_char(date_trunc('day', lo.start_time), 'YYYY-MM-DD') AS date,
        lo.provided_model_name AS model,
        count(*)::int AS count_observations,
        count(DISTINCT lo.trace_id)::int AS count_traces,
        COALESCE(sum((lo.usage_details->>'input')::double precision), 0) AS input_usage,
        COALESCE(sum((lo.usage_details->>'output')::double precision), 0) AS output_usage,
        COALESCE(sum((lo.usage_details->>'total')::double precision), 0) AS total_usage,
        COALESCE(sum(lo.total_cost::double precision), 0) AS total_cost
      FROM latest_observations lo
      INNER JOIN latest_traces lt
        ON lt.id = lo.trace_id
        AND lt.project_id = lo.project_id
      GROUP BY 1, 2
    ),
    daily_model_usage AS (
      SELECT
        mu.date,
        sum(mu.count_observations)::int AS count_observations,
        sum(mu.total_cost)::double precision AS total_cost,
        json_agg(
          json_build_object(
            'model', mu.model,
            'inputUsage', mu.input_usage,
            'outputUsage', mu.output_usage,
            'totalUsage', mu.total_usage,
            'totalCost', mu.total_cost,
            'countObservations', mu.count_observations,
            'countTraces', mu.count_traces
          )
          ORDER BY mu.model NULLS FIRST
        ) AS usage
      FROM model_usage mu
      GROUP BY mu.date
    )
    SELECT
      COALESCE(dmu.date, tu.date) AS date,
      COALESCE(tu.count_traces, 0)::int AS count_traces,
      COALESCE(dmu.count_observations, 0)::int AS count_observations,
      COALESCE(dmu.total_cost, 0)::double precision AS total_cost,
      COALESCE(dmu.usage, '[]'::json) AS usage
    FROM daily_model_usage dmu
    FULL OUTER JOIN trace_usage tu ON dmu.date = tu.date
    ORDER BY date DESC
    LIMIT ${limit}
    OFFSET ${offset}
  `);

  return rows.map((row) => ({
    date: row.date,
    countTraces: Number(row.count_traces ?? 0),
    countObservations: Number(row.count_observations ?? 0),
    totalCost: Number(row.total_cost ?? 0),
    usage: Array.isArray(row.usage) ? row.usage : [],
  }));
};

export const getDailyMetricsCount = async (props: QueryType) => {
  const traceWhereSql = Prisma.join(buildTraceFilters(props), " AND ");

  const rows = await tracingPrisma.$queryRaw<DailyMetricsCountRow[]>(Prisma.sql`
    WITH latest_traces AS (
      SELECT DISTINCT ON (t.id, t.project_id)
        t.id,
        t.project_id,
        t.timestamp
      FROM clickhouse.traces t
      WHERE ${traceWhereSql}
      ORDER BY t.id, t.project_id, t.event_ts DESC
    )
    SELECT count(DISTINCT date_trunc('day', lt.timestamp))::bigint AS count
    FROM latest_traces lt
  `);

  return Number(rows[0]?.count ?? 0n);
};
