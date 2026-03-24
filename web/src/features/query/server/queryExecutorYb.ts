import { InvalidRequestError } from "@langfuse/shared";
import { Prisma, prisma as tracingPrisma } from "@langfuse/shared/src/db";
import type { QueryType, ViewVersion } from "@/src/features/query/types";

type ViewName = QueryType["view"];

const DEFAULT_ROW_LIMIT = 100;
const MAX_ROW_LIMIT = 1000;

const normalizeGranularity = (
  granularity: QueryType["timeDimension"] extends infer T
    ? T extends { granularity: infer G }
      ? G
      : never
    : never,
  fromTimestamp: string,
  toTimestamp: string,
): "minute" | "hour" | "day" | "week" | "month" => {
  if (granularity && granularity !== "auto") return granularity;

  const from = new Date(fromTimestamp).getTime();
  const to = new Date(toTimestamp).getTime();
  const hours = (to - from) / (1000 * 60 * 60);
  if (hours < 2) return "minute";
  if (hours < 72) return "hour";
  if (hours < 1440) return "day";
  if (hours < 8760) return "week";
  return "month";
};

const granularitySql = (
  timeSql: string,
  g: ReturnType<typeof normalizeGranularity>,
) => {
  if (g === "minute") return `date_trunc('minute', ${timeSql})`;
  if (g === "hour") return `date_trunc('hour', ${timeSql})`;
  if (g === "day") return `date_trunc('day', ${timeSql})`;
  if (g === "week") return `date_trunc('week', ${timeSql})`;
  return `date_trunc('month', ${timeSql})`;
};

type QueryConfig = {
  baseCte: Prisma.Sql;
  timeColumn: string;
  dimensionSql: Record<string, string>;
  measureSql: Record<string, string>;
  metadataSql?: string;
};

const buildConfig = (view: ViewName): QueryConfig => {
  if (view === "traces") {
    return {
      baseCte: Prisma.sql`
        WITH latest_traces AS (
          SELECT DISTINCT ON (t.id, t.project_id)
            t.id,
            t.project_id,
            t.timestamp,
            t.name,
            t.user_id,
            t.session_id,
            t.release,
            t.version,
            t.environment,
            t.tags,
            t.metadata
          FROM clickhouse.traces t
          WHERE t.is_deleted = false
          ORDER BY t.id, t.project_id, t.event_ts DESC
        )
      `,
      timeColumn: "lt.timestamp",
      dimensionSql: {
        id: "lt.id",
        name: "lt.name",
        userId: "lt.user_id",
        sessionId: "lt.session_id",
        release: "lt.release",
        version: "lt.version",
        environment: "lt.environment",
        tags: "lt.tags",
      },
      measureSql: {
        count: "1",
      },
      metadataSql: "lt.metadata",
    };
  }

  if (view === "observations") {
    return {
      baseCte: Prisma.sql`
        WITH latest_traces AS (
          SELECT DISTINCT ON (t.id, t.project_id)
            t.id,
            t.project_id,
            t.user_id,
            t.session_id,
            t.release,
            t.version,
            t.tags,
            t.name AS trace_name
          FROM clickhouse.traces t
          WHERE t.is_deleted = false
          ORDER BY t.id, t.project_id, t.event_ts DESC
        ),
        latest_observations AS (
          SELECT DISTINCT ON (o.id, o.project_id)
            o.id,
            o.project_id,
            o.trace_id,
            o.start_time,
            o.end_time,
            o.name,
            o.type::text AS type,
            o.level::text AS level,
            o.version,
            o.environment,
            o.provided_model_name,
            o.prompt_name,
            o.prompt_version,
            o.metadata,
            o.usage_details,
            o.cost_details,
            o.total_cost
          FROM clickhouse.observations o
          WHERE o.is_deleted = false
          ORDER BY o.id, o.project_id, o.event_ts DESC
        )
      `,
      timeColumn: "lo.start_time",
      dimensionSql: {
        id: "lo.id",
        traceId: "lo.trace_id",
        name: "lo.name",
        type: "lo.type",
        level: "lo.level",
        version: "lo.version",
        environment: "lo.environment",
        providedModelName: "lo.provided_model_name",
        promptName: "lo.prompt_name",
        promptVersion: "lo.prompt_version::text",
        userId: "lt.user_id",
        sessionId: "lt.session_id",
        traceName: "lt.trace_name",
        traceRelease: "lt.release",
        traceVersion: "lt.version",
        tags: "lt.tags",
        release: "lt.release",
      },
      measureSql: {
        count: "1",
        latency:
          "EXTRACT(EPOCH FROM (COALESCE(lo.end_time, lo.start_time) - lo.start_time)) * 1000",
        totalCost: "COALESCE(lo.total_cost::double precision, 0)",
        totalTokens:
          "COALESCE((lo.usage_details->>'total')::double precision, 0)",
      },
      metadataSql: "src.metadata",
    };
  }

  return {
    baseCte: Prisma.sql`
      WITH latest_scores AS (
        SELECT DISTINCT ON (s.id, s.project_id)
          s.id,
          s.project_id,
          s.timestamp,
          s.name,
          s.source,
          s.data_type::text AS data_type,
          s.environment,
          s.value,
          s.string_value,
          s.metadata,
          s.trace_id,
          s.observation_id,
          s.session_id,
          s.dataset_run_id
        FROM clickhouse.scores s
        WHERE s.is_deleted = false
        ORDER BY s.id, s.project_id, s.event_ts DESC
      ),
      latest_traces AS (
        SELECT DISTINCT ON (t.id, t.project_id)
          t.id,
          t.project_id,
          t.user_id,
          t.session_id,
          t.release,
          t.version,
          t.tags,
          t.name AS trace_name
        FROM clickhouse.traces t
        WHERE t.is_deleted = false
        ORDER BY t.id, t.project_id, t.event_ts DESC
      ),
      latest_observations AS (
        SELECT DISTINCT ON (o.id, o.project_id)
          o.id,
          o.project_id,
          o.name,
          o.provided_model_name
        FROM clickhouse.observations o
        WHERE o.is_deleted = false
        ORDER BY o.id, o.project_id, o.event_ts DESC
      )
    `,
    timeColumn: "ls.timestamp",
    dimensionSql: {
      id: "ls.id",
      name: "ls.name",
      source: "ls.source",
      dataType: "ls.data_type",
      environment: "ls.environment",
      traceId: "ls.trace_id",
      observationId: "ls.observation_id",
      sessionId: "ls.session_id",
      datasetRunId: "ls.dataset_run_id",
      stringValue: "ls.string_value",
      userId: "lt.user_id",
      traceName: "lt.trace_name",
      traceRelease: "lt.release",
      traceVersion: "lt.version",
      tags: "lt.tags",
      observationName: "lo.name",
      observationModelName: "lo.provided_model_name",
    },
    measureSql: {
      count: "1",
      value: "COALESCE(ls.value, 0)",
    },
    metadataSql: "ls.metadata",
  };
};

const aggregationSql = (agg: string, valueSql: string, _bins?: number) => {
  if (agg === "count") return `count(${valueSql})`;
  if (agg === "uniq") return `count(DISTINCT ${valueSql})`;
  if (agg === "sum") return `sum(${valueSql})`;
  if (agg === "avg") return `avg(${valueSql})`;
  if (agg === "min") return `min(${valueSql})`;
  if (agg === "max") return `max(${valueSql})`;
  if (agg === "p50")
    return `percentile_cont(0.5) WITHIN GROUP (ORDER BY ${valueSql})`;
  if (agg === "p75")
    return `percentile_cont(0.75) WITHIN GROUP (ORDER BY ${valueSql})`;
  if (agg === "p90")
    return `percentile_cont(0.90) WITHIN GROUP (ORDER BY ${valueSql})`;
  if (agg === "p95")
    return `percentile_cont(0.95) WITHIN GROUP (ORDER BY ${valueSql})`;
  if (agg === "p99")
    return `percentile_cont(0.99) WITHIN GROUP (ORDER BY ${valueSql})`;
  if (agg === "histogram") {
    return `json_build_array(json_build_array(min(${valueSql}), max(${valueSql}), count(${valueSql})))`;
  }
  throw new InvalidRequestError(
    `Aggregation '${agg}' is not supported in YB mode`,
  );
};

const resolveColumnExpression = ({
  filterColumn,
  cfg,
}: {
  filterColumn: string;
  cfg: QueryConfig;
}): string => {
  if (cfg.dimensionSql[filterColumn]) return cfg.dimensionSql[filterColumn];
  if (filterColumn === "metadata" && cfg.metadataSql) return cfg.metadataSql;
  throw new InvalidRequestError(
    `Unsupported filter column '${filterColumn}' in YB mode`,
  );
};

const buildFilterSql = ({
  filter,
  cfg,
}: {
  filter: QueryType["filters"][number];
  cfg: QueryConfig;
}): Prisma.Sql => {
  const expr = resolveColumnExpression({ filterColumn: filter.column, cfg });
  const exprSql = Prisma.raw(expr);

  if (filter.type === "string") {
    if (filter.operator === "=")
      return Prisma.sql`${exprSql} = ${filter.value}`;
    if (filter.operator === "contains")
      return Prisma.sql`${exprSql} ILIKE ${`%${filter.value}%`}`;
    if (filter.operator === "does not contain")
      return Prisma.sql`${exprSql} NOT ILIKE ${`%${filter.value}%`}`;
    if (filter.operator === "starts with")
      return Prisma.sql`${exprSql} ILIKE ${`${filter.value}%`}`;
    if (filter.operator === "ends with")
      return Prisma.sql`${exprSql} ILIKE ${`%${filter.value}`}`;
  }

  if (filter.type === "number") {
    if (filter.operator === "=")
      return Prisma.sql`${exprSql} = ${filter.value}`;
    if (filter.operator === ">")
      return Prisma.sql`${exprSql} > ${filter.value}`;
    if (filter.operator === "<")
      return Prisma.sql`${exprSql} < ${filter.value}`;
    if (filter.operator === ">=")
      return Prisma.sql`${exprSql} >= ${filter.value}`;
    if (filter.operator === "<=")
      return Prisma.sql`${exprSql} <= ${filter.value}`;
  }

  if (filter.type === "datetime") {
    if (filter.operator === ">")
      return Prisma.sql`${exprSql} > ${filter.value}`;
    if (filter.operator === "<")
      return Prisma.sql`${exprSql} < ${filter.value}`;
    if (filter.operator === ">=")
      return Prisma.sql`${exprSql} >= ${filter.value}`;
    if (filter.operator === "<=")
      return Prisma.sql`${exprSql} <= ${filter.value}`;
  }

  if (filter.type === "stringOptions") {
    if (filter.operator === "any of")
      return Prisma.sql`${exprSql} IN (${Prisma.join(filter.value)})`;
    if (filter.operator === "none of")
      return Prisma.sql`${exprSql} NOT IN (${Prisma.join(filter.value)})`;
  }

  if (filter.type === "arrayOptions") {
    if (filter.operator === "any of")
      return Prisma.sql`${exprSql} && ${filter.value}`;
    if (filter.operator === "none of")
      return Prisma.sql`NOT (${exprSql} && ${filter.value})`;
    if (filter.operator === "all of")
      return Prisma.sql`${exprSql} @> ${filter.value}`;
  }

  if (filter.type === "stringObject") {
    const keySql = Prisma.sql`(${exprSql} ->> ${filter.key})`;
    if (filter.operator === "=") return Prisma.sql`${keySql} = ${filter.value}`;
    if (filter.operator === "contains")
      return Prisma.sql`${keySql} ILIKE ${`%${filter.value}%`}`;
    if (filter.operator === "does not contain")
      return Prisma.sql`${keySql} NOT ILIKE ${`%${filter.value}%`}`;
    if (filter.operator === "starts with")
      return Prisma.sql`${keySql} ILIKE ${`${filter.value}%`}`;
    if (filter.operator === "ends with")
      return Prisma.sql`${keySql} ILIKE ${`%${filter.value}`}`;
  }

  if (filter.type === "numberObject") {
    const keySql = Prisma.sql`((${exprSql} ->> ${filter.key})::double precision)`;
    if (filter.operator === "=") return Prisma.sql`${keySql} = ${filter.value}`;
    if (filter.operator === ">") return Prisma.sql`${keySql} > ${filter.value}`;
    if (filter.operator === "<") return Prisma.sql`${keySql} < ${filter.value}`;
    if (filter.operator === ">=")
      return Prisma.sql`${keySql} >= ${filter.value}`;
    if (filter.operator === "<=")
      return Prisma.sql`${keySql} <= ${filter.value}`;
  }

  if (filter.type === "boolean") {
    if (filter.operator === "=")
      return Prisma.sql`${exprSql} = ${filter.value}`;
    if (filter.operator === "<>")
      return Prisma.sql`${exprSql} <> ${filter.value}`;
  }

  if (filter.type === "null") {
    if (filter.operator === "is null") return Prisma.sql`${exprSql} IS NULL`;
    if (filter.operator === "is not null")
      return Prisma.sql`${exprSql} IS NOT NULL`;
  }

  throw new InvalidRequestError(
    `Unsupported filter type/operator '${filter.type}/${filter.operator}' in YB mode`,
  );
};

const normalizeJsonValue = (value: unknown): unknown => {
  if (typeof value === "bigint") {
    // Public API responses must be JSON-serializable.
    // Convert bigint counters to number for compatibility with existing response schemas.
    return Number(value);
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeJsonValue(item));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([k, v]) => [
        k,
        normalizeJsonValue(v),
      ]),
    );
  }
  return value;
};

export async function executeQueryYb(
  projectId: string,
  query: QueryType,
  version: ViewVersion = "v1",
): Promise<Array<Record<string, unknown>>> {
  const view = query.view;
  if (version === "v2" && view === "traces") {
    throw new InvalidRequestError("View 'traces' is not supported in v2");
  }

  const cfg = buildConfig(view);
  const where: Prisma.Sql[] = [Prisma.sql`src.project_id = ${projectId}`];
  where.push(Prisma.sql`src_time >= ${new Date(query.fromTimestamp)}`);
  where.push(Prisma.sql`src_time <= ${new Date(query.toTimestamp)}`);
  if (view === "scores-categorical") {
    where.push(Prisma.sql`src.data_type = 'CATEGORICAL'`);
  }
  if (view === "scores-numeric") {
    where.push(Prisma.sql`src.data_type <> 'CATEGORICAL'`);
  }

  const baseFrom =
    view === "traces"
      ? Prisma.sql`FROM latest_traces src`
      : view === "observations"
        ? Prisma.sql`FROM latest_observations src LEFT JOIN latest_traces lt ON lt.id = src.trace_id AND lt.project_id = src.project_id`
        : Prisma.sql`FROM latest_scores src
          LEFT JOIN latest_traces lt ON lt.id = src.trace_id AND lt.project_id = src.project_id
          LEFT JOIN latest_observations lo ON lo.id = src.observation_id AND lo.project_id = src.project_id`;

  const sourceAlias =
    view === "traces" ? "lt" : view === "observations" ? "lo" : "ls";

  const timeExpr = cfg.timeColumn.replace(/^l[ost]\./, "src.");
  const dimSelects: string[] = [];
  const groupBys: string[] = [];
  for (const d of query.dimensions) {
    const expr = cfg.dimensionSql[d.field];
    if (!expr)
      throw new InvalidRequestError(
        `Unsupported dimension '${d.field}' in YB mode`,
      );
    const safeExpr = expr.replace(new RegExp(`^${sourceAlias}\\.`), "src.");
    dimSelects.push(`${safeExpr} AS "${d.field}"`);
    groupBys.push(safeExpr);
  }

  if (query.timeDimension) {
    const g = normalizeGranularity(
      query.timeDimension.granularity,
      query.fromTimestamp,
      query.toTimestamp,
    );
    const timeBucket = granularitySql(timeExpr, g);
    dimSelects.push(`${timeBucket} AS "time_dimension"`);
    groupBys.push(timeBucket);
  }

  const metricSelects: string[] = [];
  const histogramBins =
    query.chartConfig?.bins ??
    (query as QueryType & { config?: { bins?: number } }).config?.bins ??
    10;
  for (const m of query.metrics) {
    const measureExpr = cfg.measureSql[m.measure];
    if (!measureExpr) {
      throw new InvalidRequestError(
        `Unsupported measure '${m.measure}' in YB mode`,
      );
    }
    const safeMeasure = measureExpr.replace(
      new RegExp(`^${sourceAlias}\\.`),
      "src.",
    );
    metricSelects.push(
      `${aggregationSql(m.aggregation, safeMeasure, histogramBins)} AS "${m.aggregation}_${m.measure}"`,
    );
  }

  for (const filter of query.filters ?? []) {
    where.push(buildFilterSql({ filter, cfg }));
  }

  const publicConfig = (
    query as QueryType & { config?: { row_limit?: number } }
  ).config;
  const rowLimit = Math.min(
    query.chartConfig?.row_limit ??
      publicConfig?.row_limit ??
      DEFAULT_ROW_LIMIT,
    MAX_ROW_LIMIT,
  );
  const orderBy =
    query.orderBy && query.orderBy.length > 0
      ? query.orderBy
          .map(
            (o) =>
              `"${o.field}" ${o.direction.toUpperCase() === "DESC" ? "DESC" : "ASC"}`,
          )
          .join(", ")
      : groupBys.length > 0
        ? groupBys.map((_, idx) => `${idx + 1} ASC`).join(", ")
        : "1";

  const groupBySql =
    groupBys.length > 0
      ? Prisma.raw(`GROUP BY ${groupBys.join(", ")}`)
      : Prisma.empty;
  const rows = await tracingPrisma.$queryRaw<
    Array<Record<string, unknown>>
  >(Prisma.sql`
    ${cfg.baseCte}
    SELECT
      ${Prisma.raw([...dimSelects, ...metricSelects].join(",\n      "))}
    ${baseFrom}
    CROSS JOIN LATERAL (SELECT ${Prisma.raw(timeExpr)} AS src_time) ts
    WHERE ${Prisma.join(where, " AND ")}
    ${groupBySql}
    ORDER BY ${Prisma.raw(orderBy)}
    LIMIT ${rowLimit}
  `);

  return rows.map((row) => normalizeJsonValue(row) as Record<string, unknown>);
}
