import { OrderByState } from "../../interfaces/orderBy";
import { FilterState } from "../../types";
import { TraceRecordReadType } from "../repositories/definitions";
import Decimal from "decimal.js";
import { ScoreAggregate } from "../../features/scores";
import { parseClickhouseUTCDateTimeFormat } from "../repositories";
import { TracingSearchType } from "../../interfaces/search";
import { ObservationLevelType, TraceDomain } from "../../domain";
import { ClickHouseClientConfigOptions } from "@clickhouse/client";
import { tracingPrisma as prisma } from "../../db";
import { Prisma } from "@prisma/client";

export type TracesTableReturnType = Pick<
  TraceRecordReadType,
  | "project_id"
  | "id"
  | "name"
  | "timestamp"
  | "bookmarked"
  | "release"
  | "version"
  | "user_id"
  | "session_id"
  | "environment"
  | "tags"
  | "public"
>;

export type TracesTableUiReturnType = Pick<
  TraceDomain,
  | "id"
  | "projectId"
  | "timestamp"
  | "tags"
  | "bookmarked"
  | "name"
  | "release"
  | "version"
  | "userId"
  | "environment"
  | "sessionId"
  | "public"
>;

export type TracesMetricsUiReturnType = {
  id: string;
  projectId: string;
  promptTokens: bigint;
  completionTokens: bigint;
  totalTokens: bigint;
  latency: number | null;
  level: ObservationLevelType;
  observationCount: bigint;
  calculatedTotalCost: Decimal | null;
  calculatedInputCost: Decimal | null;
  calculatedOutputCost: Decimal | null;
  scores: ScoreAggregate;
  usageDetails: Record<string, number>;
  costDetails: Record<string, number>;
  errorCount: bigint;
  warningCount: bigint;
  defaultCount: bigint;
  debugCount: bigint;
};

export const convertToUiTableRows = (
  row: TracesTableReturnType,
): TracesTableUiReturnType => {
  return {
    id: row.id,
    projectId: row.project_id,
    timestamp: parseClickhouseUTCDateTimeFormat(row.timestamp),
    tags: row.tags,
    bookmarked: row.bookmarked,
    name: row.name ?? null,
    release: row.release ?? null,
    version: row.version ?? null,
    userId: row.user_id ?? null,
    environment: row.environment ?? null,
    sessionId: row.session_id ?? null,
    public: row.public,
  };
};

export const convertToUITableMetrics = (
  row: TracesTableMetricsClickhouseReturnType,
): Omit<TracesMetricsUiReturnType, "scores"> => {
  const usageDetails = row.usage_details ?? {};

  return {
    id: row.id,
    projectId: row.project_id,
    // UI formatIntervalSeconds expects seconds; query path currently computes milliseconds.
    latency: row.latency === null ? null : Number(row.latency) / 1000,
    promptTokens: BigInt(usageDetails.input ?? 0),
    completionTokens: BigInt(usageDetails.output ?? 0),
    totalTokens: BigInt(usageDetails.total ?? 0),
    usageDetails: Object.fromEntries(
      Object.entries(row.usage_details).map(([key, value]) => [
        key,
        Number(value),
      ]),
    ),
    costDetails: Object.fromEntries(
      Object.entries(row.cost_details).map(([key, value]) => [
        key,
        Number(value),
      ]),
    ),
    observationCount: BigInt(row.observation_count ?? 0),
    calculatedTotalCost:
      row.cost_details?.total !== undefined
        ? new Decimal(row.cost_details.total)
        : new Decimal(0),
    calculatedInputCost:
      row.cost_details?.input !== undefined
        ? new Decimal(row.cost_details.input)
        : new Decimal(0),
    calculatedOutputCost:
      row.cost_details?.output !== undefined
        ? new Decimal(row.cost_details.output)
        : new Decimal(0),
    level: row.level,
    debugCount: BigInt(row.debug_count ?? 0),
    warningCount: BigInt(row.warning_count ?? 0),
    errorCount: BigInt(row.error_count ?? 0),
    defaultCount: BigInt(row.default_count ?? 0),
  };
};

export type TracesTableMetricsClickhouseReturnType = {
  id: string;
  project_id: string;
  timestamp: Date;
  level: ObservationLevelType;
  observation_count: number | null;
  latency: string | null;
  usage_details: Record<string, number>;
  cost_details: Record<string, number>;
  scores_avg: Array<{ name: string; avg_value: number }>;
  error_count: number | null;
  warning_count: number | null;
  default_count: number | null;
  debug_count: number | null;
};

export type FetchTracesTableProps = {
  select: "count" | "rows" | "metrics" | "identifiers";
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined;
  tags?: Record<string, string>;
};

// Define return type mapping for better type safety
type SelectReturnTypeMap = {
  count: { count: string };
  metrics: TracesTableMetricsClickhouseReturnType;
  rows: TracesTableReturnType;
  identifiers: { id: string; projectId: string; timestamp: string };
};

// Function overloads for type-safe select-specific returns
async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "count" },
): Promise<Array<SelectReturnTypeMap["count"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "metrics" },
): Promise<Array<SelectReturnTypeMap["metrics"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "rows" },
): Promise<Array<SelectReturnTypeMap["rows"]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps & { select: "identifiers" },
): Promise<Array<SelectReturnTypeMap["identifiers"]>>;

// Implementation with union type for internal use
async function getTracesTableGeneric(
  props: FetchTracesTableProps,
): Promise<Array<SelectReturnTypeMap[keyof SelectReturnTypeMap]>>;

async function getTracesTableGeneric(
  props: FetchTracesTableProps,
): Promise<Array<SelectReturnTypeMap[keyof SelectReturnTypeMap]>> {
  const {
    select,
    projectId,
    filter,
    orderBy,
    limit,
    page,
    searchQuery,
    clickhouseConfigs: _clickhouseConfigs,
  } = props;
  const timestampFilters = filter.filter(
    (f) => f.column === "timestamp" && f.type === "datetime",
  ) as Array<{ operator: string; value: Date }>;
  const timestampWhere: { gte?: Date; gt?: Date; lte?: Date; lt?: Date } = {};
  for (const tf of timestampFilters) {
    if (tf.operator === ">=") timestampWhere.gte = tf.value;
    if (tf.operator === ">") timestampWhere.gt = tf.value;
    if (tf.operator === "<=") timestampWhere.lte = tf.value;
    if (tf.operator === "<") timestampWhere.lt = tf.value;
  }

  const conditions: Prisma.Sql[] = [Prisma.sql`project_id = ${projectId}`];
  if (timestampWhere.gte)
    conditions.push(Prisma.sql`timestamp >= ${timestampWhere.gte}`);
  if (timestampWhere.gt)
    conditions.push(Prisma.sql`timestamp > ${timestampWhere.gt}`);
  if (timestampWhere.lte)
    conditions.push(Prisma.sql`timestamp <= ${timestampWhere.lte}`);
  if (timestampWhere.lt)
    conditions.push(Prisma.sql`timestamp < ${timestampWhere.lt}`);
  if (searchQuery) {
    const pattern = `%${searchQuery}%`;
    conditions.push(Prisma.sql`
      (
        id ILIKE ${pattern}
        OR COALESCE(name,'') ILIKE ${pattern}
        OR COALESCE(user_id,'') ILIKE ${pattern}
        OR COALESCE(session_id,'') ILIKE ${pattern}
      )
    `);
  }
  const sortDirection =
    orderBy?.column === "timestamp" && orderBy.order?.toLowerCase() === "asc"
      ? Prisma.sql`ASC`
      : Prisma.sql`DESC`;

  if (select === "count") {
    const rows = await prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM traces
      WHERE ${Prisma.join(conditions, " AND ")}
    `);
    const count = Number(rows[0]?.count ?? 0n);
    return [{ count: String(count) }];
  }

  const traces = await prisma.$queryRaw<
    Array<{
      id: string;
      project_id: string;
      timestamp: Date;
      tags: string[] | null;
      bookmarked: boolean;
      name: string | null;
      release: string | null;
      version: string | null;
      user_id: string | null;
      environment: string | null;
      session_id: string | null;
      public: boolean;
    }>
  >(Prisma.sql`
    SELECT
      id,
      project_id,
      timestamp,
      tags,
      bookmarked,
      name,
      release,
      version,
      user_id,
      environment,
      session_id,
      public
    FROM traces
    WHERE ${Prisma.join(conditions, " AND ")}
    ORDER BY timestamp ${sortDirection}
    ${limit !== undefined ? Prisma.sql`LIMIT ${limit}` : Prisma.empty}
    ${page !== undefined && limit !== undefined ? Prisma.sql`OFFSET ${page * limit}` : Prisma.empty}
  `);

  if (select === "rows") {
    return traces.map((t) => ({
      id: t.id,
      project_id: t.project_id,
      timestamp: t.timestamp.toISOString().replace("T", " ").replace("Z", ""),
      tags: t.tags ?? [],
      bookmarked: t.bookmarked,
      name: t.name,
      release: t.release,
      version: t.version,
      user_id: t.user_id,
      environment: t.environment ?? "default",
      session_id: t.session_id,
      public: t.public,
    }));
  }

  if (select === "identifiers") {
    return traces.map((t) => ({
      id: t.id,
      projectId: t.project_id,
      timestamp: t.timestamp.toISOString().replace("T", " ").replace("Z", ""),
    }));
  }

  const traceIds = traces.map((t) => t.id);
  const observations = traceIds.length
    ? await prisma.$queryRaw<
        Array<{
          trace_id: string;
          level: ObservationLevelType;
          usage_details: Record<string, unknown> | null;
          cost_details: Record<string, unknown> | null;
          total_cost: number | null;
          start_time: Date;
          end_time: Date | null;
        }>
      >(Prisma.sql`
        SELECT trace_id, level::text AS level, usage_details, cost_details, total_cost, start_time, end_time
        FROM observations
        WHERE project_id = ${projectId}
          AND trace_id IN (${Prisma.join(traceIds)})
      `)
    : [];
  const scores = traceIds.length
    ? await prisma.$queryRaw<
        Array<{ trace_id: string; name: string; value: number | null }>
      >(Prisma.sql`
        SELECT trace_id, name, value
        FROM scores
        WHERE project_id = ${projectId}
          AND trace_id IN (${Prisma.join(traceIds)})
      `)
    : [];

  const byTraceObs = new Map<string, typeof observations>();
  for (const o of observations) {
    const key = o.trace_id ?? "";
    byTraceObs.set(key, [...(byTraceObs.get(key) ?? []), o]);
  }
  const byTraceScores = new Map<string, typeof scores>();
  for (const s of scores) {
    const key = s.trace_id ?? "";
    byTraceScores.set(key, [...(byTraceScores.get(key) ?? []), s]);
  }

  return traces.map((t) => {
    const obs = byTraceObs.get(t.id) ?? [];
    const scoreRows = byTraceScores.get(t.id) ?? [];
    const level: ObservationLevelType = obs.some((o) => o.level === "ERROR")
      ? "ERROR"
      : obs.some((o) => o.level === "WARNING")
        ? "WARNING"
        : obs.some((o) => o.level === "DEFAULT")
          ? "DEFAULT"
          : "DEBUG";
    return {
      id: t.id,
      project_id: t.project_id,
      timestamp: t.timestamp,
      level,
      observation_count: obs.length,
      latency:
        obs.length > 0
          ? String(
              Math.max(
                0,
                ...obs.map(
                  (o) =>
                    (o.end_time ?? o.start_time).getTime() -
                    o.start_time.getTime(),
                ),
              ),
            )
          : "0",
      usage_details: obs.reduce<Record<string, number>>(
        (acc, o) => {
          const usage = (o.usage_details ?? {}) as Record<string, unknown>;
          for (const [k, v] of Object.entries(usage)) {
            const n = Number(v ?? 0);
            if (Number.isFinite(n)) acc[k] = (acc[k] ?? 0) + n;
          }
          return acc;
        },
        { input: 0, output: 0, total: 0 },
      ),
      cost_details: obs.reduce<Record<string, number>>(
        (acc, o) => {
          const cost = (o.cost_details ?? {}) as Record<string, unknown>;
          const totalCost = Number(o.total_cost ?? NaN);
          const detailTotal = Number(cost.total ?? NaN);
          const resolvedTotal = Number.isFinite(totalCost)
            ? totalCost
            : Number.isFinite(detailTotal)
              ? detailTotal
              : 0;
          acc.total += resolvedTotal;

          for (const [k, v] of Object.entries(cost)) {
            if (k === "total") continue;
            const n = Number(v ?? 0);
            if (Number.isFinite(n)) acc[k] = (acc[k] ?? 0) + n;
          }
          return acc;
        },
        { input: 0, output: 0, total: 0 },
      ),
      scores_avg: Object.values(
        scoreRows.reduce<
          Record<string, { name: string; sum: number; count: number }>
        >((acc, s) => {
          const key = s.name;
          const next = acc[key] ?? { name: s.name, sum: 0, count: 0 };
          next.sum += Number(s.value ?? 0);
          next.count += 1;
          acc[key] = next;
          return acc;
        }, {}),
      ).map((s) => ({
        name: s.name,
        avg_value: s.count > 0 ? s.sum / s.count : 0,
      })),
      error_count: obs.filter((o) => o.level === "ERROR").length,
      warning_count: obs.filter((o) => o.level === "WARNING").length,
      default_count: obs.filter((o) => o.level === "DEFAULT").length,
      debug_count: obs.filter((o) => o.level === "DEBUG").length,
    };
  });
}

export const getTracesTableCount = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
}) => {
  const countRows = await getTracesTableGeneric({
    select: "count",
    tags: { kind: "count" },
    ...props,
  });

  const converted = countRows.map((row) => ({
    count: Number(row.count),
  }));

  return converted.length > 0 ? converted[0].count : 0;
};

export const getTracesTableMetrics = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined;
}): Promise<Array<Omit<TracesMetricsUiReturnType, "scores">>> => {
  const countRows = await getTracesTableGeneric({
    select: "metrics",
    tags: { kind: "analytic" },
    ...props,
  });

  return countRows.map(convertToUITableMetrics);
};

export const getTracesTable = async (p: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined;
}) => {
  const {
    projectId,
    filter,
    searchQuery,
    searchType,
    orderBy,
    limit,
    page,
    clickhouseConfigs,
  } = p;
  const rows = await getTracesTableGeneric({
    select: "rows",
    tags: { kind: "list" },
    projectId,
    filter,
    searchQuery,
    searchType,
    orderBy,
    limit,
    page,
    clickhouseConfigs,
  });

  return rows.map(convertToUiTableRows);
};

export const getTraceIdentifiers = async (props: {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined;
}) => {
  const {
    projectId,
    filter,
    searchQuery,
    searchType,
    orderBy,
    limit,
    page,
    clickhouseConfigs,
  } = props;
  const identifiers = await getTracesTableGeneric({
    select: "identifiers",
    tags: { kind: "list" },
    projectId,
    filter,
    searchQuery,
    searchType,
    orderBy,
    limit,
    page,
    clickhouseConfigs,
  });

  return identifiers.map((row) => ({
    id: row.id,
    projectId: row.projectId,
    timestamp: parseClickhouseUTCDateTimeFormat(row.timestamp),
  }));
};
