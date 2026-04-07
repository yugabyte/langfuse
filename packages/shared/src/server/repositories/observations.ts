import { parseClickhouseUTCDateTimeFormat } from "./clickhouse";
import { logger } from "../logger";
import { InternalServerError, LangfuseNotFoundError } from "../../errors";
import { tracingPrisma as prisma } from "../../db";
import { ObservationRecordReadType } from "./definitions";
import { FilterState } from "../../types";
import { FullObservations } from "../queries";
import { OrderByState } from "../../interfaces/orderBy";
import { getTracesByIds } from "./traces";
import { PreferredClickhouseService } from "../clickhouse/client";
import {
  convertObservation,
  enrichObservationWithModelData,
} from "./observations_converters";
import { env } from "../../env";
import { TracingSearchType } from "../../interfaces/search";
import { ClickHouseClientConfigOptions } from "@clickhouse/client";
import type { AnalyticsGenerationEvent } from "../analytics-integrations/types";
import { ObservationType } from "../../domain";
import { recordDistribution } from "../instrumentation";
import { DEFAULT_RENDERING_PROPS, RenderingProps } from "../utils/rendering";
import { Prisma } from "@prisma/client";

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

const toJsonValue = (value: unknown) => {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

const toJsonString = (value: unknown) => JSON.stringify(value ?? {});
const toJsonArrayString = (value: unknown) => JSON.stringify(value ?? []);

const parseDateInput = (value: unknown) => {
  if (value instanceof Date) return value;
  if (typeof value === "number") return new Date(value);
  if (typeof value === "string" && /^\d+$/.test(value)) {
    return new Date(Number(value));
  }
  return parseClickhouseUTCDateTimeFormat(String(value));
};

const OBSERVATION_TYPE_VALUES = new Set([
  "SPAN",
  "GENERATION",
  "EVENT",
  "AGENT",
  "TOOL",
  "CHAIN",
  "RETRIEVER",
  "EVALUATOR",
  "EMBEDDING",
  "GUARDRAIL",
] as const);

const OBSERVATION_LEVEL_VALUES = new Set([
  "DEBUG",
  "DEFAULT",
  "WARNING",
  "ERROR",
] as const);

const toObservationTypeEnum = (value: unknown) => {
  const normalized = String(value ?? "SPAN").toUpperCase();
  return OBSERVATION_TYPE_VALUES.has(normalized as any) ? normalized : "SPAN";
};

const toObservationLevelEnum = (value: unknown) => {
  const normalized = String(value ?? "DEFAULT").toUpperCase();
  return OBSERVATION_LEVEL_VALUES.has(normalized as any)
    ? normalized
    : "DEFAULT";
};

type PgObservationRow = {
  id: string;
  trace_id: string | null;
  project_id: string;
  type: string;
  parent_observation_id: string | null;
  environment: string | null;
  start_time: Date;
  end_time: Date | null;
  name: string | null;
  metadata: Record<string, unknown> | null;
  level: string | null;
  status_message: string | null;
  version: string | null;
  input: string | null;
  output: string | null;
  provided_model_name: string | null;
  internal_model_id: string | null;
  model_parameters: Record<string, unknown> | null;
  provided_usage_details: Record<string, number> | null;
  usage_details: Record<string, number> | null;
  provided_cost_details: Record<string, number> | null;
  cost_details: Record<string, number> | null;
  total_cost: number | null;
  usage_pricing_tier_id: string | null;
  usage_pricing_tier_name: string | null;
  completion_start_time: Date | null;
  prompt_id: string | null;
  prompt_name: string | null;
  prompt_version: number | null;
  tool_definitions: Record<string, string> | null;
  tool_calls: string[] | null;
  tool_call_names: string[] | null;
  created_at: Date;
  updated_at: Date;
  event_ts: Date;
  is_deleted: boolean;
};

const toObservationRecordReadType = (
  record: PgObservationRow,
  includeIO: boolean,
): ObservationRecordReadType => {
  return {
    id: record.id,
    trace_id: record.trace_id,
    project_id: record.project_id,
    type: record.type,
    parent_observation_id: record.parent_observation_id,
    environment: record.environment ?? "default",
    name: record.name,
    metadata: includeIO ? toClickhouseMetadataRecord(record.metadata) : {},
    level: record.level,
    status_message: record.status_message,
    version: record.version,
    input: includeIO ? record.input : null,
    output: includeIO ? record.output : null,
    provided_model_name: record.provided_model_name,
    internal_model_id: record.internal_model_id,
    model_parameters: record.model_parameters
      ? JSON.stringify(record.model_parameters)
      : null,
    total_cost: record.total_cost ?? null,
    usage_pricing_tier_id: record.usage_pricing_tier_id,
    usage_pricing_tier_name: record.usage_pricing_tier_name,
    prompt_id: record.prompt_id,
    prompt_name: record.prompt_name,
    prompt_version: record.prompt_version,
    tool_definitions: record.tool_definitions ?? {},
    tool_calls: record.tool_calls ?? [],
    tool_call_names: record.tool_call_names ?? [],
    is_deleted: record.is_deleted ? 1 : 0,
    created_at: toClickhouseDateTimeString(record.created_at) ?? "",
    updated_at: toClickhouseDateTimeString(record.updated_at) ?? "",
    start_time: toClickhouseDateTimeString(record.start_time) ?? "",
    end_time: toClickhouseDateTimeString(record.end_time) ?? null,
    completion_start_time:
      toClickhouseDateTimeString(record.completion_start_time) ?? null,
    event_ts: toClickhouseDateTimeString(record.event_ts) ?? "",
    provided_usage_details: record.provided_usage_details ?? {},
    provided_cost_details: record.provided_cost_details ?? {},
    usage_details: record.usage_details ?? {},
    cost_details: record.cost_details ?? {},
  };
};

/**
 * Checks if observation exists in clickhouse.
 *
 * @param {string} projectId - Project ID for the observation
 * @param {string} id - ID of the observation
 * @param {Date} startTime - Timestamp for time-based filtering, uses event payload or job timestamp
 * @returns {Promise<boolean>} - True if observation exists
 *
 * Notes:
 * • Filters with two days lookback window subject to startTime
 * • Used for validating observation references before eval job creation
 */
export const checkObservationExists = async (
  projectId: string,
  id: string,
  startTime: Date | undefined,
): Promise<boolean> => {
  const rows = await prisma.$queryRaw<Array<{ id: string }>>(Prisma.sql`
    SELECT id
    FROM observations
    WHERE id = ${id}
      AND project_id = ${projectId}
      ${startTime ? Prisma.sql`AND start_time >= ${new Date(startTime.getTime() - 2 * 24 * 60 * 60 * 1000)}` : Prisma.empty}
    LIMIT 1
  `);
  return rows.length > 0;
};

/**
 * Accepts a trace in a Clickhouse-ready format.
 * id, project_id, and timestamp must always be provided.
 */
export const upsertObservation = async (
  observation: Partial<ObservationRecordReadType>,
) => {
  if (
    !["id", "project_id", "start_time", "type"].every(
      (key) => key in observation,
    )
  ) {
    throw new Error(
      "Identifier fields must be provided to upsert Observation.",
    );
  }
  const startTime = parseDateInput(observation.start_time);

  const createdAt = observation.created_at
    ? parseDateInput(observation.created_at)
    : startTime;
  const updatedAt = observation.updated_at
    ? parseDateInput(observation.updated_at)
    : startTime;
  const observationType = toObservationTypeEnum(observation.type);
  const observationLevel = toObservationLevelEnum(observation.level);
  await prisma.$executeRaw`
    DELETE FROM observations
    WHERE project_id = ${observation.project_id as string}
      AND id = ${observation.id as string}
  `;
  await prisma.$executeRaw`
    INSERT INTO observations (
      id, project_id, trace_id, parent_observation_id, environment, type, name,
      start_time, end_time, level, status_message, version, input, output, metadata,
      provided_model_name, internal_model_id, model_parameters,
      provided_usage_details, usage_details, provided_cost_details, cost_details,
      total_cost, usage_pricing_tier_id, usage_pricing_tier_name, completion_start_time,
      prompt_id, prompt_name, prompt_version, tool_definitions, tool_calls, tool_call_names,
      created_at, updated_at, event_ts, is_deleted
    ) VALUES (
      ${observation.id as string},
      ${observation.project_id as string},
      ${observation.trace_id ?? (observation.id as string)},
      ${observation.parent_observation_id ?? null},
      ${observation.environment ?? "default"},
      ${observationType}::observation_type,
      ${observation.name ?? ""},
      ${startTime},
      ${observation.end_time ? parseDateInput(observation.end_time) : null},
      ${observationLevel}::observation_level,
      ${observation.status_message ?? null},
      ${observation.version ?? null},
      ${typeof observation.input === "string" ? observation.input : observation.input == null ? null : JSON.stringify(observation.input)},
      ${typeof observation.output === "string" ? observation.output : observation.output == null ? null : JSON.stringify(observation.output)},
      ${toJsonString(observation.metadata)}::jsonb,
      ${observation.provided_model_name ?? null},
      ${observation.internal_model_id ?? null},
      ${
        toJsonValue(observation.model_parameters) == null
          ? null
          : JSON.stringify(toJsonValue(observation.model_parameters))
      }::jsonb,
      ${toJsonString(observation.provided_usage_details)}::jsonb,
      ${toJsonString(observation.usage_details)}::jsonb,
      ${toJsonString(observation.provided_cost_details)}::jsonb,
      ${toJsonString(observation.cost_details)}::jsonb,
      ${observation.total_cost ?? null},
      ${observation.usage_pricing_tier_id ?? null},
      ${observation.usage_pricing_tier_name ?? null},
      ${observation.completion_start_time ? parseDateInput(observation.completion_start_time) : null},
      ${observation.prompt_id ?? null},
      ${observation.prompt_name ?? null},
      ${observation.prompt_version ?? null},
      ${toJsonString(observation.tool_definitions)}::jsonb,
      ${toJsonArrayString(observation.tool_calls)}::jsonb,
      ${observation.tool_call_names ?? []},
      ${createdAt},
      ${updatedAt},
      ${updatedAt},
      ${false}
    )
  `;
};

export type GetObservationsForTraceOpts<IncludeIO extends boolean> = {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  includeIO?: IncludeIO;
  preferredClickhouseService?: PreferredClickhouseService;
};

export const getObservationsForTrace = async <IncludeIO extends boolean>(
  opts: GetObservationsForTraceOpts<IncludeIO>,
) => {
  const {
    traceId,
    projectId,
    timestamp,
    includeIO = false,
    preferredClickhouseService: _preferredClickhouseService,
  } = opts;

  const recordsRaw = await prisma.$queryRaw<PgObservationRow[]>(Prisma.sql`
    SELECT *
    FROM observations
    WHERE trace_id = ${traceId}
      AND project_id = ${projectId}
      ${timestamp ? Prisma.sql`AND start_time >= ${new Date(timestamp.getTime() - 2 * 24 * 60 * 60 * 1000)}` : Prisma.empty}
    ORDER BY updated_at DESC
  `);
  const records = recordsRaw.map((r) =>
    toObservationRecordReadType(r, includeIO === true),
  );

  // Large number of observations in trace with large input / output / metadata will lead to
  // high CPU and memory consumption in the convertObservation step, where parsing occurs
  // Thus, limit the size of the payload to 5MB, follows NextJS response size limitation:
  // https://nextjs.org/docs/messages/api-routes-response-size-limit
  // See also LFE-4882 for more details
  let payloadSize = 0;

  for (const observation of records) {
    for (const key of ["input", "output"] as const) {
      const value = observation[key];

      if (value && typeof value === "string") {
        payloadSize += value.length;
      }
    }

    const metadataValues = Object.values(observation["metadata"] ?? {});

    metadataValues.forEach((value) => {
      if (value && typeof value === "string") {
        payloadSize += value.length;
      }
    });

    if (payloadSize >= env.LANGFUSE_API_TRACE_OBSERVATIONS_SIZE_LIMIT_BYTES) {
      const errorMessage = `Observations in trace are too large: ${(payloadSize / 1e6).toFixed(2)}MB exceeds limit of ${(env.LANGFUSE_API_TRACE_OBSERVATIONS_SIZE_LIMIT_BYTES / 1e6).toFixed(2)}MB`;

      throw new Error(errorMessage);
    }
  }

  return records.map((r) => {
    const observation = convertObservation({
      ...r,
      metadata: r.metadata ?? {},
    });
    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - observation.startTime.getTime(),
      {
        table: "observations",
      },
    );
    return observation;
  });
};

export const getObservationForTraceIdByName = async ({
  traceId,
  projectId,
  name,
  timestamp,
  fetchWithInputOutput = false,
}: {
  traceId: string;
  projectId: string;
  name: string;
  timestamp?: Date;
  fetchWithInputOutput?: boolean;
}) => {
  const rows = await prisma.$queryRaw<PgObservationRow[]>(Prisma.sql`
    SELECT DISTINCT ON (id, project_id) *
    FROM observations
    WHERE trace_id = ${traceId}
      AND project_id = ${projectId}
      AND name = ${name}
      ${
        timestamp
          ? Prisma.sql`AND start_time >= ${new Date(timestamp.getTime() - 2 * 24 * 60 * 60 * 1000)}`
          : Prisma.empty
      }
    ORDER BY id, project_id, event_ts DESC
  `);
  const records = rows.map((row) =>
    toObservationRecordReadType(row, fetchWithInputOutput),
  );
  return records.map((record) => convertObservation(record));
};

export const getObservationById = async ({
  id,
  projectId,
  fetchWithInputOutput = false,
  startTime,
  type,
  traceId,
  renderingProps = DEFAULT_RENDERING_PROPS,
  preferredClickhouseService: _preferredClickhouseService,
}: {
  id: string;
  projectId: string;
  fetchWithInputOutput?: boolean;
  startTime?: Date;
  type?: ObservationType;
  traceId?: string;
  renderingProps?: RenderingProps;
  preferredClickhouseService?: PreferredClickhouseService;
}) => {
  const records = await getObservationByIdInternal({
    id,
    projectId,
    fetchWithInputOutput,
    startTime,
    type,
    traceId,
    renderingProps,
    preferredClickhouseService: _preferredClickhouseService,
  });
  const mapped = records.map((record) =>
    convertObservation(record, renderingProps),
  );

  mapped.forEach((observation) => {
    recordDistribution(
      "langfuse.query_by_id_age",
      new Date().getTime() - observation.startTime.getTime(),
      {
        table: "observations",
      },
    );
  });
  if (mapped.length === 0) {
    throw new LangfuseNotFoundError(`Observation with id ${id} not found`);
  }

  if (mapped.length > 1) {
    logger.error(
      `Multiple observations found for id ${id} and project ${projectId}`,
    );
    throw new InternalServerError(
      `Multiple observations found for id ${id} and project ${projectId}`,
    );
  }
  return mapped.shift();
};

export const getObservationsById = async (
  ids: string[],
  projectId: string,
  fetchWithInputOutput: boolean = false,
) => {
  const recordsRaw = await prisma.$queryRaw<PgObservationRow[]>(Prisma.sql`
    SELECT *
    FROM observations
    WHERE id IN (${Prisma.join(ids)})
      AND project_id = ${projectId}
    ORDER BY updated_at DESC
  `);
  const records = recordsRaw.map((record) =>
    toObservationRecordReadType(record, fetchWithInputOutput),
  );
  return records.map((record) => convertObservation(record));
};

const getObservationByIdInternal = async ({
  id,
  projectId,
  fetchWithInputOutput = false,
  startTime,
  type,
  traceId,
  renderingProps = DEFAULT_RENDERING_PROPS,
  preferredClickhouseService: _preferredClickhouseService,
}: {
  id: string;
  projectId: string;
  fetchWithInputOutput?: boolean;
  startTime?: Date;
  type?: ObservationType;
  traceId?: string;
  renderingProps?: RenderingProps;
  preferredClickhouseService?: PreferredClickhouseService;
}) => {
  const startTimeWhere: { gte?: Date; lt?: Date } = {};
  if (startTime) {
    startTimeWhere.gte = new Date(
      Date.UTC(
        startTime.getUTCFullYear(),
        startTime.getUTCMonth(),
        startTime.getUTCDate(),
        0,
        0,
        0,
        0,
      ),
    );
    startTimeWhere.lt = new Date(
      Date.UTC(
        startTime.getUTCFullYear(),
        startTime.getUTCMonth(),
        startTime.getUTCDate() + 1,
        0,
        0,
        0,
        0,
      ),
    );
  }

  const conditions: Prisma.Sql[] = [
    Prisma.sql`id = ${id}`,
    Prisma.sql`project_id = ${projectId}`,
  ];
  if (type) conditions.push(Prisma.sql`type::text = ${type}`);
  if (traceId) conditions.push(Prisma.sql`trace_id = ${traceId}`);
  if (startTimeWhere.gte)
    conditions.push(Prisma.sql`start_time >= ${startTimeWhere.gte}`);
  if (startTimeWhere.lt)
    conditions.push(Prisma.sql`start_time < ${startTimeWhere.lt}`);
  const rows = await prisma.$queryRaw<PgObservationRow[]>(Prisma.sql`
    SELECT *
    FROM observations
    WHERE ${Prisma.join(conditions, " AND ")}
    ORDER BY updated_at DESC
    LIMIT 1
  `);
  const record = rows[0];

  if (!record) return [];

  const mapped = toObservationRecordReadType(record, fetchWithInputOutput);

  if (fetchWithInputOutput && renderingProps.truncated) {
    if (typeof mapped.input === "string") {
      mapped.input = mapped.input.slice(
        0,
        env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT,
      );
    }
    if (typeof mapped.output === "string") {
      mapped.output = mapped.output.slice(
        0,
        env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT,
      );
    }
  }

  return [mapped];
};

export type ObservationTableQuery = {
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  limit?: number;
  offset?: number;
  selectIOAndMetadata?: boolean;
  renderingProps?: RenderingProps;
  clickhouseConfigs?: ClickHouseClientConfigOptions | undefined;
};

export type ObservationsTableQueryResult = ObservationRecordReadType & {
  latency?: string;
  time_to_first_token?: string;
  trace_tags?: string[];
  trace_name?: string;
  trace_user_id?: string;
  // Tool counts for list view performance (ClickHouse numbers as strings)
  tool_definitions_count?: string;
  tool_calls_count?: string;
};

export const getObservationsTableCount = async (
  opts: ObservationTableQuery,
) => {
  const count = await getObservationsTableInternal<{
    count: string;
  }>({
    ...opts,
    select: "count",
    tags: { kind: "count" },
  });

  return Number(count[0].count);
};

export const getObservationsTableWithModelData = async (
  opts: ObservationTableQuery,
): Promise<FullObservations> => {
  const observationRecords = await getObservationsTableInternal<
    Omit<
      ObservationsTableQueryResult,
      "trace_tags" | "trace_name" | "trace_user_id"
    >
  >({
    ...opts,
    select: "rows",
    tags: { kind: "list" },
  });

  const uniqueModels: string[] = Array.from(
    new Set(
      observationRecords
        .map((r) => r.internal_model_id)
        .filter((r): r is string => Boolean(r)),
    ),
  );

  const [models, traces] = await Promise.all([
    uniqueModels.length > 0
      ? prisma.model.findMany({
          where: {
            id: {
              in: uniqueModels,
            },
            OR: [{ projectId: opts.projectId }, { projectId: null }],
          },
          include: {
            Price: true,
          },
        })
      : [],
    getTracesByIds(
      observationRecords
        .map((o) => o.trace_id)
        .filter((o): o is string => Boolean(o)),
      opts.projectId,
    ),
  ]);

  return observationRecords.map((o) => {
    const trace = traces.find((t) => t.id === o.trace_id);
    const model = models.find((m) => m.id === o.internal_model_id);
    return {
      ...convertObservation(o),
      latency: o.latency ? Number(o.latency) / 1000 : null,
      timeToFirstToken: o.time_to_first_token
        ? Number(o.time_to_first_token) / 1000
        : null,
      traceName: trace?.name ?? null,
      traceTags: trace?.tags ?? [],
      traceTimestamp: trace?.timestamp ?? null,
      userId: trace?.userId ?? null,
      // Tool counts for list view (actual data in toolDefinitions/toolCalls from domain)
      toolDefinitionsCount: o.tool_definitions_count
        ? Number(o.tool_definitions_count)
        : null,
      toolCallsCount: o.tool_calls_count ? Number(o.tool_calls_count) : null,
      ...enrichObservationWithModelData(model),
    };
  });
};

const getObservationsTableInternal = async <T>(
  opts: ObservationTableQuery & {
    select: "count" | "rows";
    tags: Record<string, string>;
  },
): Promise<Array<T>> => {
  const normalize = (s: string) => s.toLowerCase().replace(/\s+/g, "");
  const projectId = opts.projectId;
  const conditions: Prisma.Sql[] = [Prisma.sql`o.project_id = ${projectId}`];
  let needsTraceJoin = false;

  const columnExpr = (columnRaw: string): Prisma.Sql | null => {
    const column = normalize(columnRaw);
    switch (column) {
      case "id":
        return Prisma.sql`o.id`;
      case "name":
        return Prisma.sql`o.name`;
      case "type":
        return Prisma.sql`o.type::text`;
      case "level":
        return Prisma.sql`o.level::text`;
      case "parentobservationid":
        return Prisma.sql`o.parent_observation_id`;
      case "environment":
        return Prisma.sql`o.environment`;
      case "statusmessage":
        return Prisma.sql`o.status_message`;
      case "version":
        return Prisma.sql`o.version`;
      case "model":
        return Prisma.sql`o.provided_model_name`;
      case "modelid":
        return Prisma.sql`o.internal_model_id`;
      case "promptname":
        return Prisma.sql`o.prompt_name`;
      case "promptversion":
        return Prisma.sql`o.prompt_version`;
      case "starttime":
        return Prisma.sql`o.start_time`;
      case "endtime":
        return Prisma.sql`o.end_time`;
      case "totaltokens":
      case "tokens":
        return Prisma.sql`COALESCE((o.usage_details->>'total')::numeric, 0)`;
      case "inputtokens":
        return Prisma.sql`COALESCE((o.usage_details->>'input')::numeric, 0)`;
      case "outputtokens":
        return Prisma.sql`COALESCE((o.usage_details->>'output')::numeric, 0)`;
      case "totalcost":
        return Prisma.sql`COALESCE(o.total_cost, 0)`;
      case "inputcost":
        return Prisma.sql`COALESCE((o.cost_details->>'input')::numeric, 0)`;
      case "outputcost":
        return Prisma.sql`COALESCE((o.cost_details->>'output')::numeric, 0)`;
      case "latency":
        return Prisma.sql`COALESCE(EXTRACT(EPOCH FROM (COALESCE(o.end_time, o.start_time) - o.start_time)), 0)`;
      case "timetofirsttoken":
        return Prisma.sql`COALESCE(EXTRACT(EPOCH FROM (COALESCE(o.completion_start_time, o.start_time) - o.start_time)), 0)`;
      case "traceid":
        return Prisma.sql`o.trace_id`;
      case "tracename":
        needsTraceJoin = true;
        return Prisma.sql`t.name`;
      case "userid":
        needsTraceJoin = true;
        return Prisma.sql`t.user_id`;
      case "sessionid":
        needsTraceJoin = true;
        return Prisma.sql`t.session_id`;
      case "tracetags":
        needsTraceJoin = true;
        return Prisma.sql`t.tags::text`;
      case "traceenvironment":
        needsTraceJoin = true;
        return Prisma.sql`t.environment`;
      default:
        return null;
    }
  };

  for (const f of opts.filter) {
    const expr = columnExpr(f.column);
    if (!expr) continue;

    if (f.type === "datetime") {
      if (f.operator === ">=")
        conditions.push(Prisma.sql`${expr} >= ${f.value}`);
      if (f.operator === ">") conditions.push(Prisma.sql`${expr} > ${f.value}`);
      if (f.operator === "<=")
        conditions.push(Prisma.sql`${expr} <= ${f.value}`);
      if (f.operator === "<") conditions.push(Prisma.sql`${expr} < ${f.value}`);
      continue;
    }
    if (f.type === "number") {
      if (f.operator === "=") conditions.push(Prisma.sql`${expr} = ${f.value}`);
      if (f.operator === ">") conditions.push(Prisma.sql`${expr} > ${f.value}`);
      if (f.operator === "<") conditions.push(Prisma.sql`${expr} < ${f.value}`);
      if (f.operator === ">=")
        conditions.push(Prisma.sql`${expr} >= ${f.value}`);
      if (f.operator === "<=")
        conditions.push(Prisma.sql`${expr} <= ${f.value}`);
      continue;
    }
    if (f.type === "string") {
      if (f.operator === "=") conditions.push(Prisma.sql`${expr} = ${f.value}`);
      if (f.operator === "contains")
        conditions.push(Prisma.sql`${expr} ILIKE ${`%${f.value}%`}`);
      if (f.operator === "does not contain")
        conditions.push(Prisma.sql`${expr} NOT ILIKE ${`%${f.value}%`}`);
      if (f.operator === "starts with")
        conditions.push(Prisma.sql`${expr} ILIKE ${`${f.value}%`}`);
      if (f.operator === "ends with")
        conditions.push(Prisma.sql`${expr} ILIKE ${`%${f.value}`}`);
      continue;
    }
    if (f.type === "stringOptions") {
      if (f.operator === "any of") {
        conditions.push(Prisma.sql`${expr} IN (${Prisma.join(f.value)})`);
      } else if (f.operator === "none of") {
        conditions.push(Prisma.sql`${expr} NOT IN (${Prisma.join(f.value)})`);
      }
    }
  }

  if (opts.searchQuery) {
    const pattern = `%${opts.searchQuery}%`;
    const contentSearch = (opts.searchType ?? ["id"]).includes("content");
    const searchConds: Prisma.Sql[] = [
      Prisma.sql`o.id ILIKE ${pattern}`,
      Prisma.sql`COALESCE(o.name,'') ILIKE ${pattern}`,
      Prisma.sql`COALESCE(o.trace_id,'') ILIKE ${pattern}`,
      Prisma.sql`COALESCE(o.parent_observation_id,'') ILIKE ${pattern}`,
      Prisma.sql`COALESCE(o.status_message,'') ILIKE ${pattern}`,
      Prisma.sql`COALESCE(o.provided_model_name,'') ILIKE ${pattern}`,
    ];
    if (contentSearch) {
      searchConds.push(Prisma.sql`COALESCE(o.input,'') ILIKE ${pattern}`);
      searchConds.push(Prisma.sql`COALESCE(o.output,'') ILIKE ${pattern}`);
    }
    conditions.push(Prisma.sql`(${Prisma.join(searchConds, " OR ")})`);
  }

  const orderExpr =
    columnExpr(opts.orderBy?.column ?? "startTime") ?? Prisma.sql`o.start_time`;
  const orderDir =
    opts.orderBy?.order?.toLowerCase() === "asc"
      ? Prisma.sql`ASC`
      : Prisma.sql`DESC`;

  const fromWithJoin = Prisma.sql`
    FROM observations o
    ${needsTraceJoin ? Prisma.sql`LEFT JOIN traces t ON t.id = o.trace_id AND t.project_id = o.project_id` : Prisma.empty}
  `;

  if (opts.select === "count") {
    const rows = await prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      ${fromWithJoin}
      WHERE ${Prisma.join(conditions, " AND ")}
    `);
    return [{ count: String(Number(rows[0]?.count ?? 0n)) }] as Array<T>;
  }

  const selectRows = Prisma.sql`
    SELECT
      o.id,
      o.type::text as type,
      o.project_id,
      o.name,
      o.model_parameters,
      o.start_time,
      o.end_time,
      o.trace_id,
      o.completion_start_time,
      o.provided_usage_details,
      o.usage_details,
      o.provided_cost_details,
      o.cost_details,
      o.level::text as level,
      o.environment,
      o.status_message,
      o.version,
      o.parent_observation_id,
      o.created_at,
      o.updated_at,
      o.provided_model_name,
      o.total_cost,
      o.usage_pricing_tier_id,
      o.usage_pricing_tier_name,
      o.prompt_id,
      o.prompt_name,
      o.prompt_version,
      o.internal_model_id,
      (EXTRACT(EPOCH FROM (COALESCE(o.end_time, o.start_time) - o.start_time)) * 1000)::text AS latency,
      (EXTRACT(EPOCH FROM (COALESCE(o.completion_start_time, o.start_time) - o.start_time)) * 1000)::text AS time_to_first_token,
      (SELECT count(*) FROM jsonb_object_keys(COALESCE(o.tool_definitions, '{}'::jsonb)))::text AS tool_definitions_count,
      jsonb_array_length(COALESCE(o.tool_calls, '[]'::jsonb))::text AS tool_calls_count
      ${opts.selectIOAndMetadata ? Prisma.sql`, o.input, o.output, o.metadata` : Prisma.empty}
    ${fromWithJoin}
    WHERE ${Prisma.join(conditions, " AND ")}
    ORDER BY ${orderExpr} ${orderDir}
    ${opts.limit !== undefined ? Prisma.sql`LIMIT ${opts.limit}` : Prisma.empty}
    ${opts.offset !== undefined ? Prisma.sql`OFFSET ${opts.offset}` : Prisma.empty}
  `;

  const rows = await prisma.$queryRaw<Array<T>>(selectRows);
  return rows;
};

export const getObservationsGroupedByModel = async (
  projectId: string,
  filter: FilterState,
) => {
  const timeConditions = filter
    .filter(
      (f) =>
        f.type === "datetime" &&
        (f.column === "Start Time" || f.column === "startTime"),
    )
    .map((f) => {
      if (f.operator === ">=") return Prisma.sql`o.start_time >= ${f.value}`;
      if (f.operator === ">") return Prisma.sql`o.start_time > ${f.value}`;
      if (f.operator === "<=") return Prisma.sql`o.start_time <= ${f.value}`;
      return Prisma.sql`o.start_time < ${f.value}`;
    });
  const rows = await prisma.$queryRaw<Array<{ name: string }>>(Prisma.sql`
    SELECT o.provided_model_name as name
    FROM observations o
    WHERE o.project_id = ${projectId}
      AND o.type::text = 'GENERATION'
      AND o.provided_model_name IS NOT NULL
      ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
    GROUP BY o.provided_model_name
    ORDER BY COUNT(*) DESC
    LIMIT 1000
  `);
  return rows.map((r) => ({ model: r.name }));
};

export const getObservationsGroupedByModelId = async (
  projectId: string,
  filter: FilterState,
) => {
  const timeConditions = filter
    .filter(
      (f) =>
        f.type === "datetime" &&
        (f.column === "Start Time" || f.column === "startTime"),
    )
    .map((f) => {
      if (f.operator === ">=") return Prisma.sql`o.start_time >= ${f.value}`;
      if (f.operator === ">") return Prisma.sql`o.start_time > ${f.value}`;
      if (f.operator === "<=") return Prisma.sql`o.start_time <= ${f.value}`;
      return Prisma.sql`o.start_time < ${f.value}`;
    });
  const rows = await prisma.$queryRaw<Array<{ modelid: string }>>(Prisma.sql`
    SELECT o.internal_model_id as modelId
    FROM observations o
    WHERE o.project_id = ${projectId}
      AND o.type::text = 'GENERATION'
      AND o.internal_model_id IS NOT NULL
      ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
    GROUP BY o.internal_model_id
    ORDER BY COUNT(*) DESC
    LIMIT 1000
  `);
  return rows.map((r) => ({ modelId: r.modelid ?? (r as any).modelId }));
};

export const getObservationsGroupedByName = async (
  projectId: string,
  filter: FilterState,
  type: ObservationType | null = "GENERATION",
) => {
  const timeConditions = filter
    .filter(
      (f) =>
        f.type === "datetime" &&
        (f.column === "Start Time" || f.column === "startTime"),
    )
    .map((f) => {
      if (f.operator === ">=") return Prisma.sql`o.start_time >= ${f.value}`;
      if (f.operator === ">") return Prisma.sql`o.start_time > ${f.value}`;
      if (f.operator === "<=") return Prisma.sql`o.start_time <= ${f.value}`;
      return Prisma.sql`o.start_time < ${f.value}`;
    });
  const rows = await prisma.$queryRaw<Array<{ name: string }>>(Prisma.sql`
    SELECT o.name
    FROM observations o
    WHERE o.project_id = ${projectId}
      ${type ? Prisma.sql`AND o.type::text = ${type}` : Prisma.empty}
      AND o.name IS NOT NULL
      ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
    GROUP BY o.name
    ORDER BY COUNT(*) DESC
    LIMIT 1000
  `);
  return rows;
};

export const getObservationsGroupedByToolName = async (
  projectId: string,
  filter: FilterState,
) => {
  const timeConditions = filter
    .filter(
      (f) =>
        f.type === "datetime" &&
        (f.column === "Start Time" || f.column === "startTime"),
    )
    .map((f) => {
      if (f.operator === ">=") return Prisma.sql`o.start_time >= ${f.value}`;
      if (f.operator === ">") return Prisma.sql`o.start_time > ${f.value}`;
      if (f.operator === "<=") return Prisma.sql`o.start_time <= ${f.value}`;
      return Prisma.sql`o.start_time < ${f.value}`;
    });
  return prisma.$queryRaw<Array<{ toolName: string }>>(Prisma.sql`
    SELECT DISTINCT key AS "toolName"
    FROM observations o
    CROSS JOIN LATERAL jsonb_object_keys(COALESCE(o.tool_definitions, '{}'::jsonb)) key
    WHERE o.project_id = ${projectId}
      ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
    LIMIT 1000
  `);
};

export const getObservationsGroupedByCalledToolName = async (
  projectId: string,
  filter: FilterState,
) => {
  const timeConditions = filter
    .filter(
      (f) =>
        f.type === "datetime" &&
        (f.column === "Start Time" || f.column === "startTime"),
    )
    .map((f) => {
      if (f.operator === ">=") return Prisma.sql`o.start_time >= ${f.value}`;
      if (f.operator === ">") return Prisma.sql`o.start_time > ${f.value}`;
      if (f.operator === "<=") return Prisma.sql`o.start_time <= ${f.value}`;
      return Prisma.sql`o.start_time < ${f.value}`;
    });
  return prisma.$queryRaw<Array<{ calledToolName: string }>>(Prisma.sql`
    SELECT DISTINCT name AS "calledToolName"
    FROM observations o
    CROSS JOIN LATERAL unnest(COALESCE(o.tool_call_names, ARRAY[]::text[])) name
    WHERE o.project_id = ${projectId}
      ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
    LIMIT 1000
  `);
};

export const getObservationsGroupedByPromptName = async (
  projectId: string,
  filter: FilterState,
) => {
  const timeConditions = filter
    .filter(
      (f) =>
        f.type === "datetime" &&
        (f.column === "Start Time" || f.column === "startTime"),
    )
    .map((f) => {
      if (f.operator === ">=") return Prisma.sql`o.start_time >= ${f.value}`;
      if (f.operator === ">") return Prisma.sql`o.start_time > ${f.value}`;
      if (f.operator === "<=") return Prisma.sql`o.start_time <= ${f.value}`;
      return Prisma.sql`o.start_time < ${f.value}`;
    });
  const promptRows = await prisma.$queryRaw<Array<{ id: string }>>(Prisma.sql`
    SELECT o.prompt_id AS id
    FROM observations o
    WHERE o.project_id = ${projectId}
      AND o.type::text = 'GENERATION'
      AND o.prompt_id IS NOT NULL
      ${timeConditions.length ? Prisma.sql`AND ${Prisma.join(timeConditions, " AND ")}` : Prisma.empty}
    GROUP BY o.prompt_id
    ORDER BY COUNT(*) DESC
    LIMIT 1000
  `);

  const prompts = promptRows
    .map((r) => r.id)
    .filter((r): r is string => Boolean(r));

  const pgPrompts =
    prompts.length > 0
      ? await prisma.prompt.findMany({
          select: {
            id: true,
            name: true,
          },
          where: {
            id: {
              in: prompts,
            },
            projectId,
          },
        })
      : [];

  return pgPrompts.map((p) => ({
    promptName: p.name,
  }));
};

export const getCostForTraces = async (
  projectId: string,
  timestamp: Date,
  traceIds: string[],
) => {
  if (traceIds.length === 0) return undefined;
  const lowerBound = new Date(timestamp.getTime() - 2 * 24 * 60 * 60 * 1000);
  const rows = await prisma.$queryRaw<Array<{ total_cost: string }>>(Prisma.sql`
    WITH selected_observations AS (
      SELECT DISTINCT ON (o.id, o.project_id)
        o.total_cost
      FROM observations o
      WHERE o.project_id = ${projectId}
        AND o.trace_id IN (${Prisma.join(traceIds)})
        AND o.start_time >= ${lowerBound}
      ORDER BY o.id, o.project_id, o.event_ts DESC
    )
    SELECT COALESCE(SUM(total_cost), 0)::text AS total_cost
    FROM selected_observations
  `);
  return rows.length > 0 ? Number(rows[0].total_cost) : undefined;
};

export const deleteObservationsByTraceIds = async (
  projectId: string,
  traceIds: string[],
) => {
  const preflight = await prisma.$queryRaw<
    Array<{ min_ts: Date | null; max_ts: Date | null; cnt: bigint }>
  >(Prisma.sql`
    SELECT
      (min(start_time) - INTERVAL '1 hour') as min_ts,
      (max(start_time) + INTERVAL '1 hour') as max_ts,
      count(*)::bigint as cnt
    FROM observations
    WHERE project_id = ${projectId}
      AND trace_id IN (${Prisma.join(traceIds)})
  `);

  const count = Number(preflight[0]?.cnt ?? 0n);
  if (count === 0) {
    logger.info(
      `deleteObservationsByTraceIds: no rows found for project ${projectId}, skipping DELETE`,
    );
    return;
  }

  await prisma.$executeRaw`
    DELETE FROM observations
    WHERE project_id = ${projectId}
      AND trace_id IN (${Prisma.join(traceIds)})
      AND start_time >= ${preflight[0].min_ts}
      AND start_time <= ${preflight[0].max_ts}
  `;
};

export const hasAnyObservation = async (projectId: string) => {
  const rows = await prisma.$queryRaw<Array<{ one: number }>>(Prisma.sql`
    SELECT 1 as one
    FROM observations
    WHERE project_id = ${projectId}
    LIMIT 1
  `);
  return rows.length > 0;
};

export const deleteObservationsByProjectId = async (
  projectId: string,
): Promise<boolean> => {
  const hasData = await hasAnyObservation(projectId);
  if (!hasData) {
    return false;
  }

  await prisma.$executeRaw`
    DELETE FROM observations
    WHERE project_id = ${projectId}
  `;

  return true;
};

export const hasAnyObservationOlderThan = async (
  projectId: string,
  beforeDate: Date,
) => {
  const rows = await prisma.$queryRaw<Array<{ one: number }>>(Prisma.sql`
    SELECT 1 as one
    FROM observations
    WHERE project_id = ${projectId}
      AND start_time < ${beforeDate}
    LIMIT 1
  `);

  return rows.length > 0;
};

export const deleteObservationsOlderThanDays = async (
  projectId: string,
  beforeDate: Date,
): Promise<boolean> => {
  const hasData = await hasAnyObservationOlderThan(projectId, beforeDate);
  if (!hasData) {
    return false;
  }

  await prisma.$executeRaw`
    DELETE FROM observations
    WHERE project_id = ${projectId}
      AND start_time < ${beforeDate}
  `;

  return true;
};

export const getObservationsWithPromptName = async (
  projectId: string,
  promptNames: string[],
) => {
  if (promptNames.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<{ count: bigint; prompt_name: string }>
  >(Prisma.sql`
    SELECT count(DISTINCT id)::bigint as count, prompt_name
    FROM observations
    WHERE project_id = ${projectId}
      AND prompt_name IN (${Prisma.join(promptNames)})
      AND prompt_name IS NOT NULL
    GROUP BY prompt_name
  `);

  return rows.map((r) => ({
    count: Number(r.count),
    promptName: r.prompt_name,
  }));
};

export const getObservationMetricsForPrompts = async (
  projectId: string,
  promptIds: string[],
) => {
  if (promptIds.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<{
      count: bigint;
      prompt_id: string;
      prompt_version: number | null;
      first_observation: Date;
      last_observation: Date;
      median_input_usage: string;
      median_output_usage: string;
      median_total_cost: string;
      median_latency_ms: string;
    }>
  >(Prisma.sql`
    WITH latencies AS (
      SELECT
        o.prompt_id,
        o.prompt_version,
        o.start_time,
        o.end_time,
        COALESCE((
          SELECT SUM(v::numeric)
          FROM jsonb_each_text(COALESCE(o.usage_details, '{}'::jsonb)) as kv(k, v)
          WHERE LOWER(k) LIKE '%input%'
        ), 0) AS input_usage,
        COALESCE((
          SELECT SUM(v::numeric)
          FROM jsonb_each_text(COALESCE(o.usage_details, '{}'::jsonb)) as kv(k, v)
          WHERE LOWER(k) LIKE '%output%'
        ), 0) AS output_usage,
        COALESCE((o.cost_details->>'total')::numeric, 0) AS total_cost,
        (EXTRACT(EPOCH FROM (COALESCE(o.end_time, o.start_time) - o.start_time)) * 1000) AS latency_ms
      FROM observations o
      WHERE o.type::text = 'GENERATION'
        AND o.prompt_name IS NOT NULL
        AND o.project_id = ${projectId}
        AND o.prompt_id IN (${Prisma.join(promptIds)})
    )
    SELECT
      count(*)::bigint AS count,
      prompt_id,
      prompt_version,
      min(start_time) AS first_observation,
      max(start_time) AS last_observation,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY input_usage)::text AS median_input_usage,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY output_usage)::text AS median_output_usage,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY total_cost)::text AS median_total_cost,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::text AS median_latency_ms
    FROM latencies
    GROUP BY prompt_id, prompt_version
    ORDER BY prompt_version DESC NULLS LAST
  `);

  return rows.map((r) => ({
    count: Number(r.count),
    promptId: r.prompt_id,
    promptVersion: r.prompt_version ?? 0,
    firstObservation: r.first_observation,
    lastObservation: r.last_observation,
    medianInputUsage: Number(r.median_input_usage),
    medianOutputUsage: Number(r.median_output_usage),
    medianTotalCost: Number(r.median_total_cost),
    medianLatencyMs: Number(r.median_latency_ms),
  }));
};

export const getLatencyAndTotalCostForObservations = async (
  projectId: string,
  observationIds: string[],
  timestamp?: Date,
) => {
  if (observationIds.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<{ id: string; total_cost: string; latency_ms: string }>
  >(Prisma.sql`
    SELECT
      id,
      COALESCE((cost_details->>'total')::numeric, 0)::text AS total_cost,
      COALESCE((EXTRACT(EPOCH FROM (COALESCE(end_time, start_time) - start_time)) * 1000), 0)::text AS latency_ms
    FROM observations
    WHERE project_id = ${projectId}
      AND id IN (${Prisma.join(observationIds)})
      ${timestamp ? Prisma.sql`AND start_time >= ${timestamp}` : Prisma.empty}
  `);

  return rows.map((r) => ({
    id: r.id,
    totalCost: Number(r.total_cost),
    latency: Number(r.latency_ms) / 1000,
  }));
};

export const getLatencyAndTotalCostForObservationsByTraces = async (
  projectId: string,
  traceIds: string[],
  timestamp?: Date,
) => {
  if (traceIds.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<{ trace_id: string; total_cost: string; latency_ms: string }>
  >(Prisma.sql`
    SELECT
      trace_id,
      COALESCE(SUM((cost_details->>'total')::numeric), 0)::text AS total_cost,
      COALESCE((EXTRACT(EPOCH FROM (MAX(COALESCE(end_time, start_time)) - MIN(start_time))) * 1000), 0)::text AS latency_ms
    FROM observations
    WHERE project_id = ${projectId}
      AND trace_id IN (${Prisma.join(traceIds)})
      ${timestamp ? Prisma.sql`AND start_time >= ${timestamp}` : Prisma.empty}
    GROUP BY trace_id
  `);

  return rows.map((r) => ({
    traceId: r.trace_id,
    totalCost: Number(r.total_cost),
    latency: Number(r.latency_ms) / 1000,
  }));
};

/**
 * Tuple type for observation data from ClickHouse groupArray
 */
export type ObservationTuple = [
  id: string,
  parentObservationId: string | null,
  totalCost: string,
  inputCost: string,
  outputCost: string,
  latencyMs: number,
];

/**
 * Get observations grouped by trace ID with cost and latency data
 *
 * This is a pure data-fetching function that returns observations organized by trace.
 * For business logic like recursive cost calculations, use the utility functions
 * in the utils layer.
 */
export const getObservationsGroupedByTraceId = async (
  projectId: string,
  traceIds: string[],
  timestamp?: Date,
): Promise<Map<string, ObservationTuple[]>> => {
  if (traceIds.length === 0) return new Map();

  const rows = await prisma.$queryRaw<
    Array<{
      trace_id: string;
      id: string;
      parent_observation_id: string | null;
      total_cost: string;
      input_cost: string;
      output_cost: string;
      latency_ms: number;
    }>
  >(Prisma.sql`
    SELECT
      trace_id,
      id,
      parent_observation_id,
      COALESCE((cost_details->>'total')::numeric, 0)::text AS total_cost,
      COALESCE((cost_details->>'input')::numeric, 0)::text AS input_cost,
      COALESCE((cost_details->>'output')::numeric, 0)::text AS output_cost,
      COALESCE((EXTRACT(EPOCH FROM (COALESCE(end_time, start_time) - start_time)) * 1000), 0)::int AS latency_ms
    FROM observations
    WHERE project_id = ${projectId}
      AND trace_id IN (${Prisma.join(traceIds)})
      ${timestamp ? Prisma.sql`AND start_time >= ${timestamp}` : Prisma.empty}
  `);

  const map = new Map<string, ObservationTuple[]>();
  for (const r of rows) {
    const tuple: ObservationTuple = [
      r.id,
      r.parent_observation_id,
      r.total_cost,
      r.input_cost,
      r.output_cost,
      r.latency_ms,
    ];
    map.set(r.trace_id, [...(map.get(r.trace_id) ?? []), tuple]);
  }
  return map;
};

export const getObservationCountsByProjectInCreationInterval = async ({
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
      FROM observations
      WHERE created_at >= ${start}
        AND created_at < ${end}
      GROUP BY project_id
    `,
  );

  return rows.map((row) => ({
    projectId: row.project_id,
    count: Number(row.count),
  }));
};

export const getObservationCountOfProjectsSinceCreationDate = async ({
  projectIds,
  start,
}: {
  projectIds: string[];
  start: Date;
}) => {
  if (projectIds.length === 0) return 0;

  const rows = await prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
    SELECT count(*)::bigint as count
    FROM observations
    WHERE project_id IN (${Prisma.join(projectIds)})
      AND created_at >= ${start}
  `);
  return Number(rows[0]?.count ?? 0n);
};

export const getTraceIdsForObservations = async (
  projectId: string,
  observationIds: string[],
) => {
  if (observationIds.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<{ id: string; trace_id: string }>
  >(Prisma.sql`
    SELECT trace_id, id
    FROM observations
    WHERE project_id = ${projectId}
      AND id IN (${Prisma.join(observationIds)})
  `);

  return rows.map((row) => ({
    id: row.id,
    traceId: row.trace_id,
  }));
};

export const getObservationsForBlobStorageExport = function (
  projectId: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const iterator = (async function* () {
    const rows = await prisma.$queryRaw<Record<string, unknown>[]>(Prisma.sql`
      SELECT
        id,
        trace_id,
        project_id,
        environment,
        type::text as type,
        parent_observation_id,
        start_time,
        end_time,
        name,
        metadata,
        level::text as level,
        status_message,
        version,
        input,
        output,
        provided_model_name,
        model_parameters,
        usage_details,
        cost_details,
        completion_start_time,
        prompt_name,
        prompt_version
      FROM observations
      WHERE project_id = ${projectId}
        AND start_time >= ${minTimestamp}
        AND start_time <= ${maxTimestamp}
      ORDER BY start_time ASC
    `);

    for (const row of rows) {
      yield row;
    }
  })();

  return iterator;
};

export const getGenerationsForAnalyticsIntegrations = async function* (
  projectId: string,
  projectName: string,
  minTimestamp: Date,
  maxTimestamp: Date,
) {
  const records = await prisma.$queryRaw<Record<string, unknown>[]>(Prisma.sql`
    SELECT
      o.name as name,
      o.start_time as start_time,
      o.id as id,
      o.total_cost as total_cost,
      CASE
        WHEN o.completion_start_time IS NULL THEN NULL
        ELSE (EXTRACT(EPOCH FROM (o.completion_start_time - o.start_time)) * 1000)
      END as time_to_first_token,
      (o.usage_details->>'total') as input_tokens,
      (o.usage_details->>'output') as output_tokens,
      (o.cost_details->>'total') as total_tokens,
      o.project_id as project_id,
      CASE
        WHEN o.end_time IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (o.end_time - o.start_time))
      END as latency,
      o.provided_model_name as model,
      o.level::text as level,
      o.version as version,
      o.environment as environment,
      t.id as trace_id,
      t.name as trace_name,
      t.session_id as trace_session_id,
      t.user_id as trace_user_id,
      t.release as trace_release,
      t.tags as trace_tags,
      t.metadata->>'$posthog_session_id' as posthog_session_id,
      t.metadata->>'$mixpanel_session_id' as mixpanel_session_id
    FROM observations o
    LEFT JOIN traces t ON o.trace_id = t.id AND o.project_id = t.project_id
    WHERE o.project_id = ${projectId}
      AND t.project_id = ${projectId}
      AND o.start_time >= ${minTimestamp}
      AND o.start_time <= ${maxTimestamp}
      AND t.timestamp >= (${minTimestamp} - INTERVAL '7 days')
      AND t.timestamp <= ${maxTimestamp}
      AND o.type::text = 'GENERATION'
  `);

  const baseUrl = env.NEXTAUTH_URL?.replace("/api/auth", "");
  for (const record of records) {
    yield {
      timestamp: record.start_time,
      langfuse_generation_name: record.name,
      langfuse_trace_name: record.trace_name,
      langfuse_trace_id: record.trace_id,
      langfuse_url: `${baseUrl}/project/${projectId}/traces/${encodeURIComponent(record.trace_id as string)}?observation=${encodeURIComponent(record.id as string)}`,
      langfuse_user_url: record.trace_user_id
        ? `${baseUrl}/project/${projectId}/users/${encodeURIComponent(record.trace_user_id as string)}`
        : undefined,
      langfuse_id: record.id,
      langfuse_cost_usd: record.total_cost,
      langfuse_input_units: record.input_tokens,
      langfuse_output_units: record.output_tokens,
      langfuse_total_units: record.total_tokens,
      langfuse_session_id: record.trace_session_id,
      langfuse_project_id: projectId,
      langfuse_project_name: projectName,
      langfuse_user_id: record.trace_user_id || null,
      langfuse_latency: record.latency,
      langfuse_time_to_first_token: record.time_to_first_token,
      langfuse_release: record.trace_release,
      langfuse_version: record.version,
      langfuse_model: record.model,
      langfuse_level: record.level,
      langfuse_tags: record.trace_tags,
      langfuse_environment: record.environment,
      langfuse_event_version: "1.0.0",
      posthog_session_id: record.posthog_session_id ?? null,
      mixpanel_session_id: record.mixpanel_session_id ?? null,
    } satisfies AnalyticsGenerationEvent;
  }
};

/**
 * Get observation counts grouped by project and day within a date range.
 *
 * Returns one row per project per day with the count of observations started on that day.
 * Uses half-open interval [startDate, endDate) for filtering based on start_time.
 *
 * @param startDate - Start of date range (inclusive)
 * @param endDate - End of date range (exclusive)
 * @returns Array of { count, projectId, date } objects
 *
 * @example
 * // Get observation counts for March 1-2, 2024
 * const counts = await getObservationCountsByProjectAndDay({
 *   startDate: new Date('2024-03-01T00:00:00Z'),
 *   endDate: new Date('2024-03-03T00:00:00Z')
 * });
 *
 * Note: Uses non-deduplicating reads for faster and cheaper queries.
 * queries against clickhouse. Generous 4x overcompensation before blocking allows
 * for usage aggregation to be meaningful.
 */
export const getObservationCountsByProjectAndDay = async ({
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
      DATE(start_time)::text as date
    FROM observations
    WHERE start_time >= ${startDate}
      AND start_time < ${endDate}
    GROUP BY project_id, DATE(start_time)
  `);

  return rows.map((row) => ({
    count: Number(row.count),
    projectId: row.project_id,
    date: row.date,
  }));
};

/**
 * Get total cost grouped by evaluator ID (job_configuration_id) for the last week.
 *
 * @param projectId - Project ID
 * @param evaluatorIds - Array of evaluator IDs (job_configuration_id from metadata)
 * @returns Array of { evaluatorId, totalCost } objects
 */
export const getCostByEvaluatorIds = async (
  projectId: string,
  evaluatorIds: string[],
): Promise<Array<{ evaluatorId: string; totalCost: number }>> => {
  if (evaluatorIds.length === 0) return [];

  const rows = await prisma.$queryRaw<
    Array<{ evaluator_id: string; total_cost: string | number }>
  >(Prisma.sql`
    SELECT
      metadata->>'job_configuration_id' as evaluator_id,
      COALESCE(SUM(total_cost), 0)::text as total_cost
    FROM observations
    WHERE project_id = ${projectId}
      AND metadata->>'job_configuration_id' IN (${Prisma.join(evaluatorIds)})
      AND type::text = 'GENERATION'
      AND start_time > (NOW() - INTERVAL '7 days')
    GROUP BY metadata->>'job_configuration_id'
  `);

  return rows.map((row) => ({
    evaluatorId: row.evaluator_id,
    totalCost: Number(row.total_cost),
  }));
};
