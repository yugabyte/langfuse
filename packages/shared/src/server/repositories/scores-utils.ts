import { ScoreDataTypeType, ScoreDomain, ScoreSourceType } from "../../domain";
import { PreferredClickhouseService } from "../clickhouse/client";
import { ScoreRecordReadType } from "./definitions";
import { convertClickhouseScoreToDomain } from "./scores_converters";
import { tracingPrisma as prisma } from "../../db";
import { Prisma } from "@prisma/client";

type PgScoreRow = {
  id: string;
  timestamp: Date;
  project_id: string;
  name: string | null;
  value: number | null;
  source: string | null;
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
};

const toClickhouseDateTimeString = (value: Date | null | undefined) =>
  value ? value.toISOString().replace("T", " ").replace("Z", "") : "";

const toMetadataRecord = (value: unknown): Record<string, string> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([k, v]) => [
      k,
      typeof v === "string" ? v : JSON.stringify(v),
    ]),
  );
};

const mapRowToReadType = (row: PgScoreRow): ScoreRecordReadType => ({
  id: row.id,
  timestamp: toClickhouseDateTimeString(row.timestamp),
  project_id: row.project_id,
  name: row.name ?? "",
  value: row.value ?? 0,
  source: row.source ?? "API",
  author_user_id: row.author_user_id,
  comment: row.comment,
  trace_id: row.trace_id,
  observation_id: row.observation_id,
  config_id: row.config_id,
  string_value: row.string_value,
  queue_id: row.queue_id,
  created_at: toClickhouseDateTimeString(row.created_at),
  updated_at: toClickhouseDateTimeString(row.updated_at),
  data_type: row.data_type,
  metadata: toMetadataRecord(row.metadata),
  session_id: row.session_id,
  dataset_run_id: row.dataset_run_id,
  environment: row.environment ?? "default",
  long_string_value: row.long_string_value ?? "",
  execution_trace_id: row.execution_trace_id,
  event_ts: toClickhouseDateTimeString(row.event_ts),
  is_deleted: row.is_deleted ? 1 : 0,
});

/**
 * @internal
 * Internal utility function for getting scores by ID.
 * Do not use directly - use ScoresApiService or repository functions instead.
 */
export const _handleGetScoreById = async ({
  projectId,
  scoreId,
  source,
  scoreScope,
  scoreDataTypes,
  preferredClickhouseService: _preferredClickhouseService,
}: {
  projectId: string;
  scoreId: string;
  source?: ScoreSourceType;
  scoreScope: "traces_only" | "all";
  scoreDataTypes?: readonly ScoreDataTypeType[];
  preferredClickhouseService?: PreferredClickhouseService;
}): Promise<ScoreDomain | undefined> => {
  const rows = await prisma.$queryRaw<PgScoreRow[]>(Prisma.sql`
    SELECT DISTINCT ON (s.id, s.project_id) s.*
    FROM scores s
    WHERE s.project_id = ${projectId}
      AND s.id = ${scoreId}
      ${scoreDataTypes ? Prisma.sql`AND s.data_type::text IN (${Prisma.join(scoreDataTypes as readonly string[])})` : Prisma.empty}
      ${source ? Prisma.sql`AND s.source = ${source}` : Prisma.empty}
      ${scoreScope === "traces_only" ? Prisma.sql`AND s.session_id IS NULL AND s.dataset_run_id IS NULL` : Prisma.empty}
    ORDER BY s.id, s.project_id, s.event_ts DESC
    LIMIT 1
  `);
  return rows
    .map((row) => convertClickhouseScoreToDomain(mapRowToReadType(row)))
    .shift();
};

/**
 * @internal
 * Internal utility function for getting scores by ID.
 * Do not use directly - use ScoresApiService or repository functions instead.
 */
export const _handleGetScoresByIds = async ({
  projectId,
  scoreId,
  source,
  scoreScope,
  dataTypes,
}: {
  projectId: string;
  scoreId: string[];
  source?: ScoreSourceType;
  scoreScope: "traces_only" | "all";
  dataTypes?: readonly ScoreDataTypeType[];
}): Promise<ScoreDomain[]> => {
  const rows = await prisma.$queryRaw<PgScoreRow[]>(Prisma.sql`
    SELECT DISTINCT ON (s.id, s.project_id) s.*
    FROM scores s
    WHERE s.project_id = ${projectId}
      AND s.id IN (${Prisma.join(scoreId)})
      ${source ? Prisma.sql`AND s.source = ${source}` : Prisma.empty}
      ${scoreScope === "traces_only" ? Prisma.sql`AND s.session_id IS NULL AND s.dataset_run_id IS NULL` : Prisma.empty}
      ${dataTypes ? Prisma.sql`AND s.data_type::text IN (${Prisma.join(dataTypes as readonly string[])})` : Prisma.empty}
    ORDER BY s.id, s.project_id, s.event_ts DESC
  `);
  return rows.map((row) =>
    convertClickhouseScoreToDomain(mapRowToReadType(row)),
  );
};
