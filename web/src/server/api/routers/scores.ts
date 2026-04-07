import { z } from "zod/v4";

import { throwIfNoProjectAccess } from "@/src/features/rbac/utils/checkProjectAccess";
import { auditLog } from "@/src/features/audit-logs/auditLog";
import { composeAggregateScoreKey } from "@/src/features/scores/lib/aggregateScores";
import {
  getDateFromOption,
  SelectedTimeOptionSchema,
} from "@/src/utils/date-range-utils";
import {
  createTRPCRouter,
  protectedProjectProcedure,
} from "@/src/server/api/trpc";
import {
  orderBy,
  paginationZod,
  singleFilter,
  timeFilter,
  UpdateAnnotationScoreData,
  validateDbScore,
  LangfuseNotFoundError,
  InternalServerError,
  BatchActionQuerySchema,
  BatchActionType,
  ActionId,
  BatchExportTableName,
  type ScoreDomain,
  type FilterState,
  CreateAnnotationScoreData,
  type ScoreConfigDomain,
  ScoreSourceEnum,
  ScoreDataTypeEnum,
  CORRECTION_NAME,
} from "@langfuse/shared";
import {
  getScoresGroupedByNameSourceType,
  upsertScore,
  logger,
  getTraceById,
  getScoreById,
  convertDateToClickhouseDateTime,
  searchExistingAnnotationScore,
  hasAnyScore,
  ScoreDeleteQueue,
  QueueJobs,
  getScoreMetadataById,
  deleteScores,
  getTracesIdentifierForSession,
  validateConfigAgainstBody,
} from "@langfuse/shared/src/server";
import { v4 } from "uuid";
import { throwIfNoEntitlement } from "@/src/features/entitlements/server/hasEntitlement";
import { createBatchActionJob } from "@/src/features/table/server/createBatchActionJob";
import { TRPCError } from "@trpc/server";
import { randomUUID } from "crypto";
import {
  isNumericDataType,
  isTraceScore,
} from "@/src/features/scores/lib/helpers";
import { toDomainWithStringifiedMetadata } from "@/src/utils/clientSideDomainTypes";
import { ScoresApiService } from "@/src/features/public-api/server/scores-api-service";
import { Prisma, prisma as tracingPrisma } from "@langfuse/shared/src/db";

const ScoreFilterOptions = z.object({
  projectId: z.string(), // Required for protectedProjectProcedure
  filter: z.array(singleFilter),
  orderBy: orderBy,
});

const ScoreAllOptions = ScoreFilterOptions.extend({
  ...paginationZod,
});
type AllScoresReturnType = Omit<ScoreDomain, "metadata"> & {
  traceName: string | null;
  traceUserId: string | null;
  traceTags: Array<string> | null;
  jobConfigurationId: string | null;
  authorUserImage: string | null;
  authorUserName: string | null;
  hasMetadata: boolean;
};

type AllScoresFromEventsReturnType = Omit<ScoreDomain, "metadata"> & {
  jobConfigurationId: string | null;
  authorUserImage: string | null;
  authorUserName: string | null;
  hasMetadata: boolean;
};

const hasMetadata = (metadata: unknown): boolean =>
  typeof metadata === "object" &&
  metadata !== null &&
  !Array.isArray(metadata) &&
  Object.keys(metadata as Record<string, unknown>).length > 0;

const extractTimestampBounds = (
  timestampFilter?: Array<z.infer<typeof timeFilter>>,
) => {
  let fromTimestamp: string | undefined;
  let toTimestamp: string | undefined;

  for (const filter of timestampFilter ?? []) {
    if (filter.column !== "timestamp") continue;
    const value = new Date(filter.value).toISOString();
    if (filter.operator === ">" || filter.operator === ">=") {
      if (!fromTimestamp || value > fromTimestamp) fromTimestamp = value;
    }
    if (filter.operator === "<" || filter.operator === "<=") {
      if (!toTimestamp || value < toTimestamp) toTimestamp = value;
    }
  }

  return { fromTimestamp, toTimestamp };
};

const extractScoreQueryFromFilters = ({
  filter,
}: {
  filter: FilterState;
}): Partial<{
  traceId: string;
  userId: string;
  name: string;
  source: string;
  dataType: string;
  environment: string | string[];
  traceTags: string | string[];
  fromTimestamp: string;
  toTimestamp: string;
  observationId: string[];
  sessionId: string;
}> => {
  const result: Partial<{
    traceId: string;
    userId: string;
    name: string;
    source: string;
    dataType: string;
    environment: string | string[];
    traceTags: string | string[];
    fromTimestamp: string;
    toTimestamp: string;
    observationId: string[];
    sessionId: string;
  }> = {};

  for (const f of filter ?? []) {
    if (f.type === "datetime" && f.column === "timestamp") {
      const iso = new Date(f.value).toISOString();
      if (f.operator === ">" || f.operator === ">=") {
        if (!result.fromTimestamp || iso > result.fromTimestamp) {
          result.fromTimestamp = iso;
        }
      } else if (f.operator === "<" || f.operator === "<=") {
        if (!result.toTimestamp || iso < result.toTimestamp) {
          result.toTimestamp = iso;
        }
      }
    }

    if (f.type === "string" && f.operator === "=") {
      if (f.column === "traceId") result.traceId = f.value;
      if (f.column === "name") result.name = f.value;
      if (f.column === "source") result.source = f.value;
      if (f.column === "dataType") result.dataType = f.value;
      if (f.column === "userId") result.userId = f.value;
      if (f.column === "sessionId") result.sessionId = f.value;
      if (f.column === "observationId") result.observationId = [f.value];
      if (f.column === "environment") result.environment = f.value;
    }

    if (
      (f.type === "stringOptions" || f.type === "categoryOptions") &&
      f.operator === "any of"
    ) {
      if (f.column === "userId" && f.value.length > 0)
        result.userId = f.value[0];
      if (f.column === "name" && f.value.length > 0) result.name = f.value[0];
      if (f.column === "source" && f.value.length > 0)
        result.source = f.value[0];
      if (f.column === "dataType" && f.value.length > 0)
        result.dataType = f.value[0];
      if (f.column === "environment") result.environment = f.value;
    }

    if (
      f.type === "arrayOptions" &&
      (f.column === "trace_tags" || f.column === "tags") &&
      f.operator === "any of"
    ) {
      result.traceTags = f.value;
    }
  }

  return result;
};

const getTraceMetadataMap = async ({
  projectId,
  traceIds,
}: {
  projectId: string;
  traceIds: string[];
}) => {
  if (traceIds.length === 0)
    return new Map<
      string,
      { name: string | null; userId: string | null; tags: string[] | null }
    >();

  const rows = await tracingPrisma.$queryRaw<
    Array<{
      id: string;
      name: string | null;
      user_id: string | null;
      tags: string[] | null;
    }>
  >(Prisma.sql`
    SELECT DISTINCT ON (t.id, t.project_id)
      t.id,
      t.name,
      t.user_id,
      t.tags
    FROM clickhouse.traces t
    WHERE t.project_id = ${projectId}
      AND t.is_deleted = false
      AND t.id IN (${Prisma.join(traceIds)})
    ORDER BY t.id, t.project_id, t.event_ts DESC
  `);

  return new Map(
    rows.map((r) => [r.id, { name: r.name, userId: r.user_id, tags: r.tags }]),
  );
};

const getScoresForUiFromYb = async ({
  input,
  ctx,
}: {
  input: z.infer<typeof ScoreAllOptions>;
  ctx: { prisma: typeof tracingPrisma };
}): Promise<AllScoresReturnType[]> => {
  const service = new ScoresApiService("v2");
  const extracted = extractScoreQueryFromFilters({
    filter: input.filter ?? [],
  });
  const page = input.page + 1; // UI is 0-based, public-api service expects 1-based

  const items = (await service.generateScoresForPublicApi({
    projectId: input.projectId,
    page,
    limit: input.limit,
    fields: ["score", "trace"],
    ...extracted,
  })) as Array<
    ScoreDomain & {
      trace?: { userId?: string | null; tags?: string[] | null } | null;
    }
  >;

  const traceIds = Array.from(
    new Set(
      items.map((s) => s.traceId).filter((id): id is string => Boolean(id)),
    ),
  );
  const traceMetaById = await getTraceMetadataMap({
    projectId: input.projectId,
    traceIds,
  });

  const [jobExecutions, users] = await Promise.all([
    ctx.prisma.jobExecution.findMany({
      where: {
        projectId: input.projectId,
        jobOutputScoreId: {
          in: items.map((score) => score.id),
        },
      },
      select: {
        id: true,
        jobConfigurationId: true,
        jobOutputScoreId: true,
      },
    }),
    ctx.prisma.user.findMany({
      where: {
        id: {
          in: items
            .map((score) => score.authorUserId)
            .filter((s): s is string => Boolean(s)),
        },
      },
      select: {
        id: true,
        name: true,
        image: true,
      },
    }),
  ]);

  return items.map<AllScoresReturnType>((score) => {
    const jobExecution = jobExecutions.find(
      (je) => je.jobOutputScoreId === score.id,
    );
    const user = users.find((u) => u.id === score.authorUserId);
    const traceMeta = score.traceId
      ? traceMetaById.get(score.traceId)
      : undefined;

    return {
      ...score,
      longStringValue: score.longStringValue ?? "",
      metadata: undefined as never,
      traceName: traceMeta?.name ?? null,
      traceUserId: traceMeta?.userId ?? score.trace?.userId ?? null,
      traceTags: traceMeta?.tags ?? score.trace?.tags ?? null,
      jobConfigurationId: jobExecution?.jobConfigurationId ?? null,
      authorUserImage: user?.image ?? null,
      authorUserName: user?.name ?? null,
      hasMetadata: hasMetadata((score as { metadata?: unknown }).metadata),
    };
  });
};

const getScoreFilterOptionsFromYb = async ({
  projectId,
  timestampFilter,
}: {
  projectId: string;
  timestampFilter?: Array<z.infer<typeof timeFilter>>;
}) => {
  const { fromTimestamp, toTimestamp } =
    extractTimestampBounds(timestampFilter);

  const timeSql = Prisma.sql`
    ${fromTimestamp ? Prisma.sql`AND s.timestamp >= ${new Date(fromTimestamp)}` : Prisma.empty}
    ${toTimestamp ? Prisma.sql`AND s.timestamp <= ${new Date(toTimestamp)}` : Prisma.empty}
  `;

  const [names, stringValues, traceNames, userIds, tags] = await Promise.all([
    tracingPrisma.$queryRaw<Array<{ value: string; count: bigint }>>(Prisma.sql`
      SELECT s.name AS value, count(*)::bigint AS count
      FROM clickhouse.scores s
      WHERE s.project_id = ${projectId}
        AND s.is_deleted = false
        ${timeSql}
      GROUP BY s.name
      ORDER BY count(*) DESC
      LIMIT 1000
    `),
    tracingPrisma.$queryRaw<Array<{ value: string; count: bigint }>>(Prisma.sql`
      SELECT s.string_value AS value, count(*)::bigint AS count
      FROM clickhouse.scores s
      WHERE s.project_id = ${projectId}
        AND s.is_deleted = false
        AND s.string_value IS NOT NULL
        ${timeSql}
      GROUP BY s.string_value
      ORDER BY count(*) DESC
      LIMIT 1000
    `),
    tracingPrisma.$queryRaw<Array<{ value: string; count: bigint }>>(Prisma.sql`
      SELECT t.name AS value, count(*)::bigint AS count
      FROM clickhouse.traces t
      WHERE t.project_id = ${projectId}
        AND t.is_deleted = false
      GROUP BY t.name
      ORDER BY count(*) DESC
      LIMIT 1000
    `),
    tracingPrisma.$queryRaw<Array<{ value: string; count: bigint }>>(Prisma.sql`
      SELECT t.user_id AS value, count(*)::bigint AS count
      FROM clickhouse.traces t
      WHERE t.project_id = ${projectId}
        AND t.is_deleted = false
        AND t.user_id IS NOT NULL
      GROUP BY t.user_id
      ORDER BY count(*) DESC
      LIMIT 1000
    `),
    tracingPrisma.$queryRaw<Array<{ value: string }>>(Prisma.sql`
      SELECT DISTINCT unnest(t.tags) AS value
      FROM clickhouse.traces t
      WHERE t.project_id = ${projectId}
        AND t.is_deleted = false
        AND t.tags IS NOT NULL
      LIMIT 1000
    `),
  ]);

  return {
    name: names.map((n) => ({ value: n.value, count: Number(n.count) })),
    tags: tags.map((t) => ({ value: t.value })),
    traceName: traceNames.map((n) => ({
      value: n.value,
      count: Number(n.count),
    })),
    userId: userIds.map((u) => ({ value: u.value, count: Number(u.count) })),
    stringValue: stringValues.map((s) => ({
      value: s.value,
      count: Number(s.count),
    })),
  };
};

export const scoresRouter = createTRPCRouter({
  /**
   * Get all scores for a project, meant for internal use and *excludes metadata of scores*
   */
  all: protectedProjectProcedure
    .input(ScoreAllOptions)
    .query(async ({ input, ctx }) => {
      const scores = await getScoresForUiFromYb({ input, ctx });
      return { scores };
    }),
  byId: protectedProjectProcedure
    .input(
      z.object({
        scoreId: z.string(), // used for matching
        projectId: z.string(), // used for security check
      }),
    )
    .query(async ({ input }) => {
      const score = await new ScoresApiService("v2").getScoreById({
        projectId: input.projectId,
        scoreId: input.scoreId,
      });
      if (!score) {
        throw new TRPCError({
          code: "NOT_FOUND",
          message: `No score with id ${input.scoreId} in project ${input.projectId}`,
        });
      }
      return toDomainWithStringifiedMetadata(score);
    }),
  countAll: protectedProjectProcedure
    .input(ScoreAllOptions)
    .query(async ({ input }) => {
      const extracted = extractScoreQueryFromFilters({
        filter: input.filter ?? [],
      });
      const totalCount = await new ScoresApiService(
        "v2",
      ).getScoresCountForPublicApi({
        projectId: input.projectId,
        page: input.page + 1,
        limit: input.limit,
        fields: ["score", "trace"],
        ...extracted,
      });

      return {
        totalCount,
      };
    }),
  /**
   * v4: Get all scores without traces JOIN. Trace metadata loaded via metricsFromEvents.
   */
  allFromEvents: protectedProjectProcedure
    .input(ScoreAllOptions)
    .query(async ({ input, ctx }) => {
      const scores = await getScoresForUiFromYb({ input, ctx });
      return {
        scores: scores.map<AllScoresFromEventsReturnType>((score) => ({
          ...score,
          hasMetadata: score.hasMetadata,
        })),
      };
    }),
  /**
   * v4: Count scores without traces JOIN.
   */
  countAllFromEvents: protectedProjectProcedure
    .input(ScoreAllOptions)
    .query(async ({ input }) => {
      const extracted = extractScoreQueryFromFilters({
        filter: input.filter ?? [],
      });
      const totalCount = await new ScoresApiService(
        "v2",
      ).getScoresCountForPublicApi({
        projectId: input.projectId,
        page: input.page + 1,
        limit: input.limit,
        fields: ["score", "trace"],
        ...extracted,
      });

      return {
        totalCount,
      };
    }),
  /**
   * v4: Load trace metadata (name, userId, tags) via eventsTracesAggregation
   * builder for a page of scores.
   */
  metricsFromEvents: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        traceIds: z.array(z.string()),
      }),
    )
    .query(async ({ input }) => {
      if (input.traceIds.length === 0) return [];
      const traceMetaById = await getTraceMetadataMap({
        projectId: input.projectId,
        traceIds: input.traceIds,
      });
      return input.traceIds
        .map((traceId) => {
          const row = traceMetaById.get(traceId);
          if (!row) return null;
          return {
            traceId,
            traceName: row.name ?? null,
            userId: row.userId ?? null,
            tags: row.tags && row.tags.length > 0 ? row.tags : null,
          };
        })
        .filter((row): row is NonNullable<typeof row> => Boolean(row));
    }),
  /**
   * v4: Filter options via events-backed aggregations instead of traces table.
   */
  filterOptionsFromEvents: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        timestampFilter: z.array(timeFilter).optional(),
      }),
    )
    .query(async ({ input }) => {
      return getScoreFilterOptionsFromYb({
        projectId: input.projectId,
        timestampFilter: input.timestampFilter,
      });
    }),
  filterOptions: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        timestampFilter: z.array(timeFilter).optional(),
      }),
    )
    .query(async ({ input }) => {
      return getScoreFilterOptionsFromYb({
        projectId: input.projectId,
        timestampFilter: input.timestampFilter,
      });
    }),
  deleteMany: protectedProjectProcedure
    .input(
      z.object({
        scoreIds: z
          .array(z.string())
          .min(1, "Minimum 1 scoreId is required.")
          .nullable(),
        projectId: z.string(),
        query: BatchActionQuerySchema.optional(),
        isBatchAction: z.boolean().default(false),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      // We reuse the trace-deletion entitlement here as this is a very similar and destructive operation.
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "traces:delete",
      });

      throwIfNoEntitlement({
        entitlement: "trace-deletion",
        projectId: input.projectId,
        sessionUser: ctx.session.user,
      });

      if (input.isBatchAction && input.query) {
        return createBatchActionJob({
          projectId: input.projectId,
          actionId: ActionId.ScoreDelete,
          actionType: BatchActionType.Delete,
          tableName: BatchExportTableName.Scores,
          session: ctx.session,
          query: input.query,
        });
      }
      if (input.scoreIds) {
        const scoreDeleteQueue = ScoreDeleteQueue.getInstance();
        if (!scoreDeleteQueue) {
          throw new TRPCError({
            code: "INTERNAL_SERVER_ERROR",
            message: "ScoreDeleteQueue not initialized",
          });
        }

        await Promise.all(
          input.scoreIds.map((scoreId) =>
            auditLog({
              resourceType: "score",
              resourceId: scoreId,
              action: "delete",
              session: ctx.session,
            }),
          ),
        );

        return scoreDeleteQueue.add(QueueJobs.ScoreDelete, {
          timestamp: new Date(),
          id: randomUUID(),
          payload: {
            projectId: input.projectId,
            scoreIds: input.scoreIds,
          },
          name: QueueJobs.ScoreDelete,
        });
      }
      throw new TRPCError({
        message:
          "Either batchAction or scoreIds must be provided to delete scores.",
        code: "BAD_REQUEST",
      });
    }),
  createAnnotationScore: protectedProjectProcedure
    .input(CreateAnnotationScoreData)
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "scores:CUD",
      });

      const inflatedParams = isTraceScore(input.scoreTarget)
        ? {
            observationId: input.scoreTarget.observationId ?? null,
            traceId: input.scoreTarget.traceId,
            sessionId: null,
          }
        : {
            observationId: null,
            traceId: null,
            sessionId: input.scoreTarget.sessionId,
          };

      if (inflatedParams.traceId) {
        const clickhouseTrace = await getTraceById({
          traceId: inflatedParams.traceId,
          projectId: input.projectId,
          clickhouseFeatureTag: "annotations-trpc",
        });

        if (!clickhouseTrace) {
          logger.error(
            `No trace with id ${inflatedParams.traceId} in project ${input.projectId} in Clickhouse`,
          );
          throw new LangfuseNotFoundError(
            `No trace with id ${inflatedParams.traceId} in project ${input.projectId} in Clickhouse`,
          );
        }
      } else if (inflatedParams.sessionId) {
        // We consider no longer writing all sessions into postgres, hence we should search for traces with the session id
        const traceIdentifiers = await getTracesIdentifierForSession(
          input.projectId,
          inflatedParams.sessionId,
        );
        if (traceIdentifiers.length === 0) {
          logger.error(
            `No trace referencing session with id ${inflatedParams.sessionId} in project ${input.projectId} in Clickhouse`,
          );
          throw new LangfuseNotFoundError(
            `No trace referencing session with id ${inflatedParams.sessionId} in project ${input.projectId} in Clickhouse`,
          );
        }
      }

      const clickhouseScore = await searchExistingAnnotationScore(
        input.projectId,
        inflatedParams.observationId,
        inflatedParams.traceId,
        inflatedParams.sessionId,
        input.name,
        input.configId,
        input.dataType,
      );

      const timestamp = input.timestamp ?? new Date();

      const score = !!clickhouseScore
        ? {
            ...clickhouseScore,
            value: input.value,
            stringValue: input.stringValue ?? null,
            comment: input.comment ?? null,
            metadata: {},
            authorUserId: ctx.session.user.id,
            queueId: input.queueId ?? null,
            timestamp,
          }
        : {
            id: input.id ?? v4(),
            projectId: input.projectId,
            environment: input.environment ?? "default",
            ...inflatedParams,
            // only trace and session scores are supported for annotation
            datasetRunId: null,
            value: input.value,
            stringValue: input.stringValue ?? null,
            dataType: input.dataType ?? null,
            configId: input.configId ?? null,
            name: input.name,
            comment: input.comment ?? null,
            metadata: {},
            authorUserId: ctx.session.user.id,
            source: ScoreSourceEnum.ANNOTATION,
            queueId: input.queueId ?? null,
            executionTraceId: null,
            createdAt: new Date(),
            updatedAt: new Date(),
            timestamp,
          };

      await upsertScore({
        id: score.id, // Reuse ID that was generated by Prisma
        timestamp: convertDateToClickhouseDateTime(timestamp),
        project_id: input.projectId,
        environment: input.environment ?? "default",
        trace_id: inflatedParams.traceId,
        observation_id: inflatedParams.observationId,
        session_id: inflatedParams.sessionId,
        name: input.name,
        value: input.value,
        source: ScoreSourceEnum.ANNOTATION,
        comment: input.comment,
        author_user_id: ctx.session.user.id,
        config_id: input.configId,
        data_type: input.dataType,
        string_value: input.stringValue,
        queue_id: input.queueId,
        created_at: convertDateToClickhouseDateTime(score.createdAt),
        updated_at: convertDateToClickhouseDateTime(score.updatedAt),
        metadata: score.metadata as Record<string, string>,
      });

      await auditLog({
        session: ctx.session,
        resourceType: "score",
        resourceId: score.id,
        action: "create",
        after: score,
      });

      return validateDbScore(score);
    }),
  updateAnnotationScore: protectedProjectProcedure
    .input(UpdateAnnotationScoreData)
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "scores:CUD",
      });

      let updatedScore: ScoreDomain | null | undefined = null;

      // Fetch the current score from Clickhouse
      const score = await getScoreById({
        projectId: input.projectId,
        scoreId: input.id,
        source: ScoreSourceEnum.ANNOTATION,
      });

      if (!score) {
        // Clickhouse is eventually consistent; if client provided timestamp, we can upsert along the ordering key
        if (!input.timestamp) {
          logger.warn(
            `No annotation score with id ${input.id} in project ${input.projectId} in Clickhouse, and no timestamp provided`,
          );
          throw new LangfuseNotFoundError(
            `No annotation score with id ${input.id} in project ${input.projectId} in Clickhouse`,
          );
        }

        logger.info(
          `Score ${input.id} not found in ClickHouse for project ${input.projectId}, upserting with provided timestamp`,
        );

        // Validate config if provided
        const config = await ctx.prisma.scoreConfig.findFirst({
          where: {
            id: input.configId,
            projectId: input.projectId,
          },
        });
        if (!config) {
          throw new LangfuseNotFoundError(
            `No score config with id ${input.configId} in project ${input.projectId}`,
          );
        }

        // Upsert with provided data
        const inflatedParams = isTraceScore(input.scoreTarget)
          ? {
              observationId: input.scoreTarget.observationId ?? null,
              traceId: input.scoreTarget.traceId,
              sessionId: null,
            }
          : {
              observationId: null,
              traceId: null,
              sessionId: input.scoreTarget.sessionId,
            };

        if (inflatedParams.traceId) {
          const clickhouseTrace = await getTraceById({
            traceId: inflatedParams.traceId,
            projectId: input.projectId,
            clickhouseFeatureTag: "annotations-trpc",
          });

          if (!clickhouseTrace) {
            logger.error(
              `No trace with id ${inflatedParams.traceId} in project ${input.projectId} in Clickhouse`,
            );
            throw new LangfuseNotFoundError(
              `No trace with id ${inflatedParams.traceId} in project ${input.projectId} in Clickhouse`,
            );
          }
        } else if (inflatedParams.sessionId) {
          // We consider no longer writing all sessions into postgres, hence we should search for traces with the session id
          const traceIdentifiers = await getTracesIdentifierForSession(
            input.projectId,
            inflatedParams.sessionId,
          );
          if (traceIdentifiers.length === 0) {
            logger.error(
              `No trace referencing session with id ${inflatedParams.sessionId} in project ${input.projectId} in Clickhouse`,
            );
            throw new LangfuseNotFoundError(
              `No trace referencing session with id ${inflatedParams.sessionId} in project ${input.projectId} in Clickhouse`,
            );
          }
        }

        const timestamp = input.timestamp;

        await upsertScore({
          id: input.id,
          timestamp: convertDateToClickhouseDateTime(timestamp),
          project_id: input.projectId,
          environment: input.environment ?? "default",
          trace_id: inflatedParams.traceId,
          observation_id: inflatedParams.observationId,
          session_id: inflatedParams.sessionId,
          name: input.name,
          value: input.value,
          source: ScoreSourceEnum.ANNOTATION,
          comment: input.comment,
          author_user_id: ctx.session.user.id,
          config_id: input.configId,
          data_type: input.dataType,
          string_value: input.stringValue,
          queue_id: input.queueId,
          created_at: convertDateToClickhouseDateTime(new Date()),
          updated_at: convertDateToClickhouseDateTime(new Date()),
          metadata: {},
        });

        const baseScore = {
          id: input.id,
          projectId: input.projectId,
          environment: input.environment ?? "default",
          traceId: inflatedParams.traceId,
          observationId: inflatedParams.observationId,
          sessionId: inflatedParams.sessionId,
          datasetRunId: null,
          name: input.name,
          value: input.value,
          dataType: input.dataType,
          configId: input.configId ?? null,
          metadata: {},
          executionTraceId: null,
          createdAt: new Date(),
          updatedAt: new Date(),
          source: ScoreSourceEnum.ANNOTATION,
          comment: input.comment ?? null,
          authorUserId: ctx.session.user.id,
          queueId: input.queueId ?? null,
          timestamp,
          longStringValue: "",
        };

        if (isNumericDataType(baseScore.dataType)) {
          updatedScore = {
            ...baseScore,
            dataType: ScoreDataTypeEnum.NUMERIC,
            stringValue: null,
          };
        } else {
          updatedScore = {
            ...baseScore,
            dataType: input.dataType as "CATEGORICAL" | "BOOLEAN",
            stringValue: input.stringValue!,
          };
        }

        await auditLog({
          session: ctx.session,
          resourceType: "score",
          resourceId: input.id,
          action: "update",
          after: updatedScore,
        });
      } else {
        // validate score against config
        if (score.configId) {
          const config = await ctx.prisma.scoreConfig.findFirst({
            where: {
              id: score.configId,
              projectId: input.projectId,
            },
          });
          if (!config) {
            throw new LangfuseNotFoundError(
              `No score config with id ${score.configId} in project ${input.projectId}`,
            );
          }
          try {
            validateConfigAgainstBody({
              body: {
                ...score,
                value: input.value,
                stringValue: isNumericDataType(score.dataType)
                  ? null
                  : input.stringValue!,
                comment: input.comment ?? null,
              } as ScoreDomain,
              config: config as ScoreConfigDomain,
              context: "ANNOTATION",
            });
          } catch {
            throw new TRPCError({
              code: "PRECONDITION_FAILED",
              message:
                "Score does not comply with config schema. Please adjust or delete score.",
            });
          }
        }

        await upsertScore({
          id: input.id,
          project_id: input.projectId,
          timestamp: convertDateToClickhouseDateTime(score.timestamp),
          value: input.value !== null ? input.value : undefined,
          string_value: input.stringValue,
          comment: input.comment,
          author_user_id: ctx.session.user.id,
          queue_id: input.queueId,
          source: ScoreSourceEnum.ANNOTATION,
          name: score.name,
          data_type: score.dataType,
          config_id: score.configId,
          trace_id: score.traceId,
          observation_id: score.observationId,
          session_id: score.sessionId,
          environment: score.environment,
          created_at: convertDateToClickhouseDateTime(score.createdAt),
          updated_at: convertDateToClickhouseDateTime(score.updatedAt),
          metadata: score.metadata as Record<string, string>,
        });

        const baseScore = {
          ...score,
          value: input.value,
          comment: input.comment ?? null,
          authorUserId: ctx.session.user.id,
          queueId: input.queueId ?? null,
          timestamp: score.timestamp,
        };

        if (isNumericDataType(score.dataType)) {
          updatedScore = {
            ...baseScore,
            dataType: ScoreDataTypeEnum.NUMERIC,
            stringValue: null,
          };
        } else {
          updatedScore = {
            ...baseScore,
            dataType: input.dataType as "CATEGORICAL" | "BOOLEAN",
            stringValue: input.stringValue!,
          };
        }

        await auditLog({
          session: ctx.session,
          resourceType: "score",
          resourceId: input.id,
          action: "update",
          before: score,
          after: updatedScore,
        });
      }

      if (!updatedScore) {
        logger.error(
          `Annotation score ${input.id} could not be updated in project ${input.projectId}`,
        );
        throw new InternalServerError(
          `Annotation score could not be updated in project ${input.projectId}`,
        );
      }

      return validateDbScore(updatedScore);
    }),
  deleteAnnotationScore: protectedProjectProcedure
    .input(z.object({ projectId: z.string(), id: z.string() }))
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "scores:CUD",
      });

      // Fetch the current score from Clickhouse
      const clickhouseScore = await getScoreById({
        projectId: input.projectId,
        scoreId: input.id,
        source: ScoreSourceEnum.ANNOTATION,
      });
      if (!clickhouseScore) {
        logger.warn(
          `No annotation score with id ${input.id} in project ${input.projectId} in Clickhouse`,
        );
        throw new LangfuseNotFoundError(
          `No annotation score with id ${input.id} in project ${input.projectId} in Clickhouse`,
        );
      }

      await auditLog({
        session: ctx.session,
        resourceType: "score",
        resourceId: input.id,
        action: "delete",
        before: clickhouseScore,
      });

      await deleteScores(input.projectId, [clickhouseScore.id]);

      return validateDbScore(clickhouseScore);
    }),
  upsertCorrection: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        id: z.string(),
        timestamp: z.date(),
        traceId: z.string(),
        observationId: z.string().optional(),
        value: z.string(),
        environment: z.string().optional(),
        queueId: z.string().optional(),
      }),
    )
    .mutation(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "scores:CUD",
      });

      const clickhouseTrace = await getTraceById({
        traceId: input.traceId,
        projectId: input.projectId,
        clickhouseFeatureTag: "annotations-trpc",
      });

      if (!clickhouseTrace) {
        logger.error(
          `No trace with id ${input.traceId} in project ${input.projectId} in Clickhouse`,
        );
        throw new LangfuseNotFoundError(
          `No trace with id ${input.traceId} in project ${input.projectId} in Clickhouse`,
        );
      }

      const clickhouseScore = await searchExistingAnnotationScore(
        input.projectId,
        input.observationId ?? null,
        input.traceId,
        null,
        CORRECTION_NAME,
        undefined,
        ScoreDataTypeEnum.CORRECTION,
      );

      const timestamp = input.timestamp;

      const score = !!clickhouseScore
        ? {
            ...clickhouseScore,
            value: 0,
            stringValue: null,
            comment: null,
            metadata: {},
            authorUserId: ctx.session.user.id,
            queueId: input.queueId ?? null,
            longStringValue: input.value,
          }
        : {
            id: input.id,
            projectId: input.projectId,
            environment: input.environment ?? "default",
            traceId: input.traceId,
            observationId: input.observationId ?? null,
            sessionId: null,
            // only trace and session scores are supported for annotation
            datasetRunId: null,
            value: 0,
            stringValue: null,
            dataType: ScoreDataTypeEnum.CORRECTION,
            configId: null,
            name: CORRECTION_NAME,
            comment: null,
            metadata: {},
            authorUserId: ctx.session.user.id,
            source: ScoreSourceEnum.ANNOTATION,
            queueId: input.queueId ?? null,
            executionTraceId: null,
            createdAt: new Date(),
            updatedAt: new Date(),
            timestamp,
            longStringValue: input.value,
          };

      await upsertScore({
        id: score.id, // Reuse ID that was generated by Prisma
        timestamp: convertDateToClickhouseDateTime(timestamp),
        project_id: input.projectId,
        environment: input.environment ?? "default",
        trace_id: input.traceId,
        observation_id: input.observationId ?? null,
        session_id: null,
        name: CORRECTION_NAME,
        value: 0,
        source: ScoreSourceEnum.ANNOTATION,
        comment: null,
        author_user_id: ctx.session.user.id,
        config_id: null,
        data_type: ScoreDataTypeEnum.CORRECTION,
        string_value: null,
        queue_id: input.queueId ?? null,
        created_at: convertDateToClickhouseDateTime(score.createdAt),
        updated_at: convertDateToClickhouseDateTime(score.updatedAt),
        metadata: score.metadata as Record<string, string>,
        long_string_value: input.value,
      });

      await auditLog({
        session: ctx.session,
        resourceType: "score",
        resourceId: score.id,
        action: "create",
        after: score,
      });

      return validateDbScore(score);
    }),
  /**
   * @deprecated, use getScoreColumns instead
   */
  getScoreKeysAndProps: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        selectedTimeOption: SelectedTimeOptionSchema,
      }),
    )
    .query(async ({ input }) => {
      const date = getDateFromOption(input.selectedTimeOption);
      const res = await getScoresGroupedByNameSourceType({
        projectId: input.projectId,
        fromTimestamp: date,
        filter: [],
      });
      return res.map(({ name, source, dataType }) => ({
        key: composeAggregateScoreKey({ name, source, dataType }),
        name: name,
        source: source,
        dataType: dataType,
      }));
    }),
  getScoreColumns: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        filter: z.array(singleFilter).optional(),
        fromTimestamp: z.date().optional(),
        toTimestamp: z.date().optional(),
      }),
    )
    .query(async ({ input }) => {
      const { projectId, filter, fromTimestamp, toTimestamp } = input;

      const groupedScores = await getScoresGroupedByNameSourceType({
        projectId,
        filter: filter || [],
        fromTimestamp,
        toTimestamp,
      });

      const scoreColumns = groupedScores.map(({ name, source, dataType }) => ({
        key: composeAggregateScoreKey({ name, source, dataType }),
        name,
        source,
        dataType,
      }));

      return { scoreColumns };
    }),
  hasAny: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
      }),
    )
    .query(async ({ input }) => {
      return await hasAnyScore(input.projectId);
    }),
  getScoreMetadataById: protectedProjectProcedure
    .input(z.object({ projectId: z.string(), id: z.string() }))
    .query(async ({ input }) => {
      return (await getScoreMetadataById(input.projectId, input.id)) ?? null;
    }),
});
