import type { Observation } from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";

import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";

import {
  GetObservationsV2Query,
  GetObservationsV2Response,
  encodeCursor,
  transformDbToApiObservation,
} from "@/src/features/public-api/types/observations";
import { generateObservationsV2ForPublicApi } from "@/src/features/public-api/server/observations";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Observations V2",
    querySchema: GetObservationsV2Query,
    responseSchema: GetObservationsV2Response,
    fn: async ({ query, auth }) => {
      const filterProps = {
        projectId: auth.scope.projectId,
        limit: query.limit,
        traceId: query.traceId ?? undefined,
        userId: query.userId ?? undefined,
        level: query.level ?? undefined,
        name: query.name ?? undefined,
        type: query.type ?? undefined,
        environment: query.environment ?? undefined,
        parentObservationId: query.parentObservationId ?? undefined,
        fromStartTime: query.fromStartTime ?? undefined,
        toStartTime: query.toStartTime ?? undefined,
        version: query.version ?? undefined,
        cursor: query.cursor ?? undefined,
      };

      const items = await generateObservationsV2ForPublicApi({
        props: filterProps,
        advancedFilters: query.filter,
      });

      // Determine if there are more results (we fetched limit+1)
      const hasMore = items.length > query.limit;
      const dataToReturn = hasMore ? items.slice(0, query.limit) : items;

      const uniqueModels: string[] = Array.from(
        new Set(
          dataToReturn
            .map((r: Observation) => r.internalModelId)
            .filter((r): r is string => Boolean(r)),
        ),
      );

      const models =
        uniqueModels.length > 0
          ? await prisma.model.findMany({
              where: {
                id: {
                  in: uniqueModels,
                },
                OR: [{ projectId: auth.scope.projectId }, { projectId: null }],
              },
              include: {
                Price: true,
              },
            })
          : [];
      const modelById = new Map(models.map((model) => [model.id, model]));

      const transformedItems = dataToReturn
        .map((i: Observation) => {
          const model = i.internalModelId
            ? modelById.get(i.internalModelId)
            : undefined;
          return {
            ...i,
            modelId: model?.id ?? null,
            inputPrice:
              model?.Price?.find((m) => m.usageType === "input")?.price ?? null,
            outputPrice:
              model?.Price?.find((m) => m.usageType === "output")?.price ??
              null,
            totalPrice:
              model?.Price?.find((m) => m.usageType === "total")?.price ?? null,
          };
        })
        .map(transformDbToApiObservation);

      // Generate cursor if there are more results
      const lastItemIdx = dataToReturn.length - 1;
      const meta =
        hasMore && dataToReturn.length > 0
          ? {
              cursor: encodeCursor({
                lastStartTimeTo: dataToReturn[lastItemIdx].startTime,
                lastTraceId: dataToReturn[lastItemIdx].traceId ?? "",
                lastId: dataToReturn[lastItemIdx].id,
              }),
            }
          : {};

      return {
        data: transformedItems,
        meta,
      };
    },
  }),
});
