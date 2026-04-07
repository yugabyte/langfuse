import { prisma } from "@langfuse/shared/src/db";
import type { Observation } from "@langfuse/shared";

import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";

import {
  GetObservationsV1Query,
  GetObservationsV1Response,
  transformDbToApiObservation,
} from "@/src/features/public-api/types/observations";
import {
  generateObservationsForPublicApi,
  getObservationsCountForPublicApi,
} from "@/src/features/public-api/server/observations";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Observations",
    querySchema: GetObservationsV1Query,
    responseSchema: GetObservationsV1Response,
    fn: async ({ query, auth }) => {
      const filterProps = {
        projectId: auth.scope.projectId,
        page: query.page,
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
        advancedFilters: query.filter,
      };

      const [items, count] = await Promise.all([
        generateObservationsForPublicApi({
          props: filterProps,
          advancedFilters: query.filter,
        }),
        getObservationsCountForPublicApi({
          props: filterProps,
          advancedFilters: query.filter,
        }),
      ]);
      const uniqueModels: string[] = Array.from(
        new Set(
          items
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
      const finalCount = count ? count : 0;

      return {
        data: items
          .map((i: Observation) => {
            const model = i.internalModelId
              ? modelById.get(i.internalModelId)
              : undefined;
            return {
              ...i,
              modelId: model?.id ?? null,
              inputPrice:
                model?.Price?.find((m) => m.usageType === "input")?.price ??
                null,
              outputPrice:
                model?.Price?.find((m) => m.usageType === "output")?.price ??
                null,
              totalPrice:
                model?.Price?.find((m) => m.usageType === "total")?.price ??
                null,
            };
          })
          .map(transformDbToApiObservation),
        meta: {
          page: query.page,
          limit: query.limit,
          totalItems: finalCount,
          totalPages: Math.ceil(finalCount / query.limit),
        },
      };
    },
  }),
});
