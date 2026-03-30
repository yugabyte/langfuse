import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import { logger } from "@langfuse/shared/src/server";
import {
  GetMetricsV1Query,
  GetMetricsV1Response,
} from "@/src/features/public-api/types/metrics";
import { executeQueryYb } from "@/src/features/query/server/queryExecutorYb";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Metrics",
    rateLimitResource: "public-api-metrics",
    querySchema: GetMetricsV1Query,
    responseSchema: GetMetricsV1Response,
    fn: async ({ query, auth }) => {
      const queryParams = query.query;
      logger.info("Executing public metrics query in YB mode", {
        query: queryParams,
        projectId: auth.scope.projectId,
      });
      const result = await executeQueryYb(
        auth.scope.projectId,
        queryParams,
        "v1",
      );
      return { data: result };
    },
  }),
});
