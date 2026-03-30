import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import { logger } from "@langfuse/shared/src/server";
import {
  GetMetricsV2Query,
  GetMetricsV2Response,
} from "@/src/features/public-api/types/metrics";
import { InvalidRequestError } from "@langfuse/shared";
import { executeQueryYb } from "@/src/features/query/server/queryExecutorYb";
import { validateQuery } from "@/src/features/query/server/queryExecutor";

const DEFAULT_ROW_LIMIT = 100;

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Metrics V2",
    rateLimitResource: "public-api-metrics", // Same rate limit as v1
    querySchema: GetMetricsV2Query,
    responseSchema: GetMetricsV2Response,
    fn: async ({ query, auth }) => {
      const validation = validateQuery(query.query as any, "v2");
      if (!validation.valid) {
        throw new InvalidRequestError(validation.reason);
      }

      const queryParams = {
        ...query.query,
        config: {
          ...query.query.config,
          row_limit: query.query.config?.row_limit ?? DEFAULT_ROW_LIMIT,
        },
      };

      logger.info("Executing v2 public metrics query in YB mode", {
        query: queryParams,
        version: "v2",
        projectId: auth.scope.projectId,
      });
      const result = await executeQueryYb(
        auth.scope.projectId,
        queryParams as any,
        "v2",
      );
      return { data: result };
    },
  }),
});
