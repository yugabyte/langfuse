import { VERSION } from "@/src/constants";
import { cors, runMiddleware } from "@/src/features/public-api/server/cors";
import { telemetry } from "@/src/features/telemetry";
import { Prisma, prisma } from "@langfuse/shared/src/db";
import { logger, traceException } from "@langfuse/shared/src/server";
import { type NextApiRequest, type NextApiResponse } from "next";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    await runMiddleware(req, res, cors);
    await telemetry();
    const failIfNoRecentEvents = req.query.failIfNoRecentEvents === "true";
    const failIfDatabaseUnavailable =
      req.query.failIfDatabaseUnavailable === "true";

    try {
      if (failIfDatabaseUnavailable) {
        await prisma.$queryRaw`SELECT 1;`;
      }
    } catch (e) {
      logger.error("Couldn't connect to database", e);
      traceException(e);
      return res.status(503).json({
        status: "Database not available",
        version: VERSION.replace("v", ""),
      });
    }

    try {
      if (failIfNoRecentEvents) {
        const now = new Date();
        const [traces, observations] = await Promise.all([
          prisma.$queryRaw<Array<{ id: string }>>(Prisma.sql`
            SELECT t.id
            FROM clickhouse.traces t
            WHERE t.timestamp <= ${now}
              AND t.timestamp >= ${new Date(now.getTime() - 3 * 60 * 1000)}
              AND t.is_deleted = false
            LIMIT 1
          `),
          prisma.$queryRaw<Array<{ id: string }>>(Prisma.sql`
            SELECT o.id
            FROM clickhouse.observations o
            WHERE o.start_time <= ${now}
              AND o.start_time >= ${new Date(now.getTime() - 3 * 60 * 1000)}
              AND o.is_deleted = false
            LIMIT 1
          `),
        ]);
        if (traces.length === 0 || observations.length === 0) {
          return res.status(503).json({
            status: `No ${
              traces.length === 0
                ? "traces"
                : observations.length === 0
                  ? "observations"
                  : "<should not happen>"
            } within the last 3 minutes`,
            version: VERSION.replace("v", ""),
          });
        }
      }
    } catch (e) {
      logger.error("Couldn't fetch recent events", e);
      traceException(e);
      return res.status(503).json({
        status: "Couldn't fetch recent events",
        version: VERSION.replace("v", ""),
      });
    }
  } catch (e) {
    traceException(e);
    logger.error("Health check failed", e);
    return res.status(503).json({
      status: "Health check failed",
      version: VERSION.replace("v", ""),
    });
  }
  return res.status(200).json({
    status: "OK",
    version: VERSION.replace("v", ""),
  });
}
