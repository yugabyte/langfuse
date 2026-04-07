import { AGGREGATABLE_SCORE_TYPES } from "../../domain/scores";
import { tracingPrisma as prisma } from "../../db";
import { Prisma } from "@prisma/client";
import { logger } from "../logger";

const serializeErrorForLogs = (error: unknown) => {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return { message: String(error) };
};

const stringifyErrorForMessage = (error: unknown) =>
  JSON.stringify(serializeErrorForLogs(error));

export type EnvironmentFilterProps = {
  projectId: string;
  fromTimestamp?: Date;
};

export const getEnvironmentsForProject = async (
  props: EnvironmentFilterProps,
): Promise<{ environment: string }[]> => {
  const { projectId, fromTimestamp } = props;
  try {
    const envColumns = await prisma.$queryRaw<
      Array<{ table_name: string }>
    >(Prisma.sql`
      SELECT table_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND column_name = 'environment'
        AND table_name IN ('traces', 'observations', 'scores')
    `);
    const tableSet = new Set(envColumns.map((r) => r.table_name));

    const results: Array<{ environment: string | null }> = [];

    if (tableSet.has("traces")) {
      const rows = await prisma.$queryRaw<
        Array<{ environment: string | null }>
      >(
        Prisma.sql`
          SELECT DISTINCT environment
          FROM traces
          WHERE project_id = ${projectId}
          ${fromTimestamp ? Prisma.sql`AND timestamp >= ${fromTimestamp}` : Prisma.empty}
        `,
      );
      results.push(...rows);
    }
    if (tableSet.has("observations")) {
      const rows = await prisma.$queryRaw<
        Array<{ environment: string | null }>
      >(
        Prisma.sql`
          SELECT DISTINCT environment
          FROM observations
          WHERE project_id = ${projectId}
          ${fromTimestamp ? Prisma.sql`AND start_time >= ${fromTimestamp}` : Prisma.empty}
        `,
      );
      results.push(...rows);
    }
    if (tableSet.has("scores")) {
      const rows = await prisma.$queryRaw<
        Array<{ environment: string | null }>
      >(
        Prisma.sql`
          SELECT DISTINCT environment
          FROM scores
          WHERE project_id = ${projectId}
            AND data_type::text IN (${Prisma.join(AGGREGATABLE_SCORE_TYPES as unknown as string[])})
          ${fromTimestamp ? Prisma.sql`AND timestamp >= ${fromTimestamp}` : Prisma.empty}
        `,
      );
      results.push(...rows);
    }
    // Always add default environment to list
    results.push({ environment: "default" });

    const environments: string[] = [];
    for (const row of results) {
      if (typeof row.environment === "string" && row.environment.length > 0) {
        environments.push(row.environment);
      }
    }

    return Array.from(new Set(environments)).map((environment) => ({
      environment,
    }));
  } catch (error) {
    logger.error(
      `getEnvironmentsForProject failed; projectId=${projectId}; hasFromTimestamp=${!!fromTimestamp}; error=${stringifyErrorForMessage(error)}`,
    );
    throw error;
  }
};
