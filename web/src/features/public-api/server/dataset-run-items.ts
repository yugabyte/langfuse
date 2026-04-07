import { prisma } from "@langfuse/shared/src/db";
import { isPresent } from "@langfuse/shared";

type DatasetRunItemsQueryType = {
  datasetId: string;
  runId: string;
  page?: number;
  limit?: number;
  projectId: string;
};

export const generateDatasetRunItemsForPublicApi = async ({
  props,
}: {
  props: DatasetRunItemsQueryType;
}) => {
  const { datasetId, projectId, runId, limit, page } = props;
  const datasetItems = await prisma.datasetItem.findMany({
    where: {
      projectId,
      datasetId,
    },
    select: {
      id: true,
    },
  });
  const datasetItemIds = datasetItems.map((item) => item.id);
  if (datasetItemIds.length === 0) return [];

  const items = await prisma.datasetRunItems.findMany({
    where: {
      projectId,
      datasetItemId: {
        in: datasetItemIds,
      },
      datasetRunId: runId,
    },
    include: {
      datasetRun: {
        select: {
          name: true,
        },
      },
    },
    orderBy: {
      createdAt: "desc",
    },
    take: limit,
    skip:
      isPresent(page) && isPresent(limit) && page >= 1
        ? (page - 1) * limit
        : undefined,
  });

  const mappedItems = items.map((item) => ({
    id: item.id,
    datasetRunId: item.datasetRunId,
    datasetRunName: item.datasetRun.name,
    datasetItemId: item.datasetItemId,
    traceId: item.traceId,
    observationId: item.observationId ?? null,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
  }));
  return mappedItems;
};

export const getDatasetRunItemsCountForPublicApi = async ({
  props,
}: {
  props: DatasetRunItemsQueryType;
}) => {
  const { datasetId, projectId, runId } = props;
  const datasetItems = await prisma.datasetItem.findMany({
    where: {
      projectId,
      datasetId,
    },
    select: {
      id: true,
    },
  });
  const datasetItemIds = datasetItems.map((item) => item.id);
  if (datasetItemIds.length === 0) return 0;

  const count = await prisma.datasetRunItems.count({
    where: {
      projectId,
      datasetItemId: {
        in: datasetItemIds,
      },
      datasetRunId: runId,
    },
  });
  return count;
};
