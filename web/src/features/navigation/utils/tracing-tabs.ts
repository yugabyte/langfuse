import { OBSERVATIONS_TAB_ENABLED } from "@/src/features/tracing/tracing-ui-config";

export const TRACING_TABS = {
  TRACES: "traces",
  OBSERVATIONS: "observations",
} as const;

export type TracingTab = (typeof TRACING_TABS)[keyof typeof TRACING_TABS];

export const getTracingTabs = (projectId: string) => {
  const tabs: Array<{
    value: TracingTab;
    label: string;
    href: string;
  }> = [
    {
      value: TRACING_TABS.TRACES,
      label: "Traces",
      href: `/project/${projectId}/traces`,
    },
  ];
  if (OBSERVATIONS_TAB_ENABLED) {
    tabs.push({
      value: TRACING_TABS.OBSERVATIONS,
      label: "Observations",
      href: `/project/${projectId}/observations`,
    });
  }
  return tabs;
};
