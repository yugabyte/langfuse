/**
 * When true, redirects non-tracing project routes to /project/[projectId]/traces.
 * Use for tracing-only UI deployments.
 */
export const TRACING_ONLY_REDIRECTS = true;

/**
 * Project path segments that should redirect to traces when TRACING_ONLY_REDIRECTS is true.
 * These are the first segment of paths like /project/xxx/datasets, /project/xxx/evals, etc.
 */
export const REDIRECT_PROJECT_SEGMENTS = [
  "observations",
  "scores",
  "datasets",
  "evals",
  "prompts",
  "sessions",
  "users",
  "dashboards",
  "widgets",
  "annotation-queues",
  "automations",
  "experiments",
  "playground",
  "models",
];
