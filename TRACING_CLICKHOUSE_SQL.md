# Tracing UI – ClickHouse Tables & SQL Reference

## 1. Table Relationships (Joins / Subqueries)

All three tables are linked via **trace_id** and **project_id**:

```
                    ┌─────────────┐
                    │   traces    │  (base table)
                    │ id, project │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               │
    ┌──────────────┐  ┌──────────────┐    │
    │ observations │  │   scores      │    │
    │ trace_id     │  │ trace_id      │    │
    │ project_id   │  │ project_id    │    │
    └──────────────┘  │ observation_id│   │  (optional, for obs-level scores)
                      └──────────────┘    │
                                          │
    observations and scores do NOT join   │
    to each other in tracing UI.           │
    Both join only to traces.             │
```

- **traces** – base table
- **observations** – child of traces (`trace_id`, `project_id`)
- **scores** – trace-level (`trace_id`, `observation_id IS NULL`) or observation-level (`trace_id`, `observation_id`)

**Joins used in tracing UI:**
- `traces` LEFT JOIN `observations_stats` (CTE from `observations`) ON `trace_id` + `project_id`
- `traces` LEFT JOIN `scores_avg` (CTE from `scores`) ON `trace_id` + `project_id`
- No direct JOIN between `observations` and `scores` in tracing queries

---

## 2. SQL Queries by Use Case

### 2.1 Traces Table (List View)

**Source:** `packages/shared/src/server/services/traces-ui-table-service.ts` → `getTracesTableGeneric`

**API:** `traces.all`, `traces.countAll`, `traces.metrics`

#### Main query (count / rows / metrics / identifiers)

```sql
WITH observations_stats AS (
  SELECT
    COUNT(*) AS observation_count,
    sumMap(usage_details) as usage_details,
    SUM(total_cost) AS total_cost,
    date_diff('millisecond', least(min(start_time), min(end_time)), greatest(max(start_time), max(end_time))) as latency_milliseconds,
    countIf(level = 'ERROR') as error_count,
    countIf(level = 'WARNING') as warning_count,
    countIf(level = 'DEFAULT') as default_count,
    countIf(level = 'DEBUG') as debug_count,
    multiIf(...) AS aggregated_level,
    sumMap(cost_details) as cost_details,
    trace_id,
    project_id
  FROM observations o [FINAL]
  WHERE o.project_id = {projectId}
    AND o.start_time >= {traceTimestamp} - INTERVAL 2 DAY  -- optional time filter
    [AND observation filters]
  GROUP BY trace_id, project_id
),
scores_avg AS (
  SELECT project_id, trace_id,
    groupArrayIf(tuple(name, avg_value), data_type IN ('NUMERIC', 'BOOLEAN')) AS scores_avg,
    groupArrayIf(concat(name, ':', string_value), data_type = 'CATEGORICAL' AND notEmpty(string_value)) AS score_categories
  FROM (
    SELECT project_id, trace_id, name, data_type, string_value, avg(value) as avg_value
    FROM scores s FINAL
    WHERE project_id = {projectId}
      AND s.timestamp >= {traceTimestamp} - INTERVAL 1 HOUR  -- optional
      [AND score filters]
    GROUP BY project_id, trace_id, name, data_type, string_value
  ) tmp
  GROUP BY project_id, trace_id
)
SELECT [count | rows | metrics | identifiers]
FROM traces t [FINAL]
LEFT JOIN observations_stats o ON o.project_id = t.project_id AND o.trace_id = t.id
LEFT JOIN scores_avg s ON s.project_id = t.project_id AND s.trace_id = t.id
WHERE t.project_id = {projectId}
  AND [trace filters]
  [search]
ORDER BY [orderBy]
[LIMIT 1 BY id, project_id]  -- for default time order
LIMIT {limit} OFFSET {offset}
```

**Select variants:**
- **count:** `uniqExact(t.id) as count`
- **rows:** trace columns (id, name, tags, timestamp, etc.)
- **metrics:** latency, tokens, cost, scores, observation counts
- **identifiers:** id, projectId, timestamp

---

### 2.2 Trace Detail (Single Trace)

**Source:** `packages/shared/src/server/repositories/traces.ts`, `observations.ts`, `scores.ts`

**API:** `traces.byId`, `traces.byIdWithObservationsAndScores`

#### 2.2.1 Trace by ID (`getTraceById`)

```sql
SELECT
  id, name, user_id, metadata, release, version, project_id,
  environment, public, bookmarked, tags,
  input, output, session_id, 0 as is_deleted,
  timestamp, created_at, updated_at
FROM traces
WHERE id = {traceId}
  AND project_id = {projectId}
  [AND toDate(timestamp) = toDate({timestamp})]
  [AND timestamp >= {fromTimestamp}]
ORDER BY event_ts DESC
LIMIT 1
```

**Tables:** `traces` only

---

#### 2.2.2 Observations for Trace (`getObservationsForTrace`)

```sql
SELECT
  id, trace_id, project_id, type, parent_observation_id, environment,
  start_time, end_time, name, level, status_message, version,
  [input, output, metadata,]  -- optional
  provided_model_name, internal_model_id, model_parameters,
  provided_usage_details, usage_details, provided_cost_details, cost_details, total_cost,
  usage_pricing_tier_id, usage_pricing_tier_name, completion_start_time,
  prompt_id, prompt_name, prompt_version, tool_definitions, tool_calls, tool_call_names,
  created_at, updated_at, event_ts
FROM observations
WHERE trace_id = {traceId}
  AND project_id = {projectId}
  [AND start_time >= {traceTimestamp} - INTERVAL 1 HOUR]
ORDER BY event_ts DESC
LIMIT 1 BY id, project_id
```

**Tables:** `observations` only (filtered by `trace_id`)

---

#### 2.2.3 Scores for Trace (`getScoresForTraces`)

```sql
SELECT *
FROM scores s
WHERE s.project_id = {projectId}
  AND s.trace_id IN ({traceIds})
  [AND s.timestamp >= {traceTimestamp} - INTERVAL 1 HOUR]
ORDER BY s.event_ts DESC
LIMIT 1 BY s.id, s.project_id
[LIMIT {limit} OFFSET {offset}]
```

**Tables:** `scores` only (filtered by `trace_id`)

---

### 2.3 Filter Options (Dropdowns)

**API:** `traces.filterOptions`

#### 2.3.1 Trace Names (`getTracesGroupedByName`)

```sql
SELECT name, count(*) as count
FROM traces t
WHERE t.project_id = {projectId}
  AND t.name IS NOT NULL
  [AND timestamp filters]
GROUP BY name
ORDER BY count(*) DESC
LIMIT 1000
```

**Tables:** `traces` only

---

#### 2.3.2 Tags (`getTracesGroupedByTags`)

```sql
SELECT distinct(arrayJoin(tags)) as value
FROM traces t
WHERE t.project_id = {projectId}
  [AND filters]
LIMIT 1000
```

**Tables:** `traces` only

---

#### 2.3.3 Users (`getTracesGroupedByUsers`)

```sql
SELECT user_id as user, count(*) as count
FROM traces t
WHERE t.project_id = {projectId}
  AND t.user_id IS NOT NULL
  AND t.user_id != ''
  [AND filters]
  [AND search]
GROUP BY user
ORDER BY count DESC
LIMIT {limit} OFFSET {offset}
```

**Tables:** `traces` only

---

#### 2.3.4 Sessions (`getTracesGroupedBySessionId`)

```sql
SELECT session_id, count(*) as count
FROM traces t
WHERE t.project_id = {projectId}
  AND t.session_id IS NOT NULL
  AND t.session_id != ''
  [AND filters]
  [AND search]
GROUP BY session_id
ORDER BY count DESC
LIMIT {limit} OFFSET {offset}
```

**Tables:** `traces` only

---

#### 2.3.5 Numeric Score Names (`getNumericScoresGroupedByName`)

```sql
SELECT name
FROM scores s
WHERE s.project_id = {projectId}
  AND has(['NUMERIC', 'BOOLEAN'], s.data_type)
  [AND timestamp filters]
GROUP BY name
ORDER BY count() DESC
LIMIT 1000
```

**Tables:** `scores` only

---

#### 2.3.6 Categorical Score Names (`getCategoricalScoresGroupedByName`)

```sql
SELECT name AS label, groupArray(DISTINCT string_value) AS values
FROM scores s
WHERE s.project_id = {projectId}
  AND s.data_type = 'CATEGORICAL'
  [AND timestamp filters]
GROUP BY name
ORDER BY count() DESC
LIMIT 1000
```

**Tables:** `scores` only (plus Postgres `score_configs` for category labels)

---

### 2.4 Other Trace-Related Queries

#### 2.4.1 Check Trace Exists (`checkTraceExistsAndGetTimestamp`)

```sql
WITH observations_agg AS (
  SELECT multiIf(...) AS aggregated_level,
    countIf(level = 'ERROR') as error_count, ...
    trace_id, project_id
  FROM observations o FINAL
  WHERE o.project_id = {projectId}
    AND o.start_time >= {traceTimestamp} - INTERVAL 2 DAY
    AND o.start_time >= {timestamp} - INTERVAL 2 DAY
  GROUP BY trace_id, project_id
)
SELECT t.id, t.project_id, t.timestamp
FROM traces t FINAL
[INNER JOIN observations_agg o ON t.id = o.trace_id AND t.project_id = o.project_id]
WHERE [filters]
  AND t.project_id = {projectId}
  AND t.timestamp >= {timestamp} - INTERVAL 1 HOUR
  [AND t.timestamp <= {maxTimeStamp}]
  [AND toDate(t.timestamp) = toDate({exactTimestamp})]
GROUP BY t.id, t.project_id, t.timestamp
```

**Tables:** `traces`, `observations` (JOIN)

---

#### 2.4.2 Get Traces by IDs (`getTracesByIds`)

```sql
SELECT *
FROM traces
WHERE id IN ({traceIds})
  AND project_id = {projectId}
  [AND timestamp >= {timestamp}]
ORDER BY event_ts DESC
LIMIT 1 BY id, project_id
```

**Tables:** `traces` only

---

## 3. Summary: Tables per Query

| Query | traces | observations | scores |
|-------|--------|--------------|--------|
| Traces table (list) | ✓ (base) | ✓ (CTE → JOIN) | ✓ (CTE → JOIN) |
| getTraceById | ✓ | | |
| getObservationsForTrace | | ✓ | |
| getScoresForTraces | | | ✓ |
| getTracesGroupedByName | ✓ | | |
| getTracesGroupedByTags | ✓ | | |
| getTracesGroupedByUsers | ✓ | | |
| getTracesGroupedBySessionId | ✓ | | |
| getNumericScoresGroupedByName | | | ✓ |
| getCategoricalScoresGroupedByName | | | ✓ |
| checkTraceExistsAndGetTimestamp | ✓ | ✓ (JOIN) | |
| getTracesByIds | ✓ | | |

---

## 4. Time Intervals (constants.ts)

| Constant | Value | Usage |
|---------|-------|-------|
| `OBSERVATIONS_TO_TRACE_INTERVAL` | INTERVAL 2 DAY | Filter observations when trace timestamp is known |
| `TRACE_TO_OBSERVATIONS_INTERVAL` | INTERVAL 1 HOUR | Filter observations by trace timestamp |
| `SCORE_TO_TRACE_OBSERVATIONS_INTERVAL` | INTERVAL 1 HOUR | Filter scores by trace/observation timestamp |

---

## 5. PostgreSQL Tables (Strict ClickHouse parity: only ORDER BY + partitioning)

This section keeps only what ClickHouse provides by default for these tables:
- monthly time partitioning
- one ORDER-key-like index per table
- no extra query-specific indexes

> ClickHouse uses `ReplacingMergeTree(event_ts, is_deleted)`, monthly partitions, and sort keys.
> Postgres cannot emulate `FINAL` exactly; use upsert semantics for latest row state.

### 5.1 Datatype mapping used here

| ClickHouse | Postgres |
|---|---|
| `String` | `TEXT` |
| `DateTime64(3)` | `TIMESTAMPTZ` |
| `Bool` / `UInt8` flags | `BOOLEAN` |
| `Array(String)` | `TEXT[]` |
| `Map(...)` | `JSONB` |
| `Decimal(18,12)` | `NUMERIC(18,12)` |
| `Float64` | `DOUBLE PRECISION` |

### 5.2 DDL (partitioned tables)

```sql
-- Optional enum types (or use TEXT + CHECK).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'observation_level') THEN
    CREATE TYPE observation_level AS ENUM ('ERROR', 'WARNING', 'DEFAULT', 'DEBUG');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'observation_type') THEN
    CREATE TYPE observation_type AS ENUM ('SPAN', 'GENERATION', 'EVENT');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'score_data_type') THEN
    CREATE TYPE score_data_type AS ENUM ('NUMERIC', 'BOOLEAN', 'CATEGORICAL');
  END IF;
END $$;

-- IMPORTANT for partitioned tables:
-- Primary/unique constraints must include the partition key.

-- 1) traces
-- ClickHouse: PARTITION BY toYYYYMM(timestamp), ORDER BY (project_id, toDate(timestamp), id)
CREATE TABLE IF NOT EXISTS traces (
  id          TEXT        NOT NULL,
  project_id  TEXT        NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL,
  name        TEXT,
  user_id     TEXT,
  session_id  TEXT,
  environment TEXT,
  public      BOOLEAN     NOT NULL DEFAULT FALSE,
  bookmarked  BOOLEAN     NOT NULL DEFAULT FALSE,
  tags        TEXT[]      NOT NULL DEFAULT '{}',
  input       TEXT,
  output      TEXT,
  metadata    JSONB       NOT NULL DEFAULT '{}'::jsonb,
  release     TEXT,
  version     TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  event_ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_deleted  BOOLEAN     NOT NULL DEFAULT FALSE,
  PRIMARY KEY (project_id, id, "timestamp")
) PARTITION BY RANGE ("timestamp");

-- 2) observations
-- ClickHouse: PARTITION BY toYYYYMM(start_time), ORDER BY (project_id, toDate(start_time), id)
CREATE TABLE IF NOT EXISTS observations (
  id                     TEXT              NOT NULL,
  project_id             TEXT              NOT NULL,
  trace_id               TEXT              NOT NULL,
  parent_observation_id  TEXT,
  environment            TEXT,
  type                   observation_type  NOT NULL,
  name                   TEXT              NOT NULL,
  start_time             TIMESTAMPTZ       NOT NULL,
  end_time               TIMESTAMPTZ,
  level                  observation_level NOT NULL DEFAULT 'DEFAULT',
  status_message         TEXT,
  version                TEXT,
  input                  TEXT,
  output                 TEXT,
  metadata               JSONB             NOT NULL DEFAULT '{}'::jsonb,
  provided_model_name    TEXT,
  internal_model_id      TEXT,
  model_parameters       JSONB,
  provided_usage_details JSONB             NOT NULL DEFAULT '{}'::jsonb,
  usage_details          JSONB             NOT NULL DEFAULT '{}'::jsonb,
  provided_cost_details  JSONB             NOT NULL DEFAULT '{}'::jsonb,
  cost_details           JSONB             NOT NULL DEFAULT '{}'::jsonb,
  total_cost             NUMERIC(18,12),
  usage_pricing_tier_id  TEXT,
  usage_pricing_tier_name TEXT,
  completion_start_time  TIMESTAMPTZ,
  prompt_id              TEXT,
  prompt_name            TEXT,
  prompt_version         INTEGER,
  tool_definitions       JSONB             NOT NULL DEFAULT '{}'::jsonb,
  tool_calls             JSONB             NOT NULL DEFAULT '[]'::jsonb,
  tool_call_names        TEXT[]            NOT NULL DEFAULT '{}',
  created_at             TIMESTAMPTZ       NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ       NOT NULL DEFAULT now(),
  event_ts               TIMESTAMPTZ       NOT NULL DEFAULT now(),
  is_deleted             BOOLEAN           NOT NULL DEFAULT FALSE,
  PRIMARY KEY (project_id, id, start_time)
) PARTITION BY RANGE (start_time);

-- 3) scores
-- ClickHouse: PARTITION BY toYYYYMM(timestamp), ORDER BY (project_id, toDate(timestamp), id)
CREATE TABLE IF NOT EXISTS scores (
  id                 TEXT            NOT NULL,
  project_id         TEXT            NOT NULL,
  "timestamp"        TIMESTAMPTZ     NOT NULL,
  trace_id           TEXT            NOT NULL,
  observation_id     TEXT,
  session_id         TEXT,
  dataset_run_id     TEXT,
  environment        TEXT,
  name               TEXT            NOT NULL,
  value              DOUBLE PRECISION,
  string_value       TEXT,
  long_string_value  TEXT,
  data_type          score_data_type NOT NULL,
  source             TEXT,
  comment            TEXT,
  author_user_id     TEXT,
  config_id          TEXT,
  queue_id           TEXT,
  execution_trace_id TEXT,
  metadata           JSONB           NOT NULL DEFAULT '{}'::jsonb,
  created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
  event_ts           TIMESTAMPTZ     NOT NULL DEFAULT now(),
  is_deleted         BOOLEAN         NOT NULL DEFAULT FALSE,
  PRIMARY KEY (project_id, id, "timestamp")
) PARTITION BY RANGE ("timestamp");

```

### 5.3 Monthly partitions (create as needed)

```sql
-- Example partitions for March 2026
CREATE TABLE IF NOT EXISTS traces_2026_03
  PARTITION OF traces
  FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE IF NOT EXISTS observations_2026_03
  PARTITION OF observations
  FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE IF NOT EXISTS scores_2026_03
  PARTITION OF scores
  FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
```

### 5.4 Exact parity note

- This is intentionally minimal: no `name/user/session`, no GIN, no partial indexes.
- Add extra indexes only if you later choose performance tuning beyond strict ClickHouse parity.

### 5.5 Corresponding indexes (ClickHouse -> Postgres)

If you want Postgres indexes corresponding to ClickHouse Bloom indexes used in tracing tables,
use the mapping below. These are **optional** beyond strict parity.

| ClickHouse index | Table | Postgres equivalent (recommended) |
|---|---|---|
| `idx_id id TYPE bloom_filter(...)` | `traces` | `CREATE INDEX IF NOT EXISTS idx_traces_id ON traces (id);` |
| `idx_res_metadata_key mapKeys(metadata) TYPE bloom_filter(...)` | `traces` | `CREATE INDEX IF NOT EXISTS idx_traces_metadata_gin ON traces USING GIN (metadata);` |
| `idx_res_metadata_value mapValues(metadata) TYPE bloom_filter(...)` | `traces` | `CREATE INDEX IF NOT EXISTS idx_traces_metadata_gin ON traces USING GIN (metadata);` |
| `idx_session_id session_id TYPE bloom_filter(...)` | `traces` | `CREATE INDEX IF NOT EXISTS idx_traces_session_id ON traces (project_id, session_id);` |
| `idx_user_id user_id TYPE bloom_filter(...)` | `traces` | `CREATE INDEX IF NOT EXISTS idx_traces_user_id ON traces (project_id, user_id);` |
| `idx_id id TYPE bloom_filter(...)` | `observations` | `CREATE INDEX IF NOT EXISTS idx_observations_id ON observations (id);` |
| `idx_trace_id trace_id TYPE bloom_filter(...)` | `observations` | `CREATE INDEX IF NOT EXISTS idx_observations_trace_id ON observations (project_id, trace_id);` |
| `idx_res_metadata_key mapKeys(metadata) TYPE bloom_filter(...)` | `observations` | `CREATE INDEX IF NOT EXISTS idx_observations_metadata_gin ON observations USING GIN (metadata);` |
| `idx_res_metadata_value mapValues(metadata) TYPE bloom_filter(...)` | `observations` | `CREATE INDEX IF NOT EXISTS idx_observations_metadata_gin ON observations USING GIN (metadata);` |
| `idx_id id TYPE bloom_filter(...)` | `scores` | `CREATE INDEX IF NOT EXISTS idx_scores_id ON scores (id);` |
| `idx_project_trace_observation (project_id, trace_id, observation_id) TYPE bloom_filter(...)` | `scores` | `CREATE INDEX IF NOT EXISTS idx_scores_project_trace_observation ON scores (project_id, trace_id, observation_id);` |
| `idx_project_session (project_id, session_id) TYPE bloom_filter(...)` | `scores` | `CREATE INDEX IF NOT EXISTS idx_scores_project_session ON scores (project_id, session_id);` |
| `idx_project_dataset_run (project_id, dataset_run_id) TYPE bloom_filter(...)` | `scores` | `CREATE INDEX IF NOT EXISTS idx_scores_project_dataset_run ON scores (project_id, dataset_run_id);` |

Notes:
- ClickHouse Bloom indexes are data-skipping indexes; Postgres uses B-tree/GIN and planner statistics.
- For metadata key/value lookups, `GIN (metadata)` is the closest practical equivalent.
