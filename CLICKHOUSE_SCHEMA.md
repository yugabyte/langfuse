# Langfuse ClickHouse Schema Reference

All tables live in the `default` database. All use `ReplacingMergeTree(event_ts, is_deleted)` —
meaning duplicate rows (same `id`) are merged by ClickHouse in the background, and `is_deleted=1`
acts as a soft delete.

---

## 1. `traces`

**What it stores:** One row per top-level LLM run (one LangGraph invocation = one trace).

**Where used in UI:**
- **Tracing → Traces table** — the main table you see at `/project/.../traces`
- **Trace detail page** — clicking a row opens the full trace view
- **Sessions page** — groups traces by `session_id`
- **Dashboard** — trace count charts use the `analytics_traces` materialized view derived from this table. This view can be used for our meko langfuse dashboard.
- **Search / filters** — name, user_id, tags, session_id, environment filters
  

| Column | Type | Description |
|---|---|---|
| `id` | `String` | Unique trace ID |
| `timestamp` | `DateTime64(3)` | When the trace started |
| `name` | `String` | Trace name — e.g. `"LangGraph"` |
| `user_id` | `Nullable(String)` | Who triggered it (set via SDK) |
| `session_id` | `Nullable(String)` | Groups related traces into a session |
| `project_id` | `String` | FK to Postgres `projects.id` |
| `environment` | `LowCardinality(String)` | e.g. `"default"`, `"production"` |
| `public` | `Bool` | Whether trace is publicly shareable |
| `bookmarked` | `Bool` | Starred/bookmarked in UI |
| `tags` | `Array(String)` | e.g. `["soccer", "rag"]` |
| `input` | `Nullable(String)` | Full input payload (ZSTD compressed) |
| `output` | `Nullable(String)` | Full output payload (ZSTD compressed) |
| `metadata` | `Map(LowCardinality(String), String)` | Key-value metadata |
| `release` | `Nullable(String)` | App release/version string |
| `version` | `Nullable(String)` | SDK version |
| `created_at` | `DateTime64(3)` | Row creation time |
| `updated_at` | `DateTime64(3)` | Last update time |
| `event_ts` | `DateTime64(3)` | Used by ReplacingMergeTree for deduplication |
| `is_deleted` | `UInt8` | `1` = soft deleted |

**Partition:** `toYYYYMM(timestamp)`
**Order by:** `(project_id, toDate(timestamp), id)`

---

## 2. `observations`

**What it stores:** One row per individual step inside a trace — every LangGraph node, every LLM call,
every retrieval. Types: `SPAN`, `GENERATION`, `EVENT`.

**Where used in UI:**
- **Tracing → Observations table** — the flat list at `/project/.../observations`
- **Trace detail → graph view** — the visual node graph (`TraceGraphCanvas`)
- **Observation detail page** — clicking an observation row opens its full detail
- **Dashboard** — latency/cost charts (via `analytics_observations` materialized view)
- **Cost tracking** — token usage and cost columns come from this table

| Column | Type | Description |
|---|---|---|
| `id` | `String` | Unique observation ID |
| `trace_id` | `String` | Parent trace ID |
| `parent_observation_id` | `Nullable(String)` | Parent span — builds the tree structure |
| `project_id` | `String` | FK to Postgres `projects.id` |
| `environment` | `LowCardinality(String)` | Environment tag |
| `type` | `LowCardinality(String)` | `SPAN` / `GENERATION` / `EVENT` |
| `name` | `String` | Node name — e.g. `"ChatOpenAI"`, `"PgDistRagRetriever"`, `"grade"` |
| `start_time` | `DateTime64(3)` | When the step started |
| `end_time` | `Nullable(DateTime64(3))` | When it finished (`null` for events) |
| `level` | `LowCardinality(String)` | `DEFAULT` / `DEBUG` / `WARNING` / `ERROR` |
| `status_message` | `Nullable(String)` | Error message if failed |
| `input` | `Nullable(String)` | Step input (ZSTD compressed) |
| `output` | `Nullable(String)` | Step output (ZSTD compressed) |
| `metadata` | `Map(LowCardinality(String), String)` | Key-value metadata |
| `provided_model_name` | `Nullable(String)` | Model as sent by SDK — e.g. `"gpt-4o"` |
| `internal_model_id` | `Nullable(String)` | Langfuse model registry ID |
| `model_parameters` | `Nullable(String)` | JSON: temperature, max_tokens, etc. |
| `provided_usage_details` | `Map(LowCardinality(String), UInt64)` | Raw token counts from SDK |
| `usage_details` | `Map(LowCardinality(String), UInt64)` | Normalized: `input`, `output`, `total` tokens |
| `provided_cost_details` | `Map(LowCardinality(String), Decimal(18,12))` | Raw cost from SDK |
| `cost_details` | `Map(LowCardinality(String), Decimal(18,12))` | Normalized costs per type |
| `total_cost` | `Nullable(Decimal(18,12))` | Total USD cost |
| `completion_start_time` | `Nullable(DateTime64(3))` | Time to first token (streaming) |
| `prompt_id` | `Nullable(String)` | Linked Langfuse prompt template ID |
| `prompt_name` | `Nullable(String)` | Prompt template name |
| `prompt_version` | `Nullable(UInt16)` | Prompt template version |
| `tool_definitions` | `Map(String, String)` | Tool schemas passed to LLM |
| `tool_calls` | `Array(String)` | Tool call IDs |
| `tool_call_names` | `Array(String)` | Tool names called |
| `usage_pricing_tier_id` | `Nullable(String)` | Pricing tier ID |
| `usage_pricing_tier_name` | `Nullable(String)` | Pricing tier name |
| `created_at` | `DateTime64(3)` | Row creation time |
| `updated_at` | `DateTime64(3)` | Last update time |
| `event_ts` | `DateTime64(3)` | Deduplication key |
| `is_deleted` | `UInt8` | `1` = soft deleted |

**Partition:** `toYYYYMM(start_time)`
**Order by:** `(project_id, toDate(start_time), id)`

---

## 3. `scores`

**What it stores:** One row per quality evaluation attached to a trace or observation.
Sources: `API` (programmatic), `HUMAN` (UI annotation), `LLM` (auto-eval).

**Where used in UI:**
- **Trace detail → Scores panel** — scores attached to a trace show here
- **Scores page** — `/project/.../scores` flat list of all scores
- **Dashboard** — score distribution charts (via `analytics_scores` materialized view)
- **Annotation queues** — human review workflows use `queue_id`
- **Evals** — LLM-as-judge results use `execution_trace_id` to link back to the eval run

| Column | Type | Description |
|---|---|---|
| `id` | `String` | Unique score ID |
| `timestamp` | `DateTime64(3)` | When the score was created |
| `project_id` | `String` | FK to Postgres `projects.id` |
| `environment` | `LowCardinality(String)` | Environment tag |
| `trace_id` | `Nullable(String)` | Which trace was scored |
| `observation_id` | `Nullable(String)` | Which specific span (optional, can be trace-level) |
| `session_id` | `Nullable(String)` | Session the scored trace belongs to |
| `dataset_run_id` | `Nullable(String)` | Experiment run that produced this score |
| `name` | `String` | Score name — e.g. `"relevance"`, `"answer_quality"` |
| `value` | `Float64` | Numeric score value |
| `string_value` | `Nullable(String)` | Categorical value — e.g. `"PASS"` / `"FAIL"` |
| `long_string_value` | `String` | Long text feedback (ZSTD compressed) |
| `data_type` | `String` | `NUMERIC` / `BOOLEAN` / `CATEGORICAL` |
| `source` | `String` | `API` / `HUMAN` / `LLM` |
| `comment` | `Nullable(String)` | Reviewer's note (ZSTD compressed) |
| `author_user_id` | `Nullable(String)` | Who scored it (for HUMAN scores) |
| `config_id` | `Nullable(String)` | FK to Postgres `score_configs.id` |
| `queue_id` | `Nullable(String)` | Annotation queue ID |
| `execution_trace_id` | `Nullable(String)` | Trace ID of the LLM judge run that produced this score |
| `metadata` | `Map(LowCardinality(String), String)` | Key-value metadata |
| `created_at` | `DateTime64(3)` | Row creation time |
| `updated_at` | `DateTime64(3)` | Last update time |
| `event_ts` | `DateTime64(3)` | Deduplication key |
| `is_deleted` | `UInt8` | `1` = soft deleted |

**Partition:** `toYYYYMM(timestamp)`
**Order by:** `(project_id, toDate(timestamp), id)`

---

## Query to inspect live schema

```sql
-- Run against ClickHouse (port 18123)
DESCRIBE TABLE default.traces;
DESCRIBE TABLE default.observations;
DESCRIBE TABLE default.scores;
```

```bash
# From terminal
curl "http://localhost:18123/?query=DESCRIBE+TABLE+default.traces+FORMAT+PrettyCompact&user=clickhouse&password=clickhouse"
```

---

## UI Page → Table mapping

| UI Page | URL | Tables used |
|---|---|---|
| Traces list | `/project/.../traces` | `traces` |
| Observations list | `/project/.../observations` | `observations` |
| Trace detail | `/project/.../traces/[id]` | `traces` + `observations` + `scores` |
| Sessions | `/project/.../sessions` | `traces` (grouped by session_id) |
| Scores | `/project/.../scores` | `scores` |
| Dashboard | `/project/.../dashboard` | `analytics_traces`, `analytics_observations`, `analytics_scores` (materialized views) |

---

## Indexes being used (ClickHouse)

ClickHouse does not use Postgres-style B-tree indexes by default.  
For these tables, indexing behavior comes from:

1. **Partition key** (`PARTITION BY`) -> partition pruning
2. **Primary/sparse index from sort key** (`ORDER BY`) -> data skipping within partitions
3. **ReplacingMergeTree(event_ts, is_deleted)** -> dedup/version merge behavior
4. **Data-skipping indexes (Bloom filters)** -> optional extra skipping on selected columns

| Table | Engine | Partition key | ORDER BY (sparse primary index key) | Bloom/data-skipping indexes from migrations |
|---|---|---|---|---|
| `traces` | `ReplacingMergeTree(event_ts, is_deleted)` | `toYYYYMM(timestamp)` | `(project_id, toDate(timestamp), id)` | `idx_id`, `idx_res_metadata_key`, `idx_res_metadata_value`, `idx_session_id`, `idx_user_id` |
| `observations` | `ReplacingMergeTree(event_ts, is_deleted)` | `toYYYYMM(start_time)` | `(project_id, toDate(start_time), id)` | `idx_id`, `idx_trace_id`, `idx_res_metadata_key`, `idx_res_metadata_value` *(note: `idx_project_id` was dropped in a later migration)* |
| `scores` | `ReplacingMergeTree(event_ts, is_deleted)` | `toYYYYMM(timestamp)` | `(project_id, toDate(timestamp), id)` | `idx_id`, `idx_project_trace_observation`, `idx_project_session`, `idx_project_dataset_run` |

### Practical meaning

- Filters like `project_id = ... AND timestamp/start_time >= ...` are fast due to partition pruning + ORDER BY key skipping.
- `ORDER BY ... id` helps deterministic latest-record style access with merge-tree semantics.
- Bloom filters are in use for the tables above and complement partition + ORDER BY pruning.

### Per-index explanation

All indexes listed below are Bloom/data-skipping indexes: they help ClickHouse skip granules that cannot match a predicate.

#### `traces`
- `idx_id`: speeds direct trace lookup by `id` (`WHERE id = ...`, `id IN (...)`).
  - **Table:** `traces`
  - **Defined in ClickHouse as:** `INDEX idx_id id TYPE bloom_filter(0.001) GRANULARITY 1`
- `idx_res_metadata_key`: helps filters that check metadata keys (`mapKeys(metadata)`).
  - **Table:** `traces`
  - **Defined in ClickHouse as:** `INDEX idx_res_metadata_key mapKeys(metadata) TYPE bloom_filter(0.01) GRANULARITY 1`
- `idx_res_metadata_value`: helps filters that match metadata values (`mapValues(metadata)`).
  - **Table:** `traces`
  - **Defined in ClickHouse as:** `INDEX idx_res_metadata_value mapValues(metadata) TYPE bloom_filter(0.01) GRANULARITY 1`
- `idx_session_id`: helps session-based filtering (`WHERE session_id = ...`).
  - **Table:** `traces`
  - **Defined in ClickHouse as:** `ALTER TABLE traces ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE bloom_filter() GRANULARITY 1`
- `idx_user_id`: helps user-based filtering (`WHERE user_id = ...`).
  - **Table:** `traces`
  - **Defined in ClickHouse as:** `ALTER TABLE traces ADD INDEX IF NOT EXISTS idx_user_id user_id TYPE bloom_filter() GRANULARITY 1`

#### `observations`
- `idx_id`: speeds direct observation lookup by `id`.
  - **Table:** `observations`
  - **Defined in ClickHouse as:** `INDEX idx_id id TYPE bloom_filter() GRANULARITY 1`
- `idx_trace_id`: helps trace detail fetches (`WHERE trace_id = ...`).
  - **Table:** `observations`
  - **Defined in ClickHouse as:** `INDEX idx_trace_id trace_id TYPE bloom_filter() GRANULARITY 1`
- `idx_res_metadata_key`: helps metadata-key filters on observations.
  - **Table:** `observations`
  - **Defined in ClickHouse as:** `ALTER TABLE observations ADD INDEX IF NOT EXISTS idx_res_metadata_key mapKeys(metadata) TYPE bloom_filter(0.01) GRANULARITY 1`
- `idx_res_metadata_value`: helps metadata-value filters on observations.
  - **Table:** `observations`
  - **Defined in ClickHouse as:** `ALTER TABLE observations ADD INDEX IF NOT EXISTS idx_res_metadata_value mapValues(metadata) TYPE bloom_filter(0.01) GRANULARITY 1`
- `idx_project_id` (dropped later): removed because `project_id` is already well served by table sort/partition strategy.
  - **Table:** `observations`
  - **Defined in ClickHouse as:** `ALTER TABLE observations DROP INDEX IF EXISTS idx_project_id`

#### `scores`
- `idx_id`: speeds direct score lookup by `id`.
  - **Table:** `scores`
  - **Defined in ClickHouse as:** `INDEX idx_id id TYPE bloom_filter(0.001) GRANULARITY 1`
- `idx_project_trace_observation`: helps score fetches scoped by `(project_id, trace_id, observation_id)`.
  - **Table:** `scores`
  - **Defined in ClickHouse as:** `INDEX idx_project_trace_observation (project_id, trace_id, observation_id) TYPE bloom_filter(0.001) GRANULARITY 1`
- `idx_project_session`: helps session-scoped score filtering.
  - **Table:** `scores`
  - **Defined in ClickHouse as:** `ALTER TABLE scores ADD INDEX IF NOT EXISTS idx_project_session (project_id, session_id) TYPE bloom_filter(0.001) GRANULARITY 1`
- `idx_project_dataset_run`: helps dataset-run/experiment score filtering.
  - **Table:** `scores`
  - **Defined in ClickHouse as:** `ALTER TABLE scores ADD INDEX IF NOT EXISTS idx_project_dataset_run (project_id, dataset_run_id) TYPE bloom_filter(0.001) GRANULARITY 1`

