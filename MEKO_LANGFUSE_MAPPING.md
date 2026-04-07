# Meko ↔ Langfuse Mapping

## Concept Mapping

| Meko | Langfuse |
|---|---|
| Team / Company | Organization |
| User (human) | Organization Member |
| Datapack | Project |
| Agent | Like a "user" of a datapack — the entity making MCP calls |
| Agent MCP call | Trace |
| Agent conversation | Session |

---

## Structure

**Each user has:**
- **m agents** — agents that can make MCP calls
- **n datapacks** — projects (knowledge bases, configs)

**Agents are like users of a datapack** — they're the ones that call into a datapack and produce traces. An agent can access **any** datapack (cross-datapack access).

```
User (human)
├── Agent 1, Agent 2, ... Agent m
├── Datapack A, Datapack B, ... Datapack n
│
└── Agents can access any datapack
      (Agent 1 → Datapack A, Agent 2 → Datapack B, Agent 1 → Datapack B, etc.)
```

---

## Langfuse Structure

```
Organization: "Acme Corp"
│
├── Member: user-alice  (human, OWNER)
├── Member: user-bob    (human, MEMBER)
│
├── Project: "datapack-foo"    ← one of user's n datapacks
│     ├── API keys:  pk-lf-..., sk-lf-...
│     ├── Traces from Agent 1, Agent 2, ... (any of user's m agents)
│     └── Sessions
│
└── Project: "datapack-bar"    ← another datapack
      ├── API keys:  pk-lf-...
      ├── Traces from any agent that has access
      └── Sessions
```

- **User** = human (org member)
- **Agent** = represented as `user_id` on trace, or `metadata['agent_id']` — the "caller" of the datapack
- **Datapack** = Project — each has its own API keys; agents use the key of the datapack they're calling
- **Agents can access any datapack** — Agent 1 can send traces to Datapack A or B by using that datapack's API key

---

## Trace Structure

Each MCP agent call produces one trace with the following spans:

```
Trace: "LangGraph"
  session_id: <conversation UUID>
  tags:       ["knowledge_base"] or ["memory"] or ["transactional"]
  │
  ├── analyze          (SPAN)       — decides: RAG or memory?
  ├── retrieve         (RETRIEVER)  — PgDistRagRetriever hits pg_dist_rag
  ├── generate         (GENERATION) — LLM call, tokens + cost tracked
  ├── grade            (SPAN)       — checks answer quality
  └── rewrite          (SPAN)       — if grade fails, rewrites query → retry
```

---

## Session

A session groups multiple traces from the same conversation.

```python
# All traces in one conversation share the same session_id
langfuse_handler = CallbackHandler(
    session_id="conv-uuid-123",   # set once per conversation
)
```

- `session_id` is controlled by your app — Langfuse just stores it
- In `run.sh`: auto-generated per run via `uuidgen`
- Override: `export LANGFUSE_SESSION_ID="my-session"` before calling `run.sh`

---

## Representing Agents

Agents are the "users" of a datapack. When an agent makes an MCP call:

```python
# Option 1: Use user_id for agent identity
langfuse_handler = CallbackHandler(
    user_id="agent-1",        # which agent made this call
    session_id=session_id,
)

# Option 2: Use metadata
langfuse_handler = CallbackHandler(
    session_id=session_id,
    metadata={"agent_id": "agent-1"},
)
```

- **Datapack** = which project (use that project's API key)
- **Agent** = `user_id` or `metadata['agent_id']` on the trace
- Filter in Langfuse UI by user_id or metadata to see traces per agent

---

## Tagging by Data Type (ABAC attributes)

```python
# When agent touches knowledge base
langfuse_handler = CallbackHandler(
    session_id=session_id,
    tags=["knowledge_base"],
)

# When agent touches memory
langfuse_handler = CallbackHandler(
    session_id=session_id,
    tags=["memory"],
)

# When agent touches transactional data
langfuse_handler = CallbackHandler(
    session_id=session_id,
    tags=["transactional"],
)
```

Filter by tag in the Langfuse UI → Traces → filter by tag.

---

## SDK Configuration (per datapack)

```bash
# Set once per datapack in your environment
export LANGFUSE_PUBLIC_KEY="pk-lf-<datapack-public-key>"
export LANGFUSE_SECRET_KEY="sk-lf-<datapack-secret-key>"
export LANGFUSE_HOST="http://localhost:3000"          # self-hosted
# export LANGFUSE_HOST="https://cloud.langfuse.com"  # cloud

# Optional: pin session across multiple calls
export LANGFUSE_SESSION_ID="conv-uuid-123"
```

---

## What's Stored Where

| Data | Storage |
|---|---|
| Trace content (spans, tokens, cost, latency) | ClickHouse |
| Trace metadata (bookmarked, tags, session_id) | PostgreSQL |
| Project config, API keys, members, roles | PostgreSQL |
| Raw SDK events (before processing) | MinIO (S3) |
| Queue jobs (ingestion pipeline) | Redis |
