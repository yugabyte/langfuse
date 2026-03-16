--
-- YSQL database dump
--

-- Dumped from database version 15.12-YB-2025.2.1.0-b0
-- Dumped by ysql_dump version 15.12-YB-2025.2.1.0-b0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: observation_level; Type: TYPE; Schema: public; Owner: yugabyte
--

CREATE TYPE public.observation_level AS ENUM (
    'ERROR',
    'WARNING',
    'DEFAULT',
    'DEBUG'
);


ALTER TYPE public.observation_level OWNER TO yugabyte;

--
-- Name: observation_type; Type: TYPE; Schema: public; Owner: yugabyte
--

CREATE TYPE public.observation_type AS ENUM (
    'SPAN',
    'GENERATION',
    'EVENT',
    'AGENT',
    'TOOL',
    'CHAIN',
    'RETRIEVER',
    'EVALUATOR',
    'EMBEDDING',
    'GUARDRAIL'
);


ALTER TYPE public.observation_type OWNER TO yugabyte;

--
-- Name: score_data_type; Type: TYPE; Schema: public; Owner: yugabyte
--

CREATE TYPE public.score_data_type AS ENUM (
    'NUMERIC',
    'BOOLEAN',
    'CATEGORICAL'
);


ALTER TYPE public.score_data_type OWNER TO yugabyte;

SET default_tablespace = '';

--
-- Name: observations; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE public.observations (
    id text NOT NULL,
    project_id text NOT NULL,
    trace_id text NOT NULL,
    parent_observation_id text,
    environment text,
    type public.observation_type NOT NULL,
    name text NOT NULL,
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone,
    level public.observation_level DEFAULT 'DEFAULT'::public.observation_level NOT NULL,
    status_message text,
    version text,
    input text,
    output text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    provided_model_name text,
    internal_model_id text,
    model_parameters jsonb,
    provided_usage_details jsonb DEFAULT '{}'::jsonb NOT NULL,
    usage_details jsonb DEFAULT '{}'::jsonb NOT NULL,
    provided_cost_details jsonb DEFAULT '{}'::jsonb NOT NULL,
    cost_details jsonb DEFAULT '{}'::jsonb NOT NULL,
    total_cost numeric(18,12),
    usage_pricing_tier_id text,
    usage_pricing_tier_name text,
    completion_start_time timestamp with time zone,
    prompt_id text,
    prompt_name text,
    prompt_version integer,
    tool_definitions jsonb DEFAULT '{}'::jsonb NOT NULL,
    tool_calls jsonb DEFAULT '[]'::jsonb NOT NULL,
    tool_call_names text[] DEFAULT '{}'::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    event_ts timestamp with time zone DEFAULT now() NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT observations_pkey PRIMARY KEY((project_id) HASH, id ASC, start_time ASC)
)
PARTITION BY RANGE (start_time);


ALTER TABLE public.observations OWNER TO yugabyte;

SET default_table_access_method = heap;

--
-- Name: observations_2026_03; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE IF NOT EXISTS public.observations_2026_03
  PARTITION OF public.observations
  FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

--
-- Name: observations_default; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE IF NOT EXISTS public.observations_default
  PARTITION OF public.observations DEFAULT;

--
-- Name: scores; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE public.scores (
    id text NOT NULL,
    project_id text NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    trace_id text NOT NULL,
    observation_id text,
    session_id text,
    dataset_run_id text,
    environment text,
    name text NOT NULL,
    value double precision,
    string_value text,
    long_string_value text,
    data_type public.score_data_type NOT NULL,
    source text,
    comment text,
    author_user_id text,
    config_id text,
    queue_id text,
    execution_trace_id text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    event_ts timestamp with time zone DEFAULT now() NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT scores_pkey PRIMARY KEY((project_id) HASH, id ASC, "timestamp" ASC)
)
PARTITION BY RANGE ("timestamp");


ALTER TABLE public.scores OWNER TO yugabyte;

--
-- Name: scores_2026_03; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE IF NOT EXISTS public.scores_2026_03
  PARTITION OF public.scores
  FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

--
-- Name: scores_default; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE IF NOT EXISTS public.scores_default
  PARTITION OF public.scores DEFAULT;

--
-- Name: traces; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE public.traces (
    id text NOT NULL,
    project_id text NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    name text,
    user_id text,
    session_id text,
    environment text,
    public boolean DEFAULT false NOT NULL,
    bookmarked boolean DEFAULT false NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    input text,
    output text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    release text,
    version text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    event_ts timestamp with time zone DEFAULT now() NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT traces_pkey PRIMARY KEY((project_id) HASH, id ASC, "timestamp" ASC)
)
PARTITION BY RANGE ("timestamp");


ALTER TABLE public.traces OWNER TO yugabyte;

--
-- Name: traces_2026_03; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE IF NOT EXISTS public.traces_2026_03
  PARTITION OF public.traces
  FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

--
-- Name: traces_default; Type: TABLE; Schema: public; Owner: yugabyte
--

CREATE TABLE IF NOT EXISTS public.traces_default
  PARTITION OF public.traces DEFAULT;

--
-- Data for Name: observations_2026_03; Type: TABLE DATA; Schema: public; Owner: yugabyte
--

COPY public.observations_2026_03 (id, project_id, trace_id, parent_observation_id, environment, type, name, start_time, end_time, level, status_message, version, input, output, metadata, provided_model_name, internal_model_id, model_parameters, provided_usage_details, usage_details, provided_cost_details, cost_details, total_cost, usage_pricing_tier_id, usage_pricing_tier_name, completion_start_time, prompt_id, prompt_name, prompt_version, tool_definitions, tool_calls, tool_call_names, created_at, updated_at, event_ts, is_deleted) FROM stdin;
\.


--
-- Data for Name: observations_default; Type: TABLE DATA; Schema: public; Owner: yugabyte
--

COPY public.observations_default (id, project_id, trace_id, parent_observation_id, environment, type, name, start_time, end_time, level, status_message, version, input, output, metadata, provided_model_name, internal_model_id, model_parameters, provided_usage_details, usage_details, provided_cost_details, cost_details, total_cost, usage_pricing_tier_id, usage_pricing_tier_name, completion_start_time, prompt_id, prompt_name, prompt_version, tool_definitions, tool_calls, tool_call_names, created_at, updated_at, event_ts, is_deleted) FROM stdin;
\.


--
-- Data for Name: scores_2026_03; Type: TABLE DATA; Schema: public; Owner: yugabyte
--

COPY public.scores_2026_03 (id, project_id, "timestamp", trace_id, observation_id, session_id, dataset_run_id, environment, name, value, string_value, long_string_value, data_type, source, comment, author_user_id, config_id, queue_id, execution_trace_id, metadata, created_at, updated_at, event_ts, is_deleted) FROM stdin;
\.


--
-- Data for Name: scores_default; Type: TABLE DATA; Schema: public; Owner: yugabyte
--

COPY public.scores_default (id, project_id, "timestamp", trace_id, observation_id, session_id, dataset_run_id, environment, name, value, string_value, long_string_value, data_type, source, comment, author_user_id, config_id, queue_id, execution_trace_id, metadata, created_at, updated_at, event_ts, is_deleted) FROM stdin;
\.


--
-- Data for Name: traces_2026_03; Type: TABLE DATA; Schema: public; Owner: yugabyte
--

COPY public.traces_2026_03 (id, project_id, "timestamp", name, user_id, session_id, environment, public, bookmarked, tags, input, output, metadata, release, version, created_at, updated_at, event_ts, is_deleted) FROM stdin;
\.


--
-- Data for Name: traces_default; Type: TABLE DATA; Schema: public; Owner: yugabyte
--

COPY public.traces_default (id, project_id, "timestamp", name, user_id, session_id, environment, public, bookmarked, tags, input, output, metadata, release, version, created_at, updated_at, event_ts, is_deleted) FROM stdin;
\.


--
-- YSQL database dump complete
--

