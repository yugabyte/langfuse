import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple


LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "http://localhost:3000").rstrip("/")
LANGFUSE_PUBLIC_KEY = os.getenv(
    "LANGFUSE_PUBLIC_KEY",
    "pk-lf-local-tracing-project",
)
LANGFUSE_SECRET_KEY = os.getenv(
    "LANGFUSE_SECRET_KEY",
    "sk-lf-local-tracing-project-secret",
)
POLL_TIMEOUT_SECONDS = int(os.getenv("LANGFUSE_E2E_TIMEOUT_SECONDS", "30"))


def _auth_header() -> str:
    token = f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode("utf-8")
    return f"Basic {base64.b64encode(token).decode('utf-8')}"


def _request(
    method: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, Any]]:
    request_headers = {
        "Content-Type": "application/json",
    }
    if headers:
        request_headers.update(headers)

    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url=f"{LANGFUSE_HOST}{path}",
        data=body,
        headers=request_headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(req) as response:
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return response.status, parsed
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        parsed = json.loads(raw) if raw else {}
        return e.code, parsed


def _wait_until_trace_is_fetchable(trace_id: str) -> Dict[str, Any]:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    last_status = None
    last_body: Dict[str, Any] = {}

    while time.time() < deadline:
        current_status, trace_response = _request(
            "GET",
            f"/api/public/traces/{trace_id}",
            headers={"Authorization": _auth_header()},
        )
        last_status = current_status
        last_body = trace_response
        if current_status == 200 and trace_response.get("id") == trace_id:
            return trace_response
        time.sleep(1)

    raise AssertionError(
        "Trace was not fetchable in time. "
        f"trace_id={trace_id}, last_status={last_status}, last_body={last_body}"
    )


def _recent_iso_timestamps() -> Tuple[str, str]:
    now = datetime.now(timezone.utc)
    trace_ts = now.isoformat().replace("+00:00", "Z")
    span_ts = (now + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    return trace_ts, span_ts


def test_ingestion_accepts_valid_batch_and_trace_is_fetchable():
    trace_id = str(uuid.uuid4())
    span_id = str(uuid.uuid4())
    trace_ts, span_ts = _recent_iso_timestamps()

    payload = {
        "batch": [
            {
                "id": str(uuid.uuid4()),
                "type": "trace-create",
                "timestamp": trace_ts,
                "body": {
                    "id": trace_id,
                    "name": "e2e-api-trace",
                    "timestamp": trace_ts,
                    "userId": "e2e-user",
                },
            },
            {
                "id": str(uuid.uuid4()),
                "type": "span-create",
                "timestamp": span_ts,
                "body": {
                    "id": span_id,
                    "traceId": trace_id,
                    "name": "e2e-api-span",
                    "startTime": span_ts,
                },
            },
        ]
    }

    status, response = _request(
        "POST",
        "/api/public/ingestion",
        payload=payload,
        headers={"Authorization": _auth_header()},
    )

    assert status == 207
    assert isinstance(response, dict)
    assert "successes" in response or "errors" in response

    trace_response = _wait_until_trace_is_fetchable(trace_id)
    assert trace_response.get("name") == "e2e-api-trace"


def test_ingestion_rejects_invalid_payload_shape():
    status, response = _request(
        "POST",
        "/api/public/ingestion",
        payload={"not": "a valid ingestion payload"},
        headers={"Authorization": _auth_header()},
    )

    assert status == 400
    assert response.get("message") == "Invalid request data"


def test_ingestion_requires_auth():
    status, response = _request(
        "POST",
        "/api/public/ingestion",
        payload={"batch": []},
    )

    assert status == 401
    assert "message" in response


def test_otel_traces_rejects_invalid_content_type():
    status, response = _request(
        "POST",
        "/api/public/otel/v1/traces",
        payload={"resourceSpans": []},
        headers={
            "Authorization": _auth_header(),
            "Content-Type": "text/plain",
        },
    )

    assert status == 400
    assert response.get("error") == "Invalid content type"


def test_otel_traces_accepts_empty_json_payload():
    status, response = _request(
        "POST",
        "/api/public/otel/v1/traces",
        payload={"resourceSpans": []},
        headers={"Authorization": _auth_header()},
    )

    assert status == 200
    assert response == {}


def test_basic_flow_add_data_then_verify_then_validate_errors():
    """
    Basic API flow:
    1) Add data via ingestion API
    2) Verify trace can be fetched
    3) Validate bad request payload is rejected
    4) Validate missing auth is rejected
    """

    trace_id = str(uuid.uuid4())
    span_id = str(uuid.uuid4())
    trace_ts, span_ts = _recent_iso_timestamps()

    # STEP 1: add data
    ingest_payload = {
        "batch": [
            {
                "id": str(uuid.uuid4()),
                "type": "trace-create",
                "timestamp": trace_ts,
                "body": {
                    "id": trace_id,
                    "name": "basic-flow-trace",
                    "timestamp": trace_ts,
                    "userId": "basic-flow-user",
                },
            },
            {
                "id": str(uuid.uuid4()),
                "type": "span-create",
                "timestamp": span_ts,
                "body": {
                    "id": span_id,
                    "traceId": trace_id,
                    "name": "basic-flow-span",
                    "startTime": span_ts,
                },
            },
        ]
    }
    ingestion_status, ingestion_body = _request(
        "POST",
        "/api/public/ingestion",
        payload=ingest_payload,
        headers={"Authorization": _auth_header()},
    )
    assert ingestion_status == 207
    assert isinstance(ingestion_body, dict)

    # STEP 2: verify data
    trace_response = _wait_until_trace_is_fetchable(trace_id)
    assert trace_response.get("id") == trace_id
    assert trace_response.get("name") == "basic-flow-trace"

    # STEP 3: request validation
    invalid_status, invalid_body = _request(
        "POST",
        "/api/public/ingestion",
        payload={"invalid": "payload"},
        headers={"Authorization": _auth_header()},
    )
    assert invalid_status == 400
    assert invalid_body.get("message") == "Invalid request data"

    # STEP 4: auth validation
    unauthorized_status, _ = _request(
        "POST",
        "/api/public/ingestion",
        payload={"batch": []},
    )
    assert unauthorized_status == 401
