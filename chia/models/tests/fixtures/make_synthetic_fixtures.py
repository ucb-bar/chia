"""Generate synthetic opencode export fixtures for error types we can't trigger
deterministically (rate-limit, billing, context overflow, output length, 5xx,
aborted).

The shapes mirror the REAL captures in this directory (see README.md): an
``APIError`` carries ``{message, statusCode, isRetryable, responseHeaders}`` and
lives at ``messages[].info.error`` — verified against opencode 1.15.13's real
output. Run from the repo root:

    python chia/models/tests/fixtures/make_synthetic_fixtures.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def _export(name, data, text=""):
    """Minimal export whose assistant message carries info.error = {name, data}."""
    parts = [{"type": "step-start"}]
    if text:
        parts.append({"type": "text", "text": text})
    parts.append({"type": "step-finish"})
    return {
        "info": {"id": "ses_synthetic", "directory": "/tmp"},
        "messages": [
            {"info": {"role": "user"}, "parts": [{"type": "text", "text": "hi"}]},
            {"info": {"role": "assistant", "tokens": {"input": 0, "output": 0},
                      "cost": 0.0, "error": {"name": name, "data": data}},
             "parts": parts},
        ],
    }


FIXTURES = {
    # APIError 429 with a real-style Retry-After header (drives reset_time).
    "opencode_export_rate_limit_429.json": _export(
        "APIError",
        {"message": "Rate limit exceeded", "statusCode": 429, "isRetryable": True,
         "responseHeaders": {"retry-after": "30"},
         "responseBody": "{\"error\":{\"message\":\"rate limit\"}}"},
    ),
    # APIError with a billing-flavored message (no 429/401) -> BillingError.
    "opencode_export_billing_402.json": _export(
        "APIError",
        {"message": "Quota exceeded for this billing plan.", "statusCode": 402,
         "isRetryable": False},
    ),
    # Context window exceeded; partial text may ride along (exit-0 truncation).
    "opencode_export_context_overflow.json": _export(
        "ContextOverflowError",
        {"message": "prompt is too long: 250000 tokens > 200000 maximum"},
        text="partial answer before overflow...",
    ),
    # Output token limit hit; opencode sends no data fields for this one.
    "opencode_export_output_length.json": _export(
        "MessageOutputLengthError", {},
    ),
    # 5xx / explicitly retryable -> ServerError.
    "opencode_export_server_503.json": _export(
        "APIError",
        {"message": "Service Unavailable", "statusCode": 503, "isRetryable": True,
         "responseHeaders": {"retry-after": "10"}},
    ),
    # APIError 400, non-retryable, non-billing -> InvalidRequestError.
    "opencode_export_invalid_request_400.json": _export(
        "APIError",
        {"message": "Bad request: unsupported parameter", "statusCode": 400,
         "isRetryable": False},
    ),
    # Aborted/cancelled -> UnknownOpenCodeError (no specific mapping).
    "opencode_export_aborted.json": _export(
        "MessageAbortedError", {"message": "Request was cancelled"},
    ),
}


def main():
    for fname, obj in FIXTURES.items():
        with open(os.path.join(HERE, fname), "w") as f:
            json.dump(obj, f, indent=2)
        print(f"wrote {fname}")


if __name__ == "__main__":
    main()
