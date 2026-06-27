"""Capture REAL Vertex Gemini (google-genai) error shapes for fixtures.

Run with GCP ADC + project/location set. Triggers cheap error cases (unknown
model, invalid argument) by default — both rejected at validation, so no
generation cost. The over-context case is opt-in (CAPTURE_OVER_CONTEXT=1) since
Gemini's context window is huge and a successful huge request would bill input
tokens. Prints a sanitized JSON blob (project/location redacted) to paste back.

    GOOGLE_CLOUD_PROJECT=your-proj GOOGLE_CLOUD_LOCATION=us-central1 \
      VERTEX_TEST_MODEL=gemini-2.5-flash \
      python \
      chia/models/tests/fixtures/vertex/capture_real_fixtures.py
"""
import json
import os

from google import genai
from google.genai import errors as genai_errors
from google.genai import types

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
MODEL = os.environ.get("VERTEX_TEST_MODEL", "gemini-2.5-flash")

client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)


def _redact(s):
    if not isinstance(s, str):
        return s
    return s.replace(PROJECT, "<PROJECT>").replace(LOCATION, "<LOCATION>")


def capture(label, *, model=MODEL, text="hi", config=None):
    try:
        client.models.generate_content(
            model=model,
            contents=[types.Content(role="user", parts=[types.Part.from_text(text=text)])],
            config=config or types.GenerateContentConfig(max_output_tokens=16),
        )
        return {"label": label, "ok": True}
    except genai_errors.APIError as e:
        return {
            "label": label,
            "code": getattr(e, "code", None),               # HTTP status int (what we classify on)
            "status": _redact(getattr(e, "status", None)),  # e.g. NOT_FOUND / INVALID_ARGUMENT
            "message": _redact((getattr(e, "message", None) or str(e)))[:400],
        }
    except Exception as e:
        return {"label": label, "non_api_error": f"{type(e).__name__}: {_redact(str(e))[:200]}"}


out = {}

# a) Unknown model id
out["unknown_model"] = capture("unknown_model", model="gemini-nonexistent-v99")

# b) Invalid argument (max_output_tokens absurdly large -> server 400)
out["invalid_argument"] = capture(
    "invalid_argument",
    config=types.GenerateContentConfig(max_output_tokens=100_000_000),
)

# c) Over-context — OPT-IN (may bill input tokens if it isn't rejected at validation)
if os.environ.get("CAPTURE_OVER_CONTEXT") == "1":
    out["over_context"] = capture("over_context", text="data " * 1_500_000)
else:
    out["over_context"] = {"label": "over_context", "skipped": "set CAPTURE_OVER_CONTEXT=1 to run"}

print("===VERTEX_CAPTURE_JSON_BEGIN===")
print(json.dumps(out, indent=2))
print("===VERTEX_CAPTURE_JSON_END===")
