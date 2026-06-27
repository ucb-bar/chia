"""Capture REAL Bedrock Converse error shapes for fixtures.

Run with your AWS creds + region set, and a real (tool-capable) model in
BEDROCK_TEST_MODEL. Triggers only PRE-INFERENCE rejections (bad creds / bad
model / bad params / over-context) so there's effectively no inference cost.
Prints a sanitized JSON blob (no RequestId / ResponseMetadata) to paste back.

    AWS_REGION=us-east-1 BEDROCK_TEST_MODEL='us.amazon.nova-lite-v1:0' \
      python /tmp/bedrock_capture.py
"""
import json
import os

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
MODEL = os.environ["BEDROCK_TEST_MODEL"]


def _sanitize(err_response):
    """Keep only the error contract + HTTP status; drop request-id/metadata."""
    error = err_response.get("Error", {})
    return {
        "code": error.get("Code", ""),
        "message": error.get("Message", ""),
        "http_status": err_response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
    }


def capture(label, *, client=None, model=MODEL, messages=None, max_tokens=16):
    c = client or boto3.client("bedrock-runtime", region_name=REGION)
    msgs = messages or [{"role": "user", "content": [{"text": "hi"}]}]
    try:
        c.converse(modelId=model, messages=msgs,
                   inferenceConfig={"maxTokens": max_tokens})
        return {"label": label, "ok": True}
    except ClientError as e:
        rec = {"label": label}
        rec.update(_sanitize(e.response))
        return rec
    except Exception as e:
        return {"label": label, "non_client_error": f"{type(e).__name__}: {e}"}


out = {}

# a) Bad credentials (pre-auth reject; no real key used)
bogus = boto3.client(
    "bedrock-runtime", region_name=REGION,
    aws_access_key_id="AKIAFAKEFAKEFAKEFAKE",
    aws_secret_access_key="fakefakefakefakefakefakefakefakefakefake",
)
out["bad_credentials"] = capture("bad_credentials", client=bogus)

# b) Unknown model id
out["unknown_model"] = capture("unknown_model", model="fake.nonexistent-model-v99")

# c) Bad parameter (maxTokens below the minimum)
out["bad_param_maxtokens"] = capture("bad_param_maxtokens", max_tokens=0)

# d) Over-context: a very large prompt vs the model's window
big = "data " * 400000  # ~2MB of text, well over any context window
out["over_context"] = capture("over_context", messages=[{"role": "user", "content": [{"text": big}]}])

print("===BEDROCK_CAPTURE_JSON_BEGIN===")
print(json.dumps(out, indent=2))
print("===BEDROCK_CAPTURE_JSON_END===")
