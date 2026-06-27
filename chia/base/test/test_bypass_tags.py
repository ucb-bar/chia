"""Test bypass tagging mechanism on a live Ray cluster.

Tests all combinations of bypass behavior:
  1. Bypass all calls to a function (no tag filtering)
  2. Bypass only calls with a specific tag
  3. Bypass calls matching a regex tag pattern
  4. No tags field in YAML = bypass all calls regardless of tag
  5. Function not listed in YAML = never bypassed
  6. Nested ChiaFunction calls from workers with tags
  7. Nested mixed: one function bypassed, another runs for real
  8. Real YAML file from the filesystem (same path as production usage)

Tests 1-7 use YAML strings written to temp files. This is a workaround
because ray job submit --working-dir packages only Python files, not
YAML files. The Bypass class itself reads YAML from a file path — the
temp file approach is purely a test convenience, not a production pattern.

Test 8 uses a real YAML file from the filesystem to verify the normal
production path works (pass --bypass-yaml to point at it).

Run all tests (embedded YAML):
  ray job submit --address IP:6379 --working-dir . -- \
      python -m chia.base.test.test_bypass_tags

Run with test 8 (real YAML file, tests the production path):
  ray job submit --address IP:6379 --working-dir . -- \
      python -m chia.base.test.test_bypass_tags \
      --bypass-yaml chia/base/test/bypass_test_all.yaml
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import ray

from chia.base.ChiaFunction import ChiaFunction
from chia.base.bypass import Bypass, get_active_bypass


# ---------------------------------------------------------------------------
# Test YAML configs as strings.
#
# We embed YAML in the Python file because ray job submit only packages
# .py files. In production, the YAML lives on the head node's filesystem
# and is passed via --bypass-config. This embedding is only for testing.
# ---------------------------------------------------------------------------

YAML_ALL = """
bypass:
  simple_add:
    bypass: true
  simple_multiply:
    bypass: true
"""

YAML_SPECIFIC_TAGS = """
bypass:
  simple_add:
    bypass: true
    tags: ["special"]
"""

YAML_REGEX_TAGS = """
bypass:
  simple_add:
    bypass: true
    tags: ["iter0_.*"]
"""

YAML_NESTED = """
bypass:
  simple_add:
    bypass: true
    tags: ["nested_add"]
  simple_multiply:
    bypass: true
"""

YAML_NESTED_MIXED = """
bypass:
  simple_add:
    bypass: true
    tags: ["nested_add"]
"""


def write_yaml(content: str) -> str:
    """Write YAML string to a temp file. Returns the file path."""
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Test ChiaFunctions
# ---------------------------------------------------------------------------

@ChiaFunction()
def simple_add(a: int, b: int) -> int:
    return a + b


@ChiaFunction()
def simple_multiply(a: int, b: int) -> int:
    return a * b


@ChiaFunction()
def nested_caller(x: int, bypass_state: dict | None = None) -> dict:
    """Dispatches simple_add and simple_multiply from a worker.

    bypass_state is needed because workers are separate processes with
    an empty Bypass singleton. This restores the bypass config on the
    worker so nested chia_remote() calls can check bypass.
    """
    if bypass_state is not None:
        from chia.base.bypass import Bypass as _Bypass
        worker_bypass = _Bypass()
        worker_bypass._bypass = bypass_state.get("bypass", {})
        worker_bypass._tag_patterns = bypass_state.get("tag_patterns", {})
        worker_bypass.set_provider("simple_add", lambda tag, dp, *a, **kw: 42)
        worker_bypass.set_provider("simple_multiply", lambda tag, dp, *a, **kw: 99)

    add_result = ray.get(simple_add.chia_remote(x, 10, _chia_tag="nested_add"))
    mul_result = ray.get(simple_multiply.chia_remote(x, 10, _chia_tag="nested_mul"))
    return {"add": add_result, "mul": mul_result}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def provider_42(tag, data_path, *args, **kwargs):
    return 42

def provider_99(tag, data_path, *args, **kwargs):
    return 99

def cond_true(tag, data_path, *args, **kwargs):
    return True

def cond_false(tag, data_path, *args, **kwargs):
    return False

def cond_first_arg_even(tag, data_path, *args, **kwargs):
    """Bypass only when the first positional call arg is even."""
    return args[0] % 2 == 0

def assert_eq(name, actual, expected):
    if actual == expected:
        print(f"  PASS: {name} = {actual}")
    else:
        print(f"  FAIL: {name} = {actual}, expected {expected}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Tests 1-7: embedded YAML (temp files)
# ---------------------------------------------------------------------------

def test_1_bypass_all_no_tags():
    """Bypass all calls to a function. No tag filtering.
    Verifies the basic bypass path: YAML says bypass:true, provider
    returns 42/99 instead of doing real computation."""
    print("\n=== Test 1: bypass all, no tags ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_ALL))
    bypass.set_provider("simple_add", provider_42)
    bypass.set_provider("simple_multiply", provider_99)

    assert_eq("simple_add(1,2) bypassed", ray.get(simple_add.chia_remote(1, 2)), 42)
    assert_eq("simple_multiply(3,4) bypassed", ray.get(simple_multiply.chia_remote(3, 4)), 99)


def test_2_bypass_with_specific_tags():
    """Only calls with _chia_tag="special" are bypassed.
    Calls with a different tag or no tag run the real function."""
    print("\n=== Test 2: bypass with specific tags ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_SPECIFIC_TAGS))
    bypass.set_provider("simple_add", provider_42)

    assert_eq("tag='special' (bypassed)", ray.get(simple_add.chia_remote(1, 2, _chia_tag="special")), 42)
    assert_eq("tag='other' (real)", ray.get(simple_add.chia_remote(1, 2, _chia_tag="other")), 3)
    assert_eq("no tag (real)", ray.get(simple_add.chia_remote(1, 2)), 3)


def test_3_bypass_with_regex_tags():
    """Tags field contains a regex pattern "iter0_.*".
    Calls tagged iter0_anything match and are bypassed.
    Calls tagged iter1_anything don't match and run for real."""
    print("\n=== Test 3: bypass with regex tags ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_REGEX_TAGS))
    bypass.set_provider("simple_add", provider_42)

    assert_eq("'iter0_opt0' matches", ray.get(simple_add.chia_remote(1, 2, _chia_tag="iter0_opt0")), 42)
    assert_eq("'iter0_opt5' matches", ray.get(simple_add.chia_remote(1, 2, _chia_tag="iter0_opt5")), 42)
    assert_eq("'iter1_opt0' no match (real)", ray.get(simple_add.chia_remote(1, 2, _chia_tag="iter1_opt0")), 3)


def test_4_no_tags_field_bypasses_all():
    """YAML has bypass:true but no tags field.
    All calls are bypassed regardless of whether they have a tag or not.
    This is the default behavior when tags are not specified."""
    print("\n=== Test 4: no tags field = bypass all calls ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_ALL))
    bypass.set_provider("simple_add", provider_42)

    assert_eq("with tag (bypassed)", ray.get(simple_add.chia_remote(1, 2, _chia_tag="anything")), 42)
    assert_eq("without tag (bypassed)", ray.get(simple_add.chia_remote(1, 2)), 42)


def test_5_not_listed_not_bypassed():
    """Function not mentioned in the YAML at all.
    Should run normally with no error. This is the expected default."""
    print("\n=== Test 5: function not listed = not bypassed ===")
    bypass = Bypass()  # no yaml

    assert_eq("simple_add(5,7) real", ray.get(simple_add.chia_remote(5, 7)), 12)


def test_6_nested_calls_with_tags():
    """nested_caller runs on a worker and dispatches simple_add and
    simple_multiply via chia_remote with tags. Tests that bypass works
    for ChiaFunction calls dispatched from workers (not just head node).
    Requires passing bypass_state to the worker to restore the Bypass."""
    print("\n=== Test 6: nested ChiaFunction calls with tags ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_NESTED))
    bypass.set_provider("simple_add", provider_42)
    bypass.set_provider("simple_multiply", provider_99)

    state = {"bypass": dict(bypass._bypass), "tag_patterns": dict(bypass._tag_patterns)}
    result = ray.get(nested_caller.chia_remote(5, bypass_state=state))
    assert_eq("nested add (tag matches)", result["add"], 42)
    assert_eq("nested mul (no tag filter)", result["mul"], 99)


def test_7_nested_mixed_bypass():
    """Nested calls where simple_add is bypassed (tag matches) but
    simple_multiply is NOT in the bypass config — runs for real.
    Verifies that mixed bypass/real works within the same worker."""
    print("\n=== Test 7: nested mixed — one bypassed, one real ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_NESTED_MIXED))
    bypass.set_provider("simple_add", provider_42)

    state = {"bypass": dict(bypass._bypass), "tag_patterns": dict(bypass._tag_patterns)}
    result = ray.get(nested_caller.chia_remote(5, bypass_state=state))
    assert_eq("nested add (bypassed)", result["add"], 42)
    assert_eq("nested mul (real: 5*10)", result["mul"], 50)


# ---------------------------------------------------------------------------
# Test 8: real YAML file from filesystem (production path)
# ---------------------------------------------------------------------------

def test_8_real_yaml_file(yaml_path: str):
    """Load bypass config from a real YAML file on the filesystem.
    This test verifies the YAML parsing mechanism and is how we expect
    the bypass feature to be used in production — the YAML exists on
    the head node and is passed as a path. No temp files, no embedded
    strings.  Tests 1-7 use embedded YAML written to temp files as a
    hack so they can be self-contained and run all at once without
    depending on external files."""
    print(f"\n=== Test 8: real YAML file ({yaml_path}) ===")
    bypass = Bypass(yaml_path=yaml_path)
    bypass.set_provider("simple_add", provider_42)
    bypass.set_provider("simple_multiply", provider_99)

    # If the YAML bypasses simple_add, this should return 42
    result = ray.get(simple_add.chia_remote(1, 2))
    if bypass.is_bypassed("simple_add"):
        assert_eq("simple_add bypassed via real YAML", result, 42)
    else:
        assert_eq("simple_add real (not in YAML)", result, 3)
    print("  (YAML loaded and applied correctly)")


# ---------------------------------------------------------------------------
# Tests 9-10: file server actor (head-node file delivery)
# ---------------------------------------------------------------------------

@ChiaFunction()
def fetch_text(path: str) -> str:
    """ChiaFunction whose real impl reads a local file. Used by file-server
    tests so the bypassed return type (str from the default file provider)
    matches the function's declared return type."""
    return Path(path).read_text()


def provider_uppercase_file(tag, data_path, *args, **kwargs):
    """Custom provider — pulls the file via the bypass file server actor
    and uppercases it. Demonstrates how custom providers reach the
    file_server() handle from inside a worker."""
    server = get_active_bypass().file_server()
    text = ray.get(server.get_text.remote(data_path))
    return text.upper()


def test_9_default_provider_uses_file_server():
    """The default file provider must read via the actor, not via the
    worker's local filesystem. We write a file on the head node and
    expect the worker to receive its contents through the actor.

    On a multi-node cluster the worker's local FS does NOT have this
    file; the test only passes if the actor pathway works. On a
    single-node cluster the file is also locally visible, but the
    implementation still goes through the actor — the contents arriving
    correctly proves the mechanism is engaged."""
    print("\n=== Test 9: default provider routes through file server ===")
    content = "hello from the head node\nline 2\n"
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="bypass_test_fs_")
    with os.fdopen(fd, "w") as f:
        f.write(content)

    yaml_content = f"""
bypass:
  fetch_text:
    bypass: true
    data: {path}
"""
    bypass = Bypass(yaml_path=write_yaml(yaml_content))
    # No provider registered — _default_file_provider is selected.
    result = ray.get(fetch_text.chia_remote("ignored_path"))
    assert_eq("default file provider via actor", result, content)


def test_10_custom_provider_uses_file_server():
    """Custom providers can call get_active_bypass().file_server() to read
    arbitrary files on the head node. We register a provider that reads
    via the actor and uppercases the result."""
    print("\n=== Test 10: custom provider uses file_server() ===")
    content = "lowercase text"
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="bypass_test_fs_")
    with os.fdopen(fd, "w") as f:
        f.write(content)

    yaml_content = f"""
bypass:
  fetch_text:
    bypass: true
    data: {path}
"""
    bypass = Bypass(yaml_path=write_yaml(yaml_content))
    bypass.set_provider("fetch_text", provider_uppercase_file)
    result = ray.get(fetch_text.chia_remote("ignored_path"))
    assert_eq("custom provider via file_server()", result, content.upper())


# ---------------------------------------------------------------------------
# Tests 11-16: bypass condition gate (set_cond)
# ---------------------------------------------------------------------------

def test_11_cond_true_bypasses():
    """A registered cond that returns True lets the bypass proceed."""
    print("\n=== Test 11: cond returns True -> bypassed ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_ALL))
    bypass.set_provider("simple_add", provider_42)
    bypass.set_cond("simple_add", cond_true)

    assert_eq("cond True (bypassed)", ray.get(simple_add.chia_remote(1, 2)), 42)


def test_12_cond_false_runs_real():
    """A registered cond that returns False forces a real run; the provider
    (which would return 42) is never used."""
    print("\n=== Test 12: cond returns False -> runs for real ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_ALL))
    bypass.set_provider("simple_add", provider_42)
    bypass.set_cond("simple_add", cond_false)

    assert_eq("cond False (real: 1+2)", ray.get(simple_add.chia_remote(1, 2)), 3)


def test_13_cond_after_tag_match():
    """The cond is the last gate: it is only consulted after the tag patterns
    match. tag-match + cond True -> bypass; tag-match + cond False -> real;
    tag no-match -> real regardless of the cond."""
    print("\n=== Test 13: cond gates only after tag matching ===")
    # tag matches "iter0_.*" AND cond True -> bypassed.
    b1 = Bypass(yaml_path=write_yaml(YAML_REGEX_TAGS))
    b1.set_provider("simple_add", provider_42)
    b1.set_cond("simple_add", cond_true)
    assert_eq("tag-match + cond True (bypassed)",
              ray.get(simple_add.chia_remote(1, 2, _chia_tag="iter0_x")), 42)

    # tag matches but cond False -> real.
    b2 = Bypass(yaml_path=write_yaml(YAML_REGEX_TAGS))
    b2.set_provider("simple_add", provider_42)
    b2.set_cond("simple_add", cond_false)
    assert_eq("tag-match + cond False (real)",
              ray.get(simple_add.chia_remote(1, 2, _chia_tag="iter0_x")), 3)

    # tag does NOT match -> real; the cond (True) is never reached.
    b3 = Bypass(yaml_path=write_yaml(YAML_REGEX_TAGS))
    b3.set_provider("simple_add", provider_42)
    b3.set_cond("simple_add", cond_true)
    assert_eq("tag no-match (real, cond irrelevant)",
              ray.get(simple_add.chia_remote(1, 2, _chia_tag="iter1_x")), 3)


def test_14_no_cond_defaults_true():
    """With no cond registered, the default is to bypass (cond defaults True)."""
    print("\n=== Test 14: no cond registered -> default bypass ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_ALL))
    bypass.set_provider("simple_add", provider_42)
    # No set_cond call.
    assert_eq("default (bypassed)", ray.get(simple_add.chia_remote(1, 2)), 42)


def test_15_cond_receives_call_args():
    """The cond receives the original call args; here it bypasses only when the
    first arg is even."""
    print("\n=== Test 15: cond receives the call args ===")
    bypass = Bypass(yaml_path=write_yaml(YAML_ALL))
    bypass.set_provider("simple_add", provider_42)
    bypass.set_cond("simple_add", cond_first_arg_even)

    assert_eq("first arg even (bypassed)", ray.get(simple_add.chia_remote(2, 3)), 42)
    assert_eq("first arg odd (real: 1+3)", ray.get(simple_add.chia_remote(1, 3)), 4)


def test_16_cond_survives_state_roundtrip():
    """get_state()/load_state() carry the conds so nested worker dispatches see
    them. Verified directly via a round-trip into a fresh Bypass."""
    print("\n=== Test 16: cond survives get_state/load_state ===")
    src = Bypass(yaml_path=write_yaml(YAML_ALL))
    src.set_provider("simple_add", provider_42)
    src.set_cond("simple_add", cond_false)

    restored = Bypass()
    restored.load_state(src.get_state())
    # cond_false was carried over, so is_bypassed is gated to False.
    assert_eq("restored cond gates bypass off",
              restored.is_bypassed("simple_add", None), False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bypass-yaml", type=str, default=None,
                        help="Path to a real YAML file for test 8")
    args = parser.parse_args()

    ray.init(address="auto")
    print("Connected to Ray cluster")

    # Tests 1-7: embedded YAML (always run)
    test_1_bypass_all_no_tags()
    test_2_bypass_with_specific_tags()
    test_3_bypass_with_regex_tags()
    test_4_no_tags_field_bypasses_all()
    test_5_not_listed_not_bypassed()
    test_6_nested_calls_with_tags()
    test_7_nested_mixed_bypass()

    # Tests 9-10: file server actor (always run)
    test_9_default_provider_uses_file_server()
    test_10_custom_provider_uses_file_server()

    # Tests 11-16: bypass condition gate (set_cond)
    test_11_cond_true_bypasses()
    test_12_cond_false_runs_real()
    test_13_cond_after_tag_match()
    test_14_no_cond_defaults_true()
    test_15_cond_receives_call_args()
    test_16_cond_survives_state_roundtrip()

    # Test 8: real YAML file (only if --bypass-yaml provided)
    if args.bypass_yaml:
        test_8_real_yaml_file(args.bypass_yaml)
    else:
        print("\n=== Test 8: SKIPPED (pass --bypass-yaml to run) ===")

    print("\n=== ALL TESTS PASSED ===")
