"""Unit tests for BazelNode.

These exercise the node's own logic — argv construction, output collection,
failure/timeout handling, label parsing, digest stability — against a stub
``bazel`` executable, so no Bazel installation or workspace is required.
Behavior against real Bazel is covered by the cluster tests.

Run: pytest chia/bazel/test/test_bazel_node.py
"""

import json
import os
import stat
import textwrap

import pytest

from chia.bazel.bazel_node import BazelNode
from chia.bazel.state_def import BazelCommand


STUB = textwrap.dedent('''\
    #!/usr/bin/env python3
    """Stand-in for the bazel CLI, driven entirely by BAZEL_STUB_* env vars."""
    import json, os, sys, time

    argv = sys.argv[1:]
    with open(os.environ["BAZEL_STUB_LOG"], "a") as f:
        f.write(json.dumps(argv) + "\\n")

    sub = next((a for a in argv if not a.startswith("-")), "")
    if os.environ.get("BAZEL_STUB_SLEEP"):
        time.sleep(float(os.environ["BAZEL_STUB_SLEEP"]))

    if sub == "cquery" and "--output=files" in argv:
        print(os.environ.get("BAZEL_STUB_FILES", ""), end="")
    elif sub in ("query", "cquery"):
        print(os.environ.get("BAZEL_STUB_LABELS", ""), end="")
    elif sub == "info":
        print(os.environ.get("BAZEL_STUB_TESTLOGS", ""), end="")
    else:
        sys.stdout.write(os.environ.get("BAZEL_STUB_STDOUT", ""))
        sys.stderr.write(os.environ.get("BAZEL_STUB_STDERR", ""))
        sys.exit(int(os.environ.get("BAZEL_STUB_RC", "0")))
''')


@pytest.fixture
def stub_bazel(tmp_path):
    """A fake ``bazel`` on disk plus the env dict that steers it."""
    path = tmp_path / "bazel_stub.py"
    path.write_text(STUB)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IRWXU)
    log = tmp_path / "argv.log"
    return str(path), {"BAZEL_STUB_LOG": str(log)}, log


def calls(log):
    """Every stub invocation, as a list of argv lists."""
    return [json.loads(line) for line in log.read_text().splitlines()]


def test_build_success_collects_declared_outputs(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    workspace = tmp_path / "ws"
    (workspace / "bazel-out/bin").mkdir(parents=True)
    (workspace / "bazel-out/bin/app").write_bytes(b"ELF")
    env["BAZEL_STUB_FILES"] = "bazel-out/bin/app\n"

    result = BazelNode(str(workspace), bazel_bin=binary, env=env).build(
        ["//src:app"], collect_outputs=True,
    )

    assert result.success and result.returncode == 0
    assert result.command is BazelCommand.BUILD
    assert result.output_paths == ["bazel-out/bin/app"]
    assert result.outputs == {"bazel-out/bin/app": b"ELF"}
    assert result.duration_seconds >= 0


def test_build_lists_outputs_without_reading_them(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    env["BAZEL_STUB_FILES"] = "bazel-out/bin/app\n"

    result = BazelNode(str(tmp_path), bazel_bin=binary, env=env).build(["//src:app"])

    assert result.output_paths == ["bazel-out/bin/app"]
    assert result.outputs == {}


def test_build_failure_reports_returncode_and_skips_cquery(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    env |= {"BAZEL_STUB_RC": "1", "BAZEL_STUB_STDERR": "ERROR: missing dep\n"}

    result = BazelNode(str(tmp_path), bazel_bin=binary, env=env).build(["//src:app"])

    assert not result.success and result.returncode == 1
    assert "missing dep" in result.stderr
    assert result.output_paths == [] and result.outputs == {}
    assert [c for c in calls(log) if "cquery" in c] == []


def test_timeout_yields_minus_one_and_never_raises(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    env["BAZEL_STUB_SLEEP"] = "5"

    node = BazelNode(str(tmp_path), bazel_bin=binary, env=env, timeout_seconds=1)
    result = node.build(["//src:app"])

    assert not result.success and result.returncode == -1
    assert "timeout after 1s" in result.stderr


def test_missing_bazel_binary_is_reported_not_raised(tmp_path):
    result = BazelNode(str(tmp_path), bazel_bin=str(tmp_path / "nope")).build(["//a"])

    assert not result.success and result.returncode == -1
    assert "cannot execute" in result.stderr


def test_argv_order_startup_options_then_subcommand_then_flags(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    node = BazelNode(
        str(tmp_path), bazel_bin=binary, env=env,
        startup_options=["--host_jvm_args=-Xmx4g"],
        output_base="/tmp/ob", common_flags=["--keep_going"], config="ci",
    )

    node.build(["//src:app"], flags=["--verbose_failures"])

    argv = calls(log)[0]
    assert argv[:2] == ["--host_jvm_args=-Xmx4g", "--output_base=/tmp/ob"]
    assert argv[2] == "build"
    assert argv[3:] == ["--config=ci", "--keep_going", "--verbose_failures", "//src:app"]


def test_run_passes_binary_args_after_double_dash(tmp_path, stub_bazel):
    binary, env, log = stub_bazel

    result = BazelNode(str(tmp_path), bazel_bin=binary, env=env).run(
        "//tools:codegen", args=["--out", "gen.v"],
    )

    assert result.command is BazelCommand.RUN
    assert calls(log)[0] == ["run", "//tools:codegen", "--", "--out", "gen.v"]


def test_test_collects_logs_from_bazel_testlogs(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    testlogs = tmp_path / "testlogs"
    (testlogs / "src/app_test").mkdir(parents=True)
    (testlogs / "src/app_test/test.log").write_text("PASSED\n")
    (testlogs / "src/app_test/test.xml").write_text("<testsuites/>")
    env["BAZEL_STUB_TESTLOGS"] = f"{testlogs}\n"

    result = BazelNode(str(tmp_path), bazel_bin=binary, env=env).test(["//src:app_test"])

    assert result.command is BazelCommand.TEST
    assert result.test_logs == {
        "src/app_test/test.log": "PASSED\n",
        "src/app_test/test.xml": "<testsuites/>",
    }
    assert "--test_output=errors" in calls(log)[0]


def test_failing_test_is_a_result_not_an_exception(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    env |= {"BAZEL_STUB_RC": "3", "BAZEL_STUB_TESTLOGS": str(tmp_path / "none")}

    result = BazelNode(str(tmp_path), bazel_bin=binary, env=env).test(["//src:app_test"])

    assert not result.success and result.returncode == 3


def test_query_returns_labels_and_strips_cquery_config_suffix(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    env["BAZEL_STUB_LABELS"] = "//src:app (a1b2c3)\n//lib:core (a1b2c3)\n\n"

    labels = BazelNode(str(tmp_path), bazel_bin=binary, env=env).query(
        "deps(//src:app)", configured=True,
    )

    assert labels == ["//src:app", "//lib:core"]


def test_query_failure_returns_empty_list(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    env["BAZEL_STUB_RC"] = "7"
    node = BazelNode(str(tmp_path), bazel_bin=str(tmp_path / "nope"), env=env)

    assert node.query("deps(//src:app)") == []


def test_source_digest_tracks_content_of_transitive_sources(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    (tmp_path / "src").mkdir()
    source = tmp_path / "src/app.c"
    source.write_text("int main(){return 0;}")
    (tmp_path / "src/BUILD").write_text("cc_binary(name='app')")
    env["BAZEL_STUB_LABELS"] = "//src:app.c\n//src:BUILD\n@rules_cc//cc:defs.bzl\n"

    node = BazelNode(str(tmp_path), bazel_bin=binary, env=env)
    before = node.source_digest(["//src:app"])
    unchanged = node.source_digest(["//src:app"])
    source.write_text("int main(){return 1;}")
    after = node.source_digest(["//src:app"])

    assert len(before) == 32
    assert before == unchanged
    assert before != after


def test_source_digest_folds_in_extra_and_config(tmp_path, stub_bazel):
    binary, env, _log = stub_bazel
    env["BAZEL_STUB_LABELS"] = "//src:BUILD\n"
    (tmp_path / "src").mkdir()
    (tmp_path / "src/BUILD").write_text("cc_binary(name='app')")

    plain = BazelNode(str(tmp_path), bazel_bin=binary, env=env)
    with_config = BazelNode(str(tmp_path), bazel_bin=binary, env=env, config="opt")

    assert plain.source_digest(["//src:app"]) != with_config.source_digest(["//src:app"])
    assert plain.source_digest(["//src:app"]) != plain.source_digest(
        ["//src:app"], extra=["gcc-13"],
    )


def test_source_digest_empty_when_query_fails(tmp_path):
    node = BazelNode(str(tmp_path), bazel_bin=str(tmp_path / "nope"))

    assert node.source_digest(["//src:app"]) == ""


def test_label_to_path_handles_packages_root_and_external(tmp_path):
    to_path = BazelNode._label_to_path
    ws = str(tmp_path)

    assert to_path("//src:app.c", ws) == tmp_path / "src/app.c"
    assert to_path("//src:sub/app.c", ws) == tmp_path / "src/sub/app.c"
    assert to_path("//:top.c", ws) == tmp_path / "top.c"
    assert to_path("@rules_cc//cc:defs.bzl", ws) is None


def test_shutdown_invokes_the_server_teardown(tmp_path, stub_bazel):
    binary, env, log = stub_bazel

    result = BazelNode(str(tmp_path), bazel_bin=binary, env=env).shutdown()

    assert result.command is BazelCommand.SHUTDOWN
    assert calls(log)[0] == ["shutdown"]


def test_query_omits_build_only_flags_but_cquery_keeps_them(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    node = BazelNode(
        str(tmp_path), bazel_bin=binary, env=env,
        common_flags=["--verbose_failures"], config="ci",
    )

    node.query("deps(//src:app)")
    node.query("deps(//src:app)", configured=True)

    plain, configured = calls(log)
    assert plain == ["query", "--output=label", "deps(//src:app)"]
    assert configured[:3] == ["cquery", "--config=ci", "--verbose_failures"]


def test_output_listing_uses_the_builds_configuration(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    env["BAZEL_STUB_FILES"] = "bazel-out/bin/app\n"
    node = BazelNode(str(tmp_path), bazel_bin=binary, env=env, config="opt")

    node.build(["//src:app"], flags=["--copt=-O3"])

    cquery = next(c for c in calls(log) if c[0] == "cquery")
    assert cquery == ["cquery", "--config=opt", "--copt=-O3", "--output=files", "//src:app"]


def test_shutdown_and_info_take_no_build_flags(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    env["BAZEL_STUB_TESTLOGS"] = str(tmp_path / "none")
    node = BazelNode(str(tmp_path), bazel_bin=binary, env=env,
                     common_flags=["--verbose_failures"], config="ci")

    node.shutdown()
    node.test(["//src:app_test"])

    assert calls(log)[0] == ["shutdown"]
    assert next(c for c in calls(log) if c[0] == "info") == ["info", "bazel-testlogs"]


def test_collect_outputs_forces_a_local_download_for_remote_builds(tmp_path, stub_bazel):
    binary, env, log = stub_bazel
    env["BAZEL_STUB_FILES"] = "bazel-out/bin/app\n"
    node = BazelNode(str(tmp_path), bazel_bin=binary, env=env,
                     common_flags=["--remote_executor=grpcs://rbe.example:443"])

    node.build(["//src:app"], collect_outputs=True)

    assert "--remote_download_outputs=toplevel" in calls(log)[0]


def test_no_download_flag_added_when_outputs_are_not_collected(tmp_path, stub_bazel):
    binary, env, log = stub_bazel

    BazelNode(str(tmp_path), bazel_bin=binary, env=env).build(["//src:app"])

    assert not any(f.startswith("--remote_download") for f in calls(log)[0])


@pytest.mark.parametrize("where", ["common", "call"])
def test_callers_own_remote_download_flag_wins(tmp_path, stub_bazel, where):
    binary, env, log = stub_bazel
    override = "--remote_download_outputs=all"
    node = BazelNode(str(tmp_path), bazel_bin=binary, env=env,
                     common_flags=[override] if where == "common" else [])

    node.build(["//src:app"], flags=[override] if where == "call" else None,
               collect_outputs=True)

    download_flags = [f for f in calls(log)[0] if f.startswith("--remote_download")]
    assert download_flags == [override]
