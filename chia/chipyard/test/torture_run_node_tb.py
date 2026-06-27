from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import unittest

from chia.chipyard.state_def import (
    BuildArtifact,
    BuildTarget,
    TortureMode,
    TortureResult,
)
from chia.chipyard.torture_run_node import TortureRunNode

RUN_E2E = os.environ.get("CHIA_TORTURE_E2E") == "1"


class TestStdoutParse(unittest.TestCase):
    """Deterministic tests for the stdout parsing path — no subprocess."""

    def setUp(self):
        self.node = TortureRunNode(chipyard_path=os.environ["CHIPYARD_PATH"])

    def test_all_match(self):
        stdout = (
            "/// header ///\n"
            "// All signatures match for output/test\n"
            "////////////////////\n"
        )
        match, fails = self.node._parse_single_stdout(stdout)
        self.assertTrue(match)
        self.assertEqual(fails, [])

    def test_simulation_failed(self):
        stdout = (
            "//  Simulation failed for output/test:\n"
            "\trtlsim\n"
            "//  Mismatched sigs for output/test:\n"
        )
        match, fails = self.node._parse_single_stdout(stdout)
        self.assertFalse(match)
        self.assertEqual(fails, ["output/test"])

    def test_only_mismatch(self):
        stdout = "//  Mismatched sigs for output/test:\n\trtlsim\n"
        match, fails = self.node._parse_single_stdout(stdout)
        self.assertFalse(match)
        self.assertEqual(fails, ["output/test"])

    def test_neither_marker_means_failure(self):
        stdout = "Error: ASM file could not be compiled or generated.\n"
        match, fails = self.node._parse_single_stdout(stdout)
        self.assertFalse(match)
        self.assertEqual(fails, [])


class TestGatherTests(unittest.TestCase):
    """Filesystem-backed test for per-test artifact collection (passing AND failing)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="torture_tb_")
        self.fake_chipyard = os.path.join(self.tmp, "chipyard")
        self.fake_torture = os.path.join(self.fake_chipyard, "tools", "torture")
        self.fake_output = os.path.join(self.fake_torture, "output")
        os.makedirs(self.fake_output, exist_ok=True)

        for ext, content in [
            (".S", "asm contents\n"),
            (".dump", "dump contents\n"),
            (".spike.sig", "AAA\n"),
            (".rtlsim.sig", "BBB\n"),
        ]:
            with open(os.path.join(self.fake_output, "test" + ext), "w") as f:
                f.write(content)
        with open(os.path.join(self.fake_output, "test_pseg_3.S"), "w") as f:
            f.write("narrowed pseg\n")

        self.node = TortureRunNode(chipyard_path=self.fake_chipyard)
        self.task_dir = os.path.join(self.tmp, "task")
        os.makedirs(self.task_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_gather_failure_includes_pseg(self):
        stdout = "//  Mismatched sigs for output/test:\n\trtlsim\n"
        tests = self.node._gather_single(stdout, all_match=False,
                                         failing_binaries=["output/test"], task_dir=self.task_dir)
        self.assertEqual(len(tests), 1)
        t = tests[0]
        self.assertEqual(t.name, "test")
        self.assertFalse(t.success)
        self.assertEqual(t.test_s, "asm contents\n")
        self.assertEqual(t.test_dump, "dump contents\n")
        self.assertEqual(t.spike_sig, "AAA\n")
        self.assertEqual(t.rtlsim_sig, "BBB\n")
        self.assertEqual(t.pseg_test_s, "narrowed pseg\n")

    def test_gather_success_collects_artifacts(self):
        stdout = "//  All signatures match for output/test\n"
        tests = self.node._gather_single(stdout, all_match=True,
                                         failing_binaries=[], task_dir=self.task_dir)
        self.assertEqual(len(tests), 1)
        t = tests[0]
        self.assertEqual(t.name, "test")
        self.assertTrue(t.success)
        self.assertEqual(t.test_s, "asm contents\n")
        self.assertEqual(t.spike_sig, "AAA\n")
        # pseg lookup is skipped on success
        self.assertIsNone(t.pseg_test_s)

    def test_persist_writes_to_task_dir(self):
        stdout = "//  Mismatched sigs for output/test:\n"
        self.node._gather_single(stdout, all_match=False,
                                 failing_binaries=["output/test"], task_dir=self.task_dir)
        persist = os.path.join(self.task_dir, "tests", "test")
        self.assertTrue(os.path.isfile(os.path.join(persist, "test.S")))
        self.assertTrue(os.path.isfile(os.path.join(persist, "test.spike.sig")))
        self.assertTrue(os.path.isfile(os.path.join(persist, "test_pseg.S")))


class TestSkippedOnBuildFailure(unittest.TestCase):
    """torture() should short-circuit cleanly when handed an unsuccessful BuildArtifact."""

    def test_failed_artifact_returns_failed_result(self):
        artifact = BuildArtifact(
            name="chipyard",
            simulator_binary_content=b"",
            simulator_binary_name="simulator-chipyard.harness-RocketConfig",
            config="RocketConfig",
            config_package="chipyard",
            target=BuildTarget.VERILATOR,
            success=False,
            stdout="build went sideways",
            stderr="something failed",
            returncode=2,
        )
        node = TortureRunNode(chipyard_path=os.environ["CHIPYARD_PATH"])
        result = node.torture(artifact, mode=TortureMode.SINGLE,
                              work_dir=tempfile.mkdtemp(prefix="torture_skip_"))
        self.assertIsInstance(result, TortureResult)
        self.assertFalse(result.success)
        self.assertEqual(result.num_tests, 0)
        self.assertEqual(result.tests, [])
        self.assertIn("Torture skipped", result.stderr)


@unittest.skipUnless(RUN_E2E, "set CHIA_TORTURE_E2E=1 to run the end-to-end smoke test")
class TestEndToEnd(unittest.TestCase):
    """Real torture run — needs JDK + spike + riscv64-unknown-elf-gcc on PATH and
    RocketConfig pre-buildable. Slow (10+ min for first SBT compile)."""

    def test_single_mode_against_rocketconfig(self):
        node = TortureRunNode(chipyard_path=os.environ["CHIPYARD_PATH"], timeout_seconds=3600)
        result = node.torture_from_config(
            config="RocketConfig",
            mode=TortureMode.SINGLE,
            work_dir="/tmp/torture-e2e",
        )
        self.assertIsNotNone(result.build_artifact)
        self.assertTrue(result.build_artifact.success,
                        f"Build failed: {result.build_artifact.stderr[-2000:]}")
        self.assertTrue(result.success,
                        f"Torture failed: {result.num_failures} failures.\n"
                        f"stdout tail:\n{result.stdout[-2000:]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--chipyard-path",
        required=True,
        help="Path to the chipyard checkout.",
    )
    args, remaining = parser.parse_known_args()
    os.environ["CHIPYARD_PATH"] = args.chipyard_path
    # Hand the rest of argv (test names, -v, etc.) to unittest.
    sys.argv[1:] = remaining
    unittest.main()
