"""Multi-workload suite orchestrator.

Fans out per-workload FireSim simulation runs via Ray, manages a sliding
window of concurrent simulations, and aggregates results.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, fields
from datetime import datetime, timezone

from chia.aws.config import AWSConfig
from chia.cluster.log import get_logger
from chia.firesim.config import FireSimRunConfig
from chia.firesim.workloads import WorkloadManifest, resolve_run_configs
from chia.firesim.state_def import FireSimRunResult, SuiteRunResult

logger = get_logger("firesim.suite_runner")


class SuiteRunner:
    """Orchestrates running multiple workloads as independent FireSim simulations.

    Each workload gets its own F2 instance, FPGA flash, simulation, and teardown.
    Runs up to ``parallelism`` workloads concurrently via Ray, uploading each
    workload's artifacts and a refreshed ``summary.json`` to S3 as it completes,
    then aggregates the per-workload results and SPEC scores into a single
    :class:`SuiteRunResult`.
    """

    def __init__(
        self,
        manifest: WorkloadManifest,
        workload_names: list[str] | None,
        base_run_config: FireSimRunConfig,
        aws_config: AWSConfig,
        s3_bucket: str,
        parallelism: int = 4,
        sim_timeout: int = 14400,
        results_dir: str | None = None,
        instance_prefix: str | None = None,
        terminate_on_failure: bool = True,
        local_log_dir: str | None = None,
        log_prefix: str = "firesim-run",
    ):
        """Initialize the suite runner.

        Args:
            manifest: The suite's workload manifest, providing the suite name
                and the full set of workloads available to run.
            workload_names: Subset of workload names to run; ``None`` runs every
                workload defined in the manifest. Resolved into per-workload run
                configs via ``resolve_run_configs``.
            base_run_config: Base FireSim run config applied to every workload.
                If it carries a ``build_ref``, the AGFI and driver S3 path are
                auto-resolved from it against ``s3_bucket``.
            aws_config: AWS configuration for the runs; its ``s3_bucket`` is the
                destination bucket for uploaded results and summaries.
            s3_bucket: S3 bucket used to resolve the build (AGFI + driver) from
                ``base_run_config.build_ref`` and per-workload configs.
            parallelism: Maximum number of workloads to run concurrently in the
                sliding window of in-flight Ray tasks.
            sim_timeout: Maximum simulation runtime in seconds per workload
                (default 14400 = 4h).
            results_dir: Local directory to store per-workload results. Optional.
            instance_prefix: Optional prefix applied to launched EC2 instance
                name(s); ``None`` uses the runner's default naming.
            terminate_on_failure: If False, leave instances running when a
                simulation raises — for post-mortem SSH debugging.
            local_log_dir: Local directory to mirror each run's logs into.
                Optional; if ``None``, logs are not copied back to the head node.
            log_prefix: Filename/tag prefix for emitted log files; the suite name
                is appended per workload (default ``"firesim-run"``).
        """
        self.manifest = manifest
        self.workload_names = workload_names
        self.base_run_config = base_run_config
        self.aws_config = aws_config
        self.s3_bucket = s3_bucket
        self.parallelism = parallelism
        self.sim_timeout = sim_timeout
        self.results_dir = results_dir
        self.instance_prefix = instance_prefix
        self.terminate_on_failure = terminate_on_failure
        self.local_log_dir = local_log_dir
        self.log_prefix = log_prefix

    def run(self) -> SuiteRunResult:
        """Run all requested workloads and return aggregated results.

        Resolves the per-workload run configs, then dispatches them through Ray
        in a sliding window of up to ``parallelism`` concurrent simulations. As
        each workload finishes, its artifacts and a refreshed ``summary.json``
        are uploaded to S3 so external observers see progress incrementally.
        After all workloads complete, SPEC scores are extracted from the rootfs
        outputs and the final summary, aggregated ``results.csv``, and TMA
        counter deltas are uploaded.

        Returns:
            SuiteRunResult aggregating every workload's FireSimRunResult (keyed
            by workload name), the overall success flag, the total wall-clock
            duration, and the per-workload SPEC scores.
        """
        import boto3
        import ray

        from chia.base.ChiaFunction import get
        from chia.firesim.chia_functions import firesim_run_workload

        t0 = time.time()

        # Auto-derive agfi + driver_s3_path from build_ref if provided
        if self.base_run_config.build_ref:
            self.base_run_config.resolve_build(self.s3_bucket)

        # Resolve per-workload configs
        run_configs = resolve_run_configs(
            self.manifest,
            self.workload_names,
            self.base_run_config,
            self.s3_bucket,
        )

        if not run_configs:
            return SuiteRunResult(
                suite_name=self.manifest.suite,
                workload_results={},
                all_success=True,
                total_duration_seconds=0.0,
            )

        logger.info(
            f"Running {len(run_configs)} workloads from suite '{self.manifest.suite}' "
            f"with parallelism={self.parallelism}"
        )

        # Set up S3 upload destination once so every workload uploads into the
        # same prefix as it completes.
        results_bucket = self.aws_config.s3_bucket
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d--%H-%M-%S")
        result_id = self.base_run_config.result_id
        if result_id:
            s3_prefix = f"results/{result_id}/{self.manifest.suite}"
        else:
            s3_prefix = f"results/{self.manifest.suite}/{timestamp}"
        s3 = boto3.client("s3")

        build_info: dict = {}
        if self.base_run_config.build_ref:
            try:
                from chia.firesim.config import fetch_build_info
                build_info = fetch_build_info(results_bucket, self.base_run_config.build_ref)
            except Exception as e:
                logger.warning(f"Could not fetch build info for summary: {e}")

        aws_config_dict = asdict(self.aws_config)
        workload_results: dict[str, FireSimRunResult] = {}

        # Sliding window: maintain up to self.parallelism in-flight tasks
        pending: dict[ray.ObjectRef, str] = {}  # ref -> workload_name
        config_queue = list(run_configs)

        def _submit_next():
            if not config_queue:
                return
            cfg = config_queue.pop(0)
            name = cfg.workload_name
            logger.info(f"Submitting workload: {name}")
            suite_log_prefix = f"{self.log_prefix}-{self.manifest.suite}"
            ref = firesim_run_workload.chia_remote(
                run_config_dict=asdict(cfg),
                aws_config_dict=aws_config_dict,
                sim_timeout=self.sim_timeout,
                results_dir=self.results_dir,
                name=f"{self.manifest.suite}_{name}",
                instance_prefix=self.instance_prefix,
                terminate_on_failure=self.terminate_on_failure,
                local_log_dir=self.local_log_dir,
                log_prefix=suite_log_prefix,
            )
            pending[ref] = name

        # Fill initial window
        for _ in range(min(self.parallelism, len(config_queue))):
            _submit_next()

        # Process completions
        while pending:
            ready, _ = ray.wait(list(pending.keys()), num_returns=1)
            for ref in ready:
                name = pending.pop(ref)
                try:
                    result = get(ref)
                    workload_results[name] = result
                    status = "SUCCESS" if result.success else "FAILED"
                    logger.info(
                        f"Workload {name}: {status} "
                        f"({result.duration_seconds:.1f}s)"
                    )

                    from chia.trace.profiler import get_profiler
                    profiler = get_profiler()
                    profiler.add_info({
                        "workload_name": name,
                        "workload_wall_clock_s": result.duration_seconds,
                        "workload_success": result.success,
                    })
                except Exception as e:
                    logger.error(f"Workload {name} raised exception: {e}")
                    workload_results[name] = FireSimRunResult(
                        workload_name=name,
                        success=False,
                        uartlogs={"error": str(e)},
                    )

                # Upload this workload's artifacts and refresh summary.json so
                # external observers see progress without waiting for the whole
                # suite to finish.
                self._upload_workload(s3, results_bucket, s3_prefix, name, workload_results[name])
                self._upload_summary(
                    s3, results_bucket, s3_prefix,
                    timestamp=timestamp,
                    workload_results=workload_results,
                    all_success=all(r.success for r in workload_results.values()),
                    total_duration_seconds=time.time() - t0,
                    scores={},
                    build_info=build_info,
                )

                # Submit next workload from queue
                _submit_next()

        total_duration = time.time() - t0
        all_success = all(r.success for r in workload_results.values())

        # Extract SPEC scores from rootfs outputs
        from chia.firesim.spec_parser import compute_aggregate_score, extract_spec_scores

        scores: dict[str, dict[str, float]] = {}
        for name, result in workload_results.items():
            if result.rootfs_outputs:
                first_slot_outputs = next(iter(result.rootfs_outputs.values()), {})
                if first_slot_outputs:
                    spec_scores = extract_spec_scores(first_slot_outputs, name)
                    if spec_scores:
                        scores[name] = spec_scores

        # Summary
        succeeded = sum(1 for r in workload_results.values() if r.success)
        failed = len(workload_results) - succeeded
        logger.info(
            f"Suite '{self.manifest.suite}' complete: "
            f"{succeeded}/{len(workload_results)} succeeded, "
            f"{failed} failed, total {total_duration:.1f}s"
        )

        if scores:
            for name, sc in sorted(scores.items()):
                logger.info(
                    f"  {name}: score={sc['score']:.3f} "
                    f"(RealTime={sc['RealTime']:.1f}s)"
                )
            agg = compute_aggregate_score(scores)
            logger.info(f"  Aggregate SPEC score (geomean): {agg:.3f}")

        suite_result = SuiteRunResult(
            suite_name=self.manifest.suite,
            workload_results=workload_results,
            all_success=all_success,
            total_duration_seconds=total_duration,
            scores=scores,
        )

        # Final summary (supersedes the in-progress one), aggregated CSV,
        # and TMA counter deltas.
        self._upload_summary(
            s3, results_bucket, s3_prefix,
            timestamp=timestamp,
            workload_results=workload_results,
            all_success=all_success,
            total_duration_seconds=total_duration,
            scores=scores,
            build_info=build_info,
        )
        if scores:
            self._upload_results_csv(s3, results_bucket, s3_prefix, scores)
        self._upload_tma_results(s3, results_bucket, s3_prefix, workload_results)
        logger.info(f"Results uploaded to s3://{results_bucket}/{s3_prefix}/")

        return suite_result

    def _upload_workload(
        self,
        s3,
        results_bucket: str,
        s3_prefix: str,
        name: str,
        wr: FireSimRunResult,
    ) -> None:
        """Upload a single workload's artifacts (uartlog, rootfs outputs, sim outputs)."""
        wl_prefix = f"{s3_prefix}/{name}"

        for log_name, log_content in wr.uartlogs.items():
            if log_content:
                try:
                    s3.put_object(
                        Bucket=results_bucket,
                        Key=f"{wl_prefix}/{log_name}",
                        Body=log_content.encode(),
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload uartlog {log_name} for {name}: {e}")

        for outputs in wr.rootfs_outputs.values():
            for relpath, content in outputs.items():
                try:
                    s3.put_object(
                        Bucket=results_bucket,
                        Key=f"{wl_prefix}/output/{relpath}",
                        Body=content.encode(),
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload {relpath} for {name}: {e}")

        for outputs in wr.sim_outputs.values():
            for relpath, content in outputs.items():
                try:
                    s3.put_object(
                        Bucket=results_bucket,
                        Key=f"{wl_prefix}/sim/{relpath}",
                        Body=content.encode(),
                    )
                except Exception as e:
                    logger.warning(f"Failed to upload sim output {relpath} for {name}: {e}")

    def _upload_summary(
        self,
        s3,
        results_bucket: str,
        s3_prefix: str,
        *,
        timestamp: str,
        workload_results: dict[str, FireSimRunResult],
        all_success: bool,
        total_duration_seconds: float,
        scores: dict[str, dict[str, float]],
        build_info: dict,
    ) -> None:
        """Upload a snapshot of ``summary.json`` for the suite."""
        rc = self.base_run_config
        summary = {
            "suite_name": self.manifest.suite,
            "timestamp": timestamp,
            "all_success": all_success,
            "total_duration_seconds": total_duration_seconds,
            "scores": scores,
            "build": {
                "build_ref": rc.build_ref,
                "agfi": rc.agfi,
                "build_config": build_info.get("build_config", {}),
                "build_id": build_info.get("build_id", ""),
            },
            "run_config": {
                "hw_config_name": rc.hw_config_name,
                "instance_type": rc.instance_type,
                "num_sims": rc.num_sims,
                "market": rc.market,
                "plusarg_passthrough": rc.plusarg_passthrough,
                "sim_timeout": self.sim_timeout,
                "parallelism": self.parallelism,
            },
            "workloads": {},
        }
        for name, wr in workload_results.items():
            uartlog_tail = ""
            if wr.uartlogs:
                first_log = next(iter(wr.uartlogs.values()), "")
                uartlog_tail = first_log[-500:] if first_log else ""
            summary["workloads"][name] = {
                "success": wr.success,
                "duration_seconds": wr.duration_seconds,
                "uartlog_tail": uartlog_tail,
            }

        try:
            s3.put_object(
                Bucket=results_bucket,
                Key=f"{s3_prefix}/summary.json",
                Body=json.dumps(summary, indent=2).encode(),
                ContentType="application/json",
            )
        except Exception as e:
            logger.warning(f"Failed to upload summary: {e}")

    def _upload_results_csv(
        self,
        s3,
        results_bucket: str,
        s3_prefix: str,
        scores: dict[str, dict[str, float]],
    ) -> None:
        """Upload aggregated results.csv (scores + geomean)."""
        from chia.firesim.spec_parser import compute_aggregate_score

        csv_lines = ["name,RealTime,UserTime,KernelTime,score"]
        for name, sc in sorted(scores.items()):
            csv_lines.append(
                f"{name},{sc['RealTime']},{sc['UserTime']},{sc['KernelTime']},{sc['score']}"
            )
        agg = compute_aggregate_score(scores)
        csv_lines.append(f"AGGREGATE_GEOMEAN,,,,{agg}")

        try:
            s3.put_object(
                Bucket=results_bucket,
                Key=f"{s3_prefix}/results.csv",
                Body="\n".join(csv_lines).encode(),
                ContentType="text/csv",
            )
            logger.info(f"Uploaded results.csv to s3://{results_bucket}/{s3_prefix}/results.csv")
        except Exception as e:
            logger.warning(f"Failed to upload results.csv: {e}")

    def _upload_tma_results(
        self,
        s3,
        results_bucket: str,
        s3_prefix: str,
        workload_results: dict[str, FireSimRunResult],
    ) -> None:
        """Compute per-benchmark TMA counter deltas (after - before) and upload as CSV."""
        import csv
        import io

        tma_rows = []
        for name, wr in workload_results.items():
            for slot_outputs in wr.rootfs_outputs.values():
                before_content = None
                after_content = None
                for relpath, content in slot_outputs.items():
                    if relpath.endswith("_tma_before.csv"):
                        before_content = content
                    elif relpath.endswith("_tma_after.csv"):
                        after_content = content
                if before_content and after_content:
                    try:
                        before = {row["counter"]: int(row["value"])
                                  for row in csv.DictReader(io.StringIO(before_content))}
                        after = {row["counter"]: int(row["value"])
                                 for row in csv.DictReader(io.StringIO(after_content))}
                        delta = {k: after.get(k, 0) - before.get(k, 0) for k in after}
                        delta["_benchmark"] = name
                        tma_rows.append(delta)
                    except Exception as e:
                        logger.warning(f"Failed to compute TMA delta for {name}: {e}")

        if not tma_rows:
            return

        all_counters = sorted({k for row in tma_rows for k in row if k != "_benchmark"})
        tma_lines = ["name," + ",".join(all_counters)]
        for row in sorted(tma_rows, key=lambda r: r["_benchmark"]):
            vals = [str(row.get(c, 0)) for c in all_counters]
            tma_lines.append(f"{row['_benchmark']},{','.join(vals)}")

        try:
            s3.put_object(
                Bucket=results_bucket,
                Key=f"{s3_prefix}/tma_results.csv",
                Body="\n".join(tma_lines).encode(),
                ContentType="text/csv",
            )
            logger.info(f"Uploaded tma_results.csv to s3://{results_bucket}/{s3_prefix}/tma_results.csv")
        except Exception as e:
            logger.warning(f"Failed to upload tma_results.csv: {e}")