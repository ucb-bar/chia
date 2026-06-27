"""Result dataclasses for FireSim build and run operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BitstreamBuildResult:
    """Result of a :func:`firesim_build_bitstream` FPGA bitstream build.

    Attributes:
        recipe_name: Name of the build recipe this result is for.
        agfi: AWS Global FPGA Image ID produced by the build (e.g. ``"agfi-..."``);
            ``None`` on failure or for non-AGFI platforms (see ``bitstream_path``).
        afi: AWS FPGA Image ID (the un-globalized ``"afi-..."`` handle);
            ``None`` when not produced.
        success: True iff the build completed and a usable image was produced.
        build_log: Captured build log (also carries the failure reason on early
            recipe-resolution errors).
        hwdb_entry: The hardware-database YAML stanza for this bitstream, ready
            to paste/feed into a FireSim run config.
        driver_s3_path: S3 URI of the uploaded simulation driver tarball;
            ``None`` if not uploaded.
        build_id: Unique identifier assigned to this build.
        build_ref: ``"{recipe_name}/{build_id}"`` — the handle to pass as a run
            config's ``build_ref`` to auto-resolve the AGFI + driver for a run.
        bitstream_path: S3 URI of a raw ``.bit`` artifact, for platforms that
            don't produce an AGFI (e.g. ``corigine_xb10``); ``None`` for
            AGFI-producing platforms (``f2``/``f1``).
    """
    recipe_name: str
    agfi: str | None
    afi: str | None
    success: bool
    build_log: str
    hwdb_entry: str
    driver_s3_path: str | None = None
    build_id: str = ""
    build_ref: str = ""  # "{recipe_name}/{build_id}" — use this for runs
    # S3 URI of a raw .bit artifact for platforms that don't produce an AGFI
    # (e.g. corigine_xb10). None for AGFI-producing platforms (f2/f1).
    bitstream_path: str | None = None


@dataclass
class FireSimRunResult:
    """Result of a :func:`firesim_run_workload` FPGA simulation run.

    Attributes:
        workload_name: Name of the workload that was run.
        success: True iff the simulation completed successfully.
        uartlogs: Per-slot UART console output, mapping ``slot_name`` ->
            captured uartlog text.
        rootfs_outputs: Per-slot files copied back from each simulation's
            rootfs, mapping ``slot_name`` -> ``{relative_filepath -> file_content}``.
        sim_outputs: Per-slot host-side simulation artifacts (memory_stats,
            autocounters, etc.), mapping ``slot_name`` ->
            ``{relative_filepath -> file_content}``.
        duration_seconds: Wall-clock duration of the run, in seconds.
    """
    workload_name: str
    success: bool
    uartlogs: dict[str, str] = field(default_factory=dict)
    rootfs_outputs: dict[str, dict[str, str]] = field(default_factory=dict)
    # rootfs_outputs maps slot_name -> {relative_filepath -> file_content}
    sim_outputs: dict[str, dict[str, str]] = field(default_factory=dict)
    # sim_outputs maps slot_name -> {relative_filepath -> file_content}
    # for all host-side simulation artifacts (memory_stats, autocounters, etc.)
    duration_seconds: float = 0.0


@dataclass
class SuiteRunResult:
    suite_name: str
    workload_results: dict[str, FireSimRunResult] = field(default_factory=dict)
    all_success: bool = False
    total_duration_seconds: float = 0.0
    scores: dict[str, dict[str, float]] = field(default_factory=dict)
    # scores maps workload_name -> {RealTime, UserTime, KernelTime, score}
