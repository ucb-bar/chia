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


@dataclass
class SimJob:
    """One FireMarshal job, staged for one FPGA.

    ``benchmark_name`` names the FireSim workload, its directory under
    ``deploy/workloads/``, and its results directory. The URIs are fsspec ones
    (``s3://``, ``file://``, or a plain path).
    """
    benchmark_name: str
    rootfs_uri: str
    bootbinary_uri: str
    outputs: list[str] = field(default_factory=list)
    simulation_outputs: list[str] = field(default_factory=lambda: ["uartlog"])


@dataclass
class SimJobResult:
    """Result of running one :class:`SimJob` on one FPGA.

    ``success`` is true only if infrasetup and runworkload both exited 0;
    ``log`` holds the manager output tail, which carries the reason on failure.
    """
    benchmark_name: str
    success: bool
    uartlog: str = ""
    outputs: dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0.0
    log: str = ""


@dataclass
class RunConfig:
    """Runtime knobs for one FireSim job, patched into ``config_runtime.yaml``.

    Every field defaults to ``None``, meaning "leave whatever is in the file".
    Only the fields you set are rewritten, so a hand-edit on the node survives
    unless a field explicitly overrides it. The flip side is that settings
    persist across jobs on the same worker: pass ``trace_enable=False`` to turn
    tracing back off, not ``None``.
    """
    plusarg_passthrough: str | None = None
    profile_interval: int | None = None        # FASED memory_stats; -1 is off
    # TracerV
    trace_enable: bool | None = None
    trace_output_format: int | None = None     # 0 human, 1 binary, 2 flamegraph
    trace_selector: int | None = None          # 0 none, 1 cycle, 2 pc, 3 insn
    trace_start: int | None = None
    trace_end: int | None = None
    # AutoCounter
    autocounter_read_rate: int | None = None   # 0 is off
    # host_debug
    zero_out_dram: bool | None = None
    disable_synth_asserts: bool | None = None
    # Synthesized prints
    print_start: int | None = None
    print_end: int | None = None
    print_cycle_prefix: bool | None = None


@dataclass
class BuildRecipe:
    """What to build: the FireSim quintuplet plus the Vivado knobs.

    Mirrors one stanza of ``config_build_recipes.yaml``.
    """
    name: str
    design: str = "FireSim"
    target_config: str = "FireSimRocketConfig"
    platform_config: str = "BaseF2Config"
    platform: str = "f2"
    target_project: str = "firesim"
    fpga_frequency: int = 75
    build_strategy: str = "TIMING"
    java_heap_size: str = "16G"

    def quintuplet(self) -> str:
        return "-".join([self.platform, self.target_project, self.design,
                         self.target_config, self.platform_config])


@dataclass
class EcadBuildResult:
    """Result of an :meth:`~chia.firesim.ecad_node.BitstreamBuildNode.build_bitstream`.

    ``bitstream`` is the artifact to hand to a run, and is ``None`` unless the
    build succeeded; ``log`` carries the reason when it did not.
    """
    recipe_name: str
    success: bool
    bitstream: "FSBitstream | None" = None
    log: str = ""
