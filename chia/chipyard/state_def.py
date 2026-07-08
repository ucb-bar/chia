from dataclasses import dataclass, field
from enum import Enum


class BuildTarget(str, Enum):
    """Which Chipyard simulator flavor :class:`ChiselBuildNode` builds.
    Maps to ``make`` targets in ``chipyard/sims/verilator``.

    Attributes:
        VERILATOR: Default fast Verilator sim (``make`` in ``sims/verilator``).
            No waveform capability; binary ``simulator-<pkg>.harness-<config>``.
        VERILATOR_DEBUG: Waveform/VCD-capable Verilator sim (``make debug``).
            Binary suffixed ``-debug``. Required for ``wf_scopes`` (build-time
            spatial trace filter) and runtime wave windows / VCD capture.
        FIRESIM_METASIM_VERILATOR: A FireSim metasim (``VFireSim``) built under
            ``sims/firesim/sim``; binary name uses the FireSim quintuplet
            ``<PLATFORM>-firesim-<DESIGN>-<config>-<PLATFORM_CONFIG>``.
    """
    VERILATOR = "verilator"
    VERILATOR_DEBUG = "verilator_debug"
    FIRESIM_METASIM_VERILATOR = "firesim_metasim_verilator"


@dataclass
class WaveWindow:
    """One PC-triggered waveform-capture window.

    Waveform collection is captured on the Nth retired commit of `pc` happens
    and dumps for `cyc` BOOM-core cycles thereafter. Each window fires at most
    once; overlapping windows merge into a single contiguous dump segment.
    """
    pc: int                # PC to trigger on; serialized as bare hex (no 0x)
    cyc: int               # cycles to capture (BOOM-core domain, not testbench)
    n: int = 1             # trigger on the Nth retired commit (>=1)


@dataclass
class BuildArtifact:
    """Result of a :meth:`ChiselBuildNode.build`: a compiled simulator + metadata.

    The compiled simulator is shipped by value (the ELF bytes travel inside
    this object).

    Attributes:
        name: Free-form label carried from ``ChiselBuildNode.name`` (default
            ``"chipyard"``) for downstream identification.
        simulator_binary_content: Raw ELF bytes of the compiled simulator
            binary. Empty (``b""``) when the build failed or timed out.
        simulator_binary_name: Filename of the simulator, e.g.
            ``"simulator-chipyard.harness-MegaBoomV3Config"``.
        config: Chipyard ``CONFIG`` the binary was built for (e.g.
            ``"MegaBoomV3Config"``).
        config_package: Chipyard ``CONFIG_PACKAGE`` (Scala package, e.g.
            ``"chipyard"``).
        target: The :class:`BuildTarget` that produced this binary.
        success: True iff the build's ``make`` exited 0 and the binary exists.
        stdout: Captured stdout of the build command.
        stderr: Captured stderr of the build command.
        returncode: Exit code of the build command; ``-1`` on timeout.
        generated_src_files: ``(filename, contents)`` pairs of generated
            ``.v``/``.sv`` and ``.top.mems.conf`` collateral: populated only
            when the build was run with ``collect_generated_src=True``.
    """
    name: str # default "chipyard"
    simulator_binary_content: bytes  # raw ELF bytes of the compiled simulator binary
    simulator_binary_name: str       # e.g. "simulator-chipyard.harness-MegaBoomV3Config"
    config: str                      # e.g. "MegaBoomV3Config"
    config_package: str              # e.g. "chipyard"
    target: BuildTarget
    success: bool
    stdout: str
    stderr: str
    returncode: int
    generated_src_files: list[tuple[str, str]] = field(default_factory=list)  # [(filename, contents)]


@dataclass
class RiscvBuildArtifact:
    """Result of a :meth:`RiscvBuildNode.build`: a cross-compiled RISC-V ELF.

    Attributes:
        binary_name: Output filename, e.g. ``"hello.riscv"`` or ``"hello"``.
        binary_content: Raw ELF bytes of the produced binary. Empty (``b""``)
            when the build failed or timed out.
        target: Which toolchain/output convention was used: ``"verilator"``
            (baremetal) or ``"linux"`` (userspace).
        success: True iff ``make`` exited 0 and a non-empty binary was produced.
        stdout: Captured stdout of the build command.
        stderr: Captured stderr of the build command (includes a timeout note
            on expiry).
        returncode: Exit code of the build command; ``-1`` on timeout.
        dump: ``objdump -D`` disassembly of the ELF; ``""`` unless ``build()``
            was called with ``include_dump=True``.
    """
    binary_name: str       # e.g. "hello.riscv" (verilator) or "hello" (linux)
    binary_content: bytes  # raw ELF bytes of the produced binary
    target: str            # "verilator" | "linux"
    success: bool
    stdout: str
    stderr: str
    returncode: int
    dump: str = ""         # objdump -D output; "" unless build() was called with include_dump=True


@dataclass
class RiscvObjdumpArtifact:
    """Result of a :meth:`RiscvObjdumpNode.dump`: a RISC-V ELF disassembly.

    Attributes:
        binary_name: Filename of the input ELF.
        dump: The disassembly (stdout of the ``objdump`` invocation); ``""`` on
            failure.
        target: Which toolchain prefix was used: ``"verilator"``
            (``riscv64-unknown-elf-``) or ``"linux"``
            (``riscv64-unknown-linux-gnu-``).
        success: True iff ``objdump`` exited 0 and produced non-empty output.
        stdout: Captured stdout of the objdump invocation (same content as
            ``dump``).
        stderr: Captured stderr (includes a timeout note on expiry).
        returncode: Exit code of the objdump invocation; ``-1`` on timeout.
    """
    binary_name: str       # filename of the input ELF
    dump: str              # objdump output (stdout of the objdump invocation)
    target: str            # "verilator" | "linux": picks the toolchain prefix
    success: bool
    stdout: str
    stderr: str
    returncode: int


@dataclass
class RunResult:
    """Result of a :meth:`VerilatorRunNode.run`: captured simulator output.

    Attributes:
        test_binary_name: Filename of the input test ELF that was run.
        log: Simulator stdout captured to ``<stem>.log`` (program / HTIF
            output).
        out: the ``.out`` file, contains ``spike-dasm`` disassembly of the simulator's stderr
            (the human-readable committed-instruction trace).
        returncode: Exit code of the simulator process.
        success: True iff ``returncode == 0``.
        vcd_s3_path: Full ``s3://…`` URI of the uploaded VCD; ``""`` when no
            waveform was captured or no upload occurred (the VCD is too large
            to return inline).
        vcd_size_bytes: Size of the uploaded/kept VCD; ``0`` when neither.
        vcd_path: Worker-side path of the kept VCD (``run(keep_waveform=True)``
            moves it out of the task dir so it survives cleanup). Together with
            ``vcd_node_id`` this is the claim ticket for
            :func:`chia.chipyard.verilator_run_node.collect_waveform`, the
            S3-free transfer path. ``""`` when not kept.
        vcd_node_id: Ray node id (hex) of the worker holding ``vcd_path``;
            collection tasks are pinned to it. ``""`` when not kept.
        out_s3_path: ``s3://…`` URI of the uploaded ``.out``; ``""`` when not
            uploaded.
        log_s3_path: ``s3://…`` URI of the uploaded ``.log``; ``""`` when not
            uploaded.
        wave_windows: Echo of the :class:`WaveWindow` list the run was
            configured with.
    """
    test_binary_name: str  # filename of input ELF
    log: str               # captured stdout from simulator (→ .log)
    out: str               # spike-dasm disassembly of stderr (→ .out)
    returncode: int
    success: bool
    # Waveform capture (only populated when the run was configured to capture
    # a VCD; the file itself is uploaded to S3 or kept on the worker for
    # collect_waveform(): too large to return inline).
    vcd_s3_path: str = ""                                          # full s3://… URI; "" when not uploaded
    vcd_size_bytes: int = 0                                        # uploaded/kept VCD size (0 when neither)
    vcd_path: str = ""                                             # kept VCD's worker-side path; "" when not kept
    vcd_node_id: str = ""                                          # ray node id (hex) holding vcd_path; "" when not kept
    # Stdout/stderr S3 mirrors (populated when run was launched with
    # upload_to_s3=True; "" when no upload).
    out_s3_path: str = ""
    log_s3_path: str = ""
    wave_windows: list["WaveWindow"] = field(default_factory=list) # echoes the windows the run was configured with


@dataclass
class CosimResult:
    """Result of a :meth:`CosimNode.run`: the lockstep cospike verdict for one ELF.

    Attributes:
        elf_name: Filename of the ELF that was co-simulated.
        match: True iff the DUT agreed with Spike (no divergence aborted the run).
        matched: Committed instructions Spike checked before the run stopped.
        completed: True iff the program reached its end-of-test marker.
        first_divergence: ``{line, spike, dut}`` describing the first mismatch, or
            ``None`` on a match.
        sim_cycles: Simulation cycles executed, or ``None`` if not reported.
        failing_trace_gz: gzip'd tail window around the abort (DUT + Spike commit
            logs interleaved); ``None`` on a match.
    """
    elf_name: str
    match: bool
    matched: int                       # committed instrs spike checked before stop
    completed: bool                    # program reached its end-of-test marker
    first_divergence: dict | None      # {line, spike, dut} or None
    sim_cycles: int | None
    failing_trace_gz: bytes | None     # gz tail window around the abort; None on match


@dataclass
class SpikeResult:
    """Result of running a RISC-V ELF on the Spike ISA simulator.

    Attributes:
        test_binary_name: Filename of the input ELF that was executed.
        isa: The ``--isa=<...>`` string Spike ran with (e.g. ``"rv64gc"``).
        log: Spike stdout, i.e. the program / HTIF output.
        commit_log: Committed-instruction trace. Empty (``""``) unless the run
            requested it via ``log_commits=True``.
        returncode: Exit code of the Spike process.
        success: True iff the run succeeded. For self-checking riscv-tests this
            is exit code 0 reported over HTIF.
    """
    test_binary_name: str  # filename of input ELF
    isa: str               # the --isa=<...> Spike ran with
    log: str               # Spike stdout (program / HTIF output)
    commit_log: str        # committed-instruction trace; "" unless run(log_commits=True)
    returncode: int
    success: bool          # self-checking riscv-tests pass == exit 0 over HTIF


@dataclass
class SpikeBuildArtifact:
    """Result of (re)building the Spike simulator binary from source.

    Attributes:
        success: True iff ``make`` + install succeeded and the binary exists.
        spike_bin: Path to the (re)built spike binary (in-container).
        stdout: Captured stdout of the build command.
        stderr: Captured stderr of the build command.
        returncode: Exit code of the build command.
    """
    success: bool          # make+install succeeded and the binary exists
    spike_bin: str         # path to the (re)built spike binary (in-container)
    stdout: str
    stderr: str
    returncode: int


class TortureMode(str, Enum):
    """How a RISC-V Torture run generates and checks tests.

    Attributes:
        SINGLE: Generate one test, run it, and diff signatures
            (``make rgentest``).
        OVERNIGHT: Generate-and-test loop until ``N`` failures or ``T`` minutes
            elapse (``make rnight``).
        REPLAY: Take a pre-supplied ``test.S``, run it, and diff signatures
            (``make rtest``).
    """
    SINGLE = "single"        # gen + 1 test + diff (make rgentest)
    OVERNIGHT = "overnight"  # gen+test loop until N failures or T minutes (make rnight)
    REPLAY = "replay"        # take a pre-supplied test.S, run+diff (make rtest)


@dataclass
class TortureTestRun:
    """Artifacts produced for one generated Torture test, pass or fail.

    A test passes when the Spike reference signature matches the DUT (RTL
    simulation) signature.

    Attributes:
        name: Basename of the test, e.g. ``"test"`` or ``"test_1714571234"``.
        success: True iff the Spike signature matched the DUT signature.
        test_s: The generated assembly source (``.S``).
        test_dump: ``objdump`` output, if Torture produced one (else ``""``).
        spike_sig: The Spike reference signature.
        rtlsim_sig: The DUT (RTL simulation) signature.
        pseg_test_s: The narrowed program segment from ``testrun``'s seek mode.
            Populated for failures only; ``None`` otherwise.
    """
    name: str                       # basename, e.g. "test" or "test_1714571234"
    success: bool                   # spike sig matched DUT sig
    test_s: str                     # generated assembly source
    test_dump: str                  # objdump output (if torture produced one)
    spike_sig: str                  # reference signature
    rtlsim_sig: str                 # DUT signature
    pseg_test_s: str | None = None  # narrowed pseg from testrun's seek mode (failures only)


@dataclass
class TortureResult:
    """Result of a RISC-V Torture run against a built simulator.

    Attributes:
        name: Free-form label carried from the run for downstream
            identification.
        config: Chipyard ``CONFIG`` the simulator was built for (e.g.
            ``"MegaBoomV3Config"``).
        config_package: Chipyard ``CONFIG_PACKAGE`` (Scala package, e.g.
            ``"chipyard"``).
        mode: The :class:`TortureMode` the run used.
        success: True iff all signatures matched and the run completed.
        num_tests: Number of tests run — ``1`` for ``SINGLE``/``REPLAY``,
            ``N`` for ``OVERNIGHT``.
        num_failures: Number of tests whose signatures mismatched.
        tests: Per-test artifacts. Includes passing and failing tests for
            ``SINGLE``/``REPLAY``; failing tests only for ``OVERNIGHT``.
        stdout: Captured stdout of the Torture run.
        stderr: Captured stderr of the Torture run.
        returncode: Exit code of the Torture run.
        build_artifact: The :class:`BuildArtifact` that produced the simulator.
            Populated by ``torture_from_config``; ``None`` otherwise.
    """
    name: str
    config: str
    config_package: str
    mode: TortureMode
    success: bool                  # all signatures matched and the run completed
    num_tests: int                 # 1 for SINGLE/REPLAY, N for OVERNIGHT
    num_failures: int
    tests: list[TortureTestRun]    # per-test artifacts (passing + failing for SINGLE/REPLAY; failing only for OVERNIGHT)
    stdout: str
    stderr: str
    returncode: int
    build_artifact: BuildArtifact | None = None  # populated by torture_from_config
