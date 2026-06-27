"""Generate random RISC-V test ELFs with riscv-dv (`run.py --steps gen,gcc_compile`:
generate assembly, then link with the riscv64-unknown-elf toolchain in
chia-riscv-dv).

Backends (the `simulator` arg): `pyflow` is free (no license) but covers only the
base ISA plus partial/draft bitmanip; `xlm`/`vcs`/`questa` run the full SV flow
(bitmanip/vector/crypto) and each need their CAD tool plus a FlexLM seat, sourced
from `cad_setup`.
"""

import logging
import fcntl
import filecmp
import os
import shlex
import shutil
import signal
import subprocess
import uuid
from dataclasses import dataclass, field

from chia.base.ChiaFunction import ChiaFunction

# riscv-dv checkout inside the chia-riscv-dv image (RiscvDvBaseDockerfile).
RISCV_DV_DIR = "/home/ray/riscv-dv"
_ELF_MAGIC = b"\x7fELF"

# riscv-dv's gcc_compile step resolves the cross toolchain through these env
# vars (run.py -> get_env_var("RISCV_GCC"/"RISCV_OBJCOPY")). The binaries are on
# PATH in chia-riscv-cross-base; we resolve + export them so compile produces an
# ELF instead of warning "Please set the environment variable RISCV_GCC".
_RISCV_GCC = "riscv64-unknown-elf-gcc"
_RISCV_OBJCOPY = "riscv64-unknown-elf-objcopy"


@dataclass
class GenSpec:
    """One riscv-dv test to generate: a `testlist.yaml` entry name plus optional
    per-run overrides. `instr_cnt` and `plusargs` are appended as `--sim_opts`,
    overriding the entry's `gen_opts` without editing the yaml; `extra_args` is
    raw `run.py` CLI for anything not modelled here. A None plusarg value is a
    bare flag (`+foo`).

    Attributes:
        test: Name of the ``testlist.yaml`` entry to generate (the instruction
            mix, e.g. ``"riscv_arithmetic_basic_test"``, ``"riscv_b_ext_test"``).
            Passed as ``--test``.
        iterations: Number of test programs to generate (``--iterations``).
        instr_cnt: Optional override for the instruction count per program;
            emitted via ``--sim_opts +instr_cnt=<n>`` when set.
        plusargs: Extra generator plusargs emitted via ``--sim_opts``. ``{k: v}``
            becomes ``+k=v``; a ``None`` value becomes a bare ``+k``.
        gen_timeout: Optional per-generation timeout forwarded as
            ``--gen_timeout`` to ``run.py``.
        seed: Optional fixed RNG seed (``--seed``) for reproducible generation;
            ``None`` lets riscv-dv pick.
        extra_args: Raw additional ``run.py`` CLI tokens, appended verbatim, for
            flags not modelled here.
    """

    test: str
    iterations: int = 1
    instr_cnt: int | None = None
    plusargs: dict[str, object] = field(default_factory=dict)
    gen_timeout: int | None = None
    seed: int | None = None
    extra_args: tuple[str, ...] = ()

    def sim_opts(self) -> str:
        """Render ``instr_cnt`` + ``plusargs`` into a ``--sim_opts`` string.

        Returns:
            A space-joined string of ``+key[=value]`` tokens (empty if neither
            ``instr_cnt`` nor ``plusargs`` is set).
        """
        parts = ([f"+instr_cnt={self.instr_cnt}"] if self.instr_cnt is not None else [])
        parts += [f"+{k}" if v is None else f"+{k}={v}" for k, v in self.plusargs.items()]
        return " ".join(parts)


class RiscvDvGenNode:
    """Generates random RISC-V test ELFs with riscv-dv as cosim stimulus.
    """

    logging_name = "RiscvDvGenNode"

    def __init__(
        self,
        riscv_dv_dir: str = RISCV_DV_DIR,
        simulator: str = "pyflow",
        target: str | None = None,
        custom_target: str | None = None,
        testlist: str | None = None,
        isa: str | None = None,
        mabi: str | None = None,
        user_extension_dir: str | None = None,
        custom_instr_dir: str | None = None,
        cad_setup: str = "/ecad/tools/vlsi.bashrc",
        timeout_seconds: int = 900,
        logging_level: int = logging.DEBUG,
    ):
        """Thin pass-through to riscv-dv's run.py: each flag is forwarded only when
        set. Use GenSpec.extra_args for any flag we don't surface.

        Configs are presented as filepaths that are expected to exist
        in the node's environment:

          custom_target: your core config  (dir holding riscv_core_setting.sv)

          testlist: your test catalog (a testlist.yaml)

          user_extension_dir: your custom generator .sv (a riscv-dv user_extension/
          layout); referenced by class name from the testlist
          and compiled in during gen

          custom_instr_dir: your custom-instruction .sv (an isa/custom/ layout:
          riscv_instr_name_t enum entries + instr classes), overlaid into the
          generator's src/isa/custom/ so they generate. Defaults to
          <custom_target>/isa/custom/. (NOT a run.py flag — the enum is sealed
          before the include hooks, so it must land in src/; see
          _install_custom_instrs.)

        The rest map 1:1 to run.py flags: simulator (pyflow=free, xlm/vcs/questa=
        CAD+FlexLM), target (a built-in core-config name), isa, mabi.

        Args:
            riscv_dv_dir: Path to the riscv-dv checkout containing ``run.py``
                (defaults to the location inside the ``chia-riscv-dv`` image).
            simulator: riscv-dv backend / ``--simulator``: ``"pyflow"`` (no
                license; base ISA + partial bitmanip) or ``"xlm"``/``"vcs"``/
                ``"questa"`` (SV flow; full bitmanip/vector/crypto, needs the
                CAD tool + a FlexLM seat sourced from ``cad_setup``).
            target: Built-in core-config name (``--target``); ``None`` to omit.
            custom_target: Path to a directory holding your
                ``riscv_core_setting.sv`` (``--custom_target``). Shipped to the
                worker via the Ray working dir. Takes precedence over ``target``.
            testlist: Path to a custom ``testlist.yaml`` (``--testlist``);
                overrides the target dir's default test catalog.
            isa: Target ISA string (``--isa``, e.g. ``"rv64gc"``); ``None`` omits.
            mabi: Target ABI string (``--mabi``, e.g. ``"lp64d"``); ``None`` omits.
            user_extension_dir: Path to a riscv-dv ``user_extension/`` layout
                with your custom generator ``.sv`` (``--user_extension_dir``),
                referenced by class name from the testlist.
            custom_instr_dir: Path to an ``isa/custom/`` layout
                (``riscv_instr_name_t`` enum entries + instr classes) overlaid
                into the generator's ``src/isa/custom/`` before compile so the
                instructions generate. Defaults to ``<custom_target>/isa/custom/``.
                NOT a run.py flag — the enum is sealed before the include hooks,
                so it must land in ``src/`` (see :meth:`_install_custom_instrs`).
            cad_setup: Path to the bashrc sourced before SV-flow runs to put the
                CAD tool + FlexLM license on PATH (ignored for ``pyflow``).
            timeout_seconds: Wall-clock limit per ``run.py`` invocation; on
                expiry the process group is killed (never raises).
            logging_level: Python logging level for this node's logger.
        """
        self.riscv_dv_dir = riscv_dv_dir
        self.simulator = simulator
        self.target = target
        self.custom_target = custom_target
        self.testlist = testlist
        self.isa = isa
        self.mabi = mabi
        self.user_extension_dir = user_extension_dir
        self.custom_instr_dir = custom_instr_dir
        self.cad_setup = cad_setup
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    def _gen_flags(self) -> list[str]:
        """riscv-dv flags shared by compile_generator + generate, forwarded only
        when set (run.py owns the precedence: custom_target > target; explicit
        testlist > the target dir's default)."""
        out: list[str] = []
        for flag, val in (("--target", self.target),
                          ("--custom_target", self.custom_target),
                          ("--testlist", self.testlist),
                          ("--isa", self.isa),
                          ("--mabi", self.mabi),
                          ("--user_extension_dir", self.user_extension_dir)):
            if val:
                out += [flag, val]
        return out

    def _install_custom_instrs(self) -> None:
        """Overlay custom-instruction .sv into the generator's own src/isa/custom/
        before the testbench compile. Source = custom_instr_dir, else (when unset)
        <custom_target>/isa/custom/. riscv-dv seals the riscv_instr_name_t enum and
        builds each instr class by name from src/ *before* the --custom_target /
        --user_extension_dir include hooks, and `include resolves relative to src/
        (not the incdir) — so a custom instruction's enum+class only take effect from
        <riscv-dv>/src/isa/custom/. Idempotent, flock'd, atomic os.replace — concurrent
        gens never see a half set; no-op when neither source is set or present."""
        src = self.custom_instr_dir or (
            os.path.join(self.custom_target, "isa", "custom") if self.custom_target else None)
        if not src or not os.path.isdir(src):
            return
        dst = os.path.join(self.riscv_dv_dir, "src", "isa", "custom")
        with open(os.path.join(dst, ".chia_install.lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            for fn in sorted(os.listdir(src)):
                sp, dp = os.path.join(src, fn), os.path.join(dst, fn)
                if not fn.endswith(".sv") or (os.path.exists(dp) and filecmp.cmp(sp, dp, shallow=False)):
                    continue
                tmp = f"{dp}.{uuid.uuid4().hex[:8]}.tmp"
                shutil.copyfile(sp, tmp)
                os.replace(tmp, dp)

    @ChiaFunction(resources={"dv": 1})
    def compile_generator(self, build_dir: str, test: str) -> str:
        """Compile the generator testbench once into build_dir (riscv-dv -co), so later generate(sim_only=True) runs
        skip the testbench recompile and just simulate against it. `test`
        is any entry in the active testlist. Returns build_dir, to pass to
        generate() as build_dir.
        Multiple generate(sim_only=True) runs can reuse one build concurrently.

        Args:
            build_dir: Directory to compile the generator testbench into
                (``--output`` with ``-co``); created if absent. This same path
                is later passed to :meth:`generate` as ``build_dir``.
            test: Any entry in the active testlist. ``run.py`` needs one to load
                the list, but the ``-co`` build itself is test-independent.

        Returns:
            The ``build_dir`` path, ready to hand to ``generate(sim_only=True)``.
        """
        os.makedirs(build_dir, exist_ok=True)
        self._install_custom_instrs()
        cmd = ["python3", os.path.join(self.riscv_dv_dir, "run.py"),
               "--test", test, "--simulator", self.simulator,
               "--steps", "gen", "-co", "--output", build_dir] + self._gen_flags()
        self.logger.info(f"Compiling generator: {' '.join(cmd)} (cwd={self.riscv_dv_dir})")
        self._run(cmd)
        return build_dir

    @ChiaFunction(resources={"dv": 1})
    def generate(
        self,
        spec: GenSpec,
        work_dir: str,
        cleanup_task_dir: bool = True,
        sim_only: bool = False,
        build_dir: str | None = None,
    ) -> list[tuple[str, bytes, str]]:
        """Generate + compile `spec` and return [(name, elf_bytes, asm_text)] for
        every ELF produced. Never raises: on failure it returns the
        (possibly empty) list found.

        sim_only=True skips the testbench compile (riscv-dv -so) and reuses the build
        from a prior compile_generator(), passed as build_dir. Multiple sim_only gens can share one build_dir concurrently without colliding.

        Args:
            spec: The :class:`GenSpec` describing the test, iteration count, and
                any per-run overrides to generate.
            work_dir: Base directory; a uuid-namespaced task subdir is created
                under it per call for isolation.
            cleanup_task_dir: If True (default), remove the task dir after
                collecting ELFs (unlinks any symlinks; the shared build is left
                intact).
            sim_only: If True, skip the testbench compile (``-so``) and reuse a
                prebuilt generator from ``build_dir``. Requires ``build_dir``.
            build_dir: Path returned by :meth:`compile_generator`; symlinked
                read-only into the task dir when ``sim_only=True``. Must be
                local to this worker.

        Returns:
            A list of ``(name, elf_bytes, asm_text)`` tuples — one per ELF
            riscv-dv produced (the sibling ``.S`` source, or ``""`` if absent).
            Never raises: on failure returns the (possibly empty) list found.

        Raises:
            ValueError: If ``sim_only=True`` but no ``build_dir`` was provided.
        """
        os.makedirs(work_dir, exist_ok=True)
        task_dir = os.path.join(work_dir, uuid.uuid4().hex[:8])
        os.makedirs(task_dir, exist_ok=True)
        if not sim_only:
            self._install_custom_instrs()   # must be in src/ before generate recompiles the tb
        if sim_only:
            if not build_dir:
                raise ValueError("generate(sim_only=True) needs build_dir from compile_generator()")
            # Mirror the shared build into this run's own dir via symlinks: the
            # compiled simv is reused read-only while generated outputs stay in
            # task_dir. os.walk/rmtree don't follow symlinks, so collection sees only
            # this run's ELFs and cleanup removes the links, never the build.
            for entry in os.listdir(build_dir):
                os.symlink(os.path.join(build_dir, entry), os.path.join(task_dir, entry))

        cmd = [
            "python3", os.path.join(self.riscv_dv_dir, "run.py"),
            "--test", spec.test,
            "--simulator", self.simulator,
            "--steps", "gen,gcc_compile",
            "--iterations", str(spec.iterations),
            "--output", task_dir,
        ] + self._gen_flags()
        if sim_only:
            cmd += ["-so"]
        sim_opts = spec.sim_opts()
        if sim_opts:
            cmd += ["--sim_opts", sim_opts]
        if spec.gen_timeout is not None:
            cmd += ["--gen_timeout", str(spec.gen_timeout)]
        if spec.seed is not None:
            cmd += ["--seed", str(spec.seed)]
        cmd += list(spec.extra_args)
        self.logger.info(f"Running: {' '.join(cmd)} (cwd={self.riscv_dv_dir})")
        self._run(cmd)

        elfs = self._collect_elfs(task_dir)
        self.logger.info(f"riscv-dv produced {len(elfs)} ELF(s) for test={spec.test}")
        if cleanup_task_dir:   # rmtree unlinks the symlinks; the build is untouched
            shutil.rmtree(task_dir, ignore_errors=True)
        return elfs

    @staticmethod
    def _collect_elfs(root_dir: str) -> list[tuple[str, bytes, str]]:
        """Return (name, elf_bytes, asm_text) for every RISC-V *executable* ELF
        under root_dir, robust to riscv-dv's output naming. Checks
        e_type/e_machine, not just the magic — simulator build artifacts (e.g.
        Xcelium's x86 _sv_export.so) are ELFs too. The sibling .S is the
        generated source ("" if absent)."""
        out: list[tuple[str, bytes, str]] = []
        for dirpath, _, files in os.walk(root_dir):
            for name in sorted(files):
                path = os.path.join(dirpath, name)
                try:
                    with open(path, "rb") as f:
                        h = f.read(20)
                        # magic + ET_EXEC(2) @16 + EM_RISCV(243) @18, little-endian
                        if (h[:4] != _ELF_MAGIC or len(h) < 20
                                or int.from_bytes(h[16:18], "little") != 2
                                or int.from_bytes(h[18:20], "little") != 243):
                            continue
                        f.seek(0)
                        elf = f.read()
                except OSError:
                    continue
                try:
                    with open(os.path.splitext(path)[0] + ".S", errors="replace") as f:
                        asm = f.read()
                except OSError:
                    asm = ""
                out.append((name, elf, asm))
        return out

    def _run(self, cmd: list[str]) -> None:
        env = os.environ.copy()
        # Export the cross toolchain for riscv-dv's gcc_compile (resolved off
        # PATH so we don't hardcode the conda prefix); honor a preset if given.
        env.setdefault("RISCV_GCC", shutil.which(_RISCV_GCC) or _RISCV_GCC)
        env.setdefault("RISCV_OBJCOPY", shutil.which(_RISCV_OBJCOPY) or _RISCV_OBJCOPY)
        self.logger.info(f"RISCV_GCC={env['RISCV_GCC']} RISCV_OBJCOPY={env['RISCV_OBJCOPY']}")
        # SV flows need VCS/Xcelium + FlexLM license on PATH: source cad_setup
        # then exec. pyflow runs directly. (/ecad must be mounted in the worker.)
        if self.simulator != "pyflow":
            cmd = ["bash", "-c",
                   f"source {shlex.quote(self.cad_setup)} >/dev/null 2>&1 && exec "
                   + shlex.join(cmd)]
            self.logger.info(f"SV flow '{self.simulator}': sourcing {self.cad_setup}")
        # Own process group, group-killed in finally: run.py spawns xrun/xmsim
        # children that outlive the direct child on timeout or task cancellation
        # (orphaned sims hold real simulator licenses). A force-killed worker
        # still leaks — that path has no cleanup hook.
        p = subprocess.Popen(
            cmd, cwd=self.riscv_dv_dir, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=env, start_new_session=True,
        )
        try:
            _, err = p.communicate(timeout=self.timeout_seconds)
            if p.returncode != 0:
                self.logger.warning(
                    f"riscv-dv gen returned {p.returncode}; stderr tail:\n{err[-4000:]}"
                )
        except subprocess.TimeoutExpired:
            self.logger.warning(f"riscv-dv gen timed out after {self.timeout_seconds}s")
        finally:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
