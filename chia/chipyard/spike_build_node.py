"""Builds Spike's libriscv into a shippable artifact.

Spike is the golden model a cospike simulator checks each instruction against.
This node builds the tree as it finds it and returns the library by value, the
same contract :class:`~chia.chipyard.chisel_build_node.ChiselBuildNode` has
for Chisel. The output feeds ``ChiselBuildNode(golden_model=...)``, which
links against it and bundles it into the simulator's artifact, so run workers
need no spike installed.

Dynamic linking is the default; it lets spike and the RTL iterate
independently. For a hermetic binary use
``ChiselBuildNode(static_golden_model=True, clean_sim=True)`` - clean_sim
because make does not relink for a new library on its own.
"""
import hashlib
import io
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile

from chia.base.ChiaFunction import ChiaFunction
from chia.chipyard.state_def import SpikeBuildArtifact

# Where chipyard's build-setup.sh leaves the spike checkout and its build dir.
SPIKE_REL = "toolchains/riscv-tools/riscv-isa-sim"
# Sources whose mtime the library must beat; spike is C++ plus generated headers.
_SOURCE_EXT = (".cc", ".c", ".h", ".hh", ".hpp")
# Everything a static link needs, in dependency order: libriscv.a alone leaves
# ~900 undefined softfloat/disasm/fdt symbols.
STATIC_ARCHIVES = ("libriscv.a", "libsoftfloat.a", "libdisasm.a", "libfdt.a")


class SpikeBuildNode:
    """Builds ``libriscv`` from the riscv-isa-sim checkout inside a chipyard tree."""

    logging_name = "SpikeBuildNode"

    def __init__(
        self,
        chipyard_path: str,
        riscv_path: str | None = None,
        spike_rel: str = SPIKE_REL,
        make_jobs: int = 16,
        strip: bool = True,
        configure_args: list[str] | None = None,
        build_static: bool = False,
        collect_static: bool = False,
        collect_tools: bool = False,
        collect_headers: bool = False,
        install: bool = True,
        timeout_seconds: int = 1800,
        logging_level: int = logging.DEBUG,
    ):
        """Configure one libriscv build.

        Args:
            chipyard_path: Chipyard checkout holding the spike sources.
            riscv_path: Toolchain prefix ($RISCV). Defaults to the ``RISCV``
                env var, then ``<chipyard_path>/.conda-env/riscv-tools``.
            spike_rel: riscv-isa-sim path relative to ``chipyard_path``.
            make_jobs: ``make -j`` parallelism.
            configure_args: Extra ``./configure`` arguments, e.g.
                ``["--with-isa=rv64gcv"]`` or ``["CXXFLAGS=-O0 -g"]``. When
                given, configure is re-run and the library fully rebuilt.
                Default None keeps the existing build configuration.
            strip: Strip a copy before shipping (~177MB -> ~8MB).
            build_static: Also build :data:`STATIC_ARCHIVES` for
                ``ChiselBuildNode(static_golden_model=True)``.
            collect_static: Carry the archives (~300MB) in the artifact, for
                linking in a different container.
            collect_tools: Also carry ``spike`` and ``spike-dasm``.
            collect_headers: Also carry ``$RISCV/include/riscv`` as a tar, for
                compiling cospike in a different container.
            install: Copy the library into ``$RISCV/lib``, where chipyard's
                link step looks.
            timeout_seconds: Timeout per subprocess.
        """
        self.chipyard_path = chipyard_path
        self.spike_dir = os.path.join(chipyard_path, spike_rel)
        self.build_dir = os.path.join(self.spike_dir, "build")
        self.riscv_path = riscv_path or os.environ.get("RISCV") or os.path.join(
            chipyard_path, ".conda-env", "riscv-tools")
        self.make_jobs = make_jobs
        self.configure_args = list(configure_args) if configure_args else []
        self.strip = strip
        self.build_static = build_static
        self.collect_static = collect_static
        self.collect_tools = collect_tools
        self.collect_headers = collect_headers
        self.install = install
        self.timeout_seconds = timeout_seconds
        self.logger = logging.getLogger(self.logging_name)
        self.logger.setLevel(logging_level)

    # -- helpers ------------------------------------------------------------

    def _newest_source_mtime(self) -> float:
        """mtime of the newest spike source; the built library must beat it."""
        newest = 0.0
        for root, dirs, files in os.walk(self.spike_dir):
            dirs[:] = [d for d in dirs if d not in (".git", "build")]
            for name in files:
                if name.endswith(_SOURCE_EXT):
                    try:
                        newest = max(newest, os.path.getmtime(os.path.join(root, name)))
                    except OSError:
                        pass
        return newest

    def _read_maybe_stripped(self, path: str) -> bytes:
        """The file's bytes, stripped of debug symbols (a copy; the original
        stays debuggable) unless ``strip=False``."""
        if not self.strip:
            with open(path, "rb") as handle:
                return handle.read()
        with tempfile.TemporaryDirectory() as tmp:
            copy = os.path.join(tmp, os.path.basename(path))
            shutil.copyfile(path, copy)
            result = subprocess.run(["strip", "--strip-unneeded", copy],
                                    capture_output=True, text=True)
            if result.returncode != 0:
                self.logger.warning(
                    f"strip failed on {path} ({result.stderr.strip()[:200]}); "
                    "shipping the unstripped library")
            with open(copy, "rb") as handle:
                return handle.read()

    def _source_provenance(self) -> tuple[str, bool]:
        """(HEAD, dirty) of the spike checkout; ("", False) outside git."""
        def _git(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run(["git", *args], cwd=self.spike_dir,
                                  capture_output=True, text=True)
        head = _git("rev-parse", "HEAD")
        if head.returncode != 0:
            return "", False
        status = _git("status", "--porcelain")
        return head.stdout.strip(), bool(status.stdout.strip())

    def _headers_tar(self) -> bytes:
        include = os.path.join(self.riscv_path, "include", "riscv")
        if not os.path.isdir(include):
            self.logger.warning(f"no headers to collect at {include}")
            return b""
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            tar.add(include, arcname="riscv")
        return buffer.getvalue()

    def _failure(self, message: str, result=None) -> SpikeBuildArtifact:
        self.logger.info(message)
        return SpikeBuildArtifact(
            success=False, lib_name="libriscv.so", lib_content=b"", digest="",
            spike_bin="", stdout=result.stdout if result else "",
            stderr=(result.stderr if result else "") + "\n" + message,
            returncode=result.returncode if result else -1)

    # -- the node -----------------------------------------------------------

    @ChiaFunction(resources={"chipyard": 0.9})
    def build(self) -> SpikeBuildArtifact:
        """Rebuild libriscv from the current sources and return it by value.

        The rebuild is unconditional (make is incremental, so a current tree
        costs seconds); skipping it is how patched sources end up paired with a
        stale library. Returns ``success=False`` with the captured output on
        failure - treat that as fatal.
        """
        configure = os.path.join(self.spike_dir, "configure")
        if not os.path.exists(configure):
            return self._failure(
                f"no spike sources at {self.spike_dir}: this image's chipyard "
                "cannot rebuild the golden model")
        if self.configure_args or not os.path.isdir(self.build_dir):
            os.makedirs(self.build_dir, exist_ok=True)
            cfg_cmd = [configure, f"--prefix={self.riscv_path}",
                       *self.configure_args]
            self.logger.info(f"Configuring spike: {' '.join(cfg_cmd)}")
            cfg = subprocess.run(cfg_cmd, cwd=self.build_dir,
                                 capture_output=True, text=True,
                                 timeout=self.timeout_seconds)
            if cfg.returncode != 0:
                return self._failure(
                    f"spike configure failed:\n{cfg.stdout[-2000:]}", cfg)

        targets = ["libriscv.so"] + (list(STATIC_ARCHIVES) if self.build_static else [])
        cmd = ["make", f"-j{self.make_jobs}", *targets]
        self.logger.info(f"Building spike: {' '.join(cmd)} in {self.build_dir}")
        try:
            result = subprocess.run(cmd, cwd=self.build_dir, capture_output=True,
                                    text=True, timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired as error:
            return self._failure(
                f"spike build timed out after {self.timeout_seconds}s",
                subprocess.CompletedProcess(
                    cmd, -1,
                    error.stdout.decode(errors="replace") if error.stdout else "",
                    error.stderr.decode(errors="replace") if error.stderr else ""))
        if result.returncode != 0:
            return self._failure(
                f"rebuilding libriscv failed:\n{result.stdout[-4000:]}", result)

        built = os.path.join(self.build_dir, "libriscv.so")
        if not os.path.exists(built):
            return self._failure(f"{built} does not exist after a successful make",
                                 result)
        newest = self._newest_source_mtime()
        if os.path.getmtime(built) < newest:
            return self._failure(
                f"{built} is older than the spike sources it was built from; "
                "the simulator would be checked against the wrong model", result)

        lib_content = self._read_maybe_stripped(built)
        digest = hashlib.sha256(lib_content).hexdigest()

        static_archives: list[tuple[str, bytes]] = []
        if self.build_static:
            for name in STATIC_ARCHIVES:
                path = os.path.join(self.build_dir, name)
                if not os.path.exists(path):
                    return self._failure(
                        f"{path} does not exist after a successful make; a "
                        "static golden model cannot be linked without it", result)
                if self.collect_static:
                    with open(path, "rb") as handle:
                        static_archives.append((name, handle.read()))

        tools = []
        if self.collect_tools:
            for name in ("spike", "spike-dasm"):
                path = os.path.join(self.build_dir, name)
                if os.path.exists(path):
                    tools.append((name, self._read_maybe_stripped(path)))
                else:
                    self.logger.warning(f"{name} not found in {self.build_dir}")

        if self.install:
            self._install(lib_content)

        source_sha, source_dirty = self._source_provenance()
        self.logger.info(f"Built libriscv {digest[:12]} from "
                         f"{source_sha[:10] or 'unknown'}"
                         f"{'-dirty' if source_dirty else ''} "
                         f"({len(lib_content)} bytes)")
        return SpikeBuildArtifact(
            success=True,
            lib_name="libriscv.so",
            lib_content=lib_content,
            digest=digest,
            spike_bin=built,
            stdout=result.stdout,
            stderr=result.stderr,
            returncode=result.returncode,
            static_archives=static_archives,
            tools=tools,
            headers_tar=self._headers_tar() if self.collect_headers else b"",
            source_sha=source_sha,
            source_dirty=source_dirty,
        )

    def _install(self, lib_content: bytes) -> None:
        """Write the artifact's own bytes into ``$(RISCV)/lib``, where chipyard
        links (``-L$(RISCV)/lib``) - so the linked library and the shipped
        library are the same file."""
        lib_dir = os.path.join(self.riscv_path, "lib")
        if not os.path.isdir(lib_dir):
            self.logger.warning(f"{lib_dir} does not exist; not installing")
            return
        with open(os.path.join(lib_dir, "libriscv.so"), "wb") as handle:
            handle.write(lib_content)
        if self.build_static:
            for name in STATIC_ARCHIVES:
                source = os.path.join(self.build_dir, name)
                if os.path.exists(source):
                    shutil.copyfile(source, os.path.join(lib_dir, name))
        self.logger.info(f"Installed libriscv into {lib_dir}")


def stage_golden_model(artifact: SpikeBuildArtifact, riscv_path: str,
                       logger: logging.Logger | None = None) -> None:
    """Write a travelled :class:`SpikeBuildArtifact` into ``$RISCV`` so a build
    in another container can link it. Redundant when spike was built in the
    same container with ``install=True``."""
    log = logger or logging.getLogger(SpikeBuildNode.logging_name)
    lib_dir = os.path.join(riscv_path, "lib")
    os.makedirs(lib_dir, exist_ok=True)
    with open(os.path.join(lib_dir, artifact.lib_name), "wb") as handle:
        handle.write(artifact.lib_content)
    for name, content in artifact.static_archives:
        with open(os.path.join(lib_dir, name), "wb") as handle:
            handle.write(content)
    if artifact.headers_tar:
        include = os.path.join(riscv_path, "include")
        os.makedirs(include, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(artifact.headers_tar), mode="r:gz") as tar:
            tar.extractall(include)
    log.info(f"Staged golden model {artifact.digest[:12]} into {riscv_path}")
