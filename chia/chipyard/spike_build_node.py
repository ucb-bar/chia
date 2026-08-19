"""Builds Spike's libriscv into a shippable artifact.

Spike is the golden model a cospike simulator checks every committed
instruction against, and chipyard links it *dynamically* (``-lriscv`` in
``sims/common-sim-flags.mk``). So a change to spike is not a change to some
worker's environment - it is a new model that has to reach two places: the
container that links the simulator, and every worker that runs it.

This node makes that explicit by treating spike the way
:class:`~chia.chipyard.chisel_build_node.ChiselBuildNode` treats Chisel: it
builds the tree *as it finds it* and hands back the result by value. It does
not edit sources. Whoever owns the spike change - a patch, a loop, an agent
with a shell - owns the tree, exactly as they own the Chisel one.

The output feeds :class:`ChiselBuildNode` (``golden_model=``), which stages it
before ``make`` and, by default, bundles it into the simulator's
:class:`~chia.chipyard.state_def.BuildArtifact` so run workers need no spike
installed at all.

Dynamic is the default because it decouples the two models: a new golden model
needs no relink, so spike and the RTL can iterate on independent clocks, and one
simulator binary can be checked against several spikes. Link it statically
(``ChiselBuildNode(static_golden_model=True)``) when a single hermetic binary
matters more than that - note make will not relink for a new library on its own,
since ``-lriscv`` is an LDFLAG rather than a prerequisite of any rule, so a
static build must also pass ``clean_sim=True``.
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
# What a *static* golden model has to put on the link line, in dependency order.
# libriscv.a alone does not link: it carries ~900 undefined softfloat references
# plus a handful into disasm and fdt. The shared library hides this because it
# absorbed those archives when it was linked, which is why swapping -lriscv for
# libriscv.a and nothing else fails with hundreds of undefined symbols.
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
            chipyard_path: Absolute path to the Chipyard checkout holding the
                spike sources (``/home/ray/chipyard`` in the CHIA chipyard
                container).
            riscv_path: The toolchain prefix chipyard links against, i.e.
                ``$RISCV``. Defaults to the ``RISCV`` environment variable and
                then to ``<chipyard_path>/.conda-env/riscv-tools``. This is
                where ``install`` puts the library and where ``collect_headers``
                reads from.
            spike_rel: Location of the riscv-isa-sim checkout relative to
                ``chipyard_path``.
            make_jobs: ``make -j`` parallelism.
            strip: Run ``strip --strip-unneeded`` on a copy before reading the
                bytes. Worth leaving on: an unstripped libriscv.so is ~177MB of
                which ~8MB is code, and the artifact is shipped by value.
                The installed and linked-against library is never the stripped
                copy, so this costs no debuggability at link time.
            build_static: Also build (and install, when ``install``) the
                archives a static link needs - :data:`STATIC_ARCHIVES`, not just
                libriscv.a - for ``ChiselBuildNode(static_golden_model=True)``.
            collect_static: Carry those archives' bytes in the artifact too.
                Off by default and rarely wanted: they run to ~300MB, and they
                are only needed to link a static simulator in a *different*
                container from the one that built spike. Building and linking in
                the same container - the usual case - needs ``build_static``
                alone.
            collect_tools: Also read ``spike`` and ``spike-dasm``. The run
                worker disassembles DUT traces with spike-dasm, so a model that
                adds instructions wants its own copy travelling with it.
            collect_headers: Also read ``$RISCV/include/riscv`` as a gzipped
                tar. Only needed when the model will be staged into a container
                other than the one that built it - cospike compiles against
                these headers.
            install: Copy the library into ``$RISCV/lib`` so a
                :class:`ChiselBuildNode` running in this same container links
                against it. This is the step whose absence silently produces a
                simulator built against the *old* model.
            timeout_seconds: Timeout for the ``make`` invocation.
        """
        self.chipyard_path = chipyard_path
        self.spike_dir = os.path.join(chipyard_path, spike_rel)
        self.build_dir = os.path.join(self.spike_dir, "build")
        self.riscv_path = riscv_path or os.environ.get("RISCV") or os.path.join(
            chipyard_path, ".conda-env", "riscv-tools")
        self.make_jobs = make_jobs
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
        """mtime of the most recently touched spike source.

        The library is compared against this rather than trusted: make being
        happy is not evidence that the library on disk contains the edit, and a
        cospike run against a stale golden model reports divergences that are
        artefacts of the mismatch rather than of the design.
        """
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
        """The file's bytes, stripped of debug symbols unless disabled.

        Strips a *copy*: the original stays linkable and debuggable, and the
        artifact carries only what a run worker needs.
        """
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
        """(HEAD, dirty) of the spike checkout; ("", False) when not a git tree.

        Dirty is the normal state while a loop edits spike, which is why the
        artifact's identity is the library's digest and not this SHA.
        """
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

        Takes no arguments: like :meth:`ChiselBuildNode.build`, every input is
        the state of the tree plus the attributes set in ``__init__``.

        The rebuild is unconditional. make is incremental, so an already-current
        tree costs seconds - and the alternative, skipping when this call did not
        itself change anything, is how a container ends up with patched sources
        and a library built before them: the sources then say the fix is in while
        the model being co-simulated against does not have it.

        Returns:
            SpikeBuildArtifact: on success the library bytes and their digest,
            plus whatever ``build_static``/``collect_tools``/``collect_headers``
            asked for. On failure ``success=False`` with the captured output;
            callers should treat that as fatal rather than build a simulator
            against whatever library happens to be lying around.
        """
        if not os.path.isdir(self.build_dir):
            return self._failure(
                f"no spike build directory at {self.build_dir}: this image's "
                "chipyard does not carry spike sources, so the golden model "
                "cannot be rebuilt here")

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
        """Put the library we are shipping where chipyard's link step looks.

        ``$(RISCV)/lib``, not spike's build dir: ``sims/common-sim-flags.mk``
        links with ``-L$(RISCV)/lib``, so a rebuild that is not installed leaves
        the simulator linked against the previous model with nothing reporting
        it.

        Writes the artifact's own bytes rather than copying the build output, so
        the library the simulator is linked against and the library that travels
        with it are the same file. Otherwise ``digest`` would describe the
        shipped copy while the binary was linked against a different one, which
        is exactly the ambiguity this node exists to remove.
        """
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
    """Write a :class:`SpikeBuildArtifact` into ``$RISCV`` so a build links it.

    For the case where the model was built somewhere else: the bytes travelled,
    so put them where ``-L$(RISCV)/lib -I$(RISCV)/include`` will find them.
    A no-op for the common case of building spike and the simulator in the same
    container, where ``SpikeBuildNode(install=True)`` already did this.
    """
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
