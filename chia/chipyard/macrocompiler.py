

"""Chipyard MacroCompiler glue for SRAMs.

Parses the Chipyard/firtool ``.top.mems.conf`` into SRAMSpecs, generates the
MacroCompiler MDF JSON library describing each macro, and runs Chipyard's
``tapeout.jar`` MacroCompiler to remap the synflop SRAMs onto those macros.
"""

import json
import logging
import math
import os
import re
import subprocess
import tempfile

from dataclasses import dataclass, field

from chia.base.ChiaFunction import ChiaFunction

logger = logging.getLogger(__name__)


@dataclass
class SRAMSpec:
    name: str
    depth: int
    width: int  # bits
    ports: str
    mask_gran: int | None
    num_rw_ports: int = 0
    num_read_ports: int = 0
    num_write_ports: int = 0
    parsed_ports: list[tuple[str, bool]] = field(default_factory=list)

    @property
    def size_bytes(self) -> int:
        return self.depth * self.width // 8


_MEMS_CONF_RE = re.compile(
    r"name\s+(\S+)\s+depth\s+(\d+)\s+width\s+(\d+)\s+ports\s+(\S+)"
    r"(?:\s+mask_gran\s+(\d+))?"
)


def rename_ports(ports_str: str) -> tuple[list[tuple[str, bool]], int, int, int]:
    """Parse a comma-separated port string into named ports with counts.

    Mirrors Chipyard ConfReader.renamePorts(). Each token becomes a
    (prefix, masked) tuple with incrementing per-type counters.

    Example: "write,write,read" -> ([("W0", False), ("W1", False), ("R0", False)], 1, 2, 0)
    """
    read_count = 0
    write_count = 0
    rw_count = 0
    result: list[tuple[str, bool]] = []
    for token in ports_str.split(","):
        token = token.strip()
        if token == "read":
            result.append((f"R{read_count}", False))
            read_count += 1
        elif token == "write":
            result.append((f"W{write_count}", False))
            write_count += 1
        elif token == "mwrite":
            result.append((f"W{write_count}", True))
            write_count += 1
        elif token == "rw":
            result.append((f"RW{rw_count}", False))
            rw_count += 1
        elif token == "mrw":
            result.append((f"RW{rw_count}", True))
            rw_count += 1
        else:
            logger.warning(f"Unknown port token '{token}', treating as rw")
            result.append((f"RW{rw_count}", False))
            rw_count += 1
    return result, read_count, write_count, rw_count


def parse_mems_conf(content: str) -> list[SRAMSpec]:
    """Parse a .top.mems.conf file into a list of SRAMSpec."""
    specs = []
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = _MEMS_CONF_RE.match(line)
        if not m:
            logger.warning(f"Could not parse mems.conf line: {line}")
            continue
        name = m.group(1)
        depth = int(m.group(2))
        width = int(m.group(3))
        ports = m.group(4)
        mask_gran = int(m.group(5)) if m.group(5) else None

        # Derive port counts and parsed port list for downstream generators
        parsed_ports, num_r, num_w, num_rw = rename_ports(ports)

        specs.append(SRAMSpec(
            name=name, depth=depth, width=width, ports=ports,
            mask_gran=mask_gran,
            num_rw_ports=num_rw, num_read_ports=num_r, num_write_ports=num_w,
            parsed_ports=parsed_ports,
        ))
    return specs


def _build_port_template(prefix: str, masked: bool, mask_gran: int | None) -> dict:
    """Build a MacroCompiler MDF port entry for a single parsed port."""
    is_rw = prefix.startswith("RW")
    is_reader = prefix.startswith("R") and not is_rw
    is_writer = prefix.startswith("W")

    entry: dict = {
        "address port name": f"{prefix}_addr",
        "address port polarity": "active high",
        "clock port name": f"{prefix}_clk",
        "clock port polarity": "active high",
    }

    if is_rw:
        entry["write enable port name"] = f"{prefix}_wmode"
        entry["write enable port polarity"] = "active high"
        entry["chip enable port name"] = f"{prefix}_en"
        entry["chip enable port polarity"] = "active high"
        entry["output port name"] = f"{prefix}_rdata"
        entry["output port polarity"] = "active high"
        entry["input port name"] = f"{prefix}_wdata"
        entry["input port polarity"] = "active high"
        if masked:
            entry["mask port name"] = f"{prefix}_wmask"
            entry["mask port polarity"] = "active high"
            if mask_gran:
                entry["mask granularity"] = mask_gran
    elif is_reader:
        entry["read enable port name"] = f"{prefix}_en"
        entry["read enable port polarity"] = "active high"
        entry["output port name"] = f"{prefix}_data"
        entry["output port polarity"] = "active high"
    elif is_writer:
        entry["write enable port name"] = f"{prefix}_en"
        entry["write enable port polarity"] = "active high"
        entry["input port name"] = f"{prefix}_data"
        entry["input port polarity"] = "active high"
        if masked:
            entry["mask port name"] = f"{prefix}_mask"
            entry["mask port polarity"] = "active high"
            if mask_gran:
                entry["mask granularity"] = mask_gran

    return entry


def _compute_family(spec: SRAMSpec) -> str:
    """Compute the MDF family string, e.g., '1r2w' or '1rw'."""
    parts = []
    if spec.num_read_ports > 0:
        parts.append(f"{spec.num_read_ports}r")
    if spec.num_write_ports > 0:
        parts.append(f"{spec.num_write_ports}w")
    if spec.num_rw_ports > 0:
        parts.append(f"{spec.num_rw_ports}rw")
    return "".join(parts) or "1rw"

# Generic macrocompiler func
def generate_macrocompiler_lib(sram_specs: list[SRAMSpec], cell_prefix: str = "mapped_",) -> str:
    """Generate MDF JSON library for MacroCompiler from SRAM specs.

    Each SRAM gets a library entry named ``<cell_prefix><name>`` with exact-match
    depth/width/ports/mask_gran so MacroCompiler maps 1:1 without splitting.
    """
    entries = []
    for spec in sram_specs:
        ports = [_build_port_template(prefix, masked, spec.mask_gran)
                 for prefix, masked in spec.parsed_ports]
        entries.append({
            "type": "sram",
            "name": f"{cell_prefix}{spec.name}",
            "depth": str(spec.depth),
            "width": spec.width,
            "family": _compute_family(spec),
            "mask": "true" if spec.mask_gran else "false",
            "vt": "svt",
            "mux": 1,
            "ports": ports,
        })
    return json.dumps(entries, indent=2)


@ChiaFunction(resources={"chipyard": 1})
def remap_with_macrocompiler(
    mems_conf_content: str,
    macrocompiler_lib_json: str,
    chipyard_path: str,
) -> str | None:
    """Run MacroCompiler to remap synflop SRAMs to library macros.

    Runs on a chipyard node where Java + tapeout.jar are available. This is the
    only function in this module that touches a Chipyard resource (it shells out
    to ``java -cp <chipyard>/.classpath_cache/tapeout.jar
    tapeout.macros.MacroCompiler``), so it is the only one decorated with
    ``@ChiaFunction(resources={"chipyard": 1})``. The generators above
    (parse/lib/stub/assemble) are pure Python and stay plain callables.

    Args:
        mems_conf_content: Contents of .top.mems.conf
        macrocompiler_lib_json: MDF JSON library with SRAM entries
        chipyard_path: Path to chipyard installation (for tapeout.jar)

    Returns:
        Remapped .top.mems.v content, or None on failure.
    """
    work_dir = tempfile.mkdtemp(prefix="macrocompiler_remap_")
    conf_path = os.path.join(work_dir, "top.mems.conf")
    lib_path = os.path.join(work_dir, "lib.json")
    output_path = os.path.join(work_dir, "remapped.mems.v")

    # MacroCompiler's MemConf parser requires a trailing space on every conf
    # line; chipyard's own flow adds it with `sed 's/.*/& /'` (common.mk). Match
    # that here — without it, MacroCompiler throws "Error parsing MemConf string".
    normalized_conf = "".join(
        f"{line.rstrip()} \n" for line in mems_conf_content.splitlines() if line.strip()
    )
    with open(conf_path, "w") as f:
        f.write(normalized_conf)
    with open(lib_path, "w") as f:
        f.write(macrocompiler_lib_json)

    tapeout_jar = os.path.join(chipyard_path, ".classpath_cache", "tapeout.jar")
    if not os.path.isfile(tapeout_jar):
        print(f"  [macrocompiler] ERROR: tapeout.jar not found at {tapeout_jar}")
        return None

    # The Ray worker process starts under anaconda's python without sourcing
    # chipyard's env.sh, so `java` is not on PATH; it lives in chipyard's conda
    # env. Resolve it explicitly, falling back to PATH for non-chipyard hosts.
    java_bin = os.path.join(chipyard_path, ".conda-env", "bin", "java")
    if not os.path.isfile(java_bin):
        java_bin = "java"

    cmd = [
        java_bin, "-cp", tapeout_jar,
        "tapeout.macros.MacroCompiler",
        "-n", conf_path,
        "-v", output_path,
        "--mode", "compileavailable",
        "-l", lib_path,
    ]
    print(f"  [macrocompiler] Running MacroCompiler remap...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

    if result.returncode != 0:
        print(f"  [macrocompiler] FAILED (rc={result.returncode})")
        print(f"  [macrocompiler] stderr: {result.stderr[-500:]}")
        return None

    if not os.path.isfile(output_path):
        print(f"  [macrocompiler] No output file produced")
        return None

    with open(output_path) as f:
        remapped = f.read()

    return remapped


def assemble_generated_src_with_macros(
    generated_src_files: list[tuple[str, str]],
    remapped_mems_v: str,
    macro_stubs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Swap in the MacroCompiler-remapped .top.mems.v and add macro stubs.

    Library-agnostic: works for any MacroCompiler remap output (CACTI, SRAM22,
    etc.), since it only matches on module names, not the macro contents.

    Args:
        generated_src_files: Original generated_src from build.
        remapped_mems_v: MacroCompiler output instantiating the library macros.
        macro_stubs: List of (filename, content) blackbox stub modules for the
            mapped macros.

    Returns:
        Modified generated_src with the remapped .top.mems.v swapped in, any
        gen-collateral .sv files that redefine the remapped modules removed, and
        the macro stubs appended. The .top.mems.conf is left untouched.
    """
    new_files = []
    for fname, content in generated_src_files:
        if fname.endswith(".top.mems.v"):
            # Replace synflop .top.mems.v with remapped version
            new_files.append((fname, remapped_mems_v))
            print(f"  [assemble] Replaced {fname} with macro-remapped version")
        else:
            new_files.append((fname, content))

    # Remove any gen-collateral _ext.sv files that conflict with .top.mems.v
    # definitions (firtool emits structural _ext wrappers in .sv files, but the
    # remapped .top.mems.v now provides implementations for those module names)
    mems_v_modules = set(re.findall(r'^module\s+(\w+)', remapped_mems_v, re.MULTILINE))
    filtered = []
    for fname, content in new_files:
        # Check if this .sv file defines a module that's also in .top.mems.v
        if fname.endswith(".sv"):
            file_modules = set(re.findall(r'^module\s+(\w+)', content, re.MULTILINE))
            overlap = file_modules & mems_v_modules
            if overlap:
                print(f"  [assemble] Removing {fname} (modules {overlap} defined in .top.mems.v)")
                continue
        filtered.append((fname, content))
    new_files = filtered

    # Add blackbox stubs for each mapped macro
    for stub_name, stub_content in macro_stubs:
        new_files.append((stub_name, stub_content))
        print(f"  [assemble] Added Verilog stub: {stub_name}")

    print(f"  [assemble] Final: {len(new_files)} files")
    return new_files


def generate_macro_stubs(sram_specs: list[SRAMSpec], cell_prefix: str = "mapped_") -> list[tuple[str, str]]:
    """Generate blackbox Verilog stubs for mapped SRAM macros.

    Each stub declares the port interface matching the MacroCompiler
    instantiation (and the Liberty cell) with no body. Library-agnostic: the
    cell_prefix selects the macro family (e.g. ``"cacti_"``, ``"sram22_"``).
    """
    stubs = []
    for spec in sram_specs:
        addr_bits = max(1, math.ceil(math.log2(max(spec.depth, 2))))
        mask_bits = math.ceil(spec.width / spec.mask_gran) if spec.mask_gran else 0
        name = f"{cell_prefix}{spec.name}"

        ports = []
        for prefix, masked in spec.parsed_ports:
            is_rw = prefix.startswith("RW")
            is_reader = prefix.startswith("R") and not is_rw
            is_writer = prefix.startswith("W")

            ports.append(f"  input  [{addr_bits-1}:0] {prefix}_addr")
            ports.append(f"  input         {prefix}_clk")
            ports.append(f"  input         {prefix}_en")

            if is_rw:
                ports.append(f"  input         {prefix}_wmode")
                ports.append(f"  input  [{spec.width-1}:0] {prefix}_wdata")
                ports.append(f"  output [{spec.width-1}:0] {prefix}_rdata")
                if masked and mask_bits > 0:
                    ports.append(f"  input  [{mask_bits-1}:0] {prefix}_wmask")
            elif is_reader:
                ports.append(f"  output [{spec.width-1}:0] {prefix}_data")
            elif is_writer:
                ports.append(f"  input  [{spec.width-1}:0] {prefix}_data")
                if masked and mask_bits > 0:
                    ports.append(f"  input  [{mask_bits-1}:0] {prefix}_mask")

        lines = [f"module {name}("]
        lines.append(",\n".join(ports))
        lines.append(");")
        lines.append("endmodule")

        stub_content = "\n".join(lines) + "\n"
        stubs.append((f"{name}.v", stub_content))

    return stubs
