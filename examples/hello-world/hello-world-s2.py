
from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.chisel_build_node import *
from chia.chipyard.verilator_run_node import *
from pathlib import Path
import subprocess
from ray import ObjectRef

@ChiaFunction()
def print_hello_world():
    print("Hello World (#1) from a remote call!")

@ChiaFunction(resources={"chipyard": 1})
def compile_program(src_contents : str) -> bytes:

    with open("temp.c", "w", encoding="utf-8") as src:
        src.write(src_contents)

    cmd = []
    cmd += ["/home/ray/chipyard/.conda-env/riscv-tools/bin/riscv64-unknown-elf-gcc"]
    cmd += ["-static"]
    cmd += ["-specs=htif_nano.specs"]
    cmd += ["temp.c", "-o", "temp.riscv"]

    subprocess.run(
        cmd,
        capture_output=True,
        text=True, 
    )

    with open("temp.riscv", "rb") as elf:
        elf_data = elf.read()

    return elf_data

CHIPYARD_PATH = "/home/ray/chipyard"
CHIPYARD_CONFIG = "RocketConfig"

def main():
    get(print_hello_world.chia_remote())

    c_src = Path("helloworld.c").read_text(encoding="utf-8")
    testBinFuture : ObjectRef[bytes] = compile_program.chia_remote(c_src)

    cb_node = ChiselBuildNode(
        CHIPYARD_PATH, CHIPYARD_CONFIG, target = BuildTarget.VERILATOR
    )
    buildFuture : ObjectRef[BuildArtifact] = cb_node.build.chia_remote(cb_node)

    verilator_node = VerilatorRunNode()
    run_output : RunResult = get(
        verilator_node.run.chia_remote(
            verilator_node, buildFuture, testBinFuture,
            "helloworld.riscv", "/home/ray/"
        )
    )

    print("Output of Verilator run (run_output.log): ")
    print(run_output.log)

if __name__ == "__main__":
    main()