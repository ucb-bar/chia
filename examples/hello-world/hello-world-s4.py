
from chia.base.ChiaFunction import ChiaFunction, get
from chia.chipyard.chisel_build_node import *
from chia.chipyard.verilator_run_node import *
from pathlib import Path
import subprocess
from ray import ObjectRef
from chia.models.opencode import *
from chia.base.tools.BashTool import *

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

LLM_RTL_PROMPT = "" \
"Can you use the chipyard_bash tool to " \
"edit chipyard's generators/rocket-chip " \
"Chisel source and add a new print to the " \
"Chisel which says \"Hello World (#4) from " \
"instruction 1000!\" when instret reaches " \
"1000? If any message already exists, add " \
"the word \"again\" at the end of the existing " \
"message. Please respond explaining your change."

CHIPYARD_PATH = "/home/ray/chipyard"
CHIPYARD_CONFIG = "RocketConfig"

def main():

    get(print_hello_world.chia_remote())

    llm = OpenCodeLLM("opencode/big-pickle")
    resp : QueryResult = get(llm.prompt.chia_remote(
        llm, "Can you please respond just \"Hello World (#3) from OpenCode!\""))
    print("LLM Responded:")
    print(resp.result)

    chipyBash : BashTool = BashTool(
        "chipyard_bash", CHIPYARD_PATH,
        task_options={"resources": {"chipyard": 1}}
    )
    resp : QueryResult = get(llm.prompt.chia_remote(
        llm, LLM_RTL_PROMPT, tools=[chipyBash]
    ))
    chipyBash.stop()
    print(resp.result)

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

    # First line of run_output.out is "testing $random ..."
    # Next 1000 lines are committed instruction trace
    # 1002 line should be "Hello from instruction 1000!"
    print("Parsed lines from simulation (run_output.out): ")

    print(run_output.out.split('\n')[1001])
    print(run_output.out.split('\n')[1002]) # Should have LLM's addition
    print(run_output.out.split('\n')[1003])

if __name__ == "__main__":
    main()