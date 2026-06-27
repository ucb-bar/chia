import argparse

import m5
from m5.objects import *
from m5.objects.FuncUnit import *
from m5.objects.FuncUnitConfig import *

parser = argparse.ArgumentParser(description="RISC-V bare-metal O3 run")
parser.add_argument("--kernel", required=True, help="Path to bare-metal ELF")
parser.add_argument("--num-cpus", type=int, default=1)
parser.add_argument("--mem-size", default="2GiB")
parser.add_argument("--sys-clock", default="1GHz")
parser.add_argument("--cpu-clock", default="1GHz")
parser.add_argument("--memory-backend", default=None)  # accepted but ignored
args = parser.parse_args()

system = RiscvSystem()
system.mem_mode = "timing"
system.mem_ranges = [AddrRange(start=0x80000000, size=args.mem_size)]

system.voltage_domain = VoltageDomain()
system.clk_domain = SrcClockDomain(
    clock=args.sys_clock, voltage_domain=system.voltage_domain
)
system.cpu_voltage_domain = VoltageDomain()
system.cpu_clk_domain = SrcClockDomain(
    clock=args.cpu_clock, voltage_domain=system.cpu_voltage_domain
)

system.workload = RiscvBareMetal()
system.workload.bootloader = args.kernel

system.iobus = IOXBar()
system.membus = SystemXBar()
# BOOM MBUS beatBytes=8 (Rocket-Chip subsystem/Configs.scala:51-53)
system.membus.width = 8
system.system_port = system.membus.cpu_side_ports

system.membus.badaddr_responder = BadAddr()
system.membus.badaddr_responder.warn_access = "[WARN] -- Could be an speculative access"
system.membus.default = system.membus.badaddr_responder.pio

system.platform = HiFive()
system.platform.rtc = RiscvRTC(frequency=Frequency("100MHz"))
system.platform.clint.int_pin = system.platform.rtc.int_pin

system.iobus.cpu_side_ports = system.platform.pci_host.up_request_port()
system.iobus.mem_side_ports = system.platform.pci_host.up_response_port()
system.platform.pci_bus.cpu_side_ports = (
    system.platform.pci_host.down_request_port()
)
system.platform.pci_bus.default = system.platform.pci_host.down_response_port()
system.platform.pci_bus.config_error_port = (
    system.platform.pci_host.config_error.pio
)

system.bridge = Bridge(delay="50ns")
system.bridge.mem_side_port = system.iobus.cpu_side_ports
system.bridge.cpu_side_port = system.membus.mem_side_ports
system.bridge.ranges = system.platform._off_chip_ranges()

system.platform.attachOnChipIO(system.membus)
system.platform.attachOffChipIO(system.iobus)
system.platform.attachPlic()
system.platform.setNumCores(args.num_cpus)

# ---------------------------------------------------------------------------
# Cache hierarchy matching BOOM MegaBoom V3
# ---------------------------------------------------------------------------

class L1ICache(Cache):
    # BOOM: 32KiB, 8-way (parameters.scala: nSets=64, nWays=8, blockBytes=64)
    size = "32KiB"
    assoc = 8
    tag_latency = 1
    data_latency = 1
    response_latency = 1
    mshrs = 1
    tgts_per_mshr = 20
    writeback_clean = True
    is_read_only = True
    # BOOM: RandomReplacement (ICache.scala:65)
    replacement_policy = RandomRP()

class L1DCache(Cache):
    # BOOM: 32KiB, 8-way (parameters.scala, config-mixins.scala:236)
    # D-cache latency: tag_lat=2, data_lat=3, response_lat=4 from BOOM pipeline
    size = "32KiB"
    assoc = 8
    tag_latency = 2
    data_latency = 3
    response_latency = 4
    mshrs = 8
    tgts_per_mshr = 20
    write_buffers = 1
    writeback_clean = False
    # BOOM: random replacement (HellaCache.scala:28, dcache.scala:716)
    replacement_policy = RandomRP()

class L2Cache(Cache):
    # BOOM: 512KiB, 8-way SiFive InclusiveCache (Configs.scala, WithInclusiveCache)
    # tag=2 + data=3 + response=3 = 8 cycle total L2 hit latency
    size = "512KiB"
    assoc = 8
    tag_latency = 2
    data_latency = 3
    response_latency = 3
    mshrs = 10
    tgts_per_mshr = 20
    write_buffers = 2
    sequential_access = True
    # BOOM: LFSR-based random eviction (inclusivecache Directory.scala:117)
    replacement_policy = RandomRP()

# ---------------------------------------------------------------------------
# FU pool definitions for split issue queues matching BOOM MegaBoom V3
# ---------------------------------------------------------------------------

# --- INT IQ FU Pool ---
# BOOM: 4 ALU/Branch units, 1 with MUL, 1 with DIV (execution-units.scala)
class BoomIntAlu(FUDesc):
    opList = [OpDesc(opClass="IntAlu")]
    count = 1

class BoomIntAluSys(FUDesc):
    opList = [OpDesc(opClass="IntAlu"), OpDesc(opClass="System")]
    count = 1

class BoomIntAluMul(FUDesc):
    # BOOM MUL latency: 6 cycles (execution-units.scala: IntToFP pipe + imul)
    opList = [OpDesc(opClass="IntAlu"), OpDesc(opClass="IntMult", opLat=6)]
    count = 1

class BoomIntAluDiv(FUDesc):
    # BOOM DIV latency: ~7 cycles, unpipelined
    opList = [OpDesc(opClass="IntAlu"), OpDesc(opClass="IntDiv", opLat=7, pipelined=False)]
    count = 1

class BoomIntFUPool(FUPool):
    FUList = [BoomIntAlu(), BoomIntAluSys(), BoomIntAluMul(), BoomIntAluDiv()]

# --- MEM IQ FU Pool ---
# BOOM MegaBoom V3 D-cache store path (dcache.scala, lsu.scala):
#   dcache.scala:741: s3_valid = RegNext(s2_valid(0) && s2_hit(0) && isWrite(...))
#     — hardcoded to pipe index 0 ONLY. Pipe 1 cannot write to D-cache data array.
#   dcache.scala: assert(!(s2_valid(w) && s2_has_permission(w) && s2_hit(w)
#     && isWrite(s2_req(w).uop.mem_cmd)), "Store must go through 0th pipe in L1D")
#     — enforced for all w >= 1 (any non-zero pipe index)
#   dataWriteArb = Arbiter(2 inputs): in(0)=pipeline stores, in(1)=MSHR refills
#     Single write port on data array → max 1 store write/cycle from pipe 0 only.
# Fix: Split into pipe 0 (loads+stores, count=1) and pipe 1 (loads only, count=1).
#   Previous: BoomRdWrPort(count=2) gave BOTH FU instances store capability.
#   This let gem5's IQ issue 2 stores simultaneously — the 2nd consumed an issue
#   slot and FU cycle that BOOM would use for a concurrent load.
class BoomMemPipe0(RdWrPort):
    # Pipe 0: loads + stores (the ONLY pipe that can write D-cache data array)
    count = 1

class BoomMemPipe1(ReadPort):
    # Pipe 1: loads ONLY (no store capability — dcache.scala enforces this)
    count = 1

class BoomMemFUPool(FUPool):
    FUList = [BoomMemPipe0(), BoomMemPipe1()]

# --- FP IQ FU Pool ---
# BOOM: 2 FP units (FPU pipe + FDiv/Sqrt pipe), execution-units.scala
# BOOM FP latency: dfmaLatency=4, sfmaLatency=4 (config-mixins.scala:248)
# All FP units padded to dfmaLatency for write-port scheduling (fpu.scala:178-179)
# gem5 issueToExecuteDelay=1 accounts for BOOM's register-read stage, so opLat=4
class BoomFPFull(FUDesc):
    # Full FP unit with div/sqrt
    opList = [
        OpDesc(opClass="FloatAdd", opLat=4),
        OpDesc(opClass="FloatCmp", opLat=4),
        OpDesc(opClass="FloatCvt", opLat=4),
        OpDesc(opClass="Bf16Cvt", opLat=4),
        OpDesc(opClass="FloatMult", opLat=4),
        OpDesc(opClass="FloatMultAcc", opLat=4),
        OpDesc(opClass="FloatMisc", opLat=4),
        OpDesc(opClass="FloatDiv", opLat=15, pipelined=False),
        OpDesc(opClass="FloatSqrt", opLat=27, pipelined=False),
    ]
    count = 1

class BoomFPSimple(FUDesc):
    # Simple FP unit (no div/sqrt)
    opList = [
        OpDesc(opClass="FloatAdd", opLat=4),
        OpDesc(opClass="FloatCmp", opLat=4),
        OpDesc(opClass="FloatCvt", opLat=4),
        OpDesc(opClass="Bf16Cvt", opLat=4),
        OpDesc(opClass="FloatMult", opLat=4),
        OpDesc(opClass="FloatMultAcc", opLat=4),
        OpDesc(opClass="FloatMisc", opLat=4),
    ]
    count = 1

class BoomFPFUPool(FUPool):
    FUList = [BoomFPFull(), BoomFPSimple(), SIMD_Unit(), Matrix_Unit(), PredALU()]

# ---------------------------------------------------------------------------
# System and CPU setup
# ---------------------------------------------------------------------------

system.l2bus = L2XBar()
system.l2bus.width = 16  # BOOM MegaBoom uses WithSystemBusWidth(128) = 128-bit = 16 bytes (gem5 L2XBar default is 32 bytes = 256-bit)
system.l2 = L2Cache()
system.cpu = [
    RiscvO3CPU(cpu_id=i, clk_domain=system.cpu_clk_domain)
    for i in range(args.num_cpus)
]
for cpu in system.cpu:
    # --- BOOM MegaBoom V3 core pipeline widths ---
    # parameters.scala: fetchWidth=8 (half-words) = 4 RV64 instructions
    # config-mixins.scala:236: decodeWidth=4, numRobEntries=128
    cpu.fetchWidth = 4
    cpu.decodeWidth = 4
    cpu.renameWidth = 4
    cpu.dispatchWidth = 4
    cpu.commitWidth = 4
    cpu.issueWidth = 8

    # --- ROB, physical registers, LSQ ---
    cpu.numROBEntries = 128
    cpu.numPhysIntRegs = 128
    cpu.numPhysFloatRegs = 128
    cpu.LQEntries = 32
    cpu.SQEntries = 32

    # --- Frontend / Fetch ---
    # BOOM F0→F1→F2→F3→F4→DEC: fetchToDecodeDelay=2 models the pipeline depth
    # BOOM MegaBoom: numFetchBufferEntries=32 (config-mixins.scala:245)
    # BOOM: numFetchBufferEntries=32 (config-mixins.scala:245)
    # gem5 fetchBufferSize is in bytes; 64B = 16 insts + 2-cycle decode delay pipeline
    cpu.fetchBufferSize = 64
    cpu.fetchToDecodeDelay = 2
    cpu.decoupledFrontEnd = True
    cpu.numFTQEntries = 40
    cpu.fetchTargetWidth = 64
    cpu.maxTakenPredPerCycle = 1

    # --- D-cache ports ---
    # BOOM memWidth=2 (2 load ports), stores only through pipe 0
    cpu.cacheLoadPorts = 2
    cpu.cacheStorePorts = 1

    # --- Store set predictor ---
    cpu.store_set_clear_period = 1

    # --- Store-load forwarding ---
    # BOOM checks full-address overlap (no shift); gem5 default LSQDepCheckShift=4
    # shifts away low bits, causing false-negative forwarding → stalls.
    cpu.LSQDepCheckShift = 0

    # --- Load response throttling ---
    cpu.recvRespThrottling = True

    # --- Branch predictor: LTAGE matching BOOM TAGE-L ---
    cpu.branchPred = BranchPredictor()
    cpu.branchPred.conditionalBranchPred = LTAGE()
    cpu.branchPred.conditionalBranchPred.tage.nHistoryTables = 6
    cpu.branchPred.conditionalBranchPred.tage.tagTableTagWidths = [0, 7, 7, 8, 8, 9, 9]
    cpu.branchPred.conditionalBranchPred.tage.logTagTableSizes = [11, 7, 7, 8, 8, 7, 7]
    cpu.branchPred.conditionalBranchPred.tage.minHist = 2
    cpu.branchPred.conditionalBranchPred.tage.maxHist = 64
    cpu.branchPred.conditionalBranchPred.tage.tagTableCounterBits = 3
    cpu.branchPred.conditionalBranchPred.tage.tagTableUBits = 2

    # --- RAS (Return Address Stack) ---
    # BOOM: numRasEntries=32 (parameters.scala:68, default not overridden by MegaBoom)
    # gem5 default is 16; config.ini confirms 32. Reconstruction error — was not restored.
    cpu.branchPred.ras.numEntries = 32

    # --- BTB (Branch Target Buffer) ---
    # BOOM: BoomBTBParams(nSets=128, nWays=2) (btb.scala:14-20) → 256 entries, 2-way
    # gem5 default is 4096/1-way; config.ini confirms 256/2-way. Reconstruction error.
    cpu.branchPred.btb.numEntries = 256
    cpu.branchPred.btb.associativity = 2

    # --- Split issue queues (BOOM: INT=40, MEM=24, FP=32) ---
    cpu.instQueues = [
        IQUnit(numEntries=40, fuPool=BoomIntFUPool()),
        IQUnit(numEntries=24, fuPool=BoomMemFUPool()),
        IQUnit(numEntries=32, fuPool=BoomFPFUPool()),
    ]

    # --- Cache Hierarchy ---
    cpu.icache = L1ICache()
    cpu.dcache = L1DCache()
    cpu.dcache.prefetcher = TaggedPrefetcher(queue_size=2)

    cpu.icache.cpu_side = cpu.icache_port
    cpu.dcache.cpu_side = cpu.dcache_port

    cpu.icache.mem_side = system.l2bus.cpu_side_ports
    cpu.dcache.mem_side = system.l2bus.cpu_side_ports

    cpu.mmu.connectWalkerPorts(
        system.l2bus.cpu_side_ports, system.l2bus.cpu_side_ports
    )

    system.l2.cpu_side = system.l2bus.mem_side_ports
    system.l2.mem_side = system.membus.cpu_side_ports

    cpu.createThreads()
    cpu.createInterruptController()
    cpu.mmu.pma_checker = PMAChecker(
        uncacheable=[
            *system.platform._on_chip_ranges(),
            *system.platform._off_chip_ranges(),
        ]
    )

# ROI_BEGIN() & ROI_END() handlers
def handle_workbegin():
    print("Resetting stats at the start of ROI!")
    m5.stats.reset()

def handle_workend():
    m5.stats.dump()
    m5.stats.reset()

system.mem_ctrl = MemCtrl()
system.mem_ctrl.dram = DDR3_1600_8x8()
# DDR3-1333 (sg15) timing overrides to match BOOM's DRAMSim2 config
system.mem_ctrl.dram.tCK = "1.5ns"
system.mem_ctrl.dram.tBURST = "6ns"
system.mem_ctrl.dram.tCL = "15ns"
system.mem_ctrl.dram.tRCD = "15ns"
system.mem_ctrl.dram.tRP = "15ns"
system.mem_ctrl.dram.tRAS = "36ns"
system.mem_ctrl.dram.read_buffer_size = 16
system.mem_ctrl.dram.write_buffer_size = 16
system.mem_ctrl.dram.range = system.mem_ranges[0]
system.mem_ctrl.port = system.membus.mem_side_ports

system.exit_on_work_items = True

root = Root(full_system=True, system=system)
m5.instantiate()

print(f"Starting simulation")
while True:
    ev = m5.simulate()
    cause = ev.getCause()
    tick = m5.curTick()
    print(f"Exit @ {tick}: {cause}")

    if "workbegin" in cause:
        handle_workbegin()
        continue

    if "workend" in cause:
        handle_workend()
        continue

    if "exit" in cause or "exiting" in cause:
        break
