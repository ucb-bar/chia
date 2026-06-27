//-----------------------------------------------------------------------------
// VEXT custom riscv-dv target: rv64gc + RATIFIED Zba/Zbb/Zbc/Zbs + Zicond.
//
// Same as the base vext target, plus the RV64X custom group enabled (our core is
// 64-bit) so the Zicond instructions (czero.eqz/czero.nez, defined in isa/custom/)
// are mixed into the random arithmetic stream. RV64X is ONLY enabled here, not in
// the shared base target: czero must not be generated for extensions whose -march
// lacks _zicond (gcc would reject it) or whose DUT doesn't implement it.
//
// No shipped riscv-dv target enables the ratified Zb* groups (rv64imcb uses the
// draft RV64B, which our gcc/Spike reject). Machine-mode only, no VM, and
// aligned-only memory access (BOOM traps on misaligned; Spike doesn't — a known
// non-bug divergence we must not generate). Passed via run.py --custom_target.
//-----------------------------------------------------------------------------

// XLEN
parameter int XLEN = 64;

// No address translation (bare-metal -p style; cosim aligns at 0x80000000)
parameter satp_mode_t SATP_MODE = BARE;

// Supported Privileged mode
privileged_mode_t supported_privileged_mode[] = {MACHINE_MODE};

// Unsupported instructions
riscv_instr_name_t unsupported_instr[];

// ISA supported by the processor
riscv_instr_group_t supported_isa[$] = {RV32I, RV32M, RV64I, RV64M, RV32C, RV64C,
                                        RV32A, RV64A, RV32F, RV64F, RV32D, RV64D,
                                        RV32ZBA, RV32ZBB, RV32ZBC, RV32ZBS,
                                        RV64ZBA, RV64ZBB, RV64ZBC, RV64ZBS,
                                        RV64X};   // Zicond czero.eqz/czero.nez (isa/custom/)

// Interrupt mode support. DIRECT only: in VECTORED mode rocket/BOOM legalizes
// the mtvec base to 256-byte alignment (WARL, spec-legal) while spike keeps it
// exact — trap entries then differ on the first trap with no DUT bug. We
// inject no interrupts, so vectored tables add no coverage.
mtvec_mode_t supported_interrupt_mode[$] = {DIRECT};

// The number of interrupt vectors to be generated, only used if VECTORED
// interrupt mode is supported
int max_interrupt_vector_num = 16;

// Physical memory protection support
bit support_pmp = 0;

// Enhanced physical memory protection support
bit support_epmp = 0;

// Debug mode support
bit support_debug_mode = 0;

// Support delegate trap to user mode
bit support_umode_trap = 0;

// Support sfence.vma instruction (no VM here)
bit support_sfence = 0;

// Support unaligned load/store — OFF: BOOM traps, Spike executes -> divergence
bit support_unaligned_load_store = 1'b0;

// GPR setting
parameter int NUM_FLOAT_GPR = 32;
parameter int NUM_GPR = 32;
parameter int NUM_VEC_GPR = 32;

// ----------------------------------------------------------------------------
// Vector extension configuration (disabled)
// ----------------------------------------------------------------------------
parameter int VECTOR_EXTENSION_ENABLE = 0;
parameter int VLEN = 512;
parameter int ELEN = 32;
parameter int SELEN = 8;
parameter int VELEN = int'($ln(ELEN)/$ln(2)) - 3;
parameter int MAX_LMUL = 8;

// ----------------------------------------------------------------------------
// Multi-harts configuration
// ----------------------------------------------------------------------------
parameter int NUM_HARTS = 1;

// ----------------------------------------------------------------------------
// Previleged CSR implementation (machine mode + FP only)
// ----------------------------------------------------------------------------
`ifdef DSIM
privileged_reg_t implemented_csr[] = {
`else
const privileged_reg_t implemented_csr[] = {
`endif
    MVENDORID,  // Vendor ID
    MARCHID,    // Architecture ID
    MIMPID,     // Implementation ID
    MHARTID,    // Hardware thread ID
    MSTATUS,    // Machine status
    MISA,       // ISA and extensions
    MIE,        // Machine interrupt-enable register
    MTVEC,      // Machine trap-handler base address
    MCOUNTEREN, // Machine counter enable
    MSCRATCH,   // Scratch register for machine trap handlers
    MEPC,       // Machine exception program counter
    MCAUSE,     // Machine trap cause
    MTVAL,      // Machine bad address or instruction
    MIP,        // Machine interrupt pending
    FCSR        // Floating point control and status
};

// Implementation-specific custom CSRs
bit [11:0] custom_csr[] = {
};

// ----------------------------------------------------------------------------
// Supported interrupt/exception setting, used for functional coverage
// ----------------------------------------------------------------------------
`ifdef DSIM
interrupt_cause_t implemented_interrupt[] = {
`else
const interrupt_cause_t implemented_interrupt[] = {
`endif
    M_SOFTWARE_INTR,
    M_TIMER_INTR,
    M_EXTERNAL_INTR
};

`ifdef DSIM
exception_cause_t implemented_exception[] = {
`else
const exception_cause_t implemented_exception[] = {
`endif
    INSTRUCTION_ACCESS_FAULT,
    ILLEGAL_INSTRUCTION,
    BREAKPOINT,
    LOAD_ADDRESS_MISALIGNED,
    LOAD_ACCESS_FAULT,
    STORE_AMO_ADDRESS_MISALIGNED,
    STORE_AMO_ACCESS_FAULT,
    ECALL_MMODE
};
