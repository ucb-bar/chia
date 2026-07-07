// Base class for the scalar-crypto RV64X custom instructions (rv64x_instr.sv).
// Renders each op to assembly; operand randomization and stream insertion are
// inherited from riscv_instr. zk_imm sources the non-register immediates
// (aes64ks1i rnum, rori/roriw shamt), randomized with the instruction.

class riscv_custom_instr extends riscv_instr;

  rand bit [5:0] zk_imm;

  `uvm_object_utils(riscv_custom_instr)
  `uvm_object_new

  // Mnemonic = enum name minus the "ZK_" prefix, lowercased in convert2asm.
  virtual function string get_instr_name();
    string s = instr_name.name();
    return s.substr(3, s.len() - 1);
  endfunction : get_instr_name

  virtual function string convert2asm(string prefix = "");
    string asm_str;
    asm_str = format_string(get_instr_name(), MAX_INSTR_STR_LEN);
    case (instr_name)
      // rd, rs1
      ZK_AES64IM, ZK_BREV8, ZK_REV8,
      ZK_SHA256SIG0, ZK_SHA256SIG1, ZK_SHA256SUM0, ZK_SHA256SUM1,
      ZK_SHA512SIG0, ZK_SHA512SIG1, ZK_SHA512SUM0, ZK_SHA512SUM1:
        asm_str = $sformatf("%0s%0s, %0s", asm_str, rd.name(), rs1.name());
      ZK_AES64KS1I:   // rd, rs1, rnum (0..10)
        asm_str = $sformatf("%0s%0s, %0s, %0d", asm_str, rd.name(), rs1.name(), zk_imm % 6'd11);
      ZK_RORI:        // rd, rs1, shamt (0..63)
        asm_str = $sformatf("%0s%0s, %0s, %0d", asm_str, rd.name(), rs1.name(), zk_imm);
      ZK_RORIW:       // rd, rs1, shamt (0..31)
        asm_str = $sformatf("%0s%0s, %0s, %0d", asm_str, rd.name(), rs1.name(), zk_imm[4:0]);
      default:        // rd, rs1, rs2
        asm_str = $sformatf("%0s%0s, %0s, %0s", asm_str, rd.name(), rs1.name(), rs2.name());
    endcase
    if (comment != "")
      asm_str = {asm_str, " #", comment};
    return asm_str.tolower();
  endfunction : convert2asm

endclass : riscv_custom_instr
