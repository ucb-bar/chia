/*
 * VEXT Zicond custom-instruction class. Overrides riscv-dv's stub (which emits
 * `nop`) via the custom_target incdir. czero.eqz/czero.nez are standard R-type:
 * the assembly is "<mnemonic> rd, rs1, rs2", where rd/rs1/rs2 are the base
 * riscv_instr's randomized operands — so the generated stimulus exercises the
 * full operand space (random reg combinations, x0/zero-valued rs2 for the
 * condition-true path, plus the shared hazard/loop directed streams), the same
 * way every other R-type ALU instruction is covered. Only get_instr_name (the
 * mnemonic) and convert2asm (the operand rendering) need overriding; operand
 * randomization, category weighting and stream insertion are inherited.
 */

class riscv_custom_instr extends riscv_instr;

  `uvm_object_utils(riscv_custom_instr)
  `uvm_object_new

  virtual function string get_instr_name();
    case (instr_name)
      CZERO_EQZ: return "czero.eqz";
      CZERO_NEZ: return "czero.nez";
      default:   return instr_name.name();
    endcase
  endfunction : get_instr_name

  // R-type rendering, mirroring riscv_instr's R_FORMAT path: "name rd, rs1, rs2".
  virtual function string convert2asm(string prefix = "");
    string asm_str;
    asm_str = format_string(get_instr_name(), MAX_INSTR_STR_LEN);
    asm_str = $sformatf("%0s%0s, %0s, %0s", asm_str, rd.name(), rs1.name(), rs2.name());
    if (comment != "")
      asm_str = {asm_str, " #", comment};
    return asm_str.tolower();
  endfunction : convert2asm

endclass : riscv_custom_instr
