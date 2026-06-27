// VEXT Zicond instruction definitions (RV64X — our BOOM core is 64-bit). R_FORMAT
// + ARITHMETIC means czero rides the same operand randomization (rd/rs1/rs2 over
// the GPR file, incl. x0) and the same arithmetic-category selection/hazard
// streams as ADD/Zba — not a special case.
`DEFINE_CUSTOM_INSTR(CZERO_EQZ, R_FORMAT, ARITHMETIC, RV64X)
`DEFINE_CUSTOM_INSTR(CZERO_NEZ, R_FORMAT, ARITHMETIC, RV64X)
