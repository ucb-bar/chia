"""PFAS-in-semiconductor-manufacturing mask data for chip PFAS/carbon modeling.

All data in this module is derived from the following work, and full credit
for the underlying analysis belongs to its authors:

    Mariam Elgamal, Abdulrahman Mahmoud, Gu-Yeon Wei, David Brooks, and
    Gage Hills. "Modeling PFAS in Semiconductor Manufacturing to Quantify
    Trade-offs in Energy Efficiency and Environmental Impact of Computing
    Systems." arXiv preprint arXiv:2505.06727 (2025).

    Abstract: https://arxiv.org/abs/2505.06727
    PDF:      https://arxiv.org/pdf/2505.06727

Any use of this node should cite this work.

The paper uses the number of lithography masks as a first-order proxy for the
amount of PFAS ("forever chemicals") used in chip manufacturing, because the
PFAS-containing layers (photoresist, anti-reflective coatings, topcoats) scale
with mask count -- eq. (1): #PFAS_litho ~ number of lithography masks.

Data provenance within the paper:
  * BEOL_METAL_STACK_TYPES -- lithography masks per BEOL metal-stack recipe.
    Mask counts follow Table II ("Number of lithography process steps and masks
    per metal line process"); the recipe names are the BEOL stack types in the
    Figure 5 legend, read top-to-bottom.
  * BEOL_PROCESSES_PER_NODE -- per-node BEOL metal-stack composition, decoded
    from the stacked bars of Figure 5.
  * PROCESSES_PER_NODE -- the full per-node stack: FEOL and MOL layers (counts
    from Figure 5) prepended to the BEOL layers above.

Any transcription errors here are ours, not the authors'.

The PFAS proxy (lithography masks * area/yield) is only meaningful when considered relative
to other instances.
"""

# BEOL metal-stack type -> total lithography masks (metal-layer process + via-
# layer process), read top-to-bottom from the Figure 5 legend (arXiv:2505.06727).
# Keys match the recipe strings used in BEOL_PROCESSES_PER_NODE, so a node's BEOL
# mask count is sum(BEOL_METAL_STACK_TYPES[r] for r in BEOL_PROCESSES_PER_NODE[node]).
BEOL_METAL_STACK_TYPES: dict[str, int] = {
    "M: ArF, V: ArF": 2,
    "M: ArFi, V: ArFi": 2,
    "M: ArFi LE, V: ArFi LE": 3,
    "M: ArFi SADP + ArFi LE block, V: ArFi LE-2": 4,
    "M: ArFi SADP + ArFi SAB, V: ArFi LE-4": 7,
    "M: ArFi SAQP + ArFi SAB, V: ArFi LE-4": 7,
    "M: EUV LE, V: EUV LE": 2,
    "M: ArFi SAQP + EUV LE block, V: EUV LE": 3,
    "M: ArFi SAQP + EUV LE block, V: EUV LE-2": 4,
    "M: ArFi SAQP + EUV LE-2, V: EUV LE-2": 5,
    "M: EUV LE-2 + EUV SAB, V: EUV LE-2": 6,
    "M: i-line, V: ArF": 2,
}

# Per-node BEOL metal stack from Figure 5 of arXiv:2505.06727
# Order is bottom (finest) -> top (coarsest). 
BEOL_PROCESSES_PER_NODE: dict[str, list[str]] = {
    "130nm": ["M: ArF, V: ArF"]*5 + ["M: i-line, V: ArF"],
    "90nm":  ["M: ArF, V: ArF"]*5 + ["M: i-line, V: ArF"],
    "65nm":  ["M: ArF, V: ArF"]*8 + ["M: i-line, V: ArF"],
    "40nm":  ["M: ArFi, V: ArFi"]*5 + ["M: ArF, V: ArF"]*4 + ["M: i-line, V: ArF"],
    "28nm":  ["M: ArFi, V: ArFi"]*7 + ["M: ArF, V: ArF"]*2 + ["M: i-line, V: ArF"],
    "20nm":  ["M: ArFi LE, V: ArFi LE"]*3 + ["M: ArFi, V: ArFi"]*4 + ["M: ArF, V: ArF"]*2 + ["M: i-line, V: ArF"],
    "16nm":  ["M: ArFi LE, V: ArFi LE"]*3 + ["M: ArFi, V: ArFi"]*4 + ["M: ArF, V: ArF"]*2 + ["M: i-line, V: ArF"],
    "10nm":  ["M: ArFi SADP + ArFi LE block, V: ArFi LE-2"]*3 + ["M: ArFi LE, V: ArFi LE"]*4 + ["M: ArFi, V: ArFi"]*3 + ["M: i-line, V: ArF"],
    "7nm":   ["M: ArFi SADP + ArFi SAB, V: ArFi LE-4"]*1 + ["M: ArFi SAQP + ArFi SAB, V: ArFi LE-4"]*2 + ["M: ArFi SADP + ArFi LE block, V: ArFi LE-2"]*4 + ["M: ArFi, V: ArFi"]*7 + ["M: i-line, V: ArF"],
    "7nm+":  ["M: EUV LE, V: EUV LE"]*3 + ["M: ArFi SADP + ArFi LE block, V: ArFi LE-2"]*4 + ["M: ArFi, V: ArFi"]*7 + ["M: i-line, V: ArF"],
    "5nm":   ["M: ArFi SAQP + EUV LE-2, V: EUV LE-2"]*1 + ["M: ArFi SAQP + EUV LE block, V: EUV LE-2"]*2 + ["M: ArFi SAQP + EUV LE block, V: EUV LE"]*2 + ["M: ArFi SADP + ArFi LE block, V: ArFi LE-2"]*2 + ["M: ArFi, V: ArFi"]*9 + ["M: i-line, V: ArF"],
    "3nm":   ["M: EUV LE-2 + EUV SAB, V: EUV LE-2"]*2 + ["M: ArFi SAQP + EUV LE-2, V: EUV LE-2"]*2 + ["M: ArFi SAQP + EUV LE block, V: EUV LE"]*2 + ["M: ArFi SADP + ArFi LE block, V: ArFi LE-2"]*2 + ["M: ArFi, V: ArFi"]*11 + ["M: i-line, V: ArF"],
}

# FEOL and MOL lithography-layer counts per node (each is one PFAS mask layer),
# from arXiv:2505.06727 (the FEOL/MOL portions of the Figure 5 stacked bars).
_FEOL_LAYERS_PER_NODE: dict[str, int] = {
    n: (8 if n == "130nm" else 10) for n in BEOL_PROCESSES_PER_NODE
}
_MOL_LAYERS_PER_NODE: dict[str, int] = {
    n: (1 if n in ("130nm", "90nm", "65nm", "40nm", "28nm", "20nm") else 2)
    for n in BEOL_PROCESSES_PER_NODE
}

# Full per-node lithography stack, bottom -> top: FEOL, then MOL, then the BEOL
# metal layers. Each "FEOL"/"MOL" entry is one mask layer; the BEOL entries are
# metal-stack recipes priced by BEOL_METAL_STACK_TYPES. A node's total PFAS mask
# layers is (# "FEOL") + (# "MOL") + sum(BEOL_METAL_STACK_TYPES[r] for the BEOL
# entries r).
PROCESSES_PER_NODE: dict[str, list[str]] = {
    n: ["FEOL"] * _FEOL_LAYERS_PER_NODE[n]
    + ["MOL"] * _MOL_LAYERS_PER_NODE[n]
    + beol
    for n, beol in BEOL_PROCESSES_PER_NODE.items()
}


def num_lith_masks(node: str) -> int:
    """Number of PFAS-containing lithography masks for ``node``.

    Sums the full per-node stack in PROCESSES_PER_NODE: each FEOL and MOL layer
    counts as one mask, and each BEOL metal-stack recipe is priced by
    BEOL_METAL_STACK_TYPES (paper eq. 1: #PFAS_litho ~ number of masks).
    """
    if node not in PROCESSES_PER_NODE:
        raise KeyError(f"unknown node {node!r}; known nodes: {list(PROCESSES_PER_NODE)}")
    return sum(
        1 if layer in ("FEOL", "MOL") else BEOL_METAL_STACK_TYPES[layer]
        for layer in PROCESSES_PER_NODE[node]
    )


def get_PFAS_proxy(node: str, area: float, yield_: float) -> float:
    """PFAS manufacturing proxy for ``node`` (paper eq. 2)::

        PFAS_chip_manufacturing = #PFAS_litho * Area / Yield

    where #PFAS_litho is ``num_lith_masks(node)``. ``area`` is die area (in
    whatever unit you use consistently) and ``yield_`` is the manufacturing
    yield in (0, 1]. (``yield`` is a Python keyword, hence the trailing
    underscore.) 
    
    Like the mask proxy itself, the result is only meaningful
    relative to other instances computed the same way. This is
    not an absolute quantity of PFAS.
    """
    if not 0.0 < yield_ <= 1.0:
        raise ValueError(f"yield_ must be in (0, 1], got {yield_}")
    return num_lith_masks(node) * area / yield_