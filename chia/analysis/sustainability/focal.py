"""FOCAL: a first-order carbon model for comparing processor architectures.

Implements the Normalized Carbon Footprint (NCF) metric from Lieven Eeckhout,
"FOCAL: A First-Order Carbon Model to Assess Processor Sustainability"
(ASPLOS '24, https://doi.org/10.1145/3620665.3640415).

FOCAL weighs a design's *embodied* footprint (proxied by chip area) against its
*operational* footprint (proxied by power under a fixed-time use case, or by
energy under a fixed-work use case) using a single embodied-to-operational
weight ``alpha`` in [0, 1]. Comparing a ``test`` design against a ``ref``
baseline, the fixed-time NCF is::

    NCF(test, ref) = alpha * (A_test / A_ref) + (1 - alpha) * (P_test / P_ref)

where ``A`` is chip area and ``P`` is power consumption. ``NCF < 1`` means the
test design has a lower total carbon footprint than the reference (i.e. it is
more sustainable); ``NCF > 1`` means it is worse; ``NCF == 1`` means the two are
carbon-equal. The same expression holds for the fixed-work scenario with energy
substituted for power.

``alpha`` is the embodied-to-operational weight in [0, 1]. Paper scenarios:
``alpha = 0.8`` (embodied-dominated, e.g. mobile and datacenter hardware) and
``alpha = 0.2`` (operational-dominated, e.g. always-connected devices).
"""

from __future__ import annotations

import sys
import textwrap
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

# Embodied-to-operational weight scenarios from the FOCAL paper (Section 5).
ALPHA_EMBODIED_DOMINATED = 0.8     # e.g. mobile / datacenter hardware
ALPHA_OPERATIONAL_DOMINATED = 0.2  # e.g. always-connected devices

# The five FOCAL parameters that focal_sweep can vary.
SWEEPABLE_PARAMETERS = ("test_pwr", "ref_pwr", "test_area", "ref_area", "alpha_ref")

_PARAM_LABELS = {
    "test_pwr": "test power",
    "ref_pwr": "ref power",
    "test_area": "test area",
    "ref_area": "ref area",
    "alpha_ref": r"embodied-to-operational weight $\alpha$",
}


@dataclass
class FocalComparison:
    """FOCAL comparison of a ``test`` architecture against a ``ref`` baseline.

    Computed under the fixed-time scenario, i.e. power is the operational
    proxy. (Pass energy in place of power to evaluate the fixed-work scenario;
    the NCF expression is identical.)
    """

    ncf: float                 # normalized carbon footprint NCF(test, ref)
    embodied_ratio: float      # A_test / A_ref  (normalized embodied footprint)
    operational_ratio: float   # P_test / P_ref  (normalized operational footprint)
    alpha_ref: float           # embodied-to-operational weight used
    # Operating point echoed back for traceability.
    test_pwr: float
    ref_pwr: float
    test_area: float
    ref_area: float

    @property
    def more_sustainable(self) -> bool:
        """True if the test design has a strictly lower carbon footprint."""
        return self.ncf < 1.0

    @property
    def verdict(self) -> str:
        """Human-readable verdict for the test design relative to ref."""
        if self.ncf < 1.0:
            return "more sustainable"
        if self.ncf > 1.0:
            return "less sustainable"
        return "equally sustainable"

    @property
    def pct_change(self) -> float:
        """Percent change in carbon footprint of test vs ref.

        Negative means a reduction (more sustainable); e.g. -30.0 means the
        test design emits 30% less carbon than the reference.
        """
        return (self.ncf - 1.0) * 100.0

    @property
    def breakeven_alpha(self) -> float | None:
        """Embodied weight ``alpha`` at which test and ref are carbon-equal.

        Solves ``NCF(alpha) = 1`` for alpha at the current area and power
        ratios::

            alpha* = (1 - P_test/P_ref) / (A_test/A_ref - P_test/P_ref)

        Returns None when the embodied and operational ratios are equal (NCF
        does not depend on alpha, so there is no crossing). A value outside
        [0, 1] means the verdict never flips within the valid weight range,
        i.e. the test design is uniformly more or less sustainable.
        """
        if self.embodied_ratio == self.operational_ratio:
            return None
        return (1.0 - self.operational_ratio) / (self.embodied_ratio - self.operational_ratio)


def focal_compare(
    test_pwr: float,
    ref_pwr: float,
    test_area: float,
    ref_area: float,
    alpha_ref: float,
) -> FocalComparison:
    """Compare a ``test`` architecture against a ``ref`` baseline with FOCAL.

    Computes the Normalized Carbon Footprint (NCF) of the test design relative
    to the reference under the fixed-time scenario::

        NCF = alpha_ref * (test_area / ref_area)
              + (1 - alpha_ref) * (test_pwr / ref_pwr)

    ``NCF < 1`` means the test design has a lower total carbon footprint than
    the reference (more sustainable); ``NCF > 1`` means it is worse.

    Args:
        test_pwr: Power of the test design (operational proxy, numerator).
            Substitute energy to evaluate the fixed-work scenario instead.
        ref_pwr: Power of the reference design (operational proxy, denominator).
        test_area: Chip area of the test design (embodied proxy, numerator).
        ref_area: Chip area of the reference design (embodied proxy, denominator).
        alpha_ref: Embodied-to-operational weight in [0, 1] (the FOCAL
            ``alpha_E2O``). 0.8 = embodied-dominated, 0.2 = operational-dominated.

    Returns:
        A FocalComparison with the NCF, its embodied/operational components,
        and convenience verdicts.

    Raises:
        ValueError: if a denominator is non-positive, an input is negative, or
            ``alpha_ref`` is outside [0, 1].
    """
    if ref_pwr <= 0.0 or ref_area <= 0.0:
        raise ValueError("ref_pwr and ref_area must be positive (they are denominators)")
    if test_pwr < 0.0 or test_area < 0.0:
        raise ValueError("test_pwr and test_area must be non-negative")
    if not 0.0 <= alpha_ref <= 1.0:
        raise ValueError(f"alpha_ref must be in [0, 1], got {alpha_ref}")

    embodied_ratio = test_area / ref_area
    operational_ratio = test_pwr / ref_pwr
    ncf = alpha_ref * embodied_ratio + (1.0 - alpha_ref) * operational_ratio
    return FocalComparison(
        ncf=ncf,
        embodied_ratio=embodied_ratio,
        operational_ratio=operational_ratio,
        alpha_ref=alpha_ref,
        test_pwr=test_pwr,
        ref_pwr=ref_pwr,
        test_area=test_area,
        ref_area=ref_area,
    )


@dataclass
class FocalSweep:
    """NCF of a test/ref pair as one FOCAL parameter is swept over a range."""

    parameter: str                       # which of the five params was swept
    values: list[float]                  # swept parameter values
    ncf: list[float]                     # NCF at each swept value
    comparisons: list[FocalComparison]   # full comparison at each swept value
    breakevens: list[float]              # swept values where NCF crosses 1.0
    # Nominal operating point (the swept field holds its nominal value here).
    test_pwr: float
    ref_pwr: float
    test_area: float
    ref_area: float
    alpha_ref: float

    @property
    def crosses_unity(self) -> bool:
        """True if the verdict flips (NCF crosses 1) within the swept range."""
        return bool(self.breakevens)


@dataclass
class FocalBothRefs:
    """Two alpha sweeps comparing arch A and arch B, each taken as the reference.

    Holds the ``arch_a_ref`` sweep (A is the reference, so the test design is B)
    and the ``arch_b_ref`` sweep (B is the reference, so the test design is A).
    See ``sweep_both_arches_as_ref`` for why both directions matter.
    """

    arch_a_ref: FocalSweep   # arch A as reference, test design is arch B
    arch_b_ref: FocalSweep   # arch B as reference, test design is arch A


def focal_sweep(
    test_pwr: float,
    ref_pwr: float,
    test_area: float,
    ref_area: float,
    alpha_ref: float,
    *,
    sweep: str = "alpha_ref",
    values: Sequence[float] | None = None,
    num: int = 51,
    span: float = 2.0,
    plot_path: str | Path | None = None,
) -> FocalSweep:
    """Sweep one FOCAL parameter and report NCF across the range.

    Holds four of the five FOCAL parameters fixed at the values passed in and
    varies the fifth (``sweep``), recomputing the test-vs-ref NCF at each point.
    This mirrors the paper's sensitivity analyses: sweeping ``alpha_ref`` shows
    how the verdict shifts between operational- and embodied-dominated regimes,
    while sweeping an area or power term traces NCF against that dimension.

    Args:
        test_pwr: Nominal test power.
        ref_pwr: Nominal reference power.
        test_area: Nominal test area.
        ref_area: Nominal reference area.
        alpha_ref: Nominal embodied-to-operational weight in [0, 1].
        sweep: Name of the parameter to vary; one of ``SWEEPABLE_PARAMETERS``.
        values: Explicit values to sweep. If None, a default range is built:
            ``alpha_ref`` sweeps [0, 1]; any other parameter sweeps from
            ``nominal / span`` to ``nominal * span``.
        num: Number of points in the default range (ignored if ``values`` given).
        span: Multiplicative half-range for non-alpha default sweeps.
        plot_path: If given, save a PNG of NCF vs the swept parameter there
            (requires matplotlib, imported lazily).

    Returns:
        A FocalSweep with the swept values, the NCF at each, the per-point
        FocalComparison objects, and any NCF==1 break-even crossings.

    Raises:
        ValueError: if ``sweep`` is not a FOCAL parameter, or a default range
            cannot be built because the nominal swept value is non-positive.
    """
    if sweep not in SWEEPABLE_PARAMETERS:
        raise ValueError(f"sweep must be one of {SWEEPABLE_PARAMETERS}, got {sweep!r}")
    if sweep in ("ref_pwr", "ref_area"):
        _warn_ref_sweep(sweep)

    base = {
        "test_pwr": test_pwr,
        "ref_pwr": ref_pwr,
        "test_area": test_area,
        "ref_area": ref_area,
        "alpha_ref": alpha_ref,
    }

    if values is not None:
        swept_values = [float(v) for v in values]
    elif sweep == "alpha_ref":
        swept_values = _linspace(0.0, 1.0, num)
    else:
        nominal = base[sweep]
        if nominal <= 0.0:
            raise ValueError(
                f"cannot auto-range {sweep} from a non-positive nominal "
                f"({nominal}); pass explicit values=..."
            )
        swept_values = _linspace(nominal / span, nominal * span, num)

    comparisons: list[FocalComparison] = []
    for v in swept_values:
        params = dict(base)
        params[sweep] = v
        comparisons.append(focal_compare(**params))

    ncf = [c.ncf for c in comparisons]
    breakevens = _level_crossings(swept_values, ncf, 1.0)

    result = FocalSweep(
        parameter=sweep,
        values=swept_values,
        ncf=ncf,
        comparisons=comparisons,
        breakevens=breakevens,
        test_pwr=test_pwr,
        ref_pwr=ref_pwr,
        test_area=test_area,
        ref_area=ref_area,
        alpha_ref=alpha_ref,
    )
    if plot_path is not None:
        _plot_sweep(result, plot_path)
    return result


def sweep_both_arches_as_ref(
    arch_a_pwr: float,
    arch_a_area: float,
    arch_b_pwr: float,
    arch_b_area: float,
    *,
    values: Sequence[float] | None = None,
    num: int = 51,
    plot_path: str | Path | None = None,
) -> FocalBothRefs:
    """Sweep alpha comparing two architectures with *each* used as the reference.

    The NCF metric is not symmetric: ``NCF(X, Y) != 1 / NCF(Y, X)`` in general,
    because NCF is a weighted *sum* of the area and power ratios rather than a
    single ratio (in fact ``NCF(X, Y) * NCF(Y, X) >= 1``). The reference design
    is the denominator, so for the *same* alpha you can get a different NCF — and
    even a different verdict — depending on which architecture you pick as the
    reference. There is no privileged "baseline" here, so it is worth looking at
    both directions before concluding that one design is more sustainable.

    This runs two alpha sweeps over the same grid: one with arch A as the
    reference (test design is B) and one with arch B as the reference (test
    design is A). Compare the two curves at a given alpha to see the asymmetry.

    Args:
        arch_a_pwr: Power of architecture A.
        arch_a_area: Chip area of architecture A.
        arch_b_pwr: Power of architecture B.
        arch_b_area: Chip area of architecture B.
        values: Explicit alpha values to sweep. If None, sweeps [0, 1].
        num: Number of alpha points in the default range (ignored if ``values``).
        plot_path: If given, save a PNG overlaying both NCF curves vs alpha
            (requires matplotlib, imported lazily).

    Returns:
        A FocalBothRefs holding the ``arch_a_ref`` and ``arch_b_ref`` sweeps.
    """
    # Placeholder alpha_ref; it is overridden at every point of the alpha sweep.
    arch_a_ref = focal_sweep(
        test_pwr=arch_b_pwr, ref_pwr=arch_a_pwr,
        test_area=arch_b_area, ref_area=arch_a_area,
        alpha_ref=0.5, sweep="alpha_ref", values=values, num=num,
    )
    arch_b_ref = focal_sweep(
        test_pwr=arch_a_pwr, ref_pwr=arch_b_pwr,
        test_area=arch_a_area, ref_area=arch_b_area,
        alpha_ref=0.5, sweep="alpha_ref", values=values, num=num,
    )

    result = FocalBothRefs(arch_a_ref=arch_a_ref, arch_b_ref=arch_b_ref)
    if plot_path is not None:
        _plot_both_refs(result, plot_path)
    return result


def _warn_ref_sweep(sweep: str) -> None:
    """Loudly warn that sweeping a reference value at constant alpha_ref is dicey.

    Fires when ``focal_sweep`` is asked to vary ``ref_pwr`` or ``ref_area``:
    holding ``alpha_ref`` fixed across such a sweep is internally inconsistent,
    because the embodied-to-operational weight that actually applies to the
    reference design depends on its own power and area.
    """
    message = (
        "sweeping reference values with a constant alpha_ref yields difficult "
        "to understand results, since the true value of alpha_ref is a function "
        "of the ref_pwr and ref_area"
    )
    width = 76
    bar = "!" * width
    lines = ["", bar, "!!" + "  FOCAL WARNING  ".center(width - 4) + "!!", bar]
    for line in textwrap.wrap(f"You are sweeping {sweep!r}: {message}.", width - 6):
        lines.append("!! " + line.ljust(width - 6) + " !!")
    lines += [bar, ""]
    print("\n".join(lines), file=sys.stderr)
    warnings.warn(message, stacklevel=3)


def _linspace(lo: float, hi: float, num: int) -> list[float]:
    """Evenly spaced values from lo to hi inclusive (pure-Python linspace)."""
    if num < 1:
        return []
    if num == 1:
        return [lo]
    step = (hi - lo) / (num - 1)
    return [lo + step * i for i in range(num)]


def _level_crossings(xs: list[float], ys: list[float], level: float = 1.0) -> list[float]:
    """Find x where the piecewise-linear (xs, ys) curve crosses ``level``.

    Returns the x of every exact hit and every sign-change crossing (located by
    linear interpolation between the bracketing samples).
    """
    crossings: list[float] = []
    for i in range(len(xs) - 1):
        a = ys[i] - level
        b = ys[i + 1] - level
        if a == 0.0:
            crossings.append(xs[i])
        elif (a < 0.0 < b) or (b < 0.0 < a):
            t = a / (a - b)
            crossings.append(xs[i] + t * (xs[i + 1] - xs[i]))
    if ys and ys[-1] - level == 0.0:
        crossings.append(xs[-1])
    return crossings


def _plot_sweep(result: FocalSweep, plot_path: str | Path) -> None:
    """Save a PNG of NCF vs the swept parameter (matplotlib imported lazily)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(result.values, result.ncf, color="#2b6cb0", marker="o",
            markersize=3, linewidth=1.5, label="NCF(test, ref)")
    ax.axhline(1.0, color="grey", linestyle="--", linewidth=1.0,
               label="carbon-equal (NCF = 1)")

    ymin, ymax = ax.get_ylim()
    ax.axhspan(ymin, 1.0, color="#c6f6d5", alpha=0.3, zorder=0,
               label="test more sustainable")
    ax.set_ylim(ymin, ymax)

    for i, be in enumerate(result.breakevens):
        ax.axvline(be, color="#e53e3e", linestyle=":", linewidth=1.0,
                   label="break-even" if i == 0 else None)

    ax.set_xlabel(_PARAM_LABELS.get(result.parameter, result.parameter))
    ax.set_ylabel("normalized carbon footprint (NCF)")
    ax.set_title(f"FOCAL sweep over {result.parameter}")
    ax.grid(alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()

    out = Path(plot_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  FOCAL sweep plot -> {out}")


def _plot_both_refs(result: FocalBothRefs, plot_path: str | Path) -> None:
    """Overlay the two reference-choice NCF curves vs alpha (lazy matplotlib)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(result.arch_a_ref.values, result.arch_a_ref.ncf, color="#2b6cb0",
            marker="o", markersize=3, linewidth=1.5,
            label="A as ref  (NCF of B vs A)")
    ax.plot(result.arch_b_ref.values, result.arch_b_ref.ncf, color="#dd6b20",
            marker="s", markersize=3, linewidth=1.5,
            label="B as ref  (NCF of A vs B)")
    ax.axhline(1.0, color="grey", linestyle="--", linewidth=1.0,
               label="carbon-equal (NCF = 1)")

    ax.set_xlabel(_PARAM_LABELS["alpha_ref"])
    ax.set_ylabel("normalized carbon footprint (NCF)")
    ax.set_title("FOCAL: same alpha, each architecture as reference")
    ax.grid(alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()

    out = Path(plot_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  FOCAL both-refs plot -> {out}")
