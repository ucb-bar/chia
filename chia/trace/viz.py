"""Static analysis of Chia loop files to produce flow graphs.

Parses a Python file with AST to find @ChiaFunction-decorated functions,
.chia_remote() dispatches, get() calls, and control flow (if/for), then
renders the node graph with graphviz.

Two-phase approach:
  Phase 1 — Data flow: flat scan of all dispatch/get pairs, ordered by line
            number, connected by variable dependencies.
  Phase 2 — Decisions: find if-statements between consecutive data-flow nodes
            that test a get-result variable, and insert decision diamonds.
"""

import ast
from dataclasses import dataclass, field
from pathlib import Path

import graphviz


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class ChiaNode:
    name: str
    resources: dict[str, float] = field(default_factory=dict)
    return_type: str = ""
    lineno: int = 0


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _extract_resources(decorator: ast.Call) -> dict[str, float]:
    for kw in decorator.keywords:
        if kw.arg == "resources" and isinstance(kw.value, ast.Dict):
            res = {}
            for k, v in zip(kw.value.keys, kw.value.values):
                if isinstance(k, ast.Constant) and isinstance(v, ast.Constant):
                    res[k.value] = v.value
            return res
    return {}


def extract_chia_nodes(tree: ast.Module) -> dict[str, ChiaNode]:
    """Find @ChiaFunction and @ray.remote decorated functions."""
    nodes = {}
    for item in ast.walk(tree):
        if not isinstance(item, ast.FunctionDef):
            continue
        for dec in item.decorator_list:
            # @ChiaFunction(...) or @ray.remote(...)
            if isinstance(dec, ast.Call):
                fname = dec.func
                if isinstance(fname, ast.Name) and fname.id == "ChiaFunction":
                    ret = ast.unparse(item.returns) if item.returns else ""
                    nodes[item.name] = ChiaNode(
                        name=item.name,
                        resources=_extract_resources(dec),
                        return_type=ret,
                        lineno=item.lineno,
                    )
                elif isinstance(fname, ast.Attribute) and fname.attr == "remote":
                    ret = ast.unparse(item.returns) if item.returns else ""
                    nodes[item.name] = ChiaNode(
                        name=item.name,
                        resources=_extract_resources(dec),
                        return_type=ret,
                        lineno=item.lineno,
                    )
            # @ChiaFunction or @ray.remote (no parens)
            elif isinstance(dec, ast.Name) and dec.id == "ChiaFunction":
                ret = ast.unparse(item.returns) if item.returns else ""
                nodes[item.name] = ChiaNode(
                    name=item.name, return_type=ret, lineno=item.lineno,
                )
            elif isinstance(dec, ast.Attribute) and dec.attr == "remote":
                ret = ast.unparse(item.returns) if item.returns else ""
                nodes[item.name] = ChiaNode(
                    name=item.name, return_type=ret, lineno=item.lineno,
                )
    return nodes


def find_orchestrator(tree: ast.Module, name: str | None = None) -> ast.FunctionDef | None:
    candidates = []
    for item in ast.iter_child_nodes(tree):
        if not isinstance(item, ast.FunctionDef):
            continue
        if name and item.name == name:
            return item
        count = sum(1 for n in ast.walk(item) if _is_chia_remote(n))
        if count > 0:
            candidates.append((count, item))
    if candidates:
        candidates.sort(key=lambda x: -x[0])
        return candidates[0][1]
    return None


def _is_chia_remote(node) -> str | None:
    """Detect foo.chia_remote(...) or foo.remote(...) dispatch calls."""
    if not isinstance(node, ast.Call):
        return None
    f = node.func
    if isinstance(f, ast.Attribute) and f.attr in ("chia_remote", "remote"):
        if isinstance(f.value, ast.Name):
            return f.value.id
    return None


def _is_get_call(node) -> str | None:
    """Detect get(ref) or ray.get(ref) calls."""
    if not isinstance(node, ast.Call):
        return None
    f = node.func
    # get(ref)
    if isinstance(f, ast.Name) and f.id == "get" and node.args:
        if isinstance(node.args[0], ast.Name):
            return node.args[0].id
    # ray.get(ref) or ray.get(foo.remote(...))
    if isinstance(f, ast.Attribute) and f.attr == "get":
        if isinstance(f.value, ast.Name) and f.value.id == "ray":
            if node.args:
                arg = node.args[0]
                if isinstance(arg, ast.Name):
                    return arg.id
                # Inline: ray.get(foo.remote(...)) — extract the dispatch
                fname = _is_chia_remote(arg)
                if fname:
                    return f"__inline_{fname}"
    return None


def _assign_target(stmt) -> str:
    if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
        t = stmt.targets[0]
        if isinstance(t, ast.Name):
            return t.id
        if isinstance(t, ast.Tuple):
            return ", ".join(ast.unparse(e) for e in t.elts)
    return ""


def _find_helper_dispatches(tree: ast.Module, exclude: str) -> dict[str, list[str]]:
    helpers = {}
    for item in ast.iter_child_nodes(tree):
        if not isinstance(item, ast.FunctionDef) or item.name == exclude:
            continue
        for node in ast.walk(item):
            fname = _is_chia_remote(node)
            if fname:
                helpers.setdefault(item.name, []).append(fname)
    return helpers


# ---------------------------------------------------------------------------
# Phase 1: Flat data-flow extraction
# ---------------------------------------------------------------------------

@dataclass
class DispatchSite:
    func_name: str
    ref_var: str
    arg_exprs: list[str]
    lineno: int


@dataclass
class GetSite:
    ref_var: str
    result_var: str
    lineno: int


@dataclass
class HelperCallSite:
    helper_name: str
    dispatched_funcs: list[str]
    arg_exprs: list[str]
    result_var: str
    lineno: int


def _flat_scan(func_def: ast.FunctionDef, helper_dispatches: dict[str, list[str]]):
    """Flat scan: find all dispatch/get/helper sites by line number."""
    dispatches = []
    gets = []
    helpers = []

    for node in ast.walk(func_def):
        if not isinstance(node, ast.Assign):
            continue
        val = node.value
        if val is None:
            continue

        fname = _is_chia_remote(val)
        if fname:
            args = [ast.unparse(a) for a in val.args]
            args += [f"{kw.arg}={ast.unparse(kw.value)}" for kw in val.keywords]
            dispatches.append(DispatchSite(fname, _assign_target(node), args, node.lineno))
            continue

        ref_var = _is_get_call(val)
        if ref_var:
            if ref_var.startswith("__inline_"):
                # ray.get(foo.remote(...)) — combined dispatch+get
                inline_func = ref_var[len("__inline_"):]
                inner_call = val.args[0]  # foo.remote(...)
                args = [ast.unparse(a) for a in inner_call.args]
                args += [f"{kw.arg}={ast.unparse(kw.value)}" for kw in inner_call.keywords]
                synthetic_ref = f"_ref_{inline_func}_{node.lineno}"
                dispatches.append(DispatchSite(inline_func, synthetic_ref, args, node.lineno))
                gets.append(GetSite(synthetic_ref, _assign_target(node), node.lineno))
            else:
                gets.append(GetSite(ref_var, _assign_target(node), node.lineno))
            continue

        if isinstance(val, ast.Call) and isinstance(val.func, ast.Name):
            if val.func.id in helper_dispatches:
                args = [ast.unparse(a) for a in val.args]
                helpers.append(HelperCallSite(
                    val.func.id, helper_dispatches[val.func.id],
                    args, _assign_target(node), node.lineno,
                ))

    return dispatches, gets, helpers


def _build_data_flow(dispatches, gets, helpers, chia_nodes, func_def):
    """Build ordered list of (func_name, lineno) and data-flow edges."""
    # Map ref_var → func_name
    ref_to_func = {d.ref_var: d.func_name for d in dispatches}
    # Map ref_var → result_var via gets
    # Map result_var → producer func
    var_to_producer = {}
    for g in gets:
        func = ref_to_func.get(g.ref_var)
        if func:
            var_to_producer[g.result_var] = func

    for h in helpers:
        if h.result_var:
            for f in h.dispatched_funcs:
                var_to_producer[h.result_var] = f

    # Propagate: if `x = [... for v in results ...]` or `x = f(results)`,
    # where `results` is in var_to_producer, then `x` derives from the same
    # producer. This catches `failed_tests = [vr for vr in verilator_results ...]`.
    for node in ast.walk(func_def):
        if not isinstance(node, ast.Assign):
            continue
        target = _assign_target(node)
        if not target or target in var_to_producer:
            continue
        # Walk AST names in RHS to find producer references (avoids
        # false substring matches like "success" in "vr.success").
        for child in ast.walk(node.value):
            if isinstance(child, ast.Name) and child.id in var_to_producer:
                var_to_producer[target] = var_to_producer[child.id]
                break

    # Build ordered node list (by first appearance line number)
    node_order = []  # (func_name, lineno)
    seen = set()
    # Merge dispatches and helpers, sorted by line
    all_sites = []
    for d in dispatches:
        all_sites.append((d.lineno, d.func_name, d.arg_exprs))
    for h in helpers:
        for f in h.dispatched_funcs:
            all_sites.append((h.lineno, f, h.arg_exprs))
    all_sites.sort(key=lambda x: x[0])

    for lineno, func_name, _ in all_sites:
        if func_name not in seen:
            node_order.append((func_name, lineno))
            seen.add(func_name)

    # Build data-flow edges
    edges = []  # (src_func, dst_func, label)
    for lineno, func_name, arg_exprs in all_sites:
        for arg in arg_exprs:
            for var, producer in var_to_producer.items():
                if var in arg and producer != func_name:
                    edges.append((producer, func_name,
                                  chia_nodes[producer].return_type if producer in chia_nodes else ""))

    # Deduplicate edges
    seen_edges = set()
    unique_edges = []
    for src, dst, label in edges:
        if (src, dst) not in seen_edges:
            seen_edges.add((src, dst))
            unique_edges.append((src, dst, label))

    # Fill gaps: for consecutive nodes in line order that have no connecting
    # edge, add a sequential edge. This handles cases where data flows through
    # local processing (e.g. parse_tma_counters) that isn't a ChiaFunction.
    has_incoming = {dst for _, dst, _ in unique_edges}
    for i in range(1, len(node_order)):
        func = node_order[i][0]
        if func not in has_incoming:
            prev_func = node_order[i - 1][0]
            unique_edges.append((prev_func, func, ""))
            has_incoming.add(func)

    # Count dispatches per func
    counts = {}
    for d in dispatches:
        counts[d.func_name] = counts.get(d.func_name, 0) + 1
    for h in helpers:
        for f in h.dispatched_funcs:
            counts[f] = counts.get(f, 0) + 1

    return node_order, unique_edges, var_to_producer, counts


# ---------------------------------------------------------------------------
# Phase 2: Decision detection
# ---------------------------------------------------------------------------

@dataclass
class Decision:
    node_id: str
    label: str
    after_func: str         # the function whose result is being checked
    fail_targets: list[str] # funcs dispatched in the fail branch
    has_retry: bool         # continue in a for loop
    has_exit: bool          # break
    continuation_targets: list[str] = field(default_factory=list)  # funcs after the if (fail path for break-ifs)
    retry_target: str = ""  # first dispatch in enclosing for-loop (for continue back-edges)


def _find_decisions(func_def: ast.FunctionDef, var_to_producer: dict,
                    helper_dispatches: dict) -> list[Decision]:
    """Find if-statements that check get() results and branch to chia calls."""
    decisions = []
    counter = [0]

    def _first_dispatch_in(stmts) -> str | None:
        """Find the first chia dispatch, recursing into loop bodies."""
        for stmt in stmts:
            if isinstance(stmt, ast.Assign) and stmt.value is not None:
                fname = _is_chia_remote(stmt.value)
                if fname:
                    return fname
                if isinstance(stmt.value, ast.Call) and isinstance(stmt.value.func, ast.Name):
                    if stmt.value.func.id in helper_dispatches:
                        funcs = helper_dispatches[stmt.value.func.id]
                        if funcs:
                            return funcs[0]
            if isinstance(stmt, (ast.For, ast.While)):
                found = _first_dispatch_in(stmt.body)
                if found:
                    return found
        return None

    def _scan_stmts(stmts, loop_first_func=None):
        for i, stmt in enumerate(stmts):
            if isinstance(stmt, ast.If):
                _check_if(stmt, stmts[i + 1:], loop_first_func)
            if isinstance(stmt, (ast.For, ast.While)):
                first = _first_dispatch_in(stmt.body)
                _scan_stmts(stmt.body, first or loop_first_func)

    def _refs_var(test_node: ast.AST) -> str | None:
        """Check if an if-test references a variable from var_to_producer."""
        for node in ast.walk(test_node):
            if isinstance(node, ast.Name) and node.id in var_to_producer:
                return node.id
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id in var_to_producer:
                    return node.value.id
        return None

    def _chia_funcs_in(stmts) -> list[str]:
        funcs = []
        for node in ast.walk(ast.Module(body=stmts, type_ignores=[])):
            fname = _is_chia_remote(node)
            if fname:
                funcs.append(fname)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in helper_dispatches:
                    funcs.extend(helper_dispatches[node.func.id])
        return funcs

    def _has_continue(stmts) -> bool:
        for node in ast.walk(ast.Module(body=stmts, type_ignores=[])):
            if isinstance(node, ast.Continue):
                return True
        return False

    def _has_break(stmts) -> bool:
        for node in ast.walk(ast.Module(body=stmts, type_ignores=[])):
            if isinstance(node, ast.Break):
                return True
        return False

    def _check_if(stmt: ast.If, following_stmts=None, loop_first_func=None):
        var = _refs_var(stmt.test)
        if var is None:
            _scan_stmts(stmt.body, loop_first_func)
            if stmt.orelse:
                _scan_stmts(stmt.orelse, loop_first_func)
            return

        producer = var_to_producer[var]
        cond = ast.unparse(stmt.test)

        body_funcs = _chia_funcs_in(stmt.body)
        body_cont = _has_continue(stmt.body)
        body_brk = _has_break(stmt.body)

        else_funcs = _chia_funcs_in(stmt.orelse)

        is_duplicate_guard = (body_brk and not body_funcs and not body_cont
                              and any(d.after_func == producer for d in decisions))
        is_meaningful = body_funcs or body_cont or (body_brk and not is_duplicate_guard)

        # For `if not X: break`, the continuation (code after the if) is the
        # fail path. Find chia funcs in following statements of the same block.
        cont_targets = []
        if body_brk and not body_funcs and following_stmts:
            cont_targets = _chia_funcs_in(following_stmts)

        # Skip decisions with no fail targets and no continuation targets —
        # these are bare "continue" statements (e.g. "build still failing,
        # skip to next iteration") better shown as the while-loop back-edge.
        is_bare_retry = (not body_funcs and not cont_targets and body_cont)

        if is_meaningful and not is_bare_retry:
            counter[0] += 1
            label = _simplify_condition(cond)
            decisions.append(Decision(
                node_id=f"decision_{counter[0]}",
                label=label,
                after_func=producer,
                fail_targets=body_funcs,
                has_retry=body_cont,
                has_exit=body_brk and not body_funcs,
                continuation_targets=cont_targets,
                retry_target=loop_first_func or producer,
            ))

        # Recurse into body/else for nested decisions
        _scan_stmts(stmt.body)
        if stmt.orelse:
            _scan_stmts(stmt.orelse)

    _scan_stmts(func_def.body)
    return decisions


def _simplify_condition(cond: str) -> str:
    # TODO: These are hardcoded heuristics mapping known variable/attribute names
    # to human-readable labels. This should be generalized — e.g. by extracting
    # the tested variable name and the comparison operator to auto-generate a
    # label like "var OK?" or "var == 0?", rather than pattern-matching keywords.
    c = cond.lower()
    if "returncode" in c:
        return "build\nOK?"
    if "failed_test" in c:
        return "tests\npass?"
    if "changed_file" in c:
        return "new\nchanges?"
    if "success" in c:
        return "success?"
    # General case: truncate the raw condition into a readable diamond label
    short = cond[:25] + "..." if len(cond) > 25 else cond
    return short + "?"


# ---------------------------------------------------------------------------
# Phase 3: Assemble graph
# ---------------------------------------------------------------------------

def _has_while_true(func_def: ast.FunctionDef) -> bool:
    """Check if the orchestrator body contains a while-True loop."""
    for stmt in ast.walk(func_def):
        if isinstance(stmt, ast.While):
            test = stmt.test
            if isinstance(test, ast.Constant) and test.value is True:
                return True
            if isinstance(test, ast.NameConstant) and getattr(test, 'value', None) is True:
                return True
    return False


def _build_graph(node_order, data_edges, decisions, chia_nodes, counts, func_def=None):
    """Combine data flow edges and decisions into a final edge list."""
    # Start with data-flow edges
    # We'll insert decision diamonds into edges where applicable

    # Map: after_func → list of decisions
    func_decisions = {}
    for d in decisions:
        func_decisions.setdefault(d.after_func, []).append(d)

    final_edges = []  # (src, dst, label, style, color)
    decision_nodes = []  # (id, label)

    # Track which edges have been split by decisions
    used_data_edges = set()  # (src, dst) that have been replaced

    for d in decisions:
        decision_nodes.append((d.node_id, d.label))

        # Insert decision between after_func and the next node in data flow.
        # Skip edges to fail/continuation targets — those are error paths.
        skip_targets = set(d.fail_targets) | set(d.continuation_targets)
        next_func = None
        next_label = ""
        for src, dst, label in data_edges:
            if src == d.after_func and (src, dst) not in used_data_edges and dst not in skip_targets:
                next_func = dst
                next_label = label
                used_data_edges.add((src, dst))
                break
        # Mark error-path edges as used too (covered by decision branches)
        for src, dst, label in data_edges:
            if src == d.after_func and dst in skip_targets:
                used_data_edges.add((src, dst))

        # Edge: after_func → decision
        ret = chia_nodes[d.after_func].return_type if d.after_func in chia_nodes else ""
        final_edges.append((d.after_func, d.node_id, ret, "", ""))

        # Fail branch edges (chia funcs called in the if-body)
        for target in d.fail_targets:
            final_edges.append((d.node_id, target, "fail", "dashed", "red"))

        # Continuation targets: for `if not X: break`, the code after the if
        # is the fail/retry path. Connect decision → continuation funcs.
        if d.has_exit and d.continuation_targets:
            for target in set(d.continuation_targets):
                if target not in d.fail_targets:
                    final_edges.append((d.node_id, target, "fail", "dashed", "red"))

        # Retry back-edge: from fail target back to top of enclosing for-loop
        retry_to = d.retry_target or d.after_func
        if d.has_retry and d.fail_targets:
            for target in set(d.fail_targets):
                final_edges.append((target, retry_to,
                                    "retry", "dashed", "red"))
        # Also retry from continuation targets
        if d.has_exit and d.continuation_targets:
            for target in set(d.continuation_targets):
                final_edges.append((target, retry_to,
                                    "retry", "dashed", "red"))

        # Pass edge to next node
        if next_func:
            final_edges.append((d.node_id, next_func, "pass", "", ""))

        # Exit/stop
        if d.has_exit and "changes" in d.label:
            final_edges.append((d.node_id, "__stop__", "no", "", ""))

    # Add remaining data edges (not replaced by decisions)
    for src, dst, label in data_edges:
        if (src, dst) not in used_data_edges:
            final_edges.append((src, dst, label, "", ""))

    # Exit/stop for "new changes?" decisions (old loop style)
    for d in decisions:
        if "changes" in d.label and node_order:
            first_func = node_order[0][0]
            final_edges = [
                e for e in final_edges
                if not (e[0] == d.node_id and e[2] == "pass")
            ]
            final_edges.append((d.node_id, first_func,
                                "yes (next iteration)", "dashed", "blue"))

    # While-True loop-back: add edge from last node to first node
    if func_def is not None and _has_while_true(func_def) and len(node_order) >= 2:
        last_func = node_order[-1][0]
        first_func = node_order[0][0]
        # Only add if there isn't already a loop-back from a decision
        has_loop_back = any(e[1] == first_func and "iteration" in (e[2] or "")
                           for e in final_edges)
        if not has_loop_back:
            final_edges.append((last_func, first_func,
                                "next iteration", "dashed", "blue"))

    # Deduplicate
    seen = set()
    deduped = []
    for edge in final_edges:
        key = (edge[0], edge[1])
        if key not in seen:
            seen.add(key)
            deduped.append(edge)

    return deduped, decision_nodes


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_RESOURCE_COLORS = {
    "chipyard": "#A8D8EA",
    "verilator_run": "#A8E6CF",
    "llm": "#FFE0AC",
}


def _resource_type(resources: dict) -> str:
    for key in resources:
        if key in _RESOURCE_COLORS:
            return key
    return next(iter(resources), "default")


def _resource_color(resources: dict) -> str:
    return _RESOURCE_COLORS.get(_resource_type(resources), "#E8E8E8")


def render_flow(
    source_path: str,
    output_path: str,
    fmt: str = "svg",
    orchestrator_name: str | None = None,
):
    source = Path(source_path).read_text()
    tree = ast.parse(source, filename=source_path)

    chia_nodes = extract_chia_nodes(tree)
    orchestrator = find_orchestrator(tree, orchestrator_name)
    if orchestrator is None:
        print(f"No orchestrator function found in {source_path}")
        return

    helper_dispatches = _find_helper_dispatches(tree, orchestrator.name)

    # Phase 1: data flow
    dispatches, gets, helpers = _flat_scan(orchestrator, helper_dispatches)
    node_order, data_edges, var_to_producer, counts = _build_data_flow(
        dispatches, gets, helpers, chia_nodes, orchestrator)

    # Phase 2: decisions
    decisions = _find_decisions(orchestrator, var_to_producer, helper_dispatches)

    # Phase 3: assemble
    final_edges, decision_nodes = _build_graph(
        node_order, data_edges, decisions, chia_nodes, counts, func_def=orchestrator)

    # TODO: Add explicit entry and exit point nodes to the graph (e.g. "START"
    # oval before the first node, "STOP"/"KeyboardInterrupt" oval after the last).
    # Entry should show pre-loop setup (loading binaries, deploying MCP tools).
    # Exit should distinguish between graceful stop and error termination.

    # --- Graphviz ---
    dot = graphviz.Digraph(format=fmt)
    dot.attr(
        rankdir="TB", fontname="Helvetica",
        label=f"Chia Loop: {orchestrator.name}()\n{Path(source_path).name}",
        labelloc="t", fontsize="16",
    )
    dot.attr("node", fontname="Helvetica", fontsize="11")
    dot.attr("edge", fontname="Helvetica", fontsize="9")

    # Collect used funcs
    used_funcs = set()
    for src, dst, *_ in final_edges:
        if src in chia_nodes:
            used_funcs.add(src)
        if dst in chia_nodes:
            used_funcs.add(dst)

    # ChiaFunction nodes
    for name in used_funcs:
        cn = chia_nodes[name]
        rtype = _resource_type(cn.resources)
        color = _resource_color(cn.resources)
        parts = [f"<<B>{name}</B>"]
        if rtype != "default":
            parts.append(f"<BR/><FONT POINT-SIZE='9'>[{rtype}]</FONT>")
        if cn.return_type:
            parts.append(f"<BR/><FONT POINT-SIZE='9'>-&gt; {cn.return_type}</FONT>")
        count = counts.get(name, 1)
        if count > 1:
            parts.append(f"<BR/><FONT POINT-SIZE='9'>(x{count})</FONT>")
        dot.node(name, label="".join(parts) + ">",
                 shape="box", style="filled,rounded", fillcolor=color)

    # Decision diamonds
    for node_id, label in decision_nodes:
        dot.node(node_id, label=label, shape="diamond", style="filled",
                 fillcolor="#F0F0F0", fontsize="9", width="0.8", height="0.6")

    # Stop node (if referenced)
    if any(dst == "__stop__" for _, dst, *_ in final_edges):
        dot.node("__stop__", label="STOP", shape="oval", style="filled",
                 fillcolor="#FFB3B3")

    # Edges — back-edges (retry, loop-back) get constraint=false so they
    # don't pull nodes upward in the layout
    for src, dst, label, style, color in final_edges:
        kwargs = {}
        if style:
            kwargs["style"] = style
        if color:
            kwargs["color"] = color
        if style == "dashed":
            kwargs["constraint"] = "false"
        dot.edge(src, dst, label=label, **kwargs)

    # Legend
    resource_types_used = {_resource_type(chia_nodes[n].resources) for n in used_funcs}
    with dot.subgraph(name="cluster_legend") as lg:
        lg.attr(label="Legend", style="solid", fontsize="10", color="gray80")
        prev = None
        for i, (rtype, color) in enumerate(_RESOURCE_COLORS.items()):
            if rtype in resource_types_used:
                nid = f"_leg_{i}"
                lg.node(nid, label=f"{rtype} node", shape="box",
                        style="filled,rounded", fillcolor=color, fontsize="9")
                if prev:
                    lg.edge(prev, nid, style="invis")
                prev = nid

    out = str(output_path)
    if out.endswith(f".{fmt}"):
        out = out[: -(len(fmt) + 1)]
    dot.render(out, cleanup=True)
    print(f"Flow graph written to {out}.{fmt}")
