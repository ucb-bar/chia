"""Live-cluster smoke test for the chia DispatchProxy (chia/base/dispatch_proxy.py).

Reproduces the vext synth_boom wedge in miniature: a task on a reverse-tunneled
EC2 node (cacti resource) nests a chia_remote that demands a LAN-only resource
(chipyard). Without the proxy the nested dispatch sits in
PENDING_NODE_ASSIGNMENT forever (the EC2 owner cannot reach LAN raylet ports);
with the proxy it completes.

Run (from the repo root, against a cluster with VLSI/cacti EC2 nodes and
chipyard LAN nodes up):

    ray job submit --address IP:6379 --working-dir . -- \
        python -u chia/base/test/test_dispatch_proxy_live.py
"""
import os
import socket

import ray

from chia.base.ChiaFunction import ChiaFunction, get


@ChiaFunction(resources={"chipyard": 0.1})
def _lan_task(x):
    return f"lan_task on {socket.gethostname()} (x={x})"


@ChiaFunction(resources={"cacti": 0.1})
def _ec2_task():
    tunneled = os.environ.get("CHIA_TOOL_RELAY_HOST")
    # This nested dispatch is the wedge case: it hangs forever without the proxy.
    inner = get(_lan_task.chia_remote(42))
    inner_opt = get(_lan_task.options(num_cpus=1).chia_remote(43))
    return {"ec2_host": socket.gethostname(), "tunneled_env": tunneled,
            "inner": inner, "inner_opt": inner_opt}


def main():
    ray.init(address="auto")
    print("dispatching ec2_task (cacti) which nests lan_task (chipyard)...")
    res = get(_ec2_task.chia_remote())
    print("RESULT:", res)
    assert res["tunneled_env"], "outer task did not run on a tunneled node"
    assert "lan_task on" in res["inner"], res
    assert "lan_task on" in res["inner_opt"], res
    proxies = [a for a in ray.util.list_named_actors(all_namespaces=True)
               if "chia_dispatch_proxy" in str(a)]
    print("proxy actors:", proxies)
    assert proxies, "no DispatchProxy actor was created"
    # The proxy MUST be on the head raylet (the only raylet whose worker ports
    # are pinned + tunneled + DNAT'd from EC2). node:<head_ip> is ambiguous on
    # hosts with co-located --net=host container raylets — hence this check.
    head_node_id = next(n["NodeID"] for n in ray.nodes()
                        if "node:__internal_head__" in n.get("Resources", {}))
    from ray.util.state import list_actors
    my_job = ray.get_runtime_context().get_job_id()
    mine = [a for a in list_actors(filters=[("class_name", "=", "DispatchProxy"),
                                            ("state", "=", "ALIVE")])
            if a.name == f"chia_dispatch_proxy_{my_job}"]
    assert mine, "this job's DispatchProxy not found alive"
    for a in mine:
        print(f"proxy {a.name}: node {a.node_id[:16]} "
              f"({'HEAD' if a.node_id == head_node_id else 'NOT HEAD'})")
        assert a.node_id == head_node_id, \
            f"proxy {a.name} landed off-head on node {a.node_id}"
    # Clean up this job's detached proxy so the test leaves nothing behind.
    for a in proxies:
        try:
            ray.kill(ray.get_actor(a["name"], namespace=a["namespace"]))
        except Exception as e:  # noqa: BLE001
            print(f"cleanup of {a} failed: {e}")
    print("PROXY SMOKE TEST PASS")


if __name__ == "__main__":
    main()
