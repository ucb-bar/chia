#!/usr/bin/env python

# taken from chipyard, with bug fixes

#============================================================================
# - really simple script, which just prints out the numactl cmd to
#   prefix before your actual command. it determines this based on free
#   memory size attached to every node.
# - when you run this on a machine without `numactl`, the output is empty,
#   so `$(numa_prefix) <cmd> <args>` turns in to `<cmd> <args>`.
# - when the machine has `numactl` installed, regardless of the socket-count
#   on the machine, the resulting command is:
#   `numactl -m <socket> -C <core-id list> -- <cmd> <args>`
# - example output from `numactl -H` on a 2 socket machine:
#     available: 2 nodes (0-1)
#     node 0 cpus: 0 2 4 6 8 10 12 14 16 18 20 22
#     node 0 size: 131026 MB
#     node 0 free: 7934 MB
#     node 1 cpus: 1 3 5 7 9 11 13 15 17 19 21 23
#     node 1 size: 65536 MB
#     node 1 free: 429 MB
#     node distances:
#     node   0   1
#       0:  10  20
#       1:  20  10
#============================================================================

import subprocess
import re
import sys

def get_numa_prefix():
    which_proc = subprocess.Popen(["which", "numactl"], stdout=subprocess.PIPE)
    out, err = which_proc.communicate()

    if out != "":
        numactl_proc = subprocess.Popen(["numactl", "-H"], stdout=subprocess.PIPE)
        out, err = numactl_proc.communicate()
        lines = out.decode().split("\n")
        line_idx = 0

        head_line = lines[line_idx]
        line_idx += 1
        node_match = re.match(r"^ *available: +(\d+) nodes", head_line)
        if node_match:
            avail_nodes = node_match.group(1)
            best_node_id = ""
            best_cpus = ""
            best_free_size = 0

            # loop through available nodes, selecting the node with the most free mem
            for i in range(int(avail_nodes)):
                cpu_line      = lines[line_idx]
                # mem. size unused. skip and use mem. free
                mem_free_line = lines[line_idx + 2]
                line_idx += 3

                cpu_match = re.match(r"^ *node (\d+) cpus: (\d.*\d)$", cpu_line)
                if cpu_match:
                    node_id = cpu_match.group(1)
                    cpus = cpu_match.group(2).replace(" ", ",")

                    mem_free_match = re.match(r"^ *node " + node_id + " free: (\d+) \S+$", mem_free_line)
                    if mem_free_match:
                        free_size = mem_free_match.group(1)
                        if int(free_size) > int(best_free_size):
                            best_node_id = node_id
                            best_cpus = cpus
                            best_free_size = free_size
                    else:
                        raise Exception("[ERROR] Malformed mem free line: " + mem_free_line)

                else:
                    raise Exception("[ERROR] Malformed cpus line: " + cpu_line)

            return "numactl -m " + best_node_id + " -C " + best_cpus + " --" 
        else:
            raise Exception("[ERROR] Malformed head line: " + head_line)

if __name__ == "__main__":
    try:
        prefix = get_numa_prefix()
        sys.stdout.write(prefix)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)