import argparse
import sys


# `ray job` subcommands wrapped as verbatim pass-throughs (`stop` is custom).
RAY_JOB_PASSTHROUGH_COMMANDS = ("submit", "status", "logs", "list", "delete")


def main():
    # Dispatch pass-throughs before argparse: REMAINDER can't capture a leading
    # option-like token (e.g. `chia job submit --working-dir ...`), and this
    # also forwards `--help` to ray's own, complete help text.
    argv = sys.argv[1:]
    if len(argv) >= 2 and argv[0] == "job" and argv[1] in RAY_JOB_PASSTHROUGH_COMMANDS:
        from chia.cli.job import cmd_job_passthrough
        cmd_job_passthrough(argv[1], argv[2:])
        return

    parser = argparse.ArgumentParser(
        prog="chia",
        description="Chia cluster management for local/on-premise Ray clusters",
    )
    subparsers = parser.add_subparsers(dest="command")

    # chia up
    up_parser = subparsers.add_parser("up", help="Bring up a Chia/Ray cluster")
    up_parser.add_argument("config_file", help="Path to cluster YAML config")
    up_parser.add_argument("-y", "--yes", action="store_true",
                           help="Skip confirmation prompt")
    up_parser.add_argument("-v", "--verbose", action="store_true",
                           help="Enable verbose (DEBUG) logging")
    up_parser.add_argument("--dry-run", action="store_true",
                           help="Print plan without executing")
    up_parser.add_argument("--add", action="store_true",
                           help="Only add new nodes to an existing cluster (skip existing)")

    # chia down
    down_parser = subparsers.add_parser("down", help="Tear down a Chia/Ray cluster")
    down_parser.add_argument("config_file", help="Path to cluster YAML config")
    down_parser.add_argument("-y", "--yes", action="store_true",
                             help="Skip confirmation prompt")
    down_parser.add_argument("-v", "--verbose", action="store_true",
                             help="Enable verbose (DEBUG) logging")

    # chia viz
    viz_parser = subparsers.add_parser("viz", help="Visualize a Chia loop graph")
    viz_parser.add_argument("source_file", help="Path to a Python file with @ChiaFunction calls")
    viz_parser.add_argument("--func", default=None,
                            help="Orchestrator function name (auto-detected if omitted)")
    viz_parser.add_argument("--format", choices=["svg", "png", "pdf"], default="svg",
                            help="Output format (default: svg)")
    viz_parser.add_argument("--output-dir", default=None,
                            help="Output directory (default: same as source file)")

    # chia firesim-build
    fb_parser = subparsers.add_parser("firesim-build", help="Build an FPGA bitstream")
    fb_parser.add_argument("config_file", help="Path to cluster YAML config")
    fb_parser.add_argument("--recipe", required=True,
                           help="Build recipe name from config_build_recipes.yaml")
    fb_parser.add_argument("--instance-type", default="z1d.2xlarge",
                           help="EC2 instance type for build host (default: z1d.2xlarge)")
    fb_parser.add_argument("-v", "--verbose", action="store_true",
                           help="Enable verbose (DEBUG) logging")

    # chia firesim-run
    fr_parser = subparsers.add_parser("firesim-run", help="Run an FPGA simulation")
    fr_parser.add_argument("config_file", help="Path to cluster YAML config")
    fr_parser.add_argument("--hw-config", default=None,
                           help="Hardware config name (required unless --suite)")
    fr_parser.add_argument("--workload", default=None,
                           help="Workload name (required unless --suite)")
    fr_parser.add_argument("--agfi", default=None,
                           help="AGFI to flash (overrides hwdb lookup)")
    fr_parser.add_argument("--num-sims", type=int, default=1,
                           help="Number of simulations to run (default: 1)")
    fr_parser.add_argument("--instance-type", default="f2.12xlarge",
                           help="F2 instance type (default: f2.12xlarge)")
    fr_parser.add_argument("--plusargs", default="",
                           help="Plusargs to pass to the simulator")
    fr_parser.add_argument("--timeout", type=int, default=14400,
                           help="Simulation timeout in seconds (default: 14400)")
    fr_parser.add_argument("--suite", default=None,
                           help="Workload suite name (S3 prefix). Enables suite mode.")
    fr_parser.add_argument("--workloads", default=None,
                           help="Comma-separated workload names from suite (default: all)")
    fr_parser.add_argument("--parallelism", type=int, default=4,
                           help="Max concurrent simulations in suite mode (default: 4)")
    fr_parser.add_argument("--workload-bucket", default=None,
                           help="S3 bucket containing workload suite images (default: from config or firesim-chia-builds)")
    fr_parser.add_argument("-v", "--verbose", action="store_true",
                           help="Enable verbose (DEBUG) logging")

    # chia firesim-upload-workload
    fus_parser = subparsers.add_parser(
        "firesim-upload-workload", help="Upload FireMarshal workload images to S3")
    fus_parser.add_argument("config_file", nargs="?", default=None,
                            help="Path to cluster YAML config (optional if --s3-bucket given)")
    fus_parser.add_argument("--marshal-config", required=True,
                            help="Path to FireMarshal config JSON")
    fus_parser.add_argument("--images-dir", required=True,
                            help="Directory containing built FireMarshal images")
    fus_parser.add_argument("--suite-name", required=True,
                            help="Suite name (used as S3 prefix)")
    fus_parser.add_argument("--dest-bucket", default=None,
                            help="S3 bucket to upload to (default: firesim-chia-builds, or from config)")
    fus_parser.add_argument("--dataset", default="",
                            help="Dataset label (e.g. test, ref)")
    fus_parser.add_argument("-v", "--verbose", action="store_true",
                            help="Enable verbose (DEBUG) logging")

    # chia firesim-cleanup
    fc_parser = subparsers.add_parser("firesim-cleanup",
                                       help="Terminate orphaned chia EC2 instances")
    fc_parser.add_argument("config_file", help="Path to cluster YAML config")
    fc_parser.add_argument("-y", "--yes", action="store_true",
                           help="Skip confirmation prompt")
    fc_parser.add_argument("-v", "--verbose", action="store_true",
                           help="Enable verbose (DEBUG) logging")
    # chia job stop
    job_parser = subparsers.add_parser("job", help="Job management commands")
    job_sub = job_parser.add_subparsers(dest="job_command")
    job_stop_parser = job_sub.add_parser(
        "stop", help="Stop a Ray job (optionally kill tracked subprocesses first)")
    job_stop_parser.add_argument("job_id", help="Ray job ID to stop")
    job_stop_parser.add_argument(
        "--kill-tracked-pids", action="store_true",
        help="Kill tracked subprocesses (via the PID registry) before stopping the job")
    job_stop_parser.add_argument(
        "--grace-period", type=int, default=25,
        help="Seconds to wait for each tracked subprocess to exit after "
             "SIGTERM before escalating to SIGKILL "
             "(only used with --kill-tracked-pids; default: 25)")
    # chia job submit/status/logs/list/delete — registered here only so they
    # show up in `chia job --help`; real invocations are dispatched verbatim
    # to `ray job` before argparse runs (see top of main()).
    for name in RAY_JOB_PASSTHROUGH_COMMANDS:
        job_sub.add_parser(name, add_help=False,
                           help=f"Pass-through to `ray job {name}`")

    # chia viz-profile
    viz_profile_parser = subparsers.add_parser(
        "viz-profile", help="Visualize a profiler log as a dependency graph")
    viz_profile_parser.add_argument(
        "log_file", nargs="+",
        help="Path(s) to a ChiaProfileCollector JSONL log, or (for --format table) "
             "one or more files/directories; directories are searched recursively "
             "for ChiaProfileCollector.log files")
    viz_profile_parser.add_argument(
        "--format", choices=["svg", "png", "pdf", "html", "table"], default="svg",
        help="Output format (default: svg). 'table' aggregates exec_time_s per "
             "function and writes a CSV.")
    viz_profile_parser.add_argument(
        "--output-dir", default=None,
        help="Output directory (default: same as log file; ignored for --format table)")
    viz_profile_parser.add_argument(
        "--output", default=None,
        help="(--format table) Output CSV path. Default: stdout.")
    viz_profile_parser.add_argument(
        "--funcs", default=None,
        help="(--format table) Comma-separated function names, or path to a "
             "file with one name per line. Default: every function found.")
    viz_profile_parser.add_argument(
        "--run", type=int, default=None,
        help="Run index to visualize (default: last run)")
    viz_profile_parser.add_argument(
        "--gap-threshold", type=float, default=600.0,
        help="Timestamp gap (seconds) to segment runs (default: 600)")
    viz_profile_parser.add_argument(
        "--x-scale", type=float, default=0.5,
        help="Horizontal inches per second of wall-clock time (default: 0.5)")

    args = parser.parse_args()

    if args.command == "up":
        from chia.cli.up import cmd_up
        cmd_up(args)
    elif args.command == "down":
        from chia.cli.down import cmd_down
        cmd_down(args)
    elif args.command == "viz":
        from chia.cli.viz_cmd import cmd_viz
        cmd_viz(args)
    elif args.command == "firesim-build":
        from chia.cli.firesim_cmds import cmd_firesim_build
        cmd_firesim_build(args)
    elif args.command == "firesim-run":
        from chia.cli.firesim_cmds import cmd_firesim_run
        cmd_firesim_run(args)
    elif args.command == "firesim-upload-workload":
        from chia.cli.firesim_cmds import cmd_firesim_upload_workload
        cmd_firesim_upload_workload(args)
    elif args.command == "firesim-cleanup":
        from chia.cli.firesim_cmds import cmd_firesim_cleanup
        cmd_firesim_cleanup(args)
    elif args.command == "job":
        if args.job_command == "stop":
            from chia.cli.job import cmd_job_stop
            cmd_job_stop(args)
        else:
            job_parser.print_help()
            sys.exit(1)
    elif args.command == "viz-profile":
        from chia.cli.viz_profile_cmd import cmd_viz_profile
        cmd_viz_profile(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
