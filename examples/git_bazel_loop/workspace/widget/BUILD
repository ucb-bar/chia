load("@rules_shell//shell:sh_test.bzl", "sh_test")

genrule(
    name = "widget",
    srcs = ["tuning.txt"],
    outs = ["widget.out"],
    # sleep stands in for a real build, so a replayed generation is visible.
    cmd = "sleep 2; echo widget tuned=$$(cat $(location tuning.txt)) > $@",
    visibility = ["//visibility:public"],
)

sh_test(
    name = "widget_test",
    srcs = ["widget_test.sh"],
    data = [":widget"],
)
