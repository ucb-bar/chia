# sleep stands in for a real compile, so a cache hit is visible in the timing.

genrule(
    name = "core",
    srcs = ["core.txt"],
    outs = ["core.out"],
    cmd = "sleep 3; cat $(location core.txt) > $@",
    visibility = ["//visibility:public"],
)

genrule(
    name = "util",
    srcs = ["util.txt"],
    outs = ["util.out"],
    cmd = "sleep 3; cat $(location util.txt) > $@",
    visibility = ["//visibility:public"],
)
