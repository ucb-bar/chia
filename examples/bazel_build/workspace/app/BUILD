load("@rules_shell//shell:sh_test.bzl", "sh_test")

genrule(
    name = "bundle",
    srcs = ["//lib:core", "//lib:util"],
    outs = ["bundle.out"],
    cmd = "sleep 3; cat $(SRCS) > $@",
)

sh_test(
    name = "bundle_test",
    srcs = ["bundle_test.sh"],
    data = [":bundle"],
)
