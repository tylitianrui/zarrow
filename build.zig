const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_module = b.addModule("arrow_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const tests = b.addTest(.{
        .root_module = root_module,
    });

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run Arrow Zig unit tests");
    test_step.dependOn(&run_tests.step);
}
