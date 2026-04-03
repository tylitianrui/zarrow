const std = @import("std");

// Configure the arrow-zig package as a reusable module plus a dedicated test step.
pub fn build(b: *std.Build) void {
    // Allow downstream consumers and tests to select their own target and optimization mode.
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Expose arrow-zig as a reusable Zig module for downstream dependencies.
    _ = b.addModule("arrow_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Tests still build a runnable artifact, but the package itself does not.
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Wire the test artifact into a named build step for `zig build test`.
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run Arrow Zig unit tests");
    test_step.dependOn(&run_tests.step);
}
