const std = @import("std");
const builtin = @import("builtin");

fn hasAnyLibrary(candidates: []const []const u8) bool {
    for (candidates) |name| {
        var lib = std.DynLib.open(name) catch continue;
        lib.close();
        return true;
    }
    return false;
}

fn printCandidates(name: []const u8, candidates: []const []const u8) void {
    std.debug.print("  - {s}: ", .{name});
    for (candidates, 0..) |candidate, i| {
        if (i != 0) std.debug.print(", ", .{});
        std.debug.print("\"{s}\"", .{candidate});
    }
    std.debug.print("\n", .{});
}

pub fn main() !void {
    const zstd_default_candidates = switch (builtin.os.tag) {
        .macos => &[_][]const u8{
            "libzstd.dylib",
            "libzstd.1.dylib",
            "/opt/homebrew/lib/libzstd.dylib",
            "/usr/local/lib/libzstd.dylib",
        },
        .linux => &[_][]const u8{
            "libzstd.so.1",
            "libzstd.so",
            "/lib/x86_64-linux-gnu/libzstd.so.1",
            "/usr/lib/x86_64-linux-gnu/libzstd.so.1",
            "/lib/aarch64-linux-gnu/libzstd.so.1",
            "/usr/lib/aarch64-linux-gnu/libzstd.so.1",
            "/lib64/libzstd.so.1",
            "/usr/lib64/libzstd.so.1",
            "/lib/libzstd.so.1",
            "/usr/lib/libzstd.so.1",
        },
        .windows => &[_][]const u8{
            "zstd.dll",
            "libzstd.dll",
        },
        else => &[_][]const u8{},
    };

    const lz4_default_candidates = switch (builtin.os.tag) {
        .macos => &[_][]const u8{
            "liblz4.dylib",
            "liblz4.1.dylib",
            "/opt/homebrew/lib/liblz4.dylib",
            "/usr/local/lib/liblz4.dylib",
        },
        .linux => &[_][]const u8{
            "liblz4.so.1",
            "liblz4.so",
            "/lib/x86_64-linux-gnu/liblz4.so.1",
            "/usr/lib/x86_64-linux-gnu/liblz4.so.1",
            "/lib/aarch64-linux-gnu/liblz4.so.1",
            "/usr/lib/aarch64-linux-gnu/liblz4.so.1",
            "/lib64/liblz4.so.1",
            "/usr/lib64/liblz4.so.1",
            "/lib/liblz4.so.1",
            "/usr/lib/liblz4.so.1",
        },
        .windows => &[_][]const u8{
            "lz4.dll",
            "liblz4.dll",
        },
        else => &[_][]const u8{},
    };

    var env_opt = std.process.getEnvMap(std.heap.page_allocator) catch null;
    defer if (env_opt) |*m| m.deinit();
    const zstd_env = if (env_opt) |m| m.get("ZARROW_ZSTD_LIB") else null;
    const lz4_env = if (env_opt) |m| m.get("ZARROW_LZ4_LIB") else null;

    const has_zstd = if (zstd_env) |path| hasAnyLibrary(&[_][]const u8{path}) else hasAnyLibrary(zstd_default_candidates);
    const has_lz4 = if (lz4_env) |path| hasAnyLibrary(&[_][]const u8{path}) else hasAnyLibrary(lz4_default_candidates);
    if (has_zstd and has_lz4) return;

    std.debug.print("error: missing required IPC compression libraries for Arrow BodyCompression (ZSTD/LZ4_FRAME)\n", .{});
    std.debug.print("zarrow now requires these dependencies and does not fall back to unsupported mode.\n", .{});
    if (zstd_env != null or lz4_env != null) {
        std.debug.print("note: env overrides detected (ZARROW_ZSTD_LIB / ZARROW_LZ4_LIB)\n", .{});
    }
    std.debug.print("searched dynamic library names:\n", .{});
    if (!has_zstd) {
        if (zstd_env) |path| {
            printCandidates("zstd(env)", &[_][]const u8{path});
        } else {
            printCandidates("zstd", zstd_default_candidates);
        }
    }
    if (!has_lz4) {
        if (lz4_env) |path| {
            printCandidates("lz4(env)", &[_][]const u8{path});
        } else {
            printCandidates("lz4", lz4_default_candidates);
        }
    }

    switch (builtin.os.tag) {
        .macos => std.debug.print("hint: install with `brew install zstd lz4`\n", .{}),
        .linux => std.debug.print("hint: install dev/runtime packages for zstd and lz4 (example: `apt install libzstd-dev liblz4-dev`)\n", .{}),
        .windows => std.debug.print("hint: install zstd/lz4 and ensure DLLs are available on PATH (CI: `vcpkg install zstd:x64-windows lz4:x64-windows`)\n", .{}),
        else => std.debug.print("hint: install zstd/lz4 system libraries for your platform\n", .{}),
    }
    return error.MissingCompressionDependency;
}
