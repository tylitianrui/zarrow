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
    const zstd_candidates = switch (builtin.os.tag) {
        .macos => &[_][]const u8{
            "libzstd.dylib",
            "libzstd.1.dylib",
            "/opt/homebrew/lib/libzstd.dylib",
            "/usr/local/lib/libzstd.dylib",
        },
        .linux => &[_][]const u8{
            "libzstd.so.1",
            "libzstd.so",
        },
        .windows => &[_][]const u8{
            "zstd.dll",
            "libzstd.dll",
        },
        else => &[_][]const u8{},
    };

    const lz4_candidates = switch (builtin.os.tag) {
        .macos => &[_][]const u8{
            "liblz4.dylib",
            "liblz4.1.dylib",
            "/opt/homebrew/lib/liblz4.dylib",
            "/usr/local/lib/liblz4.dylib",
        },
        .linux => &[_][]const u8{
            "liblz4.so.1",
            "liblz4.so",
        },
        .windows => &[_][]const u8{
            "lz4.dll",
            "liblz4.dll",
        },
        else => &[_][]const u8{},
    };

    const has_zstd = hasAnyLibrary(zstd_candidates);
    const has_lz4 = hasAnyLibrary(lz4_candidates);
    if (has_zstd and has_lz4) return;

    std.debug.print("error: missing required IPC compression libraries for Arrow BodyCompression (ZSTD/LZ4_FRAME)\n", .{});
    std.debug.print("zarrow now requires these dependencies and does not fall back to unsupported mode.\n", .{});
    std.debug.print("searched dynamic library names:\n", .{});
    if (!has_zstd) printCandidates("zstd", zstd_candidates);
    if (!has_lz4) printCandidates("lz4", lz4_candidates);

    switch (builtin.os.tag) {
        .macos => std.debug.print("hint: install with `brew install zstd lz4`\n", .{}),
        .linux => std.debug.print("hint: install dev/runtime packages for zstd and lz4 (example: `apt install libzstd-dev liblz4-dev`)\n", .{}),
        .windows => std.debug.print("hint: install zstd/lz4 and ensure DLLs are available on PATH (CI: `vcpkg install zstd:x64-windows lz4:x64-windows`)\n", .{}),
        else => std.debug.print("hint: install zstd/lz4 system libraries for your platform\n", .{}),
    }
    return error.MissingCompressionDependency;
}
