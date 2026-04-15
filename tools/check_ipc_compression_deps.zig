const std = @import("std");

const required_vendor_files = [_][]const u8{
    "vendor/zstd/zstd.c",
    "vendor/zstd/zstd.h",
    "vendor/zstd/zstd_errors.h",
    "vendor/zstd.h",
    "vendor/zstd_errors.h",
    "vendor/lz4/lz4.c",
    "vendor/lz4/lz4.h",
    "vendor/lz4/lz4_all.c",
    "vendor/lz4/lz4hc.c",
    "vendor/lz4/lz4hc.h",
    "vendor/lz4/lz4frame.c",
    "vendor/lz4/lz4frame.h",
    "vendor/lz4/xxhash.c",
    "vendor/lz4/xxhash.h",
};

fn fileExists(path: []const u8) bool {
    std.fs.cwd().access(path, .{}) catch return false;
    return true;
}

pub fn main() !void {
    var missing = std.ArrayList([]const u8){};
    defer missing.deinit(std.heap.page_allocator);

    for (required_vendor_files) |path| {
        if (!fileExists(path)) {
            try missing.append(std.heap.page_allocator, path);
        }
    }

    if (missing.items.len == 0) return;

    std.debug.print("error: missing required vendored IPC compression sources for Arrow BodyCompression (ZSTD/LZ4_FRAME)\n", .{});
    std.debug.print("zarrow now builds zstd/lz4 from vendor/ and does not fall back to system libraries.\n", .{});
    std.debug.print("missing files:\n", .{});
    for (missing.items) |path| {
        std.debug.print("  - {s}\n", .{path});
    }
    std.debug.print("hint: restore vendor/zstd and vendor/lz4 sources (pinned upstream versions) before building.\n", .{});

    return error.MissingCompressionDependency;
}
