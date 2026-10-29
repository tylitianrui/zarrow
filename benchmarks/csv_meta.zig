const std = @import("std");

pub const CsvMeta = struct {
    git_sha: []const u8,
    timestamp: i64,
};

pub fn resolve(init: std.process.Init) CsvMeta {
    return .{
        .git_sha = init.environ_map.get("ZARROW_GIT_SHA") orelse "unknown",
        .timestamp = std.Io.Clock.real.now(init.io).toSeconds(),
    };
}
