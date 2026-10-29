const std = @import("std");

fn patchBackslashes(io: std.Io, allocator: std.mem.Allocator, path: []const u8) !void {
    const max_bytes: usize = 16 * 1024 * 1024;
    const original = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
    defer allocator.free(original);

    const fixed = try allocator.dupe(u8, original);
    defer allocator.free(fixed);

    var changed = false;
    for (fixed) |*c| {
        if (c.* == '\\') {
            c.* = '/';
            changed = true;
        }
    }
    if (!changed) return;
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = fixed });
}

fn patchUnionMissingSemicolon(io: std.Io, allocator: std.mem.Allocator, path: []const u8) !void {
    const max_bytes: usize = 16 * 1024 * 1024;
    const original = std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes)) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer allocator.free(original);

    const needle = "const obj2 = try obj_packed.Unpack(opts)\n";
    const replacement = "const obj2 = try obj_packed.Unpack(opts);\n";
    const fixed = try std.mem.replaceOwned(u8, allocator, original, needle, replacement);
    defer allocator.free(fixed);

    if (std.mem.eql(u8, original, fixed)) return;
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = fixed });
}

pub fn main(init: std.process.Init) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try init.minimal.args.iterateAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // executable name
    const path = args.next() orelse return error.MissingPathArgument;

    try patchBackslashes(init.io, allocator, path);

    const dir = std.fs.path.dirname(path) orelse return;
    const union_path = try std.fs.path.join(allocator, &.{ dir, "org", "apache", "arrow", "flatbuf", "Union.fb.zig" });
    defer allocator.free(union_path);
    try patchUnionMissingSemicolon(init.io, allocator, union_path);
}
