const std = @import("std");
const zarrow = @import("zarrow");

const MAX_INPUT_BYTES: usize = 4 << 20;
const MAX_BATCHES: usize = 16;

pub fn main(init: std.process.Init) !void {
    var gpa_state = std.heap.DebugAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    const bytes = try readInput(init, allocator);
    defer allocator.free(bytes);

    try runOne(allocator, bytes);
}

fn readInput(init: std.process.Init, allocator: std.mem.Allocator) ![]u8 {
    var args = try init.minimal.args.iterateAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    if (args.next()) |path| {
        return try std.Io.Dir.cwd().readFileAlloc(init.io, path, allocator, .limited(MAX_INPUT_BYTES));
    }
    return error.MissingInputPath;
}

fn runOne(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var stream = std.Io.Reader.fixed(bytes);
    var reader = zarrow.IpcStreamReader(@TypeOf(&stream)).init(allocator, &stream);
    defer reader.deinit();

    _ = reader.readSchema() catch return;

    var seen: usize = 0;
    while (seen < MAX_BATCHES) : (seen += 1) {
        const maybe_batch = reader.nextRecordBatch() catch return;
        if (maybe_batch == null) return;

        var batch = maybe_batch.?;
        batch.deinit();
    }
}
