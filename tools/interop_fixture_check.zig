const std = @import("std");
const zarrow = @import("zarrow");

const ReaderAdapter = struct {
    inner: *std.Io.Reader,
    pub const Error = anyerror;

    pub fn readNoEof(self: @This(), dest: []u8) Error!void {
        try self.inner.readSliceAll(dest);
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // exe
    const in_path = args.next() orelse {
        std.log.err("usage: interop-fixture-check <in.arrow>", .{});
        return error.InvalidArgs;
    };

    const file = try std.fs.cwd().openFile(in_path, .{});
    defer file.close();
    var io_buf: [4096]u8 = undefined;
    const fr = file.reader(&io_buf);
    const reader_adapter = ReaderAdapter{ .inner = @constCast(&fr.interface) };
    var reader = zarrow.IpcStreamReader(ReaderAdapter).init(allocator, reader_adapter);
    defer reader.deinit();

    const schema = try reader.readSchema();
    if (schema.fields.len != 2) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[0].name, "id")) return error.InvalidSchema;
    if (!std.mem.eql(u8, schema.fields[1].name, "name")) return error.InvalidSchema;
    if (schema.fields[0].data_type.* != .int32) return error.InvalidSchema;
    if (schema.fields[1].data_type.* != .string) return error.InvalidSchema;

    const batch_opt = try reader.nextRecordBatch();
    if (batch_opt == null) return error.MissingBatch;
    var batch = batch_opt.?;
    defer batch.deinit();
    if (batch.numRows() != 3) return error.InvalidBatch;

    const ids = zarrow.Int32Array{ .data = batch.columns[0].data() };
    if (ids.value(0) != 1 or ids.value(1) != 2 or ids.value(2) != 3) return error.InvalidBatch;

    const names = zarrow.StringArray{ .data = batch.columns[1].data() };
    if (!std.mem.eql(u8, names.value(0), "alice")) return error.InvalidBatch;
    if (!names.isNull(1)) return error.InvalidBatch;
    if (!std.mem.eql(u8, names.value(2), "bob")) return error.InvalidBatch;

    const done = try reader.nextRecordBatch();
    if (done != null) return error.UnexpectedExtraBatch;
}
