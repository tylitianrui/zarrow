const std = @import("std");
const zarrow = @import("zarrow");

const WriterAdapter = struct {
    inner: *std.Io.Writer,
    pub const Error = anyerror;

    pub fn writeAll(self: @This(), bytes: []const u8) Error!void {
        try self.inner.writeAll(bytes);
    }

    pub fn flush(self: @This()) Error!void {
        try self.inner.flush();
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // exe
    const out_path = args.next() orelse {
        std.log.err("usage: interop-fixture-writer <out.arrow>", .{});
        return error.InvalidArgs;
    };

    const id_type = zarrow.DataType{ .int32 = {} };
    const name_type = zarrow.DataType{ .string = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var ids_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer ids_builder.deinit();
    try ids_builder.append(1);
    try ids_builder.append(2);
    try ids_builder.append(3);
    var ids = try ids_builder.finish();
    defer ids.release();

    var names_builder = try zarrow.StringBuilder.init(allocator, 3, 12);
    defer names_builder.deinit();
    try names_builder.append("alice");
    try names_builder.appendNull();
    try names_builder.append("bob");
    var names = try names_builder.finish();
    defer names.release();

    var batch = try zarrow.RecordBatch.init(allocator, schema, &[_]zarrow.ArrayRef{ ids, names });
    defer batch.deinit();

    const file = try std.fs.cwd().createFile(out_path, .{ .truncate = true });
    defer file.close();
    var io_buf: [4096]u8 = undefined;
    const fw = file.writer(&io_buf);
    var writer_adapter = WriterAdapter{ .inner = @constCast(&fw.interface) };
    var writer = zarrow.IpcStreamWriter(WriterAdapter).init(allocator, writer_adapter);
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();
    try writer_adapter.flush();
}
