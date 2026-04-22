const std = @import("std");
const zarrow = @import("zarrow");
const c_api = zarrow.c_api;

fn ensureOk(rc: c_int, context: []const u8) !void {
    if (rc == c_api.ZARROW_C_STATUS_OK) return;
    std.debug.print("{s} failed: {s}\n", .{
        context,
        std.mem.span(c_api.zarrow_c_status_string(rc)),
    });
    return error.CAbiFailure;
}

fn releasedArrowArrayStream() zarrow.ArrowArrayStream {
    return .{
        .get_schema = null,
        .get_next = null,
        .get_last_error = null,
        .release = null,
        .private_data = null,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const id_type = zarrow.DataType{ .int32 = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var builder = try zarrow.Int32Builder.init(allocator, 3);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    try builder.append(3);
    var ids = try builder.finish();
    defer ids.release();

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{ids});
    defer batch.deinit();

    var input_stream = try zarrow.exportRecordBatchStreamToC(allocator, schema, &[_]zarrow.RecordBatch{batch});
    errdefer if (input_stream.release) |release_fn| release_fn(&input_stream);

    const import_stream_out_param_ty = @typeInfo(@TypeOf(c_api.zarrow_c_import_stream)).@"fn".params[1].type.?;
    const stream_handle_ty = @typeInfo(@typeInfo(import_stream_out_param_ty).optional.child).pointer.child;
    var stream_handle: stream_handle_ty = null;
    try ensureOk(c_api.zarrow_c_import_stream(&input_stream, &stream_handle), "zarrow_c_import_stream");
    defer c_api.zarrow_c_release_stream(stream_handle);

    var output_stream = releasedArrowArrayStream();
    try ensureOk(c_api.zarrow_c_export_stream(stream_handle, &output_stream), "zarrow_c_export_stream");

    var imported = try zarrow.importRecordBatchStreamFromC(allocator, &output_stream);
    defer imported.deinit();

    if (imported.batches.len != 1) return error.UnexpectedBatchCount;
    if (imported.batches[0].numRows() != 3) return error.UnexpectedRowCount;

    const id_arr = zarrow.Int32Array{ .data = imported.batches[0].column(0).data() };
    const v0 = try id_arr.value(0);
    const v1 = try id_arr.value(1);
    const v2 = try id_arr.value(2);
    if (v0 != 1 or v1 != 2 or v2 != 3) {
        return error.UnexpectedValues;
    }

    std.debug.print("examples/c_abi_roundtrip.zig | c abi stream roundtrip ok (rows={d})\n", .{id_arr.len()});
}
