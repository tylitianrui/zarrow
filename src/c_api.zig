const std = @import("std");
const ffi = @import("ffi/mod.zig");
const datatype = @import("datatype.zig");
const schema_mod = @import("schema.zig");
const array_mod = @import("array/array.zig");
const record_batch_mod = @import("record_batch.zig");

const DataType = datatype.DataType;
const c_allocator = std.heap.c_allocator;

const SchemaHandle = struct {
    owned: ffi.CDataOwnedSchema,
};

const ArrayHandle = struct {
    schema: ffi.CDataOwnedSchema,
    array: ffi.c_data.ArrayRef,
};

const StreamHandle = struct {
    stream: ffi.ArrowArrayStream,
};

pub const ZARROW_C_STATUS_OK: c_int = 0;
pub const ZARROW_C_STATUS_INVALID_ARGUMENT: c_int = 1;
pub const ZARROW_C_STATUS_OUT_OF_MEMORY: c_int = 2;
pub const ZARROW_C_STATUS_RELEASED: c_int = 3;
pub const ZARROW_C_STATUS_INVALID_DATA: c_int = 4;
pub const ZARROW_C_STATUS_INTERNAL: c_int = 5;

fn mapError(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => ZARROW_C_STATUS_OUT_OF_MEMORY,
        error.Released => ZARROW_C_STATUS_RELEASED,
        error.InvalidFormat,
        error.TopLevelSchemaMustBeStruct,
        error.InvalidChildren,
        error.InvalidBufferCount,
        error.InvalidLength,
        error.InvalidOffset,
        error.InvalidNullCount,
        error.MissingDictionary,
        error.UnsupportedType,
        error.InvalidStream,
        error.StreamCallbackFailed,
        error.SchemaMismatch,
        error.FieldCountMismatch,
        error.RowCountMismatch,
        error.SchemaEndiannessMismatch,
        error.SchemaFieldMismatch,
        error.InvalidArgument,
        error.LayoutMismatch,
        => ZARROW_C_STATUS_INVALID_DATA,
        else => ZARROW_C_STATUS_INTERNAL,
    };
}

pub export fn zarrow_c_abi_version() callconv(.c) u32 {
    return 1;
}

pub export fn zarrow_c_status_string(status: c_int) callconv(.c) [*c]const u8 {
    return switch (status) {
        ZARROW_C_STATUS_OK => "ok",
        ZARROW_C_STATUS_INVALID_ARGUMENT => "invalid argument",
        ZARROW_C_STATUS_OUT_OF_MEMORY => "out of memory",
        ZARROW_C_STATUS_RELEASED => "released input",
        ZARROW_C_STATUS_INVALID_DATA => "invalid data",
        ZARROW_C_STATUS_INTERNAL => "internal error",
        else => "unknown status",
    };
}

pub export fn zarrow_c_import_schema(
    c_schema: ?*ffi.ArrowSchema,
    out_handle: ?*?*SchemaHandle,
) callconv(.c) c_int {
    if (c_schema == null or out_handle == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_handle.?.* = null;

    var owned = ffi.importSchemaOwned(c_allocator, c_schema.?) catch |err| return mapError(err);
    errdefer owned.deinit();

    const handle = c_allocator.create(SchemaHandle) catch return ZARROW_C_STATUS_OUT_OF_MEMORY;
    handle.* = .{ .owned = owned };
    out_handle.?.* = handle;
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_export_schema(
    handle: ?*const SchemaHandle,
    out_schema: ?*ffi.ArrowSchema,
) callconv(.c) c_int {
    if (handle == null or out_schema == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_schema.?.* = ffi.exportSchema(c_allocator, handle.?.owned.schema) catch |err| return mapError(err);
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_release_schema(handle: ?*SchemaHandle) callconv(.c) void {
    if (handle == null) return;
    handle.?.owned.deinit();
    c_allocator.destroy(handle.?);
}

pub export fn zarrow_c_import_array(
    schema_handle: ?*const SchemaHandle,
    c_array: ?*ffi.ArrowArray,
    out_handle: ?*?*ArrayHandle,
) callconv(.c) c_int {
    if (schema_handle == null or c_array == null or out_handle == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_handle.?.* = null;

    // Keep a dedicated schema copy in the array handle so field/type pointers remain valid
    // even if the original schema handle is released.
    var exported_schema = ffi.exportSchema(c_allocator, schema_handle.?.owned.schema) catch |err| return mapError(err);
    defer if (exported_schema.release) |release_fn| release_fn(&exported_schema);
    var owned_schema = ffi.importSchemaOwned(c_allocator, &exported_schema) catch |err| return mapError(err);
    errdefer owned_schema.deinit();

    const top_level_type = DataType{ .struct_ = .{ .fields = owned_schema.schema.fields } };
    var arr = ffi.importArray(c_allocator, &top_level_type, c_array.?) catch |err| return mapError(err);
    errdefer arr.release();

    const handle = c_allocator.create(ArrayHandle) catch return ZARROW_C_STATUS_OUT_OF_MEMORY;
    handle.* = .{
        .schema = owned_schema,
        .array = arr,
    };
    out_handle.?.* = handle;
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_export_array(
    handle: ?*const ArrayHandle,
    out_array: ?*ffi.ArrowArray,
) callconv(.c) c_int {
    if (handle == null or out_array == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_array.?.* = ffi.exportArray(c_allocator, handle.?.array) catch |err| return mapError(err);
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_release_array(handle: ?*ArrayHandle) callconv(.c) void {
    if (handle == null) return;
    handle.?.array.release();
    handle.?.schema.deinit();
    c_allocator.destroy(handle.?);
}

pub export fn zarrow_c_import_stream(
    c_stream: ?*ffi.ArrowArrayStream,
    out_handle: ?*?*StreamHandle,
) callconv(.c) c_int {
    if (c_stream == null or out_handle == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    out_handle.?.* = null;

    if (c_stream.?.release == null) return ZARROW_C_STATUS_RELEASED;
    if (c_stream.?.get_schema == null or c_stream.?.get_next == null or c_stream.?.get_last_error == null) {
        return ZARROW_C_STATUS_INVALID_DATA;
    }

    const handle = c_allocator.create(StreamHandle) catch return ZARROW_C_STATUS_OUT_OF_MEMORY;
    handle.* = .{ .stream = c_stream.?.* };
    // Transfer C Stream ownership to the handle.
    c_stream.?.* = releasedArrowArrayStream();
    out_handle.?.* = handle;
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_export_stream(
    handle: ?*StreamHandle,
    out_stream: ?*ffi.ArrowArrayStream,
) callconv(.c) c_int {
    if (handle == null or out_stream == null) return ZARROW_C_STATUS_INVALID_ARGUMENT;
    if (handle.?.stream.release == null) return ZARROW_C_STATUS_RELEASED;
    // Transfer ownership out of the handle; no materialization, preserves streaming callbacks.
    out_stream.?.* = handle.?.stream;
    handle.?.stream = releasedArrowArrayStream();
    return ZARROW_C_STATUS_OK;
}

pub export fn zarrow_c_release_stream(handle: ?*StreamHandle) callconv(.c) void {
    if (handle == null) return;
    if (handle.?.stream.release) |release_fn| {
        release_fn(&handle.?.stream);
    }
    handle.?.stream = releasedArrowArrayStream();
    c_allocator.destroy(handle.?);
}

fn releasedArrowArrayStream() ffi.ArrowArrayStream {
    return .{
        .get_schema = null,
        .get_next = null,
        .get_last_error = null,
        .release = null,
        .private_data = null,
    };
}

fn releasedArrowSchema() ffi.ArrowSchema {
    return .{
        .format = null,
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
}

fn releasedArrowArray() ffi.ArrowArray {
    return .{
        .length = 0,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 0,
        .n_children = 0,
        .buffers = null,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
}

fn noOpStreamRelease(raw: ?*ffi.ArrowArrayStream) callconv(.c) void {
    if (raw) |stream| {
        stream.release = null;
        stream.private_data = null;
    }
}

test "c api status string smoke" {
    try std.testing.expectEqualStrings("ok", std.mem.span(zarrow_c_status_string(ZARROW_C_STATUS_OK)));
    try std.testing.expectEqualStrings("unknown status", std.mem.span(zarrow_c_status_string(999)));
}

test "c api schema and array import/export roundtrip" {
    const allocator = std.testing.allocator;
    const id_type = DataType{ .int32 = {} };
    const fields = [_]datatype.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = schema_mod.Schema{ .fields = fields[0..] };

    var builder = try array_mod.Int32Builder.init(allocator, 3);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    try builder.append(3);
    var ids = try builder.finish();
    defer ids.release();

    var batch = try record_batch_mod.RecordBatch.initBorrowed(allocator, schema, &[_]ffi.c_stream.ArrayRef{ids});
    defer batch.deinit();

    var stream = try ffi.exportRecordBatchStream(allocator, schema, &[_]record_batch_mod.RecordBatch{batch});
    defer if (stream.release) |release_fn| release_fn(&stream);

    var c_schema = releasedArrowSchema();
    try std.testing.expectEqual(@as(c_int, 0), stream.get_schema.?(&stream, &c_schema));
    try std.testing.expect(c_schema.release != null);

    var schema_handle: ?*SchemaHandle = null;
    try std.testing.expectEqual(ZARROW_C_STATUS_OK, zarrow_c_import_schema(&c_schema, &schema_handle));
    try std.testing.expect(c_schema.release == null);
    try std.testing.expect(schema_handle != null);

    var c_array = releasedArrowArray();
    try std.testing.expectEqual(@as(c_int, 0), stream.get_next.?(&stream, &c_array));
    try std.testing.expect(c_array.release != null);

    var array_handle: ?*ArrayHandle = null;
    try std.testing.expectEqual(ZARROW_C_STATUS_OK, zarrow_c_import_array(schema_handle, &c_array, &array_handle));
    try std.testing.expect(c_array.release == null);
    try std.testing.expect(array_handle != null);

    // Array handle keeps an internal schema copy so the source schema handle can be released early.
    zarrow_c_release_schema(schema_handle);
    schema_handle = null;

    var roundtrip = releasedArrowArray();
    try std.testing.expectEqual(ZARROW_C_STATUS_OK, zarrow_c_export_array(array_handle, &roundtrip));
    defer if (roundtrip.release) |release_fn| release_fn(&roundtrip);

    const top_level_type = DataType{ .struct_ = .{ .fields = schema.fields } };
    var imported = try ffi.importArray(allocator, &top_level_type, &roundtrip);
    defer imported.release();

    const struct_view = array_mod.StructArray{ .data = imported.data() };
    try std.testing.expectEqual(@as(usize, 1), struct_view.fieldCount());
    var id_ref = try struct_view.field(0);
    defer id_ref.release();
    const id_arr = array_mod.Int32Array{ .data = id_ref.data() };
    try std.testing.expectEqual(@as(usize, 3), id_arr.len());
    try std.testing.expectEqual(@as(i32, 1), id_arr.value(0));
    try std.testing.expectEqual(@as(i32, 2), id_arr.value(1));
    try std.testing.expectEqual(@as(i32, 3), id_arr.value(2));

    zarrow_c_release_array(array_handle);
}

test "c api invalid-argument and invalid-data paths" {
    var dummy_schema = releasedArrowSchema();
    var schema_handle: ?*SchemaHandle = null;
    try std.testing.expectEqual(ZARROW_C_STATUS_INVALID_ARGUMENT, zarrow_c_import_schema(null, &schema_handle));
    try std.testing.expectEqual(ZARROW_C_STATUS_INVALID_ARGUMENT, zarrow_c_import_schema(&dummy_schema, null));

    var out_stream = releasedArrowArrayStream();
    try std.testing.expectEqual(ZARROW_C_STATUS_INVALID_ARGUMENT, zarrow_c_export_stream(null, &out_stream));
    try std.testing.expectEqual(ZARROW_C_STATUS_INVALID_ARGUMENT, zarrow_c_export_stream(null, null));

    var released_stream = releasedArrowArrayStream();
    var stream_handle: ?*StreamHandle = null;
    try std.testing.expectEqual(ZARROW_C_STATUS_RELEASED, zarrow_c_import_stream(&released_stream, &stream_handle));

    var bad_stream = releasedArrowArrayStream();
    bad_stream.release = noOpStreamRelease;
    try std.testing.expectEqual(ZARROW_C_STATUS_INVALID_DATA, zarrow_c_import_stream(&bad_stream, &stream_handle));
}

test "c api stream import/export keeps callbacks and avoids materialization" {
    const allocator = std.testing.allocator;
    const id_type = DataType{ .int32 = {} };
    const fields = [_]datatype.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
    };
    const schema = schema_mod.Schema{ .fields = fields[0..] };

    var builder = try array_mod.Int32Builder.init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var ids = try builder.finish();
    defer ids.release();

    var batch = try record_batch_mod.RecordBatch.initBorrowed(allocator, schema, &[_]ffi.c_stream.ArrayRef{ids});
    defer batch.deinit();

    var src = try ffi.exportRecordBatchStream(allocator, schema, &[_]record_batch_mod.RecordBatch{batch});
    const original_get_next = src.get_next;
    const original_private = src.private_data;

    var handle: ?*StreamHandle = null;
    try std.testing.expectEqual(ZARROW_C_STATUS_OK, zarrow_c_import_stream(&src, &handle));
    defer zarrow_c_release_stream(handle);
    try std.testing.expect(src.release == null);
    try std.testing.expect(handle != null);

    var out = releasedArrowArrayStream();
    try std.testing.expectEqual(ZARROW_C_STATUS_OK, zarrow_c_export_stream(handle, &out));
    try std.testing.expectEqual(original_get_next, out.get_next);
    try std.testing.expectEqual(original_private, out.private_data);
    try std.testing.expect(handle.?.stream.release == null);

    // Export is move-out; second export should report released.
    var out2 = releasedArrowArrayStream();
    try std.testing.expectEqual(ZARROW_C_STATUS_RELEASED, zarrow_c_export_stream(handle, &out2));

    if (out.release) |release_fn| release_fn(&out);
}
