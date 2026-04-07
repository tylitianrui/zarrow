const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const buffer = @import("../buffer.zig");
const array_ref = @import("../array/array_ref.zig");
const array_data = @import("../array/array_data.zig");
const format = @import("format.zig");

pub const StreamError = format.StreamError;
pub const MessageType = format.MessageType;

pub const Schema = schema_mod.Schema;
pub const Field = datatype.Field;
pub const DataType = datatype.DataType;
pub const TypeId = datatype.TypeId;
pub const ArrayRef = array_ref.ArrayRef;
pub const ArrayData = array_data.ArrayData;
pub const RecordBatch = record_batch.RecordBatch;
pub const OwnedBuffer = buffer.OwnedBuffer;

pub const OwnedSchema = struct {
    arena: std.heap.ArenaAllocator,
    schema: Schema,

    pub fn deinit(self: *OwnedSchema) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *OwnedSchema) std.mem.Allocator {
        return self.arena.allocator();
    }
};

pub fn StreamReader(comptime ReaderType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        reader: ReaderType,
        header_read: bool = false,
        schema_owned: ?OwnedSchema = null,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, reader: ReaderType) Self {
            return .{ .allocator = allocator, .reader = reader };
        }

        pub fn deinit(self: *Self) void {
            if (self.schema_owned) |*owned| owned.deinit();
            self.schema_owned = null;
        }

        pub fn readSchema(self: *Self) !Schema {
            try self.ensureHeader();
            const msg = try format.readMessageHeader(self.reader);
            if (msg.msg_type != .schema) return StreamError.InvalidMessage;
            var meta_buf = try self.allocator.alloc(u8, msg.meta_len);
            defer self.allocator.free(meta_buf);
            if (msg.meta_len > 0) try self.reader.readNoEof(meta_buf);

            var arena = std.heap.ArenaAllocator.init(self.allocator);
            errdefer arena.deinit();
            var meta_stream = std.io.fixedBufferStream(meta_buf);
            const schema = try readSchemaMeta(meta_stream.reader(), arena.allocator());

            if (self.schema_owned) |*owned| owned.deinit();
            self.schema_owned = .{ .arena = arena, .schema = schema };
            return schema;
        }

        pub fn nextRecordBatch(self: *Self) !?RecordBatch {
            try self.ensureHeader();
            if (self.schema_owned == null) return StreamError.SchemaNotRead;

            const msg = try format.readMessageHeader(self.reader);
            if (msg.msg_type == .end) return null;
            if (msg.msg_type != .record_batch) return StreamError.InvalidMessage;

            var meta_buf = try self.allocator.alloc(u8, msg.meta_len);
            defer self.allocator.free(meta_buf);
            if (msg.meta_len > 0) try self.reader.readNoEof(meta_buf);

            var body_buf = try OwnedBuffer.init(self.allocator, msg.body_len);
            errdefer body_buf.deinit();
            if (msg.body_len > 0) try self.reader.readNoEof(body_buf.data[0..msg.body_len]);
            var body_shared = try body_buf.toShared(msg.body_len);

            var meta_stream = std.io.fixedBufferStream(meta_buf);
            var body_offset: usize = 0;
            const schema = self.schema_owned.?.schema;
            const batch = try readRecordBatchMeta(meta_stream.reader(), self.allocator, schema, body_shared, &body_offset);

            var owned = body_shared;
            owned.release();
            return batch;
        }

        fn ensureHeader(self: *Self) !void {
            if (self.header_read) return;
            try format.readStreamHeader(self.reader);
            self.header_read = true;
        }
    };
}

fn readSchemaMeta(reader: anytype, allocator: std.mem.Allocator) !Schema {
    const field_count = try format.readInt(reader, u32);
    const fields = try allocator.alloc(Field, field_count);
    var i: usize = 0;
    while (i < field_count) : (i += 1) {
        fields[i] = try readField(reader, allocator);
    }

    return Schema{ .fields = fields, .endianness = .native, .metadata = null };
}

fn readField(reader: anytype, allocator: std.mem.Allocator) !Field {
    const name_len = try format.readInt(reader, u16);
    const name = try allocator.alloc(u8, name_len);
    if (name_len > 0) try reader.readNoEof(name);
    const nullable = (try format.readInt(reader, u8)) != 0;
    const dtype_ptr = try allocator.create(DataType);
    dtype_ptr.* = try readDataType(reader, allocator);
    return Field{ .name = name, .data_type = dtype_ptr, .nullable = nullable };
}

fn readDataType(reader: anytype, allocator: std.mem.Allocator) !DataType {
    const raw_id = try format.readInt(reader, u8);
    const type_id = std.meta.intToEnum(TypeId, raw_id) catch return StreamError.InvalidMetadata;

    return switch (type_id) {
        .bool => DataType{ .bool = {} },
        .uint8 => DataType{ .uint8 = {} },
        .int8 => DataType{ .int8 = {} },
        .uint16 => DataType{ .uint16 = {} },
        .int16 => DataType{ .int16 = {} },
        .uint32 => DataType{ .uint32 = {} },
        .int32 => DataType{ .int32 = {} },
        .uint64 => DataType{ .uint64 = {} },
        .int64 => DataType{ .int64 = {} },
        .half_float => DataType{ .half_float = {} },
        .float => DataType{ .float = {} },
        .double => DataType{ .double = {} },
        .string => DataType{ .string = {} },
        .binary => DataType{ .binary = {} },
        .list => blk: {
            const value_field = try readField(reader, allocator);
            break :blk DataType{ .list = .{ .value_field = value_field } };
        },
        .struct_ => blk: {
            const count = try format.readInt(reader, u32);
            const fields = try allocator.alloc(Field, count);
            var i: usize = 0;
            while (i < count) : (i += 1) fields[i] = try readField(reader, allocator);
            break :blk DataType{ .struct_ = .{ .fields = fields } };
        },
        .dictionary => blk: {
            const bit_width = try format.readInt(reader, u8);
            const signed = (try format.readInt(reader, u8)) != 0;
            const ordered = (try format.readInt(reader, u8)) != 0;
            const value_ptr = try allocator.create(DataType);
            value_ptr.* = try readDataType(reader, allocator);
            break :blk DataType{ .dictionary = .{ .index_type = .{ .bit_width = bit_width, .signed = signed }, .value_type = value_ptr, .ordered = ordered } };
        },
        else => return StreamError.UnsupportedType,
    };
}

fn readRecordBatchMeta(reader: anytype, allocator: std.mem.Allocator, schema: Schema, body: array_data.SharedBuffer, body_offset: *usize) !RecordBatch {
    const column_count = try format.readInt(reader, u32);
    const num_rows = try format.readInt(reader, u64);
    if (column_count != schema.fields.len) return StreamError.InvalidMetadata;

    const columns = try allocator.alloc(ArrayRef, column_count);
    var col_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < col_count) : (i += 1) columns[i].release();
        allocator.free(columns);
    }

    for (schema.fields, 0..) |field, i| {
        columns[i] = try readArrayNode(reader, allocator, field.data_type.*, body, body_offset);
        col_count += 1;
    }

    const batch = try RecordBatch.init(allocator, schema, columns);
    var i: usize = 0;
    while (i < col_count) : (i += 1) columns[i].release();
    allocator.free(columns);
    if (batch.numRows() != @as(usize, @intCast(num_rows))) return StreamError.InvalidMetadata;
    return batch;
}

fn readArrayNode(reader: anytype, allocator: std.mem.Allocator, dt: DataType, body: array_data.SharedBuffer, body_offset: *usize) !ArrayRef {
    const length = try format.readInt(reader, u64);
    const offset = try format.readInt(reader, u64);
    const null_present = (try format.readInt(reader, u8)) != 0;
    var null_count: ?usize = null;
    if (null_present) null_count = @intCast(try format.readInt(reader, u64));

    const buffer_count = try format.readInt(reader, u32);
    const buffers = try allocator.alloc(array_data.SharedBuffer, buffer_count);
    var buf_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < buf_count) : (i += 1) {
            var owned = buffers[i];
            owned.release();
        }
        allocator.free(buffers);
    }

    var i: usize = 0;
    while (i < buffer_count) : (i += 1) {
        const buf_len = @as(usize, @intCast(try format.readInt(reader, u64)));
        const start = body_offset.*;
        const end = start + buf_len;
        if (end > body.len()) return StreamError.InvalidBody;
        buffers[i] = body.slice(start, end);
        buf_count += 1;
        body_offset.* = end;
    }

    const child_count = try format.readInt(reader, u32);
    const children = try allocator.alloc(ArrayRef, child_count);
    var child_written: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < child_written) : (j += 1) children[j].release();
        allocator.free(children);
    }

    switch (dt) {
        .list => |lst| {
            if (child_count != 1) return StreamError.InvalidMetadata;
            children[0] = try readArrayNode(reader, allocator, lst.value_field.data_type.*, body, body_offset);
            child_written = 1;
        },
        .struct_ => |st| {
            if (child_count != st.fields.len) return StreamError.InvalidMetadata;
            var idx: usize = 0;
            while (idx < st.fields.len) : (idx += 1) {
                children[idx] = try readArrayNode(reader, allocator, st.fields[idx].data_type.*, body, body_offset);
                child_written += 1;
            }
        },
        .dictionary => {
            if (child_count != 0) return StreamError.InvalidMetadata;
        },
        else => {
            if (child_count != 0) return StreamError.InvalidMetadata;
        },
    }

    const has_dict = (try format.readInt(reader, u8)) != 0;
    var dict_ref: ?ArrayRef = null;
    if (has_dict) {
        if (dt != .dictionary) return StreamError.InvalidMetadata;
        const value_type = dt.dictionary.value_type.*;
        dict_ref = try readArrayNode(reader, allocator, value_type, body, body_offset);
    } else if (dt == .dictionary) {
        return StreamError.InvalidMetadata;
    }

    const data = ArrayData{
        .data_type = dt,
        .length = @intCast(length),
        .offset = @intCast(offset),
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = dict_ref,
    };

    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

test "ipc stream roundtrip schema and batch" {
    const allocator = std.testing.allocator;

    const int_type = DataType{ .int32 = {} };
    const str_type = DataType{ .string = {} };
    const fields = [_]Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "name", .data_type = &str_type, .nullable = true },
    };
    const schema = Schema{ .fields = fields[0..] };

    var int_builder = try @import("../array/primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer int_builder.deinit();
    try int_builder.append(1);
    try int_builder.append(2);
    var int_ref = try int_builder.finish();
    defer int_ref.release();

    var str_builder = try @import("../array/string_array.zig").StringBuilder.init(allocator, 2, 8);
    defer str_builder.deinit();
    try str_builder.append("a");
    try str_builder.appendNull();
    var str_ref = try str_builder.finish();
    defer str_ref.release();

    var batch = try RecordBatch.init(allocator, schema, &[_]ArrayRef{ int_ref, str_ref });
    defer batch.deinit();

    var buffer = std.ArrayList(u8).init(allocator);
    defer buffer.deinit();

    var writer = try @import("stream_writer.zig").StreamWriter(@TypeOf(buffer.writer())).init(allocator, buffer.writer());
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var stream = std.io.fixedBufferStream(buffer.items);
    var reader = StreamReader(@TypeOf(stream.reader())).init(allocator, stream.reader());
    defer reader.deinit();

    const out_schema = try reader.readSchema();
    try std.testing.expectEqual(@as(usize, 2), out_schema.fields.len);
    try std.testing.expect(std.mem.eql(u8, out_schema.fields[0].name, "id"));
    try std.testing.expect(std.mem.eql(u8, out_schema.fields[1].name, "name"));

    const batch_opt = try reader.nextRecordBatch();
    try std.testing.expect(batch_opt != null);
    var out_batch = batch_opt.?;
    defer out_batch.deinit();

    try std.testing.expectEqual(@as(usize, 2), out_batch.numRows());
    const names = @import("../array/string_array.zig").StringArray{ .data = out_batch.columns[1].data() };
    try std.testing.expect(names.isNull(1));
}
