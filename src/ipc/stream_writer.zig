const std = @import("std");
const datatype = @import("../datatype.zig");
const schema_mod = @import("../schema.zig");
const record_batch = @import("../record_batch.zig");
const array_ref = @import("../array/array_ref.zig");
const array_data = @import("../array/array_data.zig");
const format = @import("format.zig");

pub const StreamError = format.StreamError;
pub const MessageType = format.MessageType;

pub const Schema = schema_mod.Schema;
pub const Field = datatype.Field;
pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref.ArrayRef;
pub const ArrayData = array_data.ArrayData;
pub const RecordBatch = record_batch.RecordBatch;

pub fn StreamWriter(comptime WriterType: type) type {
    return struct {
        allocator: std.mem.Allocator,
        writer: WriterType,
        header_written: bool = false,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, writer: WriterType) !Self {
            var self = Self{ .allocator = allocator, .writer = writer };
            try self.writeHeader();
            return self;
        }

        pub fn writeHeader(self: *Self) !void {
            if (self.header_written) return;
            try format.writeStreamHeader(self.writer);
            self.header_written = true;
        }

        pub fn writeSchema(self: *Self, schema: Schema) !void {
            try self.writeHeader();
            var meta = std.ArrayList(u8).init(self.allocator);
            defer meta.deinit();
            try writeSchemaMeta(meta.writer(), schema);
            try format.writeMessageHeader(self.writer, .schema, @intCast(meta.items.len), 0);
            try self.writer.writeAll(meta.items);
        }

        pub fn writeRecordBatch(self: *Self, batch: RecordBatch) !void {
            try self.writeHeader();
            var meta = std.ArrayList(u8).init(self.allocator);
            defer meta.deinit();
            var buffers = std.ArrayList(array_data.SharedBuffer).init(self.allocator);
            defer buffers.deinit();

            try format.writeInt(meta.writer(), u32, @intCast(batch.numColumns()));
            try format.writeInt(meta.writer(), u64, batch.numRows());

            for (batch.columns) |col| {
                try writeArrayNodeMeta(meta.writer(), col.data(), &buffers);
            }

            var body_len: usize = 0;
            for (buffers.items) |buf| body_len += buf.len();

            try format.writeMessageHeader(self.writer, .record_batch, @intCast(meta.items.len), @intCast(body_len));
            try self.writer.writeAll(meta.items);
            for (buffers.items) |buf| {
                if (buf.len() > 0) try self.writer.writeAll(buf.data);
            }
        }

        pub fn writeEnd(self: *Self) !void {
            try self.writeHeader();
            try format.writeMessageHeader(self.writer, .end, 0, 0);
        }
    };
}

fn writeSchemaMeta(writer: anytype, schema: Schema) !void {
    try format.writeInt(writer, u32, @intCast(schema.fields.len));
    for (schema.fields) |field| {
        try writeField(writer, field);
    }
}

fn writeField(writer: anytype, field: Field) !void {
    try format.writeInt(writer, u16, @intCast(field.name.len));
    try writer.writeAll(field.name);
    try format.writeInt(writer, u8, if (field.nullable) 1 else 0);
    try writeDataType(writer, field.data_type.*);
}

fn writeDataType(writer: anytype, dt: DataType) !void {
    const type_id = dt.id();
    try format.writeInt(writer, u8, @intFromEnum(type_id));

    switch (dt) {
        .list => |lst| {
            try writeField(writer, lst.value_field);
        },
        .struct_ => |st| {
            try format.writeInt(writer, u32, @intCast(st.fields.len));
            for (st.fields) |field| try writeField(writer, field);
        },
        .dictionary => |dict| {
            try format.writeInt(writer, u8, dict.index_type.bit_width);
            try format.writeInt(writer, u8, if (dict.index_type.signed) 1 else 0);
            try format.writeInt(writer, u8, if (dict.ordered) 1 else 0);
            try writeDataType(writer, dict.value_type.*);
        },
        .bool, .uint8, .int8, .uint16, .int16, .uint32, .int32, .uint64, .int64, .half_float, .float, .double, .string, .binary => {},
        else => return StreamError.UnsupportedType,
    }
}

fn writeArrayNodeMeta(writer: anytype, data: *const ArrayData, buffers: *std.ArrayList(array_data.SharedBuffer)) !void {
    try format.writeInt(writer, u64, data.length);
    try format.writeInt(writer, u64, data.offset);
    if (data.null_count) |count| {
        try format.writeInt(writer, u8, 1);
        try format.writeInt(writer, u64, count);
    } else {
        try format.writeInt(writer, u8, 0);
    }

    try format.writeInt(writer, u32, @intCast(data.buffers.len));
    for (data.buffers) |buf| {
        try format.writeInt(writer, u64, buf.len());
        try buffers.append(buf);
    }

    try format.writeInt(writer, u32, @intCast(data.children.len));
    for (data.children) |child| {
        try writeArrayNodeMeta(writer, child.data(), buffers);
    }

    if (data.dictionary) |dict| {
        try format.writeInt(writer, u8, 1);
        try writeArrayNodeMeta(writer, dict.data(), buffers);
    } else {
        try format.writeInt(writer, u8, 0);
    }
}
