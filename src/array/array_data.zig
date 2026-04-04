const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");

pub const Buffer = buffer.Buffer;
pub const ValidityBitmap = bitmap.ValidityBitmap;
pub const DataType = datatype.DataType;

pub const ValidationError = error{
    InvalidBufferCount,
    BufferTooSmall,
    InvalidOffsetBuffer,
    InvalidOffsets,
    InvalidNullCount,
    MissingDictionary,
    InvalidChildren,
};

/// Core Arrow array metadata and buffers.
///
/// Field semantics:
/// - data_type: logical Arrow type describing the array.
/// - length: number of logical elements.
/// - offset: logical slice offset into buffers.
/// - null_count: number of nulls (-1 means unknown; 0 means no nulls).
/// - buffers: Arrow buffers in type-specific order.
/// - children: child arrays for nested types (list/struct/union/map).
/// - dictionary: dictionary values for dictionary-encoded arrays.
///
/// Common buffer layouts:
/// - Primitive (int/float): [validity], [values].
/// - Boolean: [validity], [bit-packed values].
/// - String/Binary: [validity], [i32 offsets], [data bytes].
/// - List: [validity], [i32 offsets], children[0] = values.
/// - FixedSizeList: [validity], children[0] = values.
/// - Struct: [validity], children = fields in order.
/// - Dictionary: [validity], [indices], dictionary = values array.
/// - Union (sparse): [type_ids], children = fields; no offsets buffer.
/// - Union (dense): [type_ids], [offsets], children = fields.
/// - Map: [validity], [i32 offsets], children[0] = struct of (key, item).
/// - RunEndEncoded: [run_ends], children[0] = values.
pub const ArrayData = struct {
    data_type: DataType,
    length: usize,
    offset: usize = 0,
    null_count: isize = -1, // -1 means unknown; 0 means no nulls
    buffers: []const Buffer,
    children: []const ArrayData = &.{},
    dictionary: ?*const ArrayData = null,

    const Self = @This();

    pub fn validity(self: Self) ?ValidityBitmap {
        if (self.buffers.len == 0) return null;
        if (self.buffers[0].isEmpty()) return null;
        return ValidityBitmap.fromBuffer(self.buffers[0], self.length + self.offset);
    }

    pub fn isNull(self: Self, i: usize) bool {
        std.debug.assert(i < self.length);
        if (self.null_count == 0) return false;
        const validity_bitmap = self.validity() orelse return false;
        return !validity_bitmap.isValid(self.offset + i);
    }

    pub fn isValid(self: Self, i: usize) bool {
        return !self.isNull(i);
    }

    pub fn hasNulls(self: Self) bool {
        if (self.null_count == 0) return false;
        if (self.null_count > 0) return true;
        const validity_bitmap = self.validity() orelse return false;
        return validity_bitmap.countNulls() > 0;
    }

    pub fn setNullCountUnknown(self: *Self) void {
        self.null_count = -1;
    }

    pub fn setNullCountKnown(self: *Self, count: usize) void {
        self.null_count = @intCast(count);
    }

    pub fn slice(self: Self, offset: usize, length: usize) Self {
        std.debug.assert(offset <= self.length);
        std.debug.assert(offset + length <= self.length);

        return .{
            .data_type = self.data_type,
            .length = length,
            .offset = self.offset + offset,
            .null_count = if (self.null_count == 0) 0 else -1,
            .buffers = self.buffers,
            .children = self.children,
            .dictionary = self.dictionary,
        };
    }

    pub fn nullCount(self: *Self) usize {
        if (self.null_count >= 0) return @intCast(self.null_count);
        const validity_bitmap = self.validity() orelse {
            self.null_count = 0;
            return 0;
        };
        var count: usize = 0;
        var i: usize = 0;
        while (i < self.length) : (i += 1) {
            if (!validity_bitmap.isValid(self.offset + i)) count += 1;
        }
        self.null_count = @intCast(count);
        return count;
    }

    fn fixedWidthByteSize(dt: DataType) ?usize {
        return switch (dt) {
            .bool => 1,
            .uint8, .int8 => 1,
            .uint16, .int16 => 2,
            .uint32, .int32 => 4,
            .uint64, .int64 => 8,
            .half_float => 2,
            .float => 4,
            .double => 8,
            .date32 => 4,
            .date64 => 8,
            .time32 => 4,
            .time64 => 8,
            .timestamp => 8,
            .duration => 8,
            .interval_months => 4,
            .interval_day_time => 8,
            .interval_month_day_nano => 16,
            .decimal32 => 4,
            .decimal64 => 8,
            .decimal128 => 16,
            .decimal256 => 32,
            .fixed_size_binary => |fsb| @intCast(fsb.byte_width),
            else => null,
        };
    }

    fn offsetByteWidth(dt: DataType) ?usize {
        return switch (dt) {
            .string, .binary, .list, .list_view, .map => 4,
            .large_string, .large_binary, .large_list, .large_list_view => 8,
            else => null,
        };
    }

    fn validateOffsetsI32(offsets: []const i32, total_len: usize, data_len: usize) ValidationError!void {
        if (offsets.len < total_len + 1) return error.BufferTooSmall;
        var prev: i32 = offsets[0];
        if (prev < 0) return error.InvalidOffsets;
        var i: usize = 1;
        while (i < total_len + 1) : (i += 1) {
            const cur = offsets[i];
            if (cur < prev or cur < 0) return error.InvalidOffsets;
            prev = cur;
        }
        if (@as(usize, @intCast(prev)) > data_len) return error.InvalidOffsets;
    }

    fn validateOffsetsI64(offsets: []const i64, total_len: usize, data_len: usize) ValidationError!void {
        if (offsets.len < total_len + 1) return error.BufferTooSmall;
        var prev: i64 = offsets[0];
        if (prev < 0) return error.InvalidOffsets;
        var i: usize = 1;
        while (i < total_len + 1) : (i += 1) {
            const cur = offsets[i];
            if (cur < prev or cur < 0) return error.InvalidOffsets;
            prev = cur;
        }
        if (@as(usize, @intCast(prev)) > data_len) return error.InvalidOffsets;
    }

    pub fn validateLayout(self: Self) ValidationError!void {
        if (self.null_count < -1) return error.InvalidNullCount;

        const total_len = self.offset + self.length;
        if (self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
            const needed = bitmap.byteLength(total_len);
            if (self.buffers[0].len() < needed) return error.BufferTooSmall;
        }

        switch (self.data_type) {
            .null => {},
            .bool => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const needed = bitmap.byteLength(total_len);
                if (self.buffers[1].len() < needed) return error.BufferTooSmall;
            },
            .string, .binary, .list, .list_view, .map, .large_string, .large_binary, .large_list, .large_list_view => {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const offset_width = offsetByteWidth(self.data_type).?;
                if (self.buffers[1].len() % offset_width != 0) return error.InvalidOffsetBuffer;
                if (self.buffers.len < 3 and (self.data_type == .string or self.data_type == .binary or self.data_type == .large_string or self.data_type == .large_binary)) {
                    return error.InvalidBufferCount;
                }

                const data_len = if (self.buffers.len >= 3) self.buffers[2].len() else 0;
                if (offset_width == 4) {
                    const offsets = self.buffers[1].typedSlice(i32);
                    try validateOffsetsI32(offsets, total_len, data_len);
                } else {
                    const offsets = self.buffers[1].typedSlice(i64);
                    try validateOffsetsI64(offsets, total_len, data_len);
                }

                if (self.data_type == .list or self.data_type == .list_view or self.data_type == .large_list or self.data_type == .large_list_view or self.data_type == .map) {
                    if (self.children.len != 1) return error.InvalidChildren;
                }
            },
            .fixed_size_list, .struct_ => {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                const expected = switch (self.data_type) {
                    .fixed_size_list => 1,
                    .struct_ => |st| st.fields.len,
                    else => 0,
                };
                if (self.children.len != expected) return error.InvalidChildren;
            },
            .dictionary => |dict| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                const byte_width = dict.index_type.bit_width / 8;
                if (byte_width == 0) return error.InvalidOffsetBuffer;
                if (self.buffers[1].len() < total_len * byte_width) return error.BufferTooSmall;
                if (self.dictionary == null) return error.MissingDictionary;
            },
            .sparse_union => |uni| {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
            },
            .dense_union => |uni| {
                if (self.buffers.len < 2) return error.InvalidBufferCount;
                if (self.children.len != uni.fields.len) return error.InvalidChildren;
            },
            .run_end_encoded => {
                if (self.buffers.len < 1) return error.InvalidBufferCount;
                if (self.children.len != 1) return error.InvalidChildren;
            },
            else => {
                if (fixedWidthByteSize(self.data_type)) |byte_width| {
                    if (self.buffers.len < 2) return error.InvalidBufferCount;
                    if (byte_width == 0) return error.InvalidOffsetBuffer;
                    if (self.buffers[1].len() < total_len * byte_width) return error.BufferTooSmall;
                }
            },
        }
    }

    pub fn validateFull(self: Self) ValidationError!void {
        try self.validateLayout();

        if (self.null_count < -1) return error.InvalidNullCount;
        if (self.null_count > 0) {
            if (self.buffers.len == 0 or self.buffers[0].isEmpty()) return error.InvalidNullCount;
        }

        if (self.null_count >= 0 and self.buffers.len > 0 and !self.buffers[0].isEmpty()) {
            const validity_bitmap = ValidityBitmap.fromBuffer(self.buffers[0], self.offset + self.length);
            var count: usize = 0;
            var i: usize = 0;
            while (i < self.length) : (i += 1) {
                if (!validity_bitmap.isValid(self.offset + i)) count += 1;
            }
            if (count != @as(usize, @intCast(self.null_count))) return error.InvalidNullCount;
        }

        for (self.children) |child| try child.validateFull();
        if (self.dictionary) |dict| try dict.validateFull();
    }
};

test "array data slice updates offset and length" {
    const dtype = DataType{ .int32 = {} };
    const data = ArrayData{
        .data_type = dtype,
        .length = 10,
        .offset = 2,
        .null_count = 0,
        .buffers = &[_]Buffer{Buffer.empty},
    };

    const sliced = data.slice(3, 4);
    try std.testing.expectEqual(@as(usize, 4), sliced.length);
    try std.testing.expectEqual(@as(usize, 5), sliced.offset);
    try std.testing.expectEqual(@as(isize, 0), sliced.null_count);
}

test "array data nullCount caches when unknown" {
    const dtype = DataType{ .int32 = {} };
    var validity: [1]u8 = .{0b0000_1101};
    var data = ArrayData{
        .data_type = dtype,
        .length = 4,
        .null_count = -1,
        .buffers = &[_]Buffer{Buffer.fromSlice(validity[0..])},
    };

    try std.testing.expectEqual(@as(usize, 1), data.nullCount());
    try std.testing.expectEqual(@as(isize, 1), data.null_count);
    try std.testing.expect(data.hasNulls());
}

test "array data hasNulls handles no validity" {
    const dtype = DataType{ .int32 = {} };
    const data = ArrayData{
        .data_type = dtype,
        .length = 3,
        .null_count = -1,
        .buffers = &[_]Buffer{Buffer.empty},
    };

    try std.testing.expect(!data.hasNulls());
}

test "array data slice on unknown null count" {
    const dtype = DataType{ .int32 = {} };
    const data = ArrayData{
        .data_type = dtype,
        .length = 6,
        .offset = 1,
        .null_count = -1,
        .buffers = &[_]Buffer{Buffer.empty},
    };

    const sliced = data.slice(2, 3);
    try std.testing.expectEqual(@as(usize, 3), sliced.length);
    try std.testing.expectEqual(@as(usize, 3), sliced.offset);
    try std.testing.expectEqual(@as(isize, -1), sliced.null_count);
}

test "array data validateLayout accepts primitive" {
    const dtype = DataType{ .int32 = {} };
    var values_bytes: [3 * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(values_bytes[0..], std.mem.sliceAsBytes(&[_]i32{ 1, 2, 3 }));
    const data = ArrayData{
        .data_type = dtype,
        .length = 3,
        .null_count = 0,
        .buffers = &[_]Buffer{ Buffer.empty, Buffer.fromSlice(values_bytes[0..]) },
    };

    try data.validateLayout();
    try data.validateFull();
}

test "array data validateLayout catches invalid offsets" {
    const dtype = DataType{ .binary = {} };
    const offsets = [_]i32{ 0, 4, 2 };
    var offset_bytes: [offsets.len * @sizeOf(i32)]u8 align(buffer.ALIGNMENT) = undefined;
    @memcpy(offset_bytes[0..], std.mem.sliceAsBytes(offsets[0..]));
    const data = ArrayData{
        .data_type = dtype,
        .length = 2,
        .buffers = &[_]Buffer{
            Buffer.empty,
            Buffer.fromSlice(offset_bytes[0..]),
            Buffer.fromSlice("zzzz"),
        },
    };

    try std.testing.expectError(error.InvalidOffsets, data.validateLayout());
}
