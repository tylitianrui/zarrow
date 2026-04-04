const std = @import("std");
const bitmap = @import("../bitmap.zig");
const buffer = @import("../buffer.zig");
const datatype = @import("../datatype.zig");

pub const Buffer = buffer.Buffer;
pub const ValidityBitmap = bitmap.ValidityBitmap;
pub const DataType = datatype.DataType;

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
