const std = @import("std");

pub const Error = error{
    InvalidMetadata,
};

fn readLe(comptime T: type, buf: []const u8, offset: usize) Error!T {
    const size = @sizeOf(T);
    const end = std.math.add(usize, offset, size) catch return error.InvalidMetadata;
    if (end > buf.len) return error.InvalidMetadata;
    return std.mem.readInt(T, @ptrCast(buf[offset..end]), .little);
}

pub fn readU8(buf: []const u8, offset: usize) Error!u8 {
    if (offset >= buf.len) return error.InvalidMetadata;
    return buf[offset];
}

pub fn readU16(buf: []const u8, offset: usize) Error!u16 {
    return readLe(u16, buf, offset);
}

pub fn readI16(buf: []const u8, offset: usize) Error!i16 {
    return readLe(i16, buf, offset);
}

pub fn readU32(buf: []const u8, offset: usize) Error!u32 {
    return readLe(u32, buf, offset);
}

pub fn readI32(buf: []const u8, offset: usize) Error!i32 {
    return readLe(i32, buf, offset);
}

pub fn readI64(buf: []const u8, offset: usize) Error!i64 {
    return readLe(i64, buf, offset);
}

pub const Table = struct {
    buf: []const u8,
    object_start: usize,
    vtable_start: usize,
    vtable_len: usize,
    object_len: usize,

    /// Return absolute field offset in the underlying buffer.
    pub fn fieldAbsOffset(self: Table, field_id: usize) ?usize {
        const field_slot_start = std.math.add(usize, 4, field_id * 2) catch return null;
        if (field_slot_start + 2 > self.vtable_len) return null;

        const rel = readU16(self.buf, self.vtable_start + field_slot_start) catch return null;
        if (rel == 0) return null;

        const abs = std.math.add(usize, self.object_start, rel) catch return null;
        if (abs >= self.buf.len) return null;
        return abs;
    }

    pub fn readFieldU8(self: Table, field_id: usize) ?u8 {
        const abs = self.fieldAbsOffset(field_id) orelse return null;
        return readU8(self.buf, abs) catch null;
    }

    pub fn readFieldI16(self: Table, field_id: usize) ?i16 {
        const abs = self.fieldAbsOffset(field_id) orelse return null;
        return readI16(self.buf, abs) catch null;
    }

    pub fn readFieldI64(self: Table, field_id: usize) ?i64 {
        const abs = self.fieldAbsOffset(field_id) orelse return null;
        return readI64(self.buf, abs) catch null;
    }
};

pub fn rootTable(buf: []const u8) Error!Table {
    if (buf.len < 8) return error.InvalidMetadata;

    const root_off_u32 = try readU32(buf, 0);
    const root_off = std.math.cast(usize, root_off_u32) orelse return error.InvalidMetadata;
    if (root_off > buf.len - 4) return error.InvalidMetadata;

    const vtable_rel_i32 = try readI32(buf, root_off);
    if (vtable_rel_i32 <= 0) return error.InvalidMetadata;
    const vtable_rel = std.math.cast(usize, vtable_rel_i32) orelse return error.InvalidMetadata;
    if (vtable_rel > root_off) return error.InvalidMetadata;

    const vtable_start = root_off - vtable_rel;
    if (vtable_start > buf.len - 4) return error.InvalidMetadata;

    const vtable_len_u16 = try readU16(buf, vtable_start);
    const object_len_u16 = try readU16(buf, vtable_start + 2);
    if (vtable_len_u16 < 4) return error.InvalidMetadata;
    if (object_len_u16 == 0) return error.InvalidMetadata;

    const vtable_len = @as(usize, vtable_len_u16);
    const object_len = @as(usize, object_len_u16);
    if (vtable_start + vtable_len > buf.len) return error.InvalidMetadata;
    if (root_off + object_len > buf.len) return error.InvalidMetadata;

    return .{
        .buf = buf,
        .object_start = root_off,
        .vtable_start = vtable_start,
        .vtable_len = vtable_len,
        .object_len = object_len,
    };
}

test "fbs_lite reader rejects out-of-range root" {
    const malformed = [_]u8{
        0x10, 0x00, 0x00, 0x00,
        0xff, 0xff, 0xff, 0xff,
    };
    try std.testing.expectError(error.InvalidMetadata, rootTable(malformed[0..]));
}
