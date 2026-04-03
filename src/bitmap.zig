const std = @import("std");
const Buffer = @import("buffer.zig").Buffer;
const MutableBuffer = @import("buffer.zig").MutableBuffer;

// Return the number of bytes required to store the requested bit count.
pub fn byteLength(bit_length: usize) usize {
    return (bit_length + 7) >> 3;
}

fn bitMask(bit_index: usize) u8 {
    return @as(u8, 1) << @as(u3, @intCast(bit_index & 7));
}

pub fn bitIsSet(data: []const u8, bit_index: usize) bool {
    const byte = data[bit_index >> 3];
    return (byte & bitMask(bit_index)) != 0;
}

// Set a single bit in-place using Arrow's least-significant-bit-first bit order.
pub fn setBit(data: []u8, bit_index: usize) void {
    data[bit_index >> 3] |= bitMask(bit_index);
}

pub fn clearBit(data: []u8, bit_index: usize) void {
    data[bit_index >> 3] &= ~bitMask(bit_index);
}

pub fn writeBit(data: []u8, bit_index: usize, value: bool) void {
    if (value) setBit(data, bit_index) else clearBit(data, bit_index);
}

pub const ValidityBitmap = struct {
    data: []const u8,
    bit_len: usize,

    pub fn fromBuffer(buf: Buffer, bit_len: usize) ValidityBitmap {
        return .{ .data = buf.data, .bit_len = bit_len };
    }
    pub fn isValid(self: ValidityBitmap, i: usize) bool {
        std.debug.assert(i < self.bit_len);
        return bitIsSet(self.data, i);
    }
    pub fn isNull(self: ValidityBitmap, i: usize) bool {
        return !self.isValid(i);
    }
    pub fn countValid(self: ValidityBitmap) usize {
        return byteLength(self.bit_len);
    }
    pub fn countNulls(self: ValidityBitmap) usize {
        return self.bit_len - self.countValid();
    }
};

// MutableValidityBitmap owns writable storage for building or editing Arrow validity bits.
pub const MutableValidityBitmap = struct {
    storage: MutableBuffer,
    bit_len: usize,

    // Allocate a bitmap with all values marked valid.
    pub fn initAllValid(allocator: std.mem.Allocator, len: usize) !MutableValidityBitmap {
        return initFilled(allocator, len, true);
    }

    // Allocate a bitmap with all values marked null.
    pub fn initAllNull(allocator: std.mem.Allocator, bit_len: usize) !MutableValidityBitmap {
        return initFilled(allocator, bit_len, false);
    }

    fn initFilled(allocator: std.mem.Allocator, bit_len: usize, valid: bool) !MutableValidityBitmap {
        const used_bytes = byteLength(bit_len);
        var storage = try MutableBuffer.init(allocator, used_bytes);
        if (used_bytes > 0) {
            @memset(storage.data[0..used_bytes], if (valid) 0xff else 0x00);
            clearTrailingBits(storage.data[0..used_bytes], bit_len, valid);
        }

        return .{
            .storage = storage,
            .bit_len = bit_len,
        };
    }

    // Release the owned storage.
    pub fn deinit(self: *MutableValidityBitmap) void {
        self.storage.deinit();
    }

    // Return the number of logical bits tracked by this bitmap.
    pub fn bitLength(self: MutableValidityBitmap) usize {
        return self.bit_len;
    }

    // Mark the indexed value as valid.
    pub fn setValid(self: *MutableValidityBitmap, index: usize) void {
        std.debug.assert(index < self.bit_len);
        setBit(self.storage.data[0..byteLength(self.bit_len)], index);
    }

    // Mark the indexed value as null.
    pub fn setNull(self: *MutableValidityBitmap, index: usize) void {
        std.debug.assert(index < self.bit_len);

        const byte_index = index / 8;
        const mask = bitMask(index);
        self.storage.data[byte_index] &= ~mask;
    }

    // Read the indexed validity bit from the mutable bitmap.
    pub fn isValid(self: MutableValidityBitmap, index: usize) bool {
        return self.asBitmap().isValid(index);
    }

    // Expose the logical bitmap bytes as an immutable buffer view.
    pub fn toBuffer(self: MutableValidityBitmap) Buffer {
        return self.storage.toBuffer(byteLength(self.bit_len));
    }

    // Expose a read-only bitmap view over the current mutable storage.
    pub fn asBitmap(self: MutableValidityBitmap) ValidityBitmap {
        return ValidityBitmap.init(self.toBuffer(), self.bit_len);
    }
};

fn clearTrailingBits(bytes: []u8, len: usize, valid: bool) void {
    if (len == 0 or bytes.len == 0) return;

    const remainder = len & 7;
    if (remainder == 0) return;

    const last = bytes.len - 1;
    const keep_mask = (@as(u8, 1) << @as(u3, @intCast(remainder))) - 1;
    bytes[last] &= keep_mask;
    if (valid) bytes[last] |= keep_mask;
}

test "validity bitmap reads arrow bit ordering" {
    const raw = [_]u8{0b00001101};
    const bitmap = ValidityBitmap.init(Buffer.init(raw[0..]), 4);

    try std.testing.expect(bitmap.isValid(0));
    try std.testing.expect(!bitmap.isValid(1));
    try std.testing.expect(bitmap.isValid(2));
    try std.testing.expect(bitmap.isValid(3));
    try std.testing.expectEqual(@as(usize, 1), bitmap.nullCount());
}

test "empty validity buffer means all valid" {
    const bitmap = ValidityBitmap.init(Buffer.empty, 3);

    try std.testing.expect(bitmap.isValid(0));
    try std.testing.expect(bitmap.isValid(1));
    try std.testing.expect(bitmap.isValid(2));
    try std.testing.expect(bitmap.allValid());
}

test "mutable validity bitmap toggles bits" {
    var bitmap = try MutableValidityBitmap.initAllNull(std.testing.allocator, 10);
    defer bitmap.deinit();

    bitmap.setValid(1);
    bitmap.setValid(8);
    bitmap.setValid(9);
    bitmap.setNull(8);

    const view = bitmap.asBitmap();
    try std.testing.expect(view.isNull(0));
    try std.testing.expect(view.isValid(1));
    try std.testing.expect(view.isNull(8));
    try std.testing.expect(view.isValid(9));
    try std.testing.expectEqual(@as(usize, 8), view.nullCount());
}

test "bitIsSet reads bits using arrow bit order" {
    const data = [_]u8{
        0b10000101,
        0b00000010,
    };

    try std.testing.expect(bitIsSet(data[0..], 0));
    try std.testing.expect(!bitIsSet(data[0..], 1));
    try std.testing.expect(bitIsSet(data[0..], 2));
    try std.testing.expect(!bitIsSet(data[0..], 6));
    try std.testing.expect(bitIsSet(data[0..], 7));
    try std.testing.expect(!bitIsSet(data[0..], 8));
    try std.testing.expect(bitIsSet(data[0..], 9));
}

test "setBit writes bits using arrow bit order" {
    var data = [_]u8{ 0, 0 };

    setBit(data[0..], 0);
    setBit(data[0..], 7);
    setBit(data[0..], 9);

    try std.testing.expectEqual(@as(u8, 0b10000001), data[0]);
    try std.testing.expectEqual(@as(u8, 0b00000010), data[1]);
    try std.testing.expect(bitIsSet(data[0..], 0));
    try std.testing.expect(bitIsSet(data[0..], 7));
    try std.testing.expect(bitIsSet(data[0..], 9));
}
