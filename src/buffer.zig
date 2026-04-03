const std = @import("std");

// Default byte alignment used for Arrow-compatible buffer allocations.
pub const ALIGNMENT: usize = 64; // 64-byte alignment for buffers

// Round a requested size up to the next buffer alignment boundary.
pub fn alignedSize(size: usize) usize {
    return (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
}

// Buffer provides an immutable byte view used by Arrow values and metadata.
pub const Buffer = struct {
    data: []const u8,

    // Shared empty buffer instance for zero-length views.
    pub const empty: Buffer = .{ .data = &.{} };

    // Build a buffer view from an existing immutable byte slice.
    pub fn init(data: []const u8) Buffer {
        return .{ .data = data };
    }

    // Alias of init for call sites that prefer slice-oriented naming.
    pub fn fromSlice(data: []const u8) Buffer {
        return .{ .data = data };
    }

    // Return the number of bytes visible through this buffer.
    pub fn len(self: Buffer) usize {
        return self.data.len;
    }

    // Report whether the buffer contains no visible bytes.
    pub fn isEmpty(self: Buffer) bool {
        return self.data.len == 0;
    }

    // Return a sub-slice view over the buffer without copying data.
    pub fn slice(self: Buffer, start: usize, end: usize) []const u8 {
        return self.data[start..end];
    }

    // Interpret the buffer as a typed slice using the configured byte alignment.
    pub fn typedSlice(self: Buffer, comptime T: type) []const T {
        const aligned: []align(ALIGNMENT) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(T, aligned);
    }
};

// MutableBuffer owns writable storage that can later be exposed as a Buffer view.
pub const MutableBuffer = struct {
    data: []align(ALIGNMENT) u8,
    allocator: std.mem.Allocator,

    // Allocate a zero-initialized writable buffer with Arrow-style alignment.
    pub fn init(allocator: std.mem.Allocator, capacity: usize) !MutableBuffer {
        const actualCapacity = alignedSize(if (capacity == 0) ALIGNMENT else capacity);
        const data = try allocator.alignedAlloc(u8, ALIGNMENT, actualCapacity);
        @memset(data, 0);
        return .{ .data = data, .allocator = allocator };
    }

    // Release the storage owned by this mutable buffer.
    pub fn deinit(self: *MutableBuffer) void {
        self.allocator.free(self.data);
    }

    // Return the currently allocated byte length.
    pub fn len(self: MutableBuffer) usize {
        return self.data.len;
    }

    // Report whether the allocated byte length is zero.
    pub fn isEmpty(self: MutableBuffer) bool {
        return self.data.len == 0;
    }

    // Expose the first used bytes as an immutable Buffer view.
    pub fn toBuffer(self: MutableBuffer, used: usize) Buffer {
        return Buffer.init(self.data[0..used]);
    }

    // Interpret the mutable storage as a typed slice.
    pub fn typedSlice(self: MutableBuffer, comptime T: type) []T {
        return std.mem.bytesAsSlice(T, self.data);
    }

    // Reallocate the buffer while preserving the overlapping byte range.
    pub fn resize(self: *MutableBuffer, newSize: usize) !void {
        const actualSize = alignedSize(newSize);
        const newData = try self.allocator.alignedAlloc(u8, ALIGNMENT, actualSize);
        const copySize = @min(self.data.len, actualSize);
        @memcpy(newData[0..copySize], self.data[0..copySize]);
        if (actualSize > copySize) @memset(newData[copySize..], 0);

        self.allocator.free(self.data);
        self.data = newData;
    }
};

test "buffer exposes immutable view" {
    const data = "arrow";
    const buffer = Buffer.init(data);

    try std.testing.expectEqual(@as(usize, 5), buffer.len());
    try std.testing.expect(!buffer.isEmpty());
    try std.testing.expectEqualStrings(data, buffer.slice(0, buffer.len()));
}

test "buffer empty constant is reusable" {
    const buffer = Buffer.empty;

    try std.testing.expectEqual(@as(usize, 0), buffer.len());
    try std.testing.expect(buffer.isEmpty());
    try std.testing.expectEqualStrings("", buffer.slice(0, buffer.len()));
}

test "mutable buffer exposes initialized data" {
    var backing: [alignedSize(5)]u8 align(ALIGNMENT) = [_]u8{0} ** alignedSize(5);
    var buffer = MutableBuffer{
        .data = backing[0..],
        .allocator = std.testing.allocator,
    };

    try std.testing.expectEqual(alignedSize(@as(usize, 5)), buffer.len());
    try std.testing.expect(!buffer.isEmpty());

    @memcpy(buffer.data[0..5], "arrow");
    try std.testing.expectEqualStrings("arrow", buffer.toBuffer(5).slice(0, 5));
}
