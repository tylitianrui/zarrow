const std = @import("std");
const array_data = @import("array_data.zig");
const buffer = @import("../buffer.zig");

pub const ArrayData = array_data.ArrayData;
pub const SharedBuffer = buffer.SharedBuffer;

const empty_buffers: [0]SharedBuffer = .{};
const empty_children: [0]ArrayRef = .{};

const ArrayNode = struct {
    allocator: std.mem.Allocator,
    ref_count: std.atomic.Value(u32),
    data: ArrayData,
};

pub const ArrayRef = struct {
    node: *ArrayNode,

    pub fn retain(self: ArrayRef) ArrayRef {
        _ = self.node.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn release(self: *ArrayRef) void {
        if (self.node.ref_count.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.node.allocator;

        for (self.node.data.buffers) |buf| {
            var owned = buf;
            owned.release();
        }
        allocator.free(self.node.data.buffers);

        for (self.node.data.children) |child| {
            var owned = child;
            owned.release();
        }
        allocator.free(self.node.data.children);

        if (self.node.data.dictionary) |dict| {
            var owned = dict;
            owned.release();
        }

        allocator.destroy(self.node);
    }

    pub fn data(self: ArrayRef) *const ArrayData {
        return &self.node.data;
    }

    pub fn slice(self: ArrayRef, offset: usize, length: usize) !ArrayRef {
        var sliced = self.node.data.slice(offset, length);

        const allocator = self.node.allocator;
        const buffers = try allocator.alloc(SharedBuffer, sliced.buffers.len);
        for (sliced.buffers, 0..) |buf, i| {
            buffers[i] = buf.retain();
        }

        const children = try allocator.alloc(ArrayRef, sliced.children.len);
        for (sliced.children, 0..) |child, i| {
            children[i] = child.retain();
        }

        var dict_ref: ?ArrayRef = null;
        if (sliced.dictionary) |dict| dict_ref = dict.retain();

        sliced.buffers = buffers;
        sliced.children = children;
        sliced.dictionary = dict_ref;

        return ArrayRef.fromOwned(allocator, sliced);
    }

    /// Create an ArrayRef that takes ownership of the ArrayData layout.
    ///
    /// Requirements:
    /// - buffers/children/dictionary must be allocator-owned and releasable.
    /// - Use `fromBorrowed` if the layout borrows or uses stack/static slices.
    /// - This is an unsafe entry point for callers who already normalized slices.
    pub fn fromOwnedUnsafe(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        if (layout.buffers.len == 0) {
            std.debug.assert(@intFromPtr(layout.buffers.ptr) != @intFromPtr(empty_buffers[0..].ptr));
        }
        if (layout.children.len == 0) {
            std.debug.assert(@intFromPtr(layout.children.ptr) != @intFromPtr(empty_children[0..].ptr));
        }

        const node = try allocator.create(ArrayNode);
        node.* = .{
            .allocator = allocator,
            .ref_count = std.atomic.Value(u32).init(1),
            .data = layout,
        };
        return .{ .node = node };
    }

    /// Create an ArrayRef that owns the layout and normalizes empty slices.
    pub fn fromOwned(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        var owned = layout;
        if (owned.buffers.len == 0 and @intFromPtr(owned.buffers.ptr) == @intFromPtr(empty_buffers[0..].ptr)) {
            owned.buffers = try allocator.alloc(SharedBuffer, 0);
        }
        if (owned.children.len == 0 and @intFromPtr(owned.children.ptr) == @intFromPtr(empty_children[0..].ptr)) {
            owned.children = try allocator.alloc(ArrayRef, 0);
        }
        std.debug.assert(owned.buffers.len != 0 or @intFromPtr(owned.buffers.ptr) != @intFromPtr(empty_buffers[0..].ptr));
        std.debug.assert(owned.children.len != 0 or @intFromPtr(owned.children.ptr) != @intFromPtr(empty_children[0..].ptr));
        return fromOwnedUnsafe(allocator, owned);
    }

    /// Create an ArrayRef by retaining shared buffers and child refs.
    pub fn fromBorrowed(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        const buffers = try allocator.alloc(SharedBuffer, layout.buffers.len);
        for (layout.buffers, 0..) |buf, i| {
            buffers[i] = buf.retain();
        }

        const children = try allocator.alloc(ArrayRef, layout.children.len);
        for (layout.children, 0..) |child, i| {
            children[i] = child.retain();
        }

        var dict_ref: ?ArrayRef = null;
        if (layout.dictionary) |dict| dict_ref = dict.retain();

        var owned = layout;
        owned.buffers = buffers;
        owned.children = children;
        owned.dictionary = dict_ref;

        return ArrayRef.fromOwned(allocator, owned);
    }
};

test "array ref release handles empty slices" {
    const allocator = std.testing.allocator;
    const dtype = array_data.DataType{ .int32 = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 0,
        .buffers = &[_]SharedBuffer{},
        .children = &[_]ArrayRef{},
    };

    var array_ref = try ArrayRef.fromOwned(allocator, layout);
    array_ref.release();
}

test "array ref slice retains buffers" {
    const allocator = std.testing.allocator;

    var owned = try buffer.OwnedBuffer.init(allocator, 4);
    defer owned.deinit();
    @memcpy(owned.data[0..4], "data");
    var shared = try owned.toShared(4);
    defer shared.release();

    const buffers = try allocator.alloc(SharedBuffer, 1);
    buffers[0] = shared.retain();

    const dtype = array_data.DataType{ .binary = {} };
    const layout = ArrayData{
        .data_type = dtype,
        .length = 1,
        .buffers = buffers,
    };

    var array_ref = try ArrayRef.fromOwned(allocator, layout);
    defer array_ref.release();

    var sliced = try array_ref.slice(0, 1);
    defer sliced.release();

    const view = sliced.data();
    try std.testing.expectEqualStrings("data", view.buffers[0].data);
}
