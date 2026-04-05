const std = @import("std");
const array_data = @import("array_data.zig");
const buffer = @import("../buffer.zig");

pub const ArrayData = array_data.ArrayData;
pub const SharedBuffer = buffer.SharedBuffer;

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

    pub fn fromOwned(allocator: std.mem.Allocator, layout: ArrayData) !ArrayRef {
        const node = try allocator.create(ArrayNode);
        node.* = .{
            .allocator = allocator,
            .ref_count = std.atomic.Value(u32).init(1),
            .data = layout,
        };
        return .{ .node = node };
    }

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
