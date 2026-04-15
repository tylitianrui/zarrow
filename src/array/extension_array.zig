const std = @import("std");
const datatype = @import("../datatype.zig");
const array_data = @import("array_data.zig");
const array_ref = @import("array_ref.zig");
const builder_state = @import("builder_state.zig");

// Extension array view/builder wrappers.

const DataType = datatype.DataType;
pub const ExtensionType = datatype.ExtensionType;
const ArrayData = array_data.ArrayData;
const ArrayRef = array_ref.ArrayRef;
const BuilderState = builder_state.BuilderState;

pub const ExtensionArray = struct {
    data: *const ArrayData,

    /// Return the logical length.
    pub fn len(self: ExtensionArray) usize {
        return self.data.length;
    }

    /// Check whether the element at index is null.
    pub fn isNull(self: ExtensionArray, i: usize) bool {
        return self.data.isNull(i);
    }

    /// Return the declared extension descriptor.
    pub fn extension(self: ExtensionArray) ExtensionType {
        std.debug.assert(self.data.data_type == .extension);
        return self.data.data_type.extension;
    }
};

pub const ExtensionBuilder = struct {
    allocator: std.mem.Allocator,
    extension: ExtensionType,
    state: BuilderState = .ready,

    const Self = @This();
    pub const BuilderError = error{ AlreadyFinished, NotFinished, StorageTypeMismatch };

    /// Initialize and return a new instance.
    pub fn init(allocator: std.mem.Allocator, extension: ExtensionType) !Self {
        return .{
            .allocator = allocator,
            .extension = extension,
        };
    }

    /// Release resources owned by this instance.
    pub fn deinit(_: *Self) void {}

    /// Reset state while retaining reusable capacity when possible.
    pub fn reset(self: *Self) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.state = .ready;
    }

    /// Clear state and release reusable buffers when required.
    pub fn clear(self: *Self) BuilderError!void {
        if (self.state != .finished) return BuilderError.NotFinished;
        self.state = .ready;
    }

    /// Finalize by wrapping an already-built storage array as extension.
    pub fn finish(self: *Self, storage: ArrayRef) !ArrayRef {
        if (self.state == .finished) return BuilderError.AlreadyFinished;
        if (!datatype.dataTypeEql(storage.data().data_type, self.extension.storage_type.*)) {
            return BuilderError.StorageTypeMismatch;
        }

        var out = storage.data().*;
        out.data_type = .{ .extension = self.extension };

        self.state = .finished;
        return ArrayRef.fromBorrowed(self.allocator, out);
    }

    /// Finalize output and then reset builder state for reuse.
    pub fn finishReset(self: *Self, storage: ArrayRef) !ArrayRef {
        const finished_ref = try self.finish(storage);
        try self.reset();
        return finished_ref;
    }
};

test "extension builder wraps storage array and preserves layout" {
    const allocator = std.testing.allocator;

    var storage_builder = try @import("primitive_array.zig").PrimitiveBuilder(i32, DataType{ .int32 = {} }).init(allocator, 2);
    defer storage_builder.deinit();
    try storage_builder.append(7);
    try storage_builder.appendNull();
    try storage_builder.append(11);
    var storage_ref = try storage_builder.finish();
    defer storage_ref.release();

    const storage_type = DataType{ .int32 = {} };
    var extension_builder = try ExtensionBuilder.init(allocator, .{
        .name = "com.example.int32_ext",
        .storage_type = &storage_type,
        .metadata = "v1",
    });
    defer extension_builder.deinit();

    var ext_ref = try extension_builder.finish(storage_ref);
    defer ext_ref.release();

    const ext = ExtensionArray{ .data = ext_ref.data() };
    try std.testing.expectEqual(@as(usize, 3), ext.len());
    try std.testing.expect(ext.isNull(1));
    try std.testing.expect(ext_ref.data().data_type == .extension);
    try std.testing.expectEqualStrings("com.example.int32_ext", ext.extension().name);
    try std.testing.expectEqualStrings("v1", ext.extension().metadata.?);
    try std.testing.expect(ext.extension().storage_type.* == .int32);

    const values = @import("primitive_array.zig").PrimitiveArray(i32){ .data = ext_ref.data() };
    try std.testing.expectEqual(@as(i32, 7), values.value(0));
    try std.testing.expectEqual(@as(i32, 11), values.value(2));
}
