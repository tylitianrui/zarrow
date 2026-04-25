const std = @import("std");
const datatype = @import("datatype.zig");
const array_ref_mod = @import("array/array_ref.zig");
const array_data = @import("array/array_data.zig");
const array_mod = @import("array/array.zig");
const buffer = @import("buffer.zig");
const bitmap = @import("bitmap.zig");

pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref_mod.ArrayRef;
pub const ArrayData = array_data.ArrayData;

pub const ConcatArrayError = error{ OutOfMemory, InvalidInput, UnsupportedType };
pub const Error = ConcatArrayError || array_data.ValidationError;

pub fn concatArrayRefs(
    allocator: std.mem.Allocator,
    data_type: DataType,
    parts: []const ArrayRef,
) Error!ArrayRef {
    if (parts.len == 0) return error.InvalidInput;

    for (parts) |part| {
        if (!datatype.dataTypeEql(part.data().data_type, data_type)) return error.InvalidInput;
    }

    var acc = try parts[0].slice(0, 0);
    errdefer acc.release();

    for (parts) |part| {
        const merged = try concatTwoArrayRefs(allocator, acc, part);
        acc.release();
        acc = merged;
    }

    return acc;
}

fn concatTwoArrayRefs(
    allocator: std.mem.Allocator,
    left: ArrayRef,
    right: ArrayRef,
) Error!ArrayRef {
    if (!datatype.dataTypeEql(left.data().data_type, right.data().data_type)) return error.InvalidInput;

    const dt = left.data().data_type;
    const layout_dt = storageDataType(dt);
    var merged_storage = try switch (layout_dt) {
        .null => try concatNullArray(allocator, layout_dt, left.data(), right.data()),
        .bool => try concatBooleanArray(allocator, layout_dt, left.data(), right.data()),
        .uint8, .int8 => try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), 1),
        .uint16, .int16, .half_float => try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), 2),
        .uint32, .int32, .float, .date32, .time32, .interval_months, .decimal32 => try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), 4),
        .uint64, .int64, .double, .date64, .time64, .timestamp, .duration, .interval_day_time, .decimal64 => try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), 8),
        .decimal128, .interval_month_day_nano => try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), 16),
        .decimal256 => try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), 32),
        .fixed_size_binary => |fsb| blk: {
            const byte_width = std.math.cast(usize, fsb.byte_width) orelse return error.InvalidInput;
            break :blk try concatFixedWidthArray(allocator, layout_dt, left.data(), right.data(), byte_width);
        },
        .string, .binary => try concatVariableBinaryArrayI32(allocator, layout_dt, left.data(), right.data()),
        .large_string, .large_binary => try concatVariableBinaryArrayI64(allocator, layout_dt, left.data(), right.data()),
        .string_view => try concatStringViewArray(allocator, left.data(), right.data()),
        .binary_view => try concatBinaryViewArray(allocator, left.data(), right.data()),
        .list, .map => try concatListLikeArrayI32(allocator, layout_dt, left.data(), right.data()),
        .large_list => try concatListLikeArrayI64(allocator, layout_dt, left.data(), right.data()),
        .fixed_size_list => try concatFixedSizeListArray(allocator, layout_dt, left.data(), right.data()),
        .struct_ => try concatStructArray(allocator, layout_dt, left.data(), right.data()),
        else => error.UnsupportedType,
    };

    if (datatype.dataTypeEql(dt, layout_dt)) return merged_storage;
    const retagged = try retagArrayRefDataType(allocator, merged_storage, dt);
    merged_storage.release();
    return retagged;
}

fn storageDataType(dt: DataType) DataType {
    return switch (dt) {
        .extension => |ext| storageDataType(ext.storage_type.*),
        else => dt,
    };
}

fn retagArrayRefDataType(allocator: std.mem.Allocator, src: ArrayRef, out_dt: DataType) error{OutOfMemory}!ArrayRef {
    var out = src.data().*;
    out.data_type = out_dt;
    return ArrayRef.fromBorrowed(allocator, out);
}

fn concatListLikeArrayI32(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);
    if (left.children.len != 1 or right.children.len != 1) return error.InvalidInput;

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i32) catch return error.InvalidInput;
    const right_offsets = right.buffers[1].typedSlice(i32) catch return error.InvalidInput;

    const left_last_index = std.math.add(usize, left.offset, left.length) catch return error.InvalidInput;
    const right_last_index = std.math.add(usize, right.offset, right.length) catch return error.InvalidInput;
    if (left_last_index >= left_offsets.len or right_last_index >= right_offsets.len) return error.InvalidInput;

    const left_base = left_offsets[left.offset];
    const left_last = left_offsets[left_last_index];
    const right_base = right_offsets[right.offset];
    const right_last = right_offsets[right_last_index];
    if (left_base < 0 or left_last < left_base or right_base < 0 or right_last < right_base) return error.InvalidInput;

    const offsets_len = std.math.add(usize, total_len, 1) catch return error.InvalidInput;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i32));
    var out_offsets = offsets_owned.typedSlice(i32)[0..offsets_len];
    out_offsets[0] = 0;

    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const rel = std.math.sub(i32, left_offsets[left.offset + i + 1], left_base) catch return error.InvalidInput;
        out_offsets[i + 1] = rel;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_i = i - left.length;
        const rel = std.math.sub(i32, right_offsets[right.offset + right_i + 1], right_base) catch return error.InvalidInput;
        out_offsets[i + 1] = std.math.add(i32, left_prefix, rel) catch return error.InvalidInput;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i32));
    errdefer offsets.release();

    const left_child_start = std.math.cast(usize, left_base) orelse return error.InvalidInput;
    const left_child_end = std.math.cast(usize, left_last) orelse return error.InvalidInput;
    const right_child_start = std.math.cast(usize, right_base) orelse return error.InvalidInput;
    const right_child_end = std.math.cast(usize, right_last) orelse return error.InvalidInput;
    const left_child_len = std.math.sub(usize, left_child_end, left_child_start) catch return error.InvalidInput;
    const right_child_len = std.math.sub(usize, right_child_end, right_child_start) catch return error.InvalidInput;

    var left_child = try left.children[0].slice(left_child_start, left_child_len);
    defer left_child.release();
    var right_child = try right.children[0].slice(right_child_start, right_child_len);
    defer right_child.release();
    var merged_child = try concatTwoArrayRefs(allocator, left_child, right_child);
    errdefer merged_child.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    const children = try allocator.alloc(ArrayRef, 1);
    errdefer allocator.free(children);
    children[0] = merged_child;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatListLikeArrayI64(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);
    if (left.children.len != 1 or right.children.len != 1) return error.InvalidInput;

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i64) catch return error.InvalidInput;
    const right_offsets = right.buffers[1].typedSlice(i64) catch return error.InvalidInput;

    const left_last_index = std.math.add(usize, left.offset, left.length) catch return error.InvalidInput;
    const right_last_index = std.math.add(usize, right.offset, right.length) catch return error.InvalidInput;
    if (left_last_index >= left_offsets.len or right_last_index >= right_offsets.len) return error.InvalidInput;

    const left_base = left_offsets[left.offset];
    const left_last = left_offsets[left_last_index];
    const right_base = right_offsets[right.offset];
    const right_last = right_offsets[right_last_index];
    if (left_base < 0 or left_last < left_base or right_base < 0 or right_last < right_base) return error.InvalidInput;

    const offsets_len = std.math.add(usize, total_len, 1) catch return error.InvalidInput;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i64));
    var out_offsets = offsets_owned.typedSlice(i64)[0..offsets_len];
    out_offsets[0] = 0;

    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const rel = std.math.sub(i64, left_offsets[left.offset + i + 1], left_base) catch return error.InvalidInput;
        out_offsets[i + 1] = rel;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_i = i - left.length;
        const rel = std.math.sub(i64, right_offsets[right.offset + right_i + 1], right_base) catch return error.InvalidInput;
        out_offsets[i + 1] = std.math.add(i64, left_prefix, rel) catch return error.InvalidInput;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i64));
    errdefer offsets.release();

    const left_child_start = std.math.cast(usize, left_base) orelse return error.InvalidInput;
    const left_child_end = std.math.cast(usize, left_last) orelse return error.InvalidInput;
    const right_child_start = std.math.cast(usize, right_base) orelse return error.InvalidInput;
    const right_child_end = std.math.cast(usize, right_last) orelse return error.InvalidInput;
    const left_child_len = std.math.sub(usize, left_child_end, left_child_start) catch return error.InvalidInput;
    const right_child_len = std.math.sub(usize, right_child_end, right_child_start) catch return error.InvalidInput;

    var left_child = try left.children[0].slice(left_child_start, left_child_len);
    defer left_child.release();
    var right_child = try right.children[0].slice(right_child_start, right_child_len);
    defer right_child.release();
    var merged_child = try concatTwoArrayRefs(allocator, left_child, right_child);
    errdefer merged_child.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    const children = try allocator.alloc(ArrayRef, 1);
    errdefer allocator.free(children);
    children[0] = merged_child;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatFixedSizeListArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 1);
    try requireBufferCount(right, 1);
    if (left.children.len != 1 or right.children.len != 1) return error.InvalidInput;
    const list_size = std.math.cast(usize, dt.fixed_size_list.list_size) orelse return error.InvalidInput;
    if (list_size == 0) return error.InvalidInput;

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_child_start = std.math.mul(usize, left.offset, list_size) catch return error.InvalidInput;
    const left_child_len = std.math.mul(usize, left.length, list_size) catch return error.InvalidInput;
    const right_child_start = std.math.mul(usize, right.offset, list_size) catch return error.InvalidInput;
    const right_child_len = std.math.mul(usize, right.length, list_size) catch return error.InvalidInput;

    var left_child = try left.children[0].slice(left_child_start, left_child_len);
    defer left_child.release();
    var right_child = try right.children[0].slice(right_child_start, right_child_len);
    defer right_child.release();
    var merged_child = try concatTwoArrayRefs(allocator, left_child, right_child);
    errdefer merged_child.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 1);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    const children = try allocator.alloc(ArrayRef, 1);
    errdefer allocator.free(children);
    children[0] = merged_child;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatStructArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 1);
    try requireBufferCount(right, 1);
    if (left.children.len != dt.struct_.fields.len or right.children.len != dt.struct_.fields.len) return error.InvalidInput;

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const children = try allocator.alloc(ArrayRef, dt.struct_.fields.len);
    var child_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < child_count) : (i += 1) {
            var owned = children[i];
            owned.release();
        }
        allocator.free(children);
    }
    for (left.children, right.children, 0..) |left_child_ref, right_child_ref, idx| {
        var left_child = try sliceStructChildRef(left_child_ref, left);
        defer left_child.release();
        var right_child = try sliceStructChildRef(right_child_ref, right);
        defer right_child.release();
        children[idx] = try concatTwoArrayRefs(allocator, left_child, right_child);
        child_count += 1;
    }

    const buffers = try allocator.alloc(array_data.SharedBuffer, 1);
    errdefer allocator.free(buffers);
    buffers[0] = validity;

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn viewDataCapacityHint(data: *const ArrayData) usize {
    if (data.buffers.len >= 3) return data.buffers[2].len();
    return 0;
}

fn concatStringViewArray(
    allocator: std.mem.Allocator,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try left.validateLayout();
    try right.validateLayout();
    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const data_capacity = std.math.add(usize, viewDataCapacityHint(left), viewDataCapacityHint(right)) catch return error.InvalidInput;

    var builder = try array_mod.StringViewBuilder.init(allocator, total_len, data_capacity);
    defer builder.deinit();

    const left_view = array_mod.StringViewArray{ .data = left };
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        if (left_view.isNull(i)) {
            builder.appendNull() catch |err| return mapViewBuilderError(err);
        } else {
            builder.append(left_view.value(i)) catch |err| return mapViewBuilderError(err);
        }
    }

    const right_view = array_mod.StringViewArray{ .data = right };
    i = 0;
    while (i < right.length) : (i += 1) {
        if (right_view.isNull(i)) {
            builder.appendNull() catch |err| return mapViewBuilderError(err);
        } else {
            builder.append(right_view.value(i)) catch |err| return mapViewBuilderError(err);
        }
    }

    var out = builder.finish() catch |err| return mapViewBuilderError(err);
    errdefer out.release();
    try out.data().validateLayout();
    return out;
}

fn concatBinaryViewArray(
    allocator: std.mem.Allocator,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try left.validateLayout();
    try right.validateLayout();
    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const data_capacity = std.math.add(usize, viewDataCapacityHint(left), viewDataCapacityHint(right)) catch return error.InvalidInput;

    var builder = try array_mod.BinaryViewBuilder.init(allocator, total_len, data_capacity);
    defer builder.deinit();

    const left_view = array_mod.BinaryViewArray{ .data = left };
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        if (left_view.isNull(i)) {
            builder.appendNull() catch |err| return mapViewBuilderError(err);
        } else {
            builder.append(left_view.value(i)) catch |err| return mapViewBuilderError(err);
        }
    }

    const right_view = array_mod.BinaryViewArray{ .data = right };
    i = 0;
    while (i < right.length) : (i += 1) {
        if (right_view.isNull(i)) {
            builder.appendNull() catch |err| return mapViewBuilderError(err);
        } else {
            builder.append(right_view.value(i)) catch |err| return mapViewBuilderError(err);
        }
    }

    var out = builder.finish() catch |err| return mapViewBuilderError(err);
    errdefer out.release();
    try out.data().validateLayout();
    return out;
}

fn mapViewBuilderError(err: anyerror) ConcatArrayError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidInput,
    };
}

fn sliceStructChildRef(child_ref: ArrayRef, parent: *const ArrayData) Error!ArrayRef {
    const child_len = child_ref.data().length;
    const required_end = std.math.add(usize, parent.offset, parent.length) catch return error.InvalidInput;

    if (required_end <= child_len) {
        return child_ref.slice(parent.offset, parent.length);
    }
    if (parent.length <= child_len) {
        // Struct children coming from ArrayRef.slice() are already aligned to parent rows.
        return child_ref.slice(0, parent.length);
    }
    return error.InvalidInput;
}

fn concatNullArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const buffers = try allocator.alloc(array_data.SharedBuffer, 0);
    const children = try allocator.alloc(ArrayRef, 0);
    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = total_len,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatBooleanArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);
    try requireBitmapBits(left.buffers[1], left.offset + left.length);
    try requireBitmapBits(right.buffers[1], right.offset + right.length);

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    var values_owned = try buffer.OwnedBuffer.init(allocator, bitmap.byteLength(total_len));
    const values_bytes = values_owned.data[0..bitmap.byteLength(total_len)];
    @memset(values_bytes, 0);
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        if (bitAt(left.buffers[1], left.offset + i)) bitmap.setBit(values_bytes, i);
    }
    i = 0;
    while (i < right.length) : (i += 1) {
        if (bitAt(right.buffers[1], right.offset + i)) bitmap.setBit(values_bytes, left.length + i);
    }
    var values = try values_owned.toShared(values_bytes.len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatFixedWidthArray(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
    byte_width: usize,
) Error!ArrayRef {
    try requireBufferCount(left, 2);
    try requireBufferCount(right, 2);

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_bytes = try dataBytesForFixedWidth(left, byte_width);
    const right_bytes = try dataBytesForFixedWidth(right, byte_width);
    const total_data_len = std.math.add(usize, left_bytes.len, right_bytes.len) catch return error.InvalidInput;
    var values_owned = try buffer.OwnedBuffer.init(allocator, total_data_len);
    @memcpy(values_owned.data[0..left_bytes.len], left_bytes);
    @memcpy(values_owned.data[left_bytes.len .. left_bytes.len + right_bytes.len], right_bytes);
    var values = try values_owned.toShared(total_data_len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 2);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatVariableBinaryArrayI32(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 3);
    try requireBufferCount(right, 3);

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i32) catch return error.InvalidInput;
    const right_offsets = right.buffers[1].typedSlice(i32) catch return error.InvalidInput;
    const left_last_index = std.math.add(usize, left.offset, left.length) catch return error.InvalidInput;
    const right_last_index = std.math.add(usize, right.offset, right.length) catch return error.InvalidInput;
    if (left_last_index >= left_offsets.len or right_last_index >= right_offsets.len) return error.InvalidInput;

    const left_start = std.math.cast(usize, left_offsets[left.offset]) orelse return error.InvalidInput;
    const left_end = std.math.cast(usize, left_offsets[left_last_index]) orelse return error.InvalidInput;
    const right_start = std.math.cast(usize, right_offsets[right.offset]) orelse return error.InvalidInput;
    const right_end = std.math.cast(usize, right_offsets[right_last_index]) orelse return error.InvalidInput;
    if (left_end < left_start or right_end < right_start) return error.InvalidInput;
    if (left_end > left.buffers[2].len() or right_end > right.buffers[2].len()) return error.InvalidInput;

    const left_data_len = std.math.sub(usize, left_end, left_start) catch return error.InvalidInput;
    const right_data_len = std.math.sub(usize, right_end, right_start) catch return error.InvalidInput;
    const total_data_len = std.math.add(usize, left_data_len, right_data_len) catch return error.InvalidInput;

    const offsets_len = std.math.add(usize, total_len, 1) catch return error.InvalidInput;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i32));
    var out_offsets = offsets_owned.typedSlice(i32)[0..offsets_len];
    out_offsets[0] = 0;
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const cur = std.math.sub(i32, left_offsets[left.offset + i + 1], left_offsets[left.offset]) catch return error.InvalidInput;
        out_offsets[i + 1] = cur;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_idx = i - left.length;
        const delta_off = std.math.sub(i32, right_offsets[right.offset + right_idx + 1], right_offsets[right.offset]) catch return error.InvalidInput;
        out_offsets[i + 1] = std.math.add(i32, left_prefix, delta_off) catch return error.InvalidInput;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i32));
    errdefer offsets.release();

    var values_owned = try buffer.OwnedBuffer.init(allocator, total_data_len);
    if (left_data_len > 0) {
        @memcpy(values_owned.data[0..left_data_len], left.buffers[2].data[left_start..left_end]);
    }
    if (right_data_len > 0) {
        @memcpy(values_owned.data[left_data_len .. left_data_len + right_data_len], right.buffers[2].data[right_start..right_end]);
    }
    var values = try values_owned.toShared(total_data_len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    buffers[2] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn concatVariableBinaryArrayI64(
    allocator: std.mem.Allocator,
    dt: DataType,
    left: *const ArrayData,
    right: *const ArrayData,
) Error!ArrayRef {
    try requireBufferCount(left, 3);
    try requireBufferCount(right, 3);

    const total_len = std.math.add(usize, left.length, right.length) catch return error.InvalidInput;
    const null_count = std.math.add(usize, try nullCountForArray(left), try nullCountForArray(right)) catch return error.InvalidInput;
    const validity = try concatValidityBuffer(allocator, left, right, total_len, null_count);
    errdefer if (!validity.isEmpty()) {
        var owned = validity;
        owned.release();
    };

    const left_offsets = left.buffers[1].typedSlice(i64) catch return error.InvalidInput;
    const right_offsets = right.buffers[1].typedSlice(i64) catch return error.InvalidInput;
    const left_last_index = std.math.add(usize, left.offset, left.length) catch return error.InvalidInput;
    const right_last_index = std.math.add(usize, right.offset, right.length) catch return error.InvalidInput;
    if (left_last_index >= left_offsets.len or right_last_index >= right_offsets.len) return error.InvalidInput;

    const left_start = std.math.cast(usize, left_offsets[left.offset]) orelse return error.InvalidInput;
    const left_end = std.math.cast(usize, left_offsets[left_last_index]) orelse return error.InvalidInput;
    const right_start = std.math.cast(usize, right_offsets[right.offset]) orelse return error.InvalidInput;
    const right_end = std.math.cast(usize, right_offsets[right_last_index]) orelse return error.InvalidInput;
    if (left_end < left_start or right_end < right_start) return error.InvalidInput;
    if (left_end > left.buffers[2].len() or right_end > right.buffers[2].len()) return error.InvalidInput;

    const left_data_len = std.math.sub(usize, left_end, left_start) catch return error.InvalidInput;
    const right_data_len = std.math.sub(usize, right_end, right_start) catch return error.InvalidInput;
    const total_data_len = std.math.add(usize, left_data_len, right_data_len) catch return error.InvalidInput;

    const offsets_len = std.math.add(usize, total_len, 1) catch return error.InvalidInput;
    var offsets_owned = try buffer.OwnedBuffer.init(allocator, offsets_len * @sizeOf(i64));
    var out_offsets = offsets_owned.typedSlice(i64)[0..offsets_len];
    out_offsets[0] = 0;
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        const cur = std.math.sub(i64, left_offsets[left.offset + i + 1], left_offsets[left.offset]) catch return error.InvalidInput;
        out_offsets[i + 1] = cur;
    }
    const left_prefix = out_offsets[left.length];
    while (i < total_len) : (i += 1) {
        const right_idx = i - left.length;
        const delta_off = std.math.sub(i64, right_offsets[right.offset + right_idx + 1], right_offsets[right.offset]) catch return error.InvalidInput;
        out_offsets[i + 1] = std.math.add(i64, left_prefix, delta_off) catch return error.InvalidInput;
    }
    var offsets = try offsets_owned.toShared(offsets_len * @sizeOf(i64));
    errdefer offsets.release();

    var values_owned = try buffer.OwnedBuffer.init(allocator, total_data_len);
    if (left_data_len > 0) {
        @memcpy(values_owned.data[0..left_data_len], left.buffers[2].data[left_start..left_end]);
    }
    if (right_data_len > 0) {
        @memcpy(values_owned.data[left_data_len .. left_data_len + right_data_len], right.buffers[2].data[right_start..right_end]);
    }
    var values = try values_owned.toShared(total_data_len);
    errdefer values.release();

    const buffers = try allocator.alloc(array_data.SharedBuffer, 3);
    errdefer allocator.free(buffers);
    buffers[0] = validity;
    buffers[1] = offsets;
    buffers[2] = values;
    const children = try allocator.alloc(ArrayRef, 0);
    errdefer allocator.free(children);

    const data = ArrayData{
        .data_type = dt,
        .length = total_len,
        .null_count = null_count,
        .buffers = buffers,
        .children = children,
        .dictionary = null,
    };
    try data.validateLayout();
    return ArrayRef.fromOwnedUnsafe(allocator, data);
}

fn nullCountForArray(data: *const ArrayData) ConcatArrayError!usize {
    if (data.null_count) |count| return count;
    if (data.validity()) |v| {
        var nulls: usize = 0;
        var i: usize = 0;
        while (i < data.length) : (i += 1) {
            const is_valid = v.isValid(data.offset + i) catch return error.InvalidInput;
            if (!is_valid) nulls += 1;
        }
        return nulls;
    }
    return 0;
}

fn concatValidityBuffer(
    allocator: std.mem.Allocator,
    left: *const ArrayData,
    right: *const ArrayData,
    total_len: usize,
    total_nulls: usize,
) error{OutOfMemory}!array_data.SharedBuffer {
    if (total_nulls == 0) return array_data.SharedBuffer.empty;
    const used = bitmap.byteLength(total_len);
    var owned = try buffer.OwnedBuffer.init(allocator, used);
    const bytes = owned.data[0..used];
    @memset(bytes, 0);
    var i: usize = 0;
    while (i < left.length) : (i += 1) {
        if (isValidAt(left, i)) bitmap.setBit(bytes, i);
    }
    i = 0;
    while (i < right.length) : (i += 1) {
        if (isValidAt(right, i)) bitmap.setBit(bytes, left.length + i);
    }
    return owned.toShared(used);
}

fn isValidAt(data: *const ArrayData, index: usize) bool {
    if (data.null_count) |count| {
        if (count == 0) return true;
        if (count == data.length) return false;
    }
    const validity = data.validity() orelse return true;
    return validity.isValid(data.offset + index) catch false;
}

fn bitAt(buf: array_data.SharedBuffer, bit_index: usize) bool {
    return bitmap.bitIsSet(buf.data, bit_index);
}

fn dataBytesForFixedWidth(data: *const ArrayData, byte_width: usize) ConcatArrayError![]const u8 {
    const start = std.math.mul(usize, data.offset, byte_width) catch return error.InvalidInput;
    const data_len = std.math.mul(usize, data.length, byte_width) catch return error.InvalidInput;
    const end = std.math.add(usize, start, data_len) catch return error.InvalidInput;
    if (data.buffers.len < 2) return error.InvalidInput;
    if (end > data.buffers[1].len()) return error.InvalidInput;
    return data.buffers[1].data[start..end];
}

fn requireBufferCount(data: *const ArrayData, min_count: usize) ConcatArrayError!void {
    if (data.buffers.len < min_count) return error.InvalidInput;
}

fn requireBitmapBits(buf: array_data.SharedBuffer, bit_len: usize) ConcatArrayError!void {
    if (bitmap.byteLength(bit_len) > buf.len()) return error.InvalidInput;
}

test "concatArrayRefs supports null bool and fixed-width with sliced inputs" {
    const allocator = std.testing.allocator;
    const arr = @import("array/array.zig");

    var null_left_builder = try arr.NullBuilder.init(allocator, 0);
    try null_left_builder.appendNulls(4);
    var null_left = try null_left_builder.finish();
    defer null_left.release();

    var null_right_builder = try arr.NullBuilder.init(allocator, 0);
    try null_right_builder.appendNulls(3);
    var null_right = try null_right_builder.finish();
    defer null_right.release();

    var null_left_slice = try null_left.slice(1, 2);
    defer null_left_slice.release();
    var null_right_slice = try null_right.slice(1, 1);
    defer null_right_slice.release();

    var merged_null = try concatArrayRefs(allocator, .{ .null = {} }, &[_]ArrayRef{ null_left_slice, null_right_slice });
    defer merged_null.release();
    try merged_null.data().validateLayout();
    try std.testing.expectEqual(@as(usize, 3), merged_null.data().length);
    try std.testing.expectEqual(@as(usize, 3), merged_null.data().null_count.?);

    var bool_left_builder = try arr.BooleanBuilder.init(allocator, 4);
    defer bool_left_builder.deinit();
    try bool_left_builder.append(true);
    try bool_left_builder.append(false);
    try bool_left_builder.appendNull();
    try bool_left_builder.append(true);
    var bool_left = try bool_left_builder.finish();
    defer bool_left.release();

    var bool_right_builder = try arr.BooleanBuilder.init(allocator, 3);
    defer bool_right_builder.deinit();
    try bool_right_builder.appendNull();
    try bool_right_builder.append(false);
    try bool_right_builder.append(true);
    var bool_right = try bool_right_builder.finish();
    defer bool_right.release();

    var bool_left_slice = try bool_left.slice(1, 3);
    defer bool_left_slice.release();
    var bool_right_slice = try bool_right.slice(0, 2);
    defer bool_right_slice.release();

    var merged_bool = try concatArrayRefs(allocator, .{ .bool = {} }, &[_]ArrayRef{ bool_left_slice, bool_right_slice });
    defer merged_bool.release();
    try merged_bool.data().validateLayout();

    const bool_view = arr.BooleanArray{ .data = merged_bool.data() };
    try std.testing.expectEqual(@as(usize, 5), bool_view.len());
    try std.testing.expectEqual(false, bool_view.value(0));
    try std.testing.expect(bool_view.isNull(1));
    try std.testing.expectEqual(true, bool_view.value(2));
    try std.testing.expect(bool_view.isNull(3));
    try std.testing.expectEqual(false, bool_view.value(4));

    var i32_left_builder = try arr.Int32Builder.init(allocator, 4);
    defer i32_left_builder.deinit();
    try i32_left_builder.append(1);
    try i32_left_builder.append(2);
    try i32_left_builder.append(3);
    try i32_left_builder.append(4);
    var i32_left = try i32_left_builder.finish();
    defer i32_left.release();

    var i32_right_builder = try arr.Int32Builder.init(allocator, 3);
    defer i32_right_builder.deinit();
    try i32_right_builder.append(5);
    try i32_right_builder.append(6);
    try i32_right_builder.append(7);
    var i32_right = try i32_right_builder.finish();
    defer i32_right.release();

    var i32_left_slice = try i32_left.slice(1, 2);
    defer i32_left_slice.release();
    var i32_right_slice = try i32_right.slice(1, 2);
    defer i32_right_slice.release();

    var merged_i32 = try concatArrayRefs(allocator, .{ .int32 = {} }, &[_]ArrayRef{ i32_left_slice, i32_right_slice });
    defer merged_i32.release();
    try merged_i32.data().validateLayout();

    const i32_view = arr.Int32Array{ .data = merged_i32.data() };
    try std.testing.expectEqual(@as(usize, 4), i32_view.len());
    try std.testing.expectEqual(@as(i32, 2), i32_view.value(0));
    try std.testing.expectEqual(@as(i32, 3), i32_view.value(1));
    try std.testing.expectEqual(@as(i32, 6), i32_view.value(2));
    try std.testing.expectEqual(@as(i32, 7), i32_view.value(3));
}

test "concatArrayRefs supports string and binary families" {
    const allocator = std.testing.allocator;
    const arr = @import("array/array.zig");

    var str_left_builder = try arr.StringBuilder.init(allocator, 3, 8);
    defer str_left_builder.deinit();
    try str_left_builder.append("aa");
    try str_left_builder.append("bbb");
    try str_left_builder.append("c");
    var str_left = try str_left_builder.finish();
    defer str_left.release();

    var str_right_builder = try arr.StringBuilder.init(allocator, 2, 3);
    defer str_right_builder.deinit();
    try str_right_builder.append("d");
    try str_right_builder.append("ee");
    var str_right = try str_right_builder.finish();
    defer str_right.release();

    var str_left_slice = try str_left.slice(1, 2);
    defer str_left_slice.release();

    var merged_str = try concatArrayRefs(allocator, .{ .string = {} }, &[_]ArrayRef{ str_left_slice, str_right });
    defer merged_str.release();
    try merged_str.data().validateLayout();

    const str_view = arr.StringArray{ .data = merged_str.data() };
    try std.testing.expectEqual(@as(usize, 4), str_view.len());
    try std.testing.expectEqualStrings("bbb", str_view.value(0));
    try std.testing.expectEqualStrings("c", str_view.value(1));
    try std.testing.expectEqualStrings("d", str_view.value(2));
    try std.testing.expectEqualStrings("ee", str_view.value(3));

    var bin_left_builder = try arr.BinaryBuilder.init(allocator, 3, 8);
    defer bin_left_builder.deinit();
    try bin_left_builder.append("x1");
    try bin_left_builder.append("y22");
    try bin_left_builder.append("z");
    var bin_left = try bin_left_builder.finish();
    defer bin_left.release();

    var bin_right_builder = try arr.BinaryBuilder.init(allocator, 1, 2);
    defer bin_right_builder.deinit();
    try bin_right_builder.append("qq");
    var bin_right = try bin_right_builder.finish();
    defer bin_right.release();

    var bin_left_slice = try bin_left.slice(1, 2);
    defer bin_left_slice.release();

    var merged_bin = try concatArrayRefs(allocator, .{ .binary = {} }, &[_]ArrayRef{ bin_left_slice, bin_right });
    defer merged_bin.release();
    try merged_bin.data().validateLayout();

    const bin_view = arr.BinaryArray{ .data = merged_bin.data() };
    try std.testing.expectEqualStrings("y22", bin_view.value(0));
    try std.testing.expectEqualStrings("z", bin_view.value(1));
    try std.testing.expectEqualStrings("qq", bin_view.value(2));

    var lstr_left_builder = try arr.LargeStringBuilder.init(allocator, 2, 4);
    defer lstr_left_builder.deinit();
    try lstr_left_builder.append("l1");
    try lstr_left_builder.append("l22");
    var lstr_left = try lstr_left_builder.finish();
    defer lstr_left.release();

    var lstr_right_builder = try arr.LargeStringBuilder.init(allocator, 1, 2);
    defer lstr_right_builder.deinit();
    try lstr_right_builder.append("l3");
    var lstr_right = try lstr_right_builder.finish();
    defer lstr_right.release();

    var merged_lstr = try concatArrayRefs(allocator, .{ .large_string = {} }, &[_]ArrayRef{ lstr_left, lstr_right });
    defer merged_lstr.release();
    try merged_lstr.data().validateLayout();

    const lstr_view = arr.LargeStringArray{ .data = merged_lstr.data() };
    try std.testing.expectEqualStrings("l1", lstr_view.value(0));
    try std.testing.expectEqualStrings("l22", lstr_view.value(1));
    try std.testing.expectEqualStrings("l3", lstr_view.value(2));

    var lbin_left_builder = try arr.LargeBinaryBuilder.init(allocator, 2, 4);
    defer lbin_left_builder.deinit();
    try lbin_left_builder.append("ab");
    try lbin_left_builder.append("c");
    var lbin_left = try lbin_left_builder.finish();
    defer lbin_left.release();

    var lbin_right_builder = try arr.LargeBinaryBuilder.init(allocator, 2, 4);
    defer lbin_right_builder.deinit();
    try lbin_right_builder.append("de");
    try lbin_right_builder.append("f");
    var lbin_right = try lbin_right_builder.finish();
    defer lbin_right.release();

    var lbin_right_slice = try lbin_right.slice(0, 1);
    defer lbin_right_slice.release();

    var merged_lbin = try concatArrayRefs(allocator, .{ .large_binary = {} }, &[_]ArrayRef{ lbin_left, lbin_right_slice });
    defer merged_lbin.release();
    try merged_lbin.data().validateLayout();

    const lbin_view = arr.LargeBinaryArray{ .data = merged_lbin.data() };
    try std.testing.expectEqualStrings("ab", lbin_view.value(0));
    try std.testing.expectEqualStrings("c", lbin_view.value(1));
    try std.testing.expectEqualStrings("de", lbin_view.value(2));

    var sv_left_builder = try arr.StringViewBuilder.init(allocator, 3, 32);
    defer sv_left_builder.deinit();
    try sv_left_builder.append("small");
    try sv_left_builder.appendNull();
    try sv_left_builder.append("this string is longer than twelve");
    var sv_left = try sv_left_builder.finish();
    defer sv_left.release();

    var sv_right_builder = try arr.StringViewBuilder.init(allocator, 2, 32);
    defer sv_right_builder.deinit();
    try sv_right_builder.append("tail");
    try sv_right_builder.appendNull();
    var sv_right = try sv_right_builder.finish();
    defer sv_right.release();

    var sv_left_slice = try sv_left.slice(1, 2);
    defer sv_left_slice.release();
    var sv_right_slice = try sv_right.slice(0, 1);
    defer sv_right_slice.release();

    var merged_sv = try concatArrayRefs(allocator, .{ .string_view = {} }, &[_]ArrayRef{ sv_left_slice, sv_right_slice });
    defer merged_sv.release();
    try merged_sv.data().validateLayout();
    const sv_view = arr.StringViewArray{ .data = merged_sv.data() };
    try std.testing.expectEqual(@as(usize, 3), sv_view.len());
    try std.testing.expect(sv_view.isNull(0));
    try std.testing.expectEqualStrings("this string is longer than twelve", sv_view.value(1));
    try std.testing.expectEqualStrings("tail", sv_view.value(2));

    var bv_left_builder = try arr.BinaryViewBuilder.init(allocator, 3, 32);
    defer bv_left_builder.deinit();
    try bv_left_builder.append("aa");
    try bv_left_builder.appendNull();
    try bv_left_builder.append("this-binary-view-is-long");
    var bv_left = try bv_left_builder.finish();
    defer bv_left.release();

    var bv_right_builder = try arr.BinaryViewBuilder.init(allocator, 1, 8);
    defer bv_right_builder.deinit();
    try bv_right_builder.append("zz");
    var bv_right = try bv_right_builder.finish();
    defer bv_right.release();

    var bv_left_slice = try bv_left.slice(1, 2);
    defer bv_left_slice.release();
    var merged_bv = try concatArrayRefs(allocator, .{ .binary_view = {} }, &[_]ArrayRef{ bv_left_slice, bv_right });
    defer merged_bv.release();
    try merged_bv.data().validateLayout();
    const bv_view = arr.BinaryViewArray{ .data = merged_bv.data() };
    try std.testing.expectEqual(@as(usize, 3), bv_view.len());
    try std.testing.expect(bv_view.isNull(0));
    try std.testing.expectEqualStrings("this-binary-view-is-long", bv_view.value(1));
    try std.testing.expectEqualStrings("zz", bv_view.value(2));
}

test "concatArrayRefs supports list large_list and fixed_size_list" {
    const allocator = std.testing.allocator;
    const arr = @import("array/array.zig");

    const int_type = DataType{ .int32 = {} };
    const value_field = datatype.Field{ .name = "item", .data_type = &int_type, .nullable = true };

    var list_left_values_builder = try arr.Int32Builder.init(allocator, 4);
    defer list_left_values_builder.deinit();
    try list_left_values_builder.append(10);
    try list_left_values_builder.append(11);
    try list_left_values_builder.append(12);
    try list_left_values_builder.append(13);
    var list_left_values = try list_left_values_builder.finish();
    defer list_left_values.release();

    var list_left_builder = try arr.ListBuilder.init(allocator, 3, value_field);
    defer list_left_builder.deinit();
    try list_left_builder.appendLen(1);
    try list_left_builder.appendLen(2);
    try list_left_builder.appendLen(1);
    var list_left = try list_left_builder.finish(list_left_values);
    defer list_left.release();

    var list_right_values_builder = try arr.Int32Builder.init(allocator, 3);
    defer list_right_values_builder.deinit();
    try list_right_values_builder.append(20);
    try list_right_values_builder.append(21);
    try list_right_values_builder.append(22);
    var list_right_values = try list_right_values_builder.finish();
    defer list_right_values.release();

    var list_right_builder = try arr.ListBuilder.init(allocator, 2, value_field);
    defer list_right_builder.deinit();
    try list_right_builder.appendLen(2);
    try list_right_builder.appendLen(1);
    var list_right = try list_right_builder.finish(list_right_values);
    defer list_right.release();

    var list_left_slice = try list_left.slice(1, 2);
    defer list_left_slice.release();
    var list_right_slice = try list_right.slice(0, 1);
    defer list_right_slice.release();

    var merged_list = try concatArrayRefs(allocator, .{ .list = .{ .value_field = value_field } }, &[_]ArrayRef{ list_left_slice, list_right_slice });
    defer merged_list.release();
    try merged_list.data().validateLayout();
    const list_offsets = try merged_list.data().buffers[1].typedSlice(i32);
    try std.testing.expectEqual(@as(i32, 0), list_offsets[0]);
    try std.testing.expectEqual(@as(i32, 2), list_offsets[1]);
    try std.testing.expectEqual(@as(i32, 3), list_offsets[2]);
    try std.testing.expectEqual(@as(i32, 5), list_offsets[3]);

    const list_child = arr.Int32Array{ .data = merged_list.data().children[0].data() };
    try std.testing.expectEqual(@as(i32, 11), list_child.value(0));
    try std.testing.expectEqual(@as(i32, 12), list_child.value(1));
    try std.testing.expectEqual(@as(i32, 13), list_child.value(2));
    try std.testing.expectEqual(@as(i32, 20), list_child.value(3));
    try std.testing.expectEqual(@as(i32, 21), list_child.value(4));

    var llist_left_values_builder = try arr.Int32Builder.init(allocator, 3);
    defer llist_left_values_builder.deinit();
    try llist_left_values_builder.append(1);
    try llist_left_values_builder.append(2);
    try llist_left_values_builder.append(3);
    var llist_left_values = try llist_left_values_builder.finish();
    defer llist_left_values.release();

    var llist_left_builder = try arr.LargeListBuilder.init(allocator, 2, value_field);
    defer llist_left_builder.deinit();
    try llist_left_builder.appendLen(2);
    try llist_left_builder.appendLen(1);
    var llist_left = try llist_left_builder.finish(llist_left_values);
    defer llist_left.release();

    var llist_right_values_builder = try arr.Int32Builder.init(allocator, 2);
    defer llist_right_values_builder.deinit();
    try llist_right_values_builder.append(4);
    try llist_right_values_builder.append(5);
    var llist_right_values = try llist_right_values_builder.finish();
    defer llist_right_values.release();

    var llist_right_builder = try arr.LargeListBuilder.init(allocator, 1, value_field);
    defer llist_right_builder.deinit();
    try llist_right_builder.appendLen(2);
    var llist_right = try llist_right_builder.finish(llist_right_values);
    defer llist_right.release();

    var llist_left_slice = try llist_left.slice(1, 1);
    defer llist_left_slice.release();

    var merged_llist = try concatArrayRefs(allocator, .{ .large_list = .{ .value_field = value_field } }, &[_]ArrayRef{ llist_left_slice, llist_right });
    defer merged_llist.release();
    try merged_llist.data().validateLayout();
    const llist_offsets = try merged_llist.data().buffers[1].typedSlice(i64);
    try std.testing.expectEqual(@as(i64, 0), llist_offsets[0]);
    try std.testing.expectEqual(@as(i64, 1), llist_offsets[1]);
    try std.testing.expectEqual(@as(i64, 3), llist_offsets[2]);

    const llist_child = arr.Int32Array{ .data = merged_llist.data().children[0].data() };
    try std.testing.expectEqual(@as(i32, 3), llist_child.value(0));
    try std.testing.expectEqual(@as(i32, 4), llist_child.value(1));
    try std.testing.expectEqual(@as(i32, 5), llist_child.value(2));

    var fsl_left_values_builder = try arr.Int32Builder.init(allocator, 4);
    defer fsl_left_values_builder.deinit();
    try fsl_left_values_builder.append(1);
    try fsl_left_values_builder.append(2);
    try fsl_left_values_builder.append(3);
    try fsl_left_values_builder.append(4);
    var fsl_left_values = try fsl_left_values_builder.finish();
    defer fsl_left_values.release();

    var fsl_left_builder = try arr.FixedSizeListBuilder.init(allocator, value_field, 2);
    defer fsl_left_builder.deinit();
    try fsl_left_builder.appendValid();
    try fsl_left_builder.appendValid();
    var fsl_left = try fsl_left_builder.finish(fsl_left_values);
    defer fsl_left.release();

    var fsl_right_values_builder = try arr.Int32Builder.init(allocator, 4);
    defer fsl_right_values_builder.deinit();
    try fsl_right_values_builder.append(5);
    try fsl_right_values_builder.append(6);
    try fsl_right_values_builder.append(7);
    try fsl_right_values_builder.append(8);
    var fsl_right_values = try fsl_right_values_builder.finish();
    defer fsl_right_values.release();

    var fsl_right_builder = try arr.FixedSizeListBuilder.init(allocator, value_field, 2);
    defer fsl_right_builder.deinit();
    try fsl_right_builder.appendValid();
    try fsl_right_builder.appendValid();
    var fsl_right = try fsl_right_builder.finish(fsl_right_values);
    defer fsl_right.release();

    var fsl_left_slice = try fsl_left.slice(1, 1);
    defer fsl_left_slice.release();
    var fsl_right_slice = try fsl_right.slice(0, 1);
    defer fsl_right_slice.release();

    var merged_fsl = try concatArrayRefs(
        allocator,
        .{ .fixed_size_list = .{ .list_size = 2, .value_field = value_field } },
        &[_]ArrayRef{ fsl_left_slice, fsl_right_slice },
    );
    defer merged_fsl.release();
    try merged_fsl.data().validateLayout();

    const fsl_view = arr.FixedSizeListArray{ .data = merged_fsl.data() };
    try std.testing.expectEqual(@as(usize, 2), fsl_view.len());
    const fsl_child = arr.Int32Array{ .data = fsl_view.valuesRef().data() };
    try std.testing.expectEqual(@as(i32, 3), fsl_child.value(0));
    try std.testing.expectEqual(@as(i32, 4), fsl_child.value(1));
    try std.testing.expectEqual(@as(i32, 5), fsl_child.value(2));
    try std.testing.expectEqual(@as(i32, 6), fsl_child.value(3));

    var fsl_null_left_values_builder = try arr.Int32Builder.init(allocator, 6);
    defer fsl_null_left_values_builder.deinit();
    try fsl_null_left_values_builder.append(101);
    try fsl_null_left_values_builder.append(102);
    try fsl_null_left_values_builder.append(103);
    try fsl_null_left_values_builder.append(104);
    try fsl_null_left_values_builder.append(105);
    try fsl_null_left_values_builder.append(106);
    var fsl_null_left_values = try fsl_null_left_values_builder.finish();
    defer fsl_null_left_values.release();

    var fsl_null_left_builder = try arr.FixedSizeListBuilder.init(allocator, value_field, 2);
    defer fsl_null_left_builder.deinit();
    try fsl_null_left_builder.appendNull();
    try fsl_null_left_builder.appendValid();
    try fsl_null_left_builder.appendValid();
    var fsl_null_left = try fsl_null_left_builder.finish(fsl_null_left_values);
    defer fsl_null_left.release();

    var fsl_null_right_values_builder = try arr.Int32Builder.init(allocator, 2);
    defer fsl_null_right_values_builder.deinit();
    try fsl_null_right_values_builder.append(201);
    try fsl_null_right_values_builder.append(202);
    var fsl_null_right_values = try fsl_null_right_values_builder.finish();
    defer fsl_null_right_values.release();

    var fsl_null_right_builder = try arr.FixedSizeListBuilder.init(allocator, value_field, 2);
    defer fsl_null_right_builder.deinit();
    try fsl_null_right_builder.appendValid();
    var fsl_null_right = try fsl_null_right_builder.finish(fsl_null_right_values);
    defer fsl_null_right.release();

    var fsl_null_left_slice = try fsl_null_left.slice(1, 1);
    defer fsl_null_left_slice.release();
    var fsl_null_merged = try concatArrayRefs(
        allocator,
        .{ .fixed_size_list = .{ .list_size = 2, .value_field = value_field } },
        &[_]ArrayRef{ fsl_null_left_slice, fsl_null_right },
    );
    defer fsl_null_merged.release();
    try fsl_null_merged.data().validateLayout();
    const fsl_null_view = arr.FixedSizeListArray{ .data = fsl_null_merged.data() };
    try std.testing.expectEqual(@as(usize, 2), fsl_null_view.len());
    try std.testing.expect(!fsl_null_view.isNull(0));
    try std.testing.expect(!fsl_null_view.isNull(1));
    try std.testing.expectEqual(@as(usize, 0), fsl_null_merged.data().null_count.?);
}

test "concatArrayRefs supports recursive struct" {
    const allocator = std.testing.allocator;
    const arr = @import("array/array.zig");

    const int_type = DataType{ .int32 = {} };
    const list_value_field = datatype.Field{ .name = "item", .data_type = &int_type, .nullable = true };
    const list_type = DataType{ .list = .{ .value_field = list_value_field } };

    const struct_fields = [_]datatype.Field{
        .{ .name = "id", .data_type = &int_type, .nullable = false },
        .{ .name = "nums", .data_type = &list_type, .nullable = true },
    };

    var left_ids_builder = try arr.Int32Builder.init(allocator, 3);
    defer left_ids_builder.deinit();
    try left_ids_builder.append(10);
    try left_ids_builder.append(20);
    try left_ids_builder.append(30);
    var left_ids = try left_ids_builder.finish();
    defer left_ids.release();

    var left_vals_builder = try arr.Int32Builder.init(allocator, 4);
    defer left_vals_builder.deinit();
    try left_vals_builder.append(1);
    try left_vals_builder.append(2);
    try left_vals_builder.append(3);
    try left_vals_builder.append(4);
    var left_vals = try left_vals_builder.finish();
    defer left_vals.release();

    var left_list_builder = try arr.ListBuilder.init(allocator, 3, list_value_field);
    defer left_list_builder.deinit();
    try left_list_builder.appendLen(1);
    try left_list_builder.appendLen(2);
    try left_list_builder.appendLen(1);
    var left_list = try left_list_builder.finish(left_vals);
    defer left_list.release();

    var left_struct_builder = arr.StructBuilder.init(allocator, struct_fields[0..]);
    defer left_struct_builder.deinit();
    try left_struct_builder.appendValid();
    try left_struct_builder.appendValid();
    try left_struct_builder.appendValid();
    var left_struct = try left_struct_builder.finish(&[_]ArrayRef{ left_ids, left_list });
    defer left_struct.release();

    var right_ids_builder = try arr.Int32Builder.init(allocator, 2);
    defer right_ids_builder.deinit();
    try right_ids_builder.append(40);
    try right_ids_builder.append(50);
    var right_ids = try right_ids_builder.finish();
    defer right_ids.release();

    var right_vals_builder = try arr.Int32Builder.init(allocator, 3);
    defer right_vals_builder.deinit();
    try right_vals_builder.append(5);
    try right_vals_builder.append(6);
    try right_vals_builder.append(7);
    var right_vals = try right_vals_builder.finish();
    defer right_vals.release();

    var right_list_builder = try arr.ListBuilder.init(allocator, 2, list_value_field);
    defer right_list_builder.deinit();
    try right_list_builder.appendLen(2);
    try right_list_builder.appendLen(1);
    var right_list = try right_list_builder.finish(right_vals);
    defer right_list.release();

    var right_struct_builder = arr.StructBuilder.init(allocator, struct_fields[0..]);
    defer right_struct_builder.deinit();
    try right_struct_builder.appendValid();
    try right_struct_builder.appendValid();
    var right_struct = try right_struct_builder.finish(&[_]ArrayRef{ right_ids, right_list });
    defer right_struct.release();

    var left_struct_slice = try left_struct.slice(1, 2);
    defer left_struct_slice.release();
    var right_struct_slice = try right_struct.slice(0, 1);
    defer right_struct_slice.release();

    var merged = try concatArrayRefs(
        allocator,
        .{ .struct_ = .{ .fields = struct_fields[0..] } },
        &[_]ArrayRef{ left_struct_slice, right_struct_slice },
    );
    defer merged.release();
    try merged.data().validateLayout();

    try std.testing.expectEqual(@as(usize, 3), merged.data().length);
    const ids = arr.Int32Array{ .data = merged.data().children[0].data() };
    try std.testing.expectEqual(@as(i32, 20), ids.value(0));
    try std.testing.expectEqual(@as(i32, 30), ids.value(1));
    try std.testing.expectEqual(@as(i32, 40), ids.value(2));

    const nums = merged.data().children[1];
    const nums_offsets = try nums.data().buffers[1].typedSlice(i32);
    try std.testing.expectEqual(@as(i32, 0), nums_offsets[0]);
    try std.testing.expectEqual(@as(i32, 2), nums_offsets[1]);
    try std.testing.expectEqual(@as(i32, 3), nums_offsets[2]);
    try std.testing.expectEqual(@as(i32, 5), nums_offsets[3]);

    const nums_values = arr.Int32Array{ .data = nums.data().children[0].data() };
    try std.testing.expectEqual(@as(i32, 2), nums_values.value(0));
    try std.testing.expectEqual(@as(i32, 3), nums_values.value(1));
    try std.testing.expectEqual(@as(i32, 4), nums_values.value(2));
    try std.testing.expectEqual(@as(i32, 5), nums_values.value(3));
    try std.testing.expectEqual(@as(i32, 6), nums_values.value(4));
}

test "concatArrayRefs rejects empty input" {
    try std.testing.expectError(
        error.InvalidInput,
        concatArrayRefs(std.testing.allocator, .{ .int32 = {} }, &[_]ArrayRef{}),
    );
}
