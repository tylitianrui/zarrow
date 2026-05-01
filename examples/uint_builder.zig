const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var b8 = try zarrow.UInt8Builder.init(std.heap.page_allocator, 3);
    defer b8.deinit();
    try b8.append(1);
    try b8.appendNull();
    try b8.append(3);
    var r8 = try b8.finishReset();
    defer r8.release();
    const a8 = zarrow.UInt8Array{ .data = r8.data() };
    std.debug.assert(a8.len() == 3);
    std.debug.assert((try a8.value(0)) == 1);
    std.debug.assert(a8.isNull(1));
    std.debug.assert((try a8.value(2)) == 3);
    try b8.append(7);
    try b8.append(8);
    var r8b = try b8.finish();
    defer r8b.release();
    const a8b = zarrow.UInt8Array{ .data = r8b.data() };
    std.debug.assert(a8b.len() == 2);
    std.debug.assert((try a8b.value(0)) == 7);
    std.debug.assert((try a8b.value(1)) == 8);

    var b16 = try zarrow.UInt16Builder.init(std.heap.page_allocator, 3);
    defer b16.deinit();
    try b16.append(10);
    try b16.appendNull();
    try b16.append(30);
    var r16 = try b16.finishReset();
    defer r16.release();
    const a16 = zarrow.UInt16Array{ .data = r16.data() };
    std.debug.assert(a16.len() == 3);
    std.debug.assert((try a16.value(0)) == 10);
    std.debug.assert(a16.isNull(1));
    std.debug.assert((try a16.value(2)) == 30);

    var b32 = try zarrow.UInt32Builder.init(std.heap.page_allocator, 3);
    defer b32.deinit();
    try b32.append(100);
    try b32.appendNull();
    try b32.append(300);
    var r32 = try b32.finishReset();
    defer r32.release();
    const a32 = zarrow.UInt32Array{ .data = r32.data() };
    std.debug.assert(a32.len() == 3);
    std.debug.assert((try a32.value(0)) == 100);
    std.debug.assert(a32.isNull(1));
    std.debug.assert((try a32.value(2)) == 300);

    var b64 = try zarrow.UInt64Builder.init(std.heap.page_allocator, 3);
    defer b64.deinit();
    try b64.append(1000);
    try b64.appendNull();
    try b64.append(3000);
    var r64 = try b64.finish();
    defer r64.release();
    const a64 = zarrow.UInt64Array{ .data = r64.data() };
    std.debug.assert(a64.len() == 3);
    std.debug.assert((try a64.value(0)) == 1000);
    std.debug.assert(a64.isNull(1));
    std.debug.assert((try a64.value(2)) == 3000);

    std.debug.print(
        "examples/uint_builder.zig | u8_len={d} u16_len={d} u32_len={d} u64_len={d} u8_v0={d} u64_v2={d}\n",
        .{ a8.len(), a16.len(), a32.len(), a64.len(), try a8.value(0), try a64.value(2) },
    );
}
