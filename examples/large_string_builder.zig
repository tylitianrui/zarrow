const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.LargeStringBuilder.init(std.heap.page_allocator, 3, 32);
    defer builder.deinit();

    try builder.append("zarrow");
    try builder.appendNull();
    try builder.append("large-utf8");

    var array_ref = try builder.finishReset();
    defer array_ref.release();
    const array = zarrow.LargeStringArray{ .data = array_ref.data() };

    std.debug.assert(array.len() == 3);
    std.debug.assert(std.mem.eql(u8, array.value(0), "zarrow"));
    std.debug.assert(array.isNull(1));
    std.debug.assert(std.mem.eql(u8, array.value(2), "large-utf8"));

    try builder.append("hello");
    try builder.append("world");

    var array_ref2 = try builder.finish();
    defer array_ref2.release();
    const array2 = zarrow.LargeStringArray{ .data = array_ref2.data() };

    std.debug.assert(array2.len() == 2);
    std.debug.assert(std.mem.eql(u8, array2.value(0), "hello"));
    std.debug.assert(std.mem.eql(u8, array2.value(1), "world"));

    std.debug.print("examples/large_string_builder.zig | type=LargeStringBuilder | length={d}, value_index_0={s}, isNull_index_1={any}, value_index_2={s}, length2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
        array2.len(),
    });
}
