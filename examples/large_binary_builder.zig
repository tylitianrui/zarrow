const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.LargeBinaryBuilder.init(std.heap.page_allocator, 3, 32);
    defer builder.deinit();

    try builder.append("za");
    try builder.appendNull();
    try builder.append("rrow");

    var array_ref = try builder.finishReset();
    defer array_ref.release();
    const array = zarrow.LargeBinaryArray{ .data = array_ref.data() };

    std.debug.assert(array.len() == 3);
    std.debug.assert(std.mem.eql(u8, array.value(0), "za"));
    std.debug.assert(array.isNull(1));
    std.debug.assert(std.mem.eql(u8, array.value(2), "rrow"));

    try builder.append("again");
    try builder.appendNull();

    var array_ref2 = try builder.finish();
    defer array_ref2.release();
    const array2 = zarrow.LargeBinaryArray{ .data = array_ref2.data() };

    std.debug.assert(array2.len() == 2);
    std.debug.assert(std.mem.eql(u8, array2.value(0), "again"));
    std.debug.assert(array2.isNull(1));

    std.debug.print("examples/large_binary_builder.zig | type=LargeBinaryBuilder | length={d}, value_index_0={s}, isNull_index_1={any}, value_index_2={s}, length2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
        array2.len(),
    });
}
