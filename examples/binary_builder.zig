const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.BinaryBuilder.init(std.heap.page_allocator, 3, 16);
    defer builder.deinit();

    try builder.append("zi");
    try builder.appendNull();
    try builder.append("ggy");

    const array = builder.finish();

    std.debug.assert(array.len() == 3);
    std.debug.assert(std.mem.eql(u8, array.value(0), "zi"));
    std.debug.assert(array.isNull(1));
    std.debug.assert(std.mem.eql(u8, array.value(2), "ggy"));

    std.debug.print("examples/binary_builder.zig | type=BinaryBuilder | length={d}, value_index_0={s}, isNull_index_1={any}, value_index_2={s}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
    });
}
