const std = @import("std");
const root = @import("zarrow");

pub fn main() !void {
    var builder = try root.BooleanBuilder.init(std.heap.page_allocator, 3);
    defer builder.deinit();

    try builder.append(true);
    try builder.append(false);
    try builder.appendNull();

    var array_ref = try builder.finish();
    defer array_ref.release();
    const array = root.BooleanArray{ .data = array_ref.data() };

    std.debug.assert(array.len() == 3);
    std.debug.assert(array.value(0) == true);
    std.debug.assert(array.value(1) == false);
    std.debug.assert(array.isNull(2));

    std.debug.print("examples/boolean_builder.zig | type=BooleanBuilder | length={d}, value0={any}, value1={any}, isNull2={any}\n", .{
        array.len(),
        array.value(0),
        array.value(1),
        array.isNull(2),
    });
}
