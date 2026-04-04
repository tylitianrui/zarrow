const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    var builder = try zarrow.Int32Builder.init(std.heap.page_allocator, 3);
    defer builder.deinit();

    try builder.append(10);
    try builder.appendNull();
    try builder.append(30);

    const array = builder.finish();

    std.debug.print("examples/primitive_builder.zig | type=Int32Builder | length={d}, value0={d}, isNull1={any}, value2={d}\n", .{
        array.len(),
        array.value(0),
        array.isNull(1),
        array.value(2),
    });
}
