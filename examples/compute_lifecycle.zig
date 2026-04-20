const std = @import("std");
const zarrow = @import("zarrow");

const compute = zarrow.compute;

const CountState = struct {
    count: usize,
};

fn isUnaryArray(args: []const compute.Datum) bool {
    return args.len == 1 and args[0].isArray();
}

fn passthroughKernel(
    ctx: *compute.ExecContext,
    args: []const compute.Datum,
    options: compute.Options,
) compute.KernelError!compute.Datum {
    _ = ctx;
    _ = options;
    return args[0].retain();
}

fn countResultType(
    args: []const compute.Datum,
    options: compute.Options,
) compute.KernelError!compute.DataType {
    _ = args;
    _ = options;
    return .{ .int64 = {} };
}

fn countInit(
    ctx: *compute.ExecContext,
    options: compute.Options,
) compute.KernelError!*anyopaque {
    _ = options;
    const state = ctx.allocator.create(CountState) catch return error.OutOfMemory;
    state.* = .{ .count = 0 };
    return state;
}

fn countUpdate(
    ctx: *compute.ExecContext,
    state_ptr: *anyopaque,
    args: []const compute.Datum,
    options: compute.Options,
) compute.KernelError!void {
    _ = ctx;
    _ = options;
    if (!isUnaryArray(args)) return error.InvalidInput;
    const state: *CountState = @ptrCast(@alignCast(state_ptr));
    state.count = std.math.add(usize, state.count, args[0].array.data().length) catch return error.InvalidInput;
}

fn countMerge(
    ctx: *compute.ExecContext,
    state_ptr: *anyopaque,
    other_ptr: *anyopaque,
    options: compute.Options,
) compute.KernelError!void {
    _ = ctx;
    _ = options;
    const state: *CountState = @ptrCast(@alignCast(state_ptr));
    const other: *CountState = @ptrCast(@alignCast(other_ptr));
    state.count = std.math.add(usize, state.count, other.count) catch return error.InvalidInput;
}

fn countFinalize(
    ctx: *compute.ExecContext,
    state_ptr: *anyopaque,
    options: compute.Options,
) compute.KernelError!compute.Datum {
    _ = ctx;
    _ = options;
    const state: *CountState = @ptrCast(@alignCast(state_ptr));
    return compute.Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = @intCast(state.count) },
    });
}

fn countDeinit(ctx: *compute.ExecContext, state_ptr: *anyopaque) void {
    const state: *CountState = @ptrCast(@alignCast(state_ptr));
    ctx.allocator.destroy(state);
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var registry = compute.FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerVectorKernel("identity_i32", .{
        .signature = compute.KernelSignature.unary(compute.unaryArray),
        .exec = passthroughKernel,
    });

    try registry.registerAggregateKernel("count_rows", .{
        .signature = .{
            .arity = 1,
            .type_check = isUnaryArray,
            .result_type_fn = countResultType,
        },
        .exec = passthroughKernel,
        .aggregate_lifecycle = .{
            .init = countInit,
            .update = countUpdate,
            .merge = countMerge,
            .finalize = countFinalize,
            .deinit = countDeinit,
        },
    });

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = compute.ExecContext.initWithConfig(allocator, &registry, .{
        .safe_cast = true,
        .overflow_mode = .checked,
        .threads = 2,
        .arena_allocator = arena.allocator(),
    });

    const hello = try ctx.dupScalarString("hello-compute");
    std.debug.assert(std.mem.eql(u8, hello, "hello-compute"));

    var b1 = try zarrow.Int32Builder.init(allocator, 3);
    defer b1.deinit();
    try b1.append(1);
    try b1.append(2);
    try b1.append(3);
    var a1 = try b1.finish();
    defer a1.release();

    var b2 = try zarrow.Int32Builder.init(allocator, 2);
    defer b2.deinit();
    try b2.append(10);
    try b2.append(20);
    var a2 = try b2.finish();
    defer a2.release();

    const args1 = [_]compute.Datum{compute.Datum.fromArray(a1.retain())};
    defer {
        var d = args1[0];
        d.release();
    }
    const args2 = [_]compute.Datum{compute.Datum.fromArray(a2.retain())};
    defer {
        var d = args2[0];
        d.release();
    }

    var out = try ctx.invokeVector("identity_i32", args1[0..], compute.Options.noneValue());
    defer out.release();
    std.debug.assert(out.isArray());

    var s1 = try ctx.beginAggregate("count_rows", args1[0..], compute.Options.noneValue());
    defer s1.deinit();
    var s2 = try ctx.beginAggregate("count_rows", args2[0..], compute.Options.noneValue());
    defer s2.deinit();

    try s1.update(args1[0..]);
    try s2.update(args2[0..]);
    try s1.merge(&s2);

    var agg = try s1.finalize();
    defer agg.release();
    std.debug.assert(agg.isScalar());
    std.debug.assert(agg.scalar.value.i64 == 5);

    std.debug.print(
        "examples/compute_lifecycle.zig | threads={d} safe_cast={any} total_rows={d}\n",
        .{ ctx.threads(), ctx.safeCastEnabled(), agg.scalar.value.i64 },
    );
}
