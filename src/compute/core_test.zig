const std = @import("std");
const datatype = @import("../datatype.zig");
const array_mod = @import("../array/array.zig");
const core = @import("core.zig");

const DataType = core.DataType;
const ArrayRef = core.ArrayRef;
const ChunkedArray = core.ChunkedArray;
const Scalar = core.Scalar;
const Datum = core.Datum;
const FunctionRegistry = core.FunctionRegistry;
const ExecContext = core.ExecContext;
const KernelSignature = core.KernelSignature;
const KernelError = core.KernelError;
const Options = core.Options;
const OptionsTag = core.OptionsTag;
const SortOptions = core.SortOptions;
const SortOrder = core.SortOrder;
const SortNullPlacement = core.SortNullPlacement;
const FilterOptions = core.FilterOptions;
const UnaryExecChunkIterator = core.UnaryExecChunkIterator;
const BinaryExecChunkIterator = core.BinaryExecChunkIterator;
const NaryExecChunkIterator = core.NaryExecChunkIterator;

const intCastOrInvalidCast = core.intCastOrInvalidCast;
const arithmeticDivI64 = core.arithmeticDivI64;
const hasArity = core.hasArity;
const unaryArray = core.unaryArray;
const unaryChunked = core.unaryChunked;
const unaryScalar = core.unaryScalar;
const sameDataTypes = core.sameDataTypes;
const unaryNumeric = core.unaryNumeric;
const binarySameNumeric = core.binarySameNumeric;
const allNumeric = core.allNumeric;
const unaryNullPropagates = core.unaryNullPropagates;
const binaryNullPropagates = core.binaryNullPropagates;
const naryNullPropagates = core.naryNullPropagates;
const inferBinaryExecLen = core.inferBinaryExecLen;
const inferNaryExecLen = core.inferNaryExecLen;
const datumListValueAt = core.datumListValueAt;
const datumStructField = core.datumStructField;
const datumBuildEmptyLike = core.datumBuildEmptyLike;
const datumBuildEmptyLikeWithAllocator = core.datumBuildEmptyLikeWithAllocator;
const datumBuildNullLike = core.datumBuildNullLike;
const datumBuildNullLikeWithAllocator = core.datumBuildNullLikeWithAllocator;
const datumSliceEmpty = core.datumSliceEmpty;
const datumSelect = core.datumSelect;
const datumSelectNullable = core.datumSelectNullable;
const datumFilter = core.datumFilter;
const chunkedResolveLogicalIndices = core.chunkedResolveLogicalIndices;
const datumTake = core.datumTake;
const datumFilterSelectionIndices = core.datumFilterSelectionIndices;
const datumFilterChunkAware = core.datumFilterChunkAware;

fn isInt32Datum(args: []const Datum) bool {
    return args.len == 1 and args[0].dataType() == .int32;
}

fn isTwoInt32(args: []const Datum) bool {
    return args.len == 2 and args[0].dataType() == .int32 and args[1].dataType() == .int32;
}

fn isInt64Scalar(args: []const Datum) bool {
    return args.len == 1 and args[0].isScalar() and args[0].scalar.data_type == .int64;
}

fn isTwoInt64Scalars(args: []const Datum) bool {
    return args.len == 2 and args[0].isScalar() and args[1].isScalar() and args[0].scalar.data_type == .int64 and args[1].scalar.data_type == .int64;
}

fn allInt32Datums(args: []const Datum) bool {
    for (args) |arg| {
        if (!arg.dataType().eql(.{ .int32 = {} })) return false;
    }
    return true;
}

fn passthroughInt32Kernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    return args[0].retain();
}

fn exactArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 303 },
    });
}

fn rangeArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 202 },
    });
}

fn atLeastArityMarkerKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = args;
    _ = options;
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 101 },
    });
}

fn countLenAggregateKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    if (args.len != 1) return error.InvalidArity;

    const count: usize = switch (args[0]) {
        .array => |arr| arr.data().length,
        .chunked => |chunks| chunks.len(),
        .scalar => return error.InvalidInput,
    };

    return Datum.fromScalar(Scalar.init(.{ .int64 = {} }, .{ .i64 = @intCast(count) }));
}

fn onlyCastOptions(options: Options) bool {
    return switch (options) {
        .cast => true,
        else => false,
    };
}

fn onlyArithmeticOptions(options: Options) bool {
    return switch (options) {
        .arithmetic => true,
        else => false,
    };
}

fn onlySortOptions(options: Options) bool {
    return switch (options) {
        .sort => true,
        else => false,
    };
}

fn firstArgResultType(args: []const Datum, options: Options) KernelError!DataType {
    _ = options;
    if (args.len == 0) return error.InvalidInput;
    return args[0].dataType();
}

fn castResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 1) return error.InvalidArity;
    return switch (options) {
        .cast => |cast_opts| cast_opts.to_type orelse args[0].dataType(),
        else => error.InvalidOptions,
    };
}

fn castI64ToI32ResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 1) return error.InvalidArity;
    if (!args[0].dataType().eql(.{ .int64 = {} })) return error.InvalidCast;
    return switch (options) {
        .cast => |cast_opts| blk: {
            if (cast_opts.to_type) |to_type| {
                if (!to_type.eql(.{ .int32 = {} })) return error.InvalidCast;
            }
            break :blk DataType{ .int32 = {} };
        },
        else => error.InvalidOptions,
    };
}

fn castI64ToI32Kernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    if (args.len != 1) return error.InvalidArity;
    if (!isInt64Scalar(args)) return error.InvalidInput;

    const cast_opts = switch (options) {
        .cast => |o| o,
        else => return error.InvalidOptions,
    };
    if (cast_opts.to_type) |to_type| {
        if (!to_type.eql(.{ .int32 = {} })) return error.InvalidCast;
    }

    const out = try intCastOrInvalidCast(i32, args[0].scalar.value.i64);
    return Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = out },
    });
}

fn divI64ScalarResultType(args: []const Datum, options: Options) KernelError!DataType {
    if (args.len != 2) return error.InvalidArity;
    _ = switch (options) {
        .arithmetic => {},
        else => return error.InvalidOptions,
    };
    if (!isTwoInt64Scalars(args)) return error.InvalidInput;
    return .{ .int64 = {} };
}

fn divI64ScalarKernel(ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum {
    _ = ctx;
    if (args.len != 2) return error.InvalidArity;
    if (!isTwoInt64Scalars(args)) return error.InvalidInput;
    const arithmetic_opts = switch (options) {
        .arithmetic => |o| o,
        else => return error.InvalidOptions,
    };
    const lhs = args[0].scalar.value.i64;
    const rhs = args[1].scalar.value.i64;
    const out = try arithmeticDivI64(lhs, rhs, arithmetic_opts);
    return Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = out },
    });
}

fn countAggregateResultType(args: []const Datum, options: Options) KernelError!DataType {
    _ = args;
    _ = options;
    return .{ .int64 = {} };
}

const CountAggState = struct {
    count: usize,
};

fn countLifecycleInit(ctx: *ExecContext, options: Options) KernelError!*anyopaque {
    _ = options;
    const state = ctx.allocator.create(CountAggState) catch return error.OutOfMemory;
    state.* = .{ .count = 0 };
    return state;
}

fn countLifecycleUpdate(ctx: *ExecContext, state_ptr: *anyopaque, args: []const Datum, options: Options) KernelError!void {
    _ = ctx;
    _ = options;
    if (!unaryArray(args)) return error.InvalidInput;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    const count = args[0].array.data().length;
    state.count = std.math.add(usize, state.count, count) catch return error.Overflow;
}

fn countLifecycleMerge(ctx: *ExecContext, state_ptr: *anyopaque, other_ptr: *anyopaque, options: Options) KernelError!void {
    _ = ctx;
    _ = options;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    const other: *CountAggState = @ptrCast(@alignCast(other_ptr));
    state.count = std.math.add(usize, state.count, other.count) catch return error.Overflow;
}

fn countLifecycleFinalize(ctx: *ExecContext, state_ptr: *anyopaque, options: Options) KernelError!Datum {
    _ = ctx;
    _ = options;
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    return Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = @intCast(state.count) },
    });
}

fn countLifecycleDeinit(ctx: *ExecContext, state_ptr: *anyopaque) void {
    const state: *CountAggState = @ptrCast(@alignCast(state_ptr));
    ctx.allocator.destroy(state);
}

fn makeInt32Array(allocator: std.mem.Allocator, values: []const ?i32) !ArrayRef {
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var builder = try int32_builder.init(allocator, values.len);
    defer builder.deinit();

    for (values) |v| {
        if (v) |value| {
            try builder.append(value);
        } else {
            try builder.appendNull();
        }
    }
    return builder.finish();
}

fn makeBoolArray(allocator: std.mem.Allocator, values: []const ?bool) !ArrayRef {
    var builder = try array_mod.BooleanBuilder.init(allocator, values.len);
    defer builder.deinit();

    for (values) |v| {
        if (v) |value| {
            try builder.append(value);
        } else {
            try builder.appendNull();
        }
    }
    return builder.finish();
}

fn collectInt32ValuesFromDatum(allocator: std.mem.Allocator, datum: Datum) ![]?i32 {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .int32) break :blk error.InvalidInput;
            var out = try allocator.alloc(?i32, arr.data().length);
            const view = array_mod.Int32Array{ .data = arr.data() };
            var i: usize = 0;
            while (i < out.len) : (i += 1) {
                out[i] = if (view.isNull(i)) null else try view.value(i);
            }
            break :blk out;
        },
        .chunked => |chunks| blk: {
            if (!chunks.dataType().eql(.{ .int32 = {} })) break :blk error.InvalidInput;
            var out = try allocator.alloc(?i32, chunks.len());
            var out_index: usize = 0;
            var chunk_index: usize = 0;
            while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
                const chunk = chunks.chunk(chunk_index);
                const view = array_mod.Int32Array{ .data = chunk.data() };
                var i: usize = 0;
                while (i < view.len()) : (i += 1) {
                    out[out_index] = if (view.isNull(i)) null else try view.value(i);
                    out_index += 1;
                }
            }
            break :blk out;
        },
        .scalar => |s| blk: {
            if (s.data_type != .int32) break :blk error.InvalidInput;
            var out = try allocator.alloc(?i32, 1);
            out[0] = if (s.isNull()) null else s.value.i32;
            break :blk out;
        },
    };
}

test "compute registry registers and invokes scalar kernel" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
        },
        .exec = passthroughInt32Kernel,
    });

    const int32_builder = @import("../array/array.zig").Int32Builder;
    const int32_array = @import("../array/array.zig").Int32Array;

    var builder = try int32_builder.init(allocator, 3);
    defer builder.deinit();
    try builder.append(7);
    try builder.append(8);
    try builder.append(9);

    var arr = try builder.finish();
    defer arr.release();
    const args = [_]Datum{
        .{ .array = arr.retain() },
    };
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    var out = try ctx.invokeScalar("identity", args[0..], Options.noneValue());
    defer out.release();

    try std.testing.expect(out == .array);
    const view = int32_array{ .data = out.array.data() };
    try std.testing.expectEqual(@as(usize, 3), view.len());
    try std.testing.expectEqual(@as(i32, 7), view.value(0));
    try std.testing.expectEqual(@as(i32, 9), view.value(2));
}

test "compute registry reports function and arity errors" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
        },
        .exec = passthroughInt32Kernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    try std.testing.expectError(
        error.FunctionNotFound,
        ctx.invoke("missing", .scalar, &[_]Datum{}, Options.noneValue()),
    );
    try std.testing.expectError(
        error.InvalidArity,
        ctx.invoke("identity", .scalar, &[_]Datum{}, Options.noneValue()),
    );
}

test "compute registry keeps separate indices per function kind" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("same_name", .{
        .signature = .{ .arity = 1, .type_check = isInt32Datum },
        .exec = passthroughInt32Kernel,
    });
    try registry.registerVectorKernel("same_name", .{
        .signature = .{ .arity = 1, .type_check = isInt32Datum },
        .exec = passthroughInt32Kernel,
    });

    try std.testing.expect(registry.findFunction("same_name", .scalar) != null);
    try std.testing.expect(registry.findFunction("same_name", .vector) != null);
    try std.testing.expect(registry.findFunction("same_name", .aggregate) == null);
}

test "compute datum chunked variant retain and release" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;

    var builder = try int32_builder.init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var arr = try builder.finish();
    defer arr.release();

    var chunked = try ChunkedArray.fromSingle(allocator, arr);
    defer chunked.release();

    var datum = Datum{ .chunked = chunked.retain() };
    defer datum.release();

    try std.testing.expect(datum.dataType().eql(.{ .int32 = {} }));
    try std.testing.expectEqual(@as(usize, 2), datum.chunked.len());
}

test "compute registry helper APIs and function metadata" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });
    try registry.registerVectorKernel("identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });

    try std.testing.expect(registry.containsFunction("identity", .scalar));
    try std.testing.expect(registry.containsFunction("identity", .vector));
    try std.testing.expect(!registry.containsFunction("identity", .aggregate));
    try std.testing.expectEqual(@as(usize, 2), registry.functionCount());
    try std.testing.expectEqual(@as(usize, 1), registry.kernelCount("identity", .scalar));
    try std.testing.expectEqual(@as(usize, 1), registry.kernelCount("identity", .vector));
    try std.testing.expectEqual(@as(usize, 0), registry.kernelCount("identity", .aggregate));

    const f0 = registry.functionAt(0).?;
    try std.testing.expect(f0.kernelCount() > 0);
    try std.testing.expect(f0.kernelsSlice().len > 0);
}

test "compute datum helpers and shared type predicates" {
    const scalar_i32 = Scalar.init(.{ .int32 = {} }, .{ .i32 = 42 });
    const scalar_i64 = Scalar.init(.{ .int64 = {} }, .{ .i64 = 42 });

    const args_numeric = [_]Datum{
        Datum.fromScalar(scalar_i32),
        Datum.fromScalar(scalar_i64),
    };

    try std.testing.expect(hasArity(args_numeric[0..], 2));
    try std.testing.expect(!sameDataTypes(args_numeric[0..]));
    try std.testing.expect(allNumeric(args_numeric[0..]));
    try std.testing.expect(!binarySameNumeric(args_numeric[0..]));

    const args_same_numeric = [_]Datum{
        Datum.fromScalar(scalar_i32),
        Datum.fromScalar(scalar_i32),
    };
    try std.testing.expect(sameDataTypes(args_same_numeric[0..]));
    try std.testing.expect(binarySameNumeric(args_same_numeric[0..]));
    try std.testing.expect(unaryScalar(args_same_numeric[0..1]));
}

test "compute invoke helpers cover vector and aggregate kernels" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    const int32_array = @import("../array/array.zig").Int32Array;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerVectorKernel("vec_identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });
    try registry.registerAggregateKernel("count_len", .{
        .signature = KernelSignature.unary(unaryArray),
        .exec = countLenAggregateKernel,
    });

    var builder = try int32_builder.init(allocator, 4);
    defer builder.deinit();
    try builder.append(10);
    try builder.append(20);
    try builder.append(30);
    try builder.append(40);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    var vec_out = try ctx.invokeVector("vec_identity", args[0..], Options.noneValue());
    defer vec_out.release();
    try std.testing.expect(vec_out.isArray());
    const vec_view = int32_array{ .data = vec_out.array.data() };
    try std.testing.expectEqual(@as(usize, 4), vec_view.len());
    try std.testing.expectEqual(@as(i32, 10), vec_view.value(0));
    try std.testing.expectEqual(@as(i32, 40), vec_view.value(3));

    var agg_out = try ctx.invokeAggregate("count_len", args[0..], Options.noneValue());
    defer agg_out.release();
    try std.testing.expect(agg_out.isScalar());
    try std.testing.expectEqual(@as(i64, 4), agg_out.scalar.value.i64);
}

test "compute datum accessors and signature helpers" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var builder = try int32_builder.init(allocator, 2);
    defer builder.deinit();
    try builder.append(1);
    try builder.append(2);
    var arr = try builder.finish();
    defer arr.release();

    var chunked = try ChunkedArray.fromSingle(allocator, arr);
    defer chunked.release();

    var datum_arr = Datum.fromArray(arr.retain());
    defer datum_arr.release();
    try std.testing.expect(datum_arr.isArray());
    try std.testing.expect(!datum_arr.isChunked());
    try std.testing.expect(!datum_arr.isScalar());
    try std.testing.expect(datum_arr.asArray() != null);
    try std.testing.expect(datum_arr.asChunked() == null);
    try std.testing.expect(datum_arr.asScalar() == null);

    var datum_chunked = Datum.fromChunked(chunked.retain());
    defer datum_chunked.release();
    try std.testing.expect(datum_chunked.isChunked());
    try std.testing.expect(datum_chunked.asChunked() != null);
    try std.testing.expectEqual(@as(usize, 2), datum_chunked.asChunked().?.len());

    const scalar = Scalar.init(.{ .int32 = {} }, .{ .i32 = 7 });
    var datum_scalar = Datum.fromScalar(scalar);
    defer datum_scalar.release();
    try std.testing.expect(datum_scalar.isScalar());
    try std.testing.expectEqual(@as(i32, 7), datum_scalar.asScalar().?.value.i32);

    const args_bad = [_]Datum{ Datum.fromScalar(Scalar.init(.{ .int32 = {} }, .{ .i32 = 1 })), Datum.fromScalar(Scalar.init(.{ .int64 = {} }, .{ .i64 = 2 })) };
    const args_ok = [_]Datum{ Datum.fromScalar(scalar), Datum.fromScalar(scalar) };
    const sig_any = KernelSignature.any(2);
    const sig_binary_int32 = KernelSignature.binary(isTwoInt32);
    try std.testing.expect(sig_any.matches(args_bad[0..]));
    try std.testing.expect(!sig_binary_int32.matches(args_bad[0..]));
    try std.testing.expect(sig_binary_int32.matches(args_ok[0..]));
    const inferred_binary = try sig_binary_int32.inferResultType(args_ok[0..], Options.noneValue());
    try std.testing.expect(inferred_binary.eql(.{ .int32 = {} }));
}

test "compute registry functionAt out of range returns null" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("identity", .{
        .signature = KernelSignature.unary(isInt32Datum),
        .exec = passthroughInt32Kernel,
    });
    try std.testing.expect(registry.functionAt(0) != null);
    try std.testing.expect(registry.functionAt(1) == null);
    try std.testing.expect(registry.functionAt(99) == null);
}

test "compute kernel signature supports exact at_least and range arity" {
    const scalar_i32 = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 1 },
    });
    const one = [_]Datum{scalar_i32};
    const two = [_]Datum{ scalar_i32, scalar_i32 };
    const three = [_]Datum{ scalar_i32, scalar_i32, scalar_i32 };
    const four = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32 };
    const five = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32, scalar_i32 };

    const sig_exact = KernelSignature.any(2);
    const sig_at_least = KernelSignature.atLeast(2);
    const sig_range = KernelSignature.range(2, 4);

    try std.testing.expect(!sig_exact.matches(one[0..]));
    try std.testing.expect(sig_exact.matches(two[0..]));
    try std.testing.expect(!sig_exact.matches(three[0..]));

    try std.testing.expect(!sig_at_least.matches(one[0..]));
    try std.testing.expect(sig_at_least.matches(two[0..]));
    try std.testing.expect(sig_at_least.matches(five[0..]));

    try std.testing.expect(!sig_range.matches(one[0..]));
    try std.testing.expect(sig_range.matches(two[0..]));
    try std.testing.expect(sig_range.matches(three[0..]));
    try std.testing.expect(sig_range.matches(four[0..]));
    try std.testing.expect(!sig_range.matches(five[0..]));
}

test "compute resolveKernel prefers exact then range then at_least arity" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("pick_by_arity", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .type_check = allInt32Datums,
        },
        .exec = atLeastArityMarkerKernel,
    });
    try registry.registerScalarKernel("pick_by_arity", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .max_arity = 4,
            .type_check = allInt32Datums,
        },
        .exec = rangeArityMarkerKernel,
    });
    try registry.registerScalarKernel("pick_by_arity", .{
        .signature = .{
            .arity = 3,
            .type_check = allInt32Datums,
        },
        .exec = exactArityMarkerKernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const scalar_i32 = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 1 },
    });

    const args3 = [_]Datum{ scalar_i32, scalar_i32, scalar_i32 };
    var out3 = try ctx.invokeScalar("pick_by_arity", args3[0..], Options.noneValue());
    defer out3.release();
    try std.testing.expect(out3.isScalar());
    try std.testing.expectEqual(@as(i32, 303), out3.scalar.value.i32);

    const args4 = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32 };
    var out4 = try ctx.invokeScalar("pick_by_arity", args4[0..], Options.noneValue());
    defer out4.release();
    try std.testing.expect(out4.isScalar());
    try std.testing.expectEqual(@as(i32, 202), out4.scalar.value.i32);

    const args5 = [_]Datum{ scalar_i32, scalar_i32, scalar_i32, scalar_i32, scalar_i32 };
    var out5 = try ctx.invokeScalar("pick_by_arity", args5[0..], Options.noneValue());
    defer out5.release();
    try std.testing.expect(out5.isScalar());
    try std.testing.expectEqual(@as(i32, 101), out5.scalar.value.i32);
}

test "compute kernel resolution keeps InvalidArity InvalidOptions NoMatchingKernel precedence" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("priority", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .type_check = allInt32Datums,
            .options_check = onlyCastOptions,
        },
        .exec = atLeastArityMarkerKernel,
    });
    try registry.registerScalarKernel("priority_range", .{
        .signature = .{
            .arity = 2,
            .variadic = true,
            .max_arity = 3,
            .type_check = allInt32Datums,
        },
        .exec = rangeArityMarkerKernel,
    });

    const one_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
    };
    try std.testing.expectError(
        error.InvalidArity,
        registry.resolveKernel("priority", .scalar, one_i32[0..], Options.noneValue()),
    );
    const min_reason = registry.explainResolveKernelFailure("priority", .scalar, one_i32[0..], Options.noneValue());
    try std.testing.expect(std.mem.eql(u8, min_reason, "no kernel matched minimum arity"));

    const two_i64 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 2 } }),
    };
    try std.testing.expectError(
        error.NoMatchingKernel,
        registry.resolveKernel("priority", .scalar, two_i64[0..], Options.noneValue()),
    );

    const two_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 2 } }),
    };
    try std.testing.expectError(
        error.InvalidOptions,
        registry.resolveKernel("priority", .scalar, two_i32[0..], .{ .filter = .{ .drop_nulls = true } }),
    );

    const four_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 2 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 3 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 4 } }),
    };
    try std.testing.expectError(
        error.InvalidArity,
        registry.resolveKernel("priority_range", .scalar, four_i32[0..], Options.noneValue()),
    );
    const range_reason = registry.explainResolveKernelFailure("priority_range", .scalar, four_i32[0..], Options.noneValue());
    try std.testing.expect(std.mem.eql(u8, range_reason, "no kernel matched arity range"));
}

test "compute explainResolveKernelFailure prefers minimum arity diagnostic in mixed models" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("mixed_arity", .{
        .signature = .{
            .arity = 3,
            .variadic = true,
            .type_check = allInt32Datums,
        },
        .exec = atLeastArityMarkerKernel,
    });
    try registry.registerScalarKernel("mixed_arity", .{
        .signature = .{
            .arity = 5,
            .variadic = true,
            .max_arity = 7,
            .type_check = allInt32Datums,
        },
        .exec = rangeArityMarkerKernel,
    });

    const two_i32 = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 1 } }),
        Datum.fromScalar(.{ .data_type = .{ .int32 = {} }, .value = .{ .i32 = 2 } }),
    };
    try std.testing.expectError(
        error.InvalidArity,
        registry.resolveKernel("mixed_arity", .scalar, two_i32[0..], Options.noneValue()),
    );
    const reason = registry.explainResolveKernelFailure("mixed_arity", .scalar, two_i32[0..], Options.noneValue());
    try std.testing.expect(std.mem.eql(u8, reason, "no kernel matched minimum arity"));
}

test "compute registerKernel rejects invalid arity model combinations" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try std.testing.expectError(
        error.InvalidInput,
        registry.registerScalarKernel("invalid_sig_non_variadic_max", .{
            .signature = .{
                .arity = 2,
                .variadic = false,
                .max_arity = 3,
                .type_check = allInt32Datums,
            },
            .exec = exactArityMarkerKernel,
        }),
    );

    try std.testing.expectError(
        error.InvalidInput,
        registry.registerScalarKernel("invalid_sig_reversed_range", .{
            .signature = .{
                .arity = 4,
                .variadic = true,
                .max_arity = 3,
                .type_check = allInt32Datums,
            },
            .exec = rangeArityMarkerKernel,
        }),
    );
}

test "compute kernel signature result type inference and typed options" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
            .options_check = onlyCastOptions,
            .result_type_fn = castResultType,
        },
        .exec = passthroughInt32Kernel,
    });

    var builder = try int32_builder.init(allocator, 1);
    defer builder.deinit();
    try builder.append(5);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    const cast_to_i64 = Options{
        .cast = .{
            .safe = true,
            .to_type = .{ .int64 = {} },
        },
    };
    const inferred_i64 = try registry.resolveResultType("cast_identity", .scalar, args[0..], cast_to_i64);
    try std.testing.expect(inferred_i64.eql(.{ .int64 = {} }));

    var ctx = ExecContext.init(allocator, &registry);
    var out = try ctx.invokeScalar("cast_identity", args[0..], cast_to_i64);
    defer out.release();
    try std.testing.expect(out.isArray());

    try std.testing.expectError(
        error.InvalidOptions,
        ctx.invokeScalar("cast_identity", args[0..], .{ .filter = .{ .drop_nulls = true } }),
    );

    const sig = KernelSignature.unaryWithResult(isInt32Datum, firstArgResultType);
    const inferred_i32 = try sig.inferResultType(args[0..], Options.noneValue());
    try std.testing.expect(inferred_i32.eql(.{ .int32 = {} }));
}

test "compute explain helpers provide readable diagnostics" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_identity", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
            .options_check = onlyCastOptions,
            .result_type_fn = castResultType,
        },
        .exec = passthroughInt32Kernel,
    });

    var builder = try int32_builder.init(allocator, 1);
    defer builder.deinit();
    try builder.append(123);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    const function = registry.findFunction("cast_identity", .scalar).?;
    const signature = function.kernelsSlice()[0].signature;

    const bad_options = Options{ .filter = .{ .drop_nulls = true } };
    const mismatch = signature.explainMismatch(args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, mismatch, "options mismatch: options did not satisfy kernel options_check"));

    const infer_reason = signature.explainInferResultTypeFailure(args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, infer_reason, "cannot infer result type: options_check failed"));

    const resolve_reason = registry.explainResolveKernelFailure("cast_identity", .scalar, args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, resolve_reason, "kernel matched args but options were invalid"));

    const result_type_reason = registry.explainResolveResultTypeFailure("cast_identity", .scalar, args[0..], bad_options);
    try std.testing.expect(std.mem.eql(u8, result_type_reason, "kernel matched args but options were invalid"));
}

test "compute sort options defaults and tag dispatch" {
    const defaults = SortOptions{};
    try std.testing.expectEqual(SortOrder.ascending, defaults.order);
    try std.testing.expectEqual(SortNullPlacement.at_end, defaults.null_placement);
    try std.testing.expect(defaults.nan_placement == null);
    try std.testing.expect(!defaults.stable);

    const sort_options = Options{ .sort = .{} };
    try std.testing.expectEqual(OptionsTag.sort, sort_options.tag());
    try std.testing.expect(switch (sort_options) {
        .sort => |opts| opts.order == .ascending and opts.null_placement == .at_end and opts.nan_placement == null and !opts.stable,
        else => false,
    });
}

test "compute kernel signature options_check validates sort options" {
    const sig = KernelSignature{
        .arity = 1,
        .type_check = isInt32Datum,
        .options_check = onlySortOptions,
    };

    try std.testing.expect(sig.matchesOptions(.{ .sort = .{} }));
    try std.testing.expect(!sig.matchesOptions(.{ .cast = .{} }));
}

test "compute sort options mismatch path reports InvalidOptions" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;

    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("sort_options_gate", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt32Datum,
            .options_check = onlySortOptions,
        },
        .exec = passthroughInt32Kernel,
    });

    var builder = try int32_builder.init(allocator, 1);
    defer builder.deinit();
    try builder.append(42);
    var arr = try builder.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    try std.testing.expectError(
        error.InvalidOptions,
        ctx.invokeScalar("sort_options_gate", args[0..], .{ .filter = .{ .drop_nulls = true } }),
    );

    const reason = registry.explainResolveKernelFailure("sort_options_gate", .scalar, args[0..], .{ .filter = .{ .drop_nulls = true } });
    try std.testing.expect(std.mem.eql(u8, reason, "kernel matched args but options were invalid"));
}

test "compute scalar value temporal and decimal coverage" {
    const scalars = [_]Scalar{
        .{ .data_type = .{ .date32 = {} }, .value = .{ .date32 = 18_630 } },
        .{ .data_type = .{ .date64 = {} }, .value = .{ .date64 = 1_609_545_600_000 } },
        .{ .data_type = .{ .time32 = .{ .unit = .millisecond } }, .value = .{ .time32 = 1234 } },
        .{ .data_type = .{ .time64 = .{ .unit = .nanosecond } }, .value = .{ .time64 = 99_000 } },
        .{ .data_type = .{ .timestamp = .{ .unit = .microsecond, .timezone = "UTC" } }, .value = .{ .timestamp = 1_700_000_000_123_456 } },
        .{ .data_type = .{ .duration = .{ .unit = .nanosecond } }, .value = .{ .duration = 42 } },
        .{ .data_type = .{ .interval_months = .{ .unit = .months } }, .value = .{ .interval_months = 7 } },
        .{ .data_type = .{ .interval_day_time = .{ .unit = .day_time } }, .value = .{ .interval_day_time = -43_200_000 } },
        .{ .data_type = .{ .interval_month_day_nano = .{ .unit = .month_day_nano } }, .value = .{ .interval_month_day_nano = -123_456_789_012_345_678 } },
        .{ .data_type = .{ .decimal32 = .{ .precision = 9, .scale = 2 } }, .value = .{ .decimal32 = 123_45 } },
        .{ .data_type = .{ .decimal64 = .{ .precision = 18, .scale = 4 } }, .value = .{ .decimal64 = -9_876_543_210 } },
        .{ .data_type = .{ .decimal128 = .{ .precision = 38, .scale = 10 } }, .value = .{ .decimal128 = -987_654_321_098_765_432 } },
        .{ .data_type = .{ .decimal256 = .{ .precision = 76, .scale = 20 } }, .value = .{ .decimal256 = -987_654_321_098_765_432_109_876_543_210 } },
        .{ .data_type = .{ .string = {} }, .value = .{ .string = "borrowed-string" } },
        .{ .data_type = .{ .binary = {} }, .value = .{ .binary = "borrowed-binary" } },
    };

    try std.testing.expectEqual(@as(usize, 15), scalars.len);
    try std.testing.expect(scalars[0].data_type == .date32);
    try std.testing.expect(scalars[12].data_type == .decimal256);
    try std.testing.expectEqual(@as(i32, 18_630), scalars[0].value.date32);
    try std.testing.expectEqual(@as(i256, -987_654_321_098_765_432_109_876_543_210), scalars[12].value.decimal256);
    try std.testing.expectEqualStrings("borrowed-string", scalars[13].value.string);
    try std.testing.expectEqualStrings("borrowed-binary", scalars[14].value.binary);
}

test "compute exec context config and scalar payload duplication" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var ctx = ExecContext.initWithConfig(allocator, &registry, .{
        .safe_cast = false,
        .overflow_mode = .wrapping,
        .threads = 0,
        .arena_allocator = arena.allocator(),
    });
    try std.testing.expect(!ctx.safeCastEnabled());
    try std.testing.expect(ctx.overflowMode() == .wrapping);
    try std.testing.expectEqual(@as(usize, 1), ctx.threads());

    const s = try ctx.dupScalarString("hello");
    const b = try ctx.dupScalarBinary("world");
    try std.testing.expectEqualStrings("hello", s);
    try std.testing.expectEqualStrings("world", b);
}

test "compute aggregate lifecycle session supports init/update/merge/finalize" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerAggregateKernel("count_stateful", .{
        .signature = .{
            .arity = 1,
            .type_check = unaryArray,
            .result_type_fn = countAggregateResultType,
        },
        .exec = countLenAggregateKernel,
        .aggregate_lifecycle = .{
            .init = countLifecycleInit,
            .update = countLifecycleUpdate,
            .merge = countLifecycleMerge,
            .finalize = countLifecycleFinalize,
            .deinit = countLifecycleDeinit,
        },
    });

    var b1 = try int32_builder.init(allocator, 3);
    defer b1.deinit();
    try b1.append(1);
    try b1.append(2);
    try b1.append(3);
    var a1 = try b1.finish();
    defer a1.release();

    var b2 = try int32_builder.init(allocator, 2);
    defer b2.deinit();
    try b2.append(10);
    try b2.append(20);
    var a2 = try b2.finish();
    defer a2.release();

    const args1 = [_]Datum{Datum.fromArray(a1.retain())};
    defer {
        var d = args1[0];
        d.release();
    }
    const args2 = [_]Datum{Datum.fromArray(a2.retain())};
    defer {
        var d = args2[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    var s1 = try ctx.beginAggregate("count_stateful", args1[0..], Options.noneValue());
    defer s1.deinit();
    var s2 = try ctx.beginAggregate("count_stateful", args2[0..], Options.noneValue());
    defer s2.deinit();

    try s1.update(args1[0..]);
    try s2.update(args2[0..]);
    try s1.merge(&s2);

    var out = try s1.finalize();
    defer out.release();
    try std.testing.expect(out.isScalar());
    try std.testing.expectEqual(@as(i64, 5), out.scalar.value.i64);
}

test "compute beginAggregate returns MissingLifecycle when not provided" {
    const allocator = std.testing.allocator;
    const int32_builder = @import("../array/array.zig").Int32Builder;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerAggregateKernel("count_stateless_only", .{
        .signature = KernelSignature.unary(unaryArray),
        .exec = countLenAggregateKernel,
    });

    var b = try int32_builder.init(allocator, 1);
    defer b.deinit();
    try b.append(1);
    var arr = try b.finish();
    defer arr.release();

    const args = [_]Datum{Datum.fromArray(arr.retain())};
    defer {
        var d = args[0];
        d.release();
    }

    var ctx = ExecContext.init(allocator, &registry);
    try std.testing.expectError(
        error.MissingLifecycle,
        ctx.beginAggregate("count_stateless_only", args[0..], Options.noneValue()),
    );
}

test "compute execution helpers align chunked chunks for binary kernels" {
    const allocator = std.testing.allocator;
    const int32_array = @import("../array/array.zig").Int32Array;

    var l0 = try makeInt32Array(allocator, &[_]?i32{ 10, 11 });
    defer l0.release();
    var l1 = try makeInt32Array(allocator, &[_]?i32{ 20, 21, 22 });
    defer l1.release();
    var r0 = try makeInt32Array(allocator, &[_]?i32{100});
    defer r0.release();
    var r1 = try makeInt32Array(allocator, &[_]?i32{ 101, 102, 103, 104 });
    defer r1.release();

    var left_chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ l0, l1 });
    defer left_chunks.release();
    var right_chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ r0, r1 });
    defer right_chunks.release();

    var lhs = Datum.fromChunked(left_chunks.retain());
    defer lhs.release();
    var rhs = Datum.fromChunked(right_chunks.retain());
    defer rhs.release();

    var iter = try BinaryExecChunkIterator.init(lhs, rhs);
    const expected_chunk_lengths = [_]usize{ 1, 1, 3 };
    var idx: usize = 0;
    while (try iter.next()) |chunk_value| : (idx += 1) {
        var chunk = chunk_value;
        defer chunk.deinit();
        try std.testing.expect(idx < expected_chunk_lengths.len);
        try std.testing.expectEqual(expected_chunk_lengths[idx], chunk.len);
        try std.testing.expect(chunk.lhs == .array);
        try std.testing.expect(chunk.rhs == .array);
    }
    try std.testing.expectEqual(expected_chunk_lengths.len, idx);

    var iter_values = try BinaryExecChunkIterator.init(lhs, rhs);

    var c0 = (try iter_values.next()).?;
    defer c0.deinit();
    const l0_arr = int32_array{ .data = c0.lhs.array.data() };
    const r0_arr = int32_array{ .data = c0.rhs.array.data() };
    try std.testing.expectEqual(@as(i32, 10), l0_arr.value(0));
    try std.testing.expectEqual(@as(i32, 100), r0_arr.value(0));

    var c1 = (try iter_values.next()).?;
    defer c1.deinit();
    const l1_arr = int32_array{ .data = c1.lhs.array.data() };
    const r1_arr = int32_array{ .data = c1.rhs.array.data() };
    try std.testing.expectEqual(@as(i32, 11), l1_arr.value(0));
    try std.testing.expectEqual(@as(i32, 101), r1_arr.value(0));

    var c2 = (try iter_values.next()).?;
    defer c2.deinit();
    const l2_arr = int32_array{ .data = c2.lhs.array.data() };
    const r2_arr = int32_array{ .data = c2.rhs.array.data() };
    try std.testing.expectEqual(@as(i32, 20), l2_arr.value(0));
    try std.testing.expectEqual(@as(i32, 22), l2_arr.value(2));
    try std.testing.expectEqual(@as(i32, 102), r2_arr.value(0));
    try std.testing.expectEqual(@as(i32, 104), r2_arr.value(2));

    try std.testing.expect((try iter_values.next()) == null);
}

test "compute execution helpers support scalar broadcast and null propagation" {
    const allocator = std.testing.allocator;

    var r0 = try makeInt32Array(allocator, &[_]?i32{ 1, null });
    defer r0.release();
    var r1 = try makeInt32Array(allocator, &[_]?i32{3});
    defer r1.release();
    var right_chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ r0, r1 });
    defer right_chunks.release();

    const lhs = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .null,
    });
    var rhs = Datum.fromChunked(right_chunks.retain());
    defer rhs.release();

    try std.testing.expectEqual(@as(usize, 3), try inferBinaryExecLen(lhs, rhs));
    var iter = try BinaryExecChunkIterator.init(lhs, rhs);

    var seen: usize = 0;
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            try std.testing.expect(binaryNullPropagates(chunk.lhs, chunk.rhs, i));
            try std.testing.expect(chunk.binaryNullAt(i));
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "compute unary execution helper propagates nulls over chunked input" {
    const allocator = std.testing.allocator;

    var c0 = try makeInt32Array(allocator, &[_]?i32{ 7, null });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ null, 9 });
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    var input = Datum.fromChunked(chunks.retain());
    defer input.release();

    var iter = UnaryExecChunkIterator.init(input);
    var first = (try iter.next()).?;
    defer first.deinit();
    try std.testing.expect(!first.unaryNullAt(0));
    try std.testing.expect(first.unaryNullAt(1));

    var second = (try iter.next()).?;
    defer second.deinit();
    try std.testing.expect(unaryNullPropagates(second.values, 0));
    try std.testing.expect(!unaryNullPropagates(second.values, 1));

    try std.testing.expect((try iter.next()) == null);
}

test "compute nary execution helper supports array scalar chunked mixed broadcast" {
    const allocator = std.testing.allocator;
    const int32_array = @import("../array/array.zig").Int32Array;

    var base = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer base.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{ 10, 11 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{12});
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    const array_input = Datum.fromArray(base.retain());
    defer {
        var d = array_input;
        d.release();
    }
    const scalar_input = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 9 },
    });
    const chunked_input = Datum.fromChunked(chunks.retain());
    defer {
        var d = chunked_input;
        d.release();
    }

    const args = [_]Datum{ array_input, scalar_input, chunked_input };
    try std.testing.expectEqual(@as(usize, 3), try inferNaryExecLen(args[0..]));

    var iter = try NaryExecChunkIterator.init(allocator, args[0..]);
    defer iter.deinit();

    const expected_chunk_lengths = [_]usize{ 2, 1 };
    var idx: usize = 0;
    while (try iter.next()) |chunk_value| : (idx += 1) {
        var chunk = chunk_value;
        defer chunk.deinit();
        try std.testing.expect(idx < expected_chunk_lengths.len);
        try std.testing.expectEqual(expected_chunk_lengths[idx], chunk.len);
        try std.testing.expectEqual(@as(usize, 3), chunk.values.len);
        try std.testing.expect(chunk.values[0] == .array);
        try std.testing.expect(chunk.values[1] == .scalar);
        try std.testing.expect(chunk.values[2] == .array);

        const left = int32_array{ .data = chunk.values[0].array.data() };
        const right = int32_array{ .data = chunk.values[2].array.data() };
        if (idx == 0) {
            try std.testing.expectEqual(@as(i32, 1), left.value(0));
            try std.testing.expectEqual(@as(i32, 2), left.value(1));
            try std.testing.expectEqual(@as(i32, 10), right.value(0));
            try std.testing.expectEqual(@as(i32, 11), right.value(1));
        } else {
            try std.testing.expectEqual(@as(i32, 3), left.value(0));
            try std.testing.expectEqual(@as(i32, 12), right.value(0));
        }
        try std.testing.expectEqual(@as(i32, 9), chunk.values[1].scalar.value.i32);
    }
    try std.testing.expectEqual(expected_chunk_lengths.len, idx);
}

test "compute nary execution helper aligns misaligned chunk boundaries" {
    const allocator = std.testing.allocator;

    var a0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer a0.release();
    var a1 = try makeInt32Array(allocator, &[_]?i32{ 3, 4, 5 });
    defer a1.release();
    var b0 = try makeInt32Array(allocator, &[_]?i32{11});
    defer b0.release();
    var b1 = try makeInt32Array(allocator, &[_]?i32{ 12, 13, 14, 15 });
    defer b1.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{ 21, 22, 23 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ 24, 25 });
    defer c1.release();

    var chunks_a = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ a0, a1 });
    defer chunks_a.release();
    var chunks_b = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ b0, b1 });
    defer chunks_b.release();
    var chunks_c = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks_c.release();

    const d_a = Datum.fromChunked(chunks_a.retain());
    defer {
        var d = d_a;
        d.release();
    }
    const d_b = Datum.fromChunked(chunks_b.retain());
    defer {
        var d = d_b;
        d.release();
    }
    const d_c = Datum.fromChunked(chunks_c.retain());
    defer {
        var d = d_c;
        d.release();
    }
    const args = [_]Datum{ d_a, d_b, d_c };

    var iter = try NaryExecChunkIterator.init(allocator, args[0..]);
    defer iter.deinit();

    const expected = [_]usize{ 1, 1, 1, 2 };
    var idx: usize = 0;
    while (try iter.next()) |chunk_value| : (idx += 1) {
        var chunk = chunk_value;
        defer chunk.deinit();
        try std.testing.expect(idx < expected.len);
        try std.testing.expectEqual(expected[idx], chunk.len);
        for (chunk.values) |value| {
            try std.testing.expect(value == .array);
        }
    }
    try std.testing.expectEqual(expected.len, idx);
}

test "compute nary execution helper propagates nulls across all inputs" {
    const allocator = std.testing.allocator;

    var arr = try makeInt32Array(allocator, &[_]?i32{ 1, null, 3 });
    defer arr.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{4});
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ null, 6 });
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    const array_input = Datum.fromArray(arr.retain());
    defer {
        var d = array_input;
        d.release();
    }
    const scalar_null = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .null,
    });
    const chunked_input = Datum.fromChunked(chunks.retain());
    defer {
        var d = chunked_input;
        d.release();
    }

    const args = [_]Datum{ array_input, scalar_null, chunked_input };
    var iter = try NaryExecChunkIterator.init(allocator, args[0..]);
    defer iter.deinit();

    var seen: usize = 0;
    while (try iter.next()) |chunk_value| {
        var chunk = chunk_value;
        defer chunk.deinit();
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            try std.testing.expect(naryNullPropagates(chunk.values, i));
            try std.testing.expect(chunk.naryNullAt(i));
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "compute inferNaryExecLen rejects non-broadcast length mismatch" {
    const allocator = std.testing.allocator;

    var a = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer a.release();
    var b = try makeInt32Array(allocator, &[_]?i32{ 10, 11, 12 });
    defer b.release();

    const d_a = Datum.fromArray(a.retain());
    defer {
        var d = d_a;
        d.release();
    }
    const d_b = Datum.fromArray(b.retain());
    defer {
        var d = d_b;
        d.release();
    }
    const d_scalar = Datum.fromScalar(.{
        .data_type = .{ .int32 = {} },
        .value = .{ .i32 = 7 },
    });
    const args = [_]Datum{ d_a, d_b, d_scalar };
    try std.testing.expectError(error.InvalidInput, inferNaryExecLen(args[0..]));
}

test "compute inferBinaryExecLen rejects non-broadcast length mismatch" {
    const allocator = std.testing.allocator;

    var l = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer l.release();
    var r = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer r.release();

    const lhs = Datum.fromArray(l.retain());
    defer {
        var d = lhs;
        d.release();
    }
    const rhs = Datum.fromArray(r.retain());
    defer {
        var d = rhs;
        d.release();
    }

    try std.testing.expectError(error.InvalidInput, inferBinaryExecLen(lhs, rhs));
}

test "compute KernelError includes strategy-aware variants" {
    const overflow: KernelError = error.Overflow;
    const divide_by_zero: KernelError = error.DivideByZero;
    const invalid_cast: KernelError = error.InvalidCast;
    const not_implemented: KernelError = error.NotImplemented;

    try std.testing.expect(overflow == error.Overflow);
    try std.testing.expect(divide_by_zero == error.DivideByZero);
    try std.testing.expect(invalid_cast == error.InvalidCast);
    try std.testing.expect(not_implemented == error.NotImplemented);
}

test "compute arithmetic kernel maps divide-by-zero to DivideByZero" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("div_i64", .{
        .signature = .{
            .arity = 2,
            .type_check = isTwoInt64Scalars,
            .options_check = onlyArithmeticOptions,
            .result_type_fn = divI64ScalarResultType,
        },
        .exec = divI64ScalarKernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const args = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 42 } }),
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 0 } }),
    };

    try std.testing.expectError(
        error.DivideByZero,
        ctx.invokeScalar("div_i64", args[0..], .{ .arithmetic = .{} }),
    );
}

test "compute arithmetic kernel non-error divide-by-zero mode stays non-failing" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("div_i64_relaxed", .{
        .signature = .{
            .arity = 2,
            .type_check = isTwoInt64Scalars,
            .options_check = onlyArithmeticOptions,
            .result_type_fn = divI64ScalarResultType,
        },
        .exec = divI64ScalarKernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const args = [_]Datum{
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 42 } }),
        Datum.fromScalar(.{ .data_type = .{ .int64 = {} }, .value = .{ .i64 = 0 } }),
    };

    var out = try ctx.invokeScalar(
        "div_i64_relaxed",
        args[0..],
        .{ .arithmetic = .{ .check_overflow = true, .divide_by_zero_is_error = false } },
    );
    defer out.release();
    try std.testing.expect(out.isScalar());
    try std.testing.expectEqual(@as(i64, 0), out.scalar.value.i64);
}

test "compute cast kernel maps invalid conversion to InvalidCast" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_i64_to_i32", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt64Scalar,
            .options_check = onlyCastOptions,
            .result_type_fn = castI64ToI32ResultType,
        },
        .exec = castI64ToI32Kernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const too_large = Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = std.math.maxInt(i64) },
    });
    const args = [_]Datum{too_large};

    try std.testing.expectError(
        error.InvalidCast,
        ctx.invokeScalar(
            "cast_i64_to_i32",
            args[0..],
            .{ .cast = .{ .safe = true, .to_type = .{ .int32 = {} } } },
        ),
    );
}

test "compute cast kernel succeeds with valid conversion" {
    const allocator = std.testing.allocator;
    var registry = FunctionRegistry.init(allocator);
    defer registry.deinit();

    try registry.registerScalarKernel("cast_i64_to_i32_ok", .{
        .signature = .{
            .arity = 1,
            .type_check = isInt64Scalar,
            .options_check = onlyCastOptions,
            .result_type_fn = castI64ToI32ResultType,
        },
        .exec = castI64ToI32Kernel,
    });

    var ctx = ExecContext.init(allocator, &registry);
    const value = Datum.fromScalar(.{
        .data_type = .{ .int64 = {} },
        .value = .{ .i64 = 123 },
    });
    const args = [_]Datum{value};

    var out = try ctx.invokeScalar(
        "cast_i64_to_i32_ok",
        args[0..],
        .{ .cast = .{ .safe = true, .to_type = .{ .int32 = {} } } },
    );
    defer out.release();
    try std.testing.expect(out.isScalar());
    try std.testing.expectEqual(@as(i32, 123), out.scalar.value.i32);
}

test "compute nested scalar payload is retained across datum and exec chunk lifecycle" {
    const allocator = std.testing.allocator;

    const item_type = DataType{ .int32 = {} };
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &item_type,
        .nullable = true,
    };

    var values = try makeInt32Array(allocator, &[_]?i32{ 10, 20 });
    defer values.release();

    var list_builder = try array_mod.ListBuilder.init(allocator, 1, item_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2);
    var list_array = try list_builder.finish(values);
    defer list_array.release();

    var scalar = try Scalar.initNested(list_array.data().data_type, list_array);
    defer scalar.release();
    try std.testing.expect(scalar.value == .list);
    try std.testing.expect(scalar.payload != null);

    var datum = Datum.fromScalar(scalar.retain());
    defer datum.release();

    var iter_input = datum.retain();
    defer iter_input.release();
    var iter = UnaryExecChunkIterator.init(iter_input);
    var chunk = (try iter.next()).?;
    defer chunk.deinit();
    try std.testing.expectEqual(@as(usize, 1), chunk.len);
    try std.testing.expect(chunk.values == .scalar);
    try std.testing.expect(chunk.values.scalar.value == .list);
    try std.testing.expect(chunk.values.scalar.payload != null);
}

test "compute datum list extraction supports array chunked and scalar inputs" {
    const allocator = std.testing.allocator;
    const int32_array = @import("../array/array.zig").Int32Array;

    const item_type = DataType{ .int32 = {} };
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &item_type,
        .nullable = true,
    };

    var values = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer values.release();

    var list_builder = try array_mod.ListBuilder.init(allocator, 3, item_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2); // [1, 2]
    try list_builder.appendNull(); // null
    try list_builder.appendLen(1); // [3]
    var list_array = try list_builder.finish(values);
    defer list_array.release();

    var list_slice = try list_array.slice(1, 2); // [null, [3]]
    defer list_slice.release();
    var array_datum = Datum.fromArray(list_slice.retain());
    defer array_datum.release();
    var out_array = try datumListValueAt(array_datum, 1);
    defer out_array.release();
    const out_array_view = int32_array{ .data = out_array.data() };
    try std.testing.expectEqual(@as(usize, 1), out_array_view.len());
    try std.testing.expectEqual(@as(i32, 3), out_array_view.value(0));

    var list_chunk0 = try list_array.slice(0, 1);
    defer list_chunk0.release();
    var list_chunk1 = try list_array.slice(1, 2);
    defer list_chunk1.release();
    var chunked = try ChunkedArray.init(allocator, list_array.data().data_type, &[_]ArrayRef{ list_chunk0, list_chunk1 });
    defer chunked.release();
    var chunked_datum = Datum.fromChunked(chunked.retain());
    defer chunked_datum.release();
    var out_chunked = try datumListValueAt(chunked_datum, 2);
    defer out_chunked.release();
    const out_chunked_view = int32_array{ .data = out_chunked.data() };
    try std.testing.expectEqual(@as(usize, 1), out_chunked_view.len());
    try std.testing.expectEqual(@as(i32, 3), out_chunked_view.value(0));

    var scalar_payload = try list_array.slice(2, 1);
    defer scalar_payload.release();
    var scalar_datum = Datum.fromScalar(try Scalar.initNested(list_array.data().data_type, scalar_payload));
    defer scalar_datum.release();
    var out_scalar = try datumListValueAt(scalar_datum, 1234);
    defer out_scalar.release();
    const out_scalar_view = int32_array{ .data = out_scalar.data() };
    try std.testing.expectEqual(@as(usize, 1), out_scalar_view.len());
    try std.testing.expectEqual(@as(i32, 3), out_scalar_view.value(0));

    var null_payload = try list_array.slice(1, 1);
    defer null_payload.release();
    var null_scalar_datum = Datum.fromScalar(try Scalar.initNested(list_array.data().data_type, null_payload));
    defer null_scalar_datum.release();
    try std.testing.expectError(error.InvalidInput, datumListValueAt(null_scalar_datum, 0));
}

test "compute datumStructField supports scalar struct bool fields" {
    const allocator = std.testing.allocator;

    const bool_type = DataType{ .bool = {} };
    const fields = [_]datatype.Field{
        .{ .name = "a", .data_type = &bool_type, .nullable = true },
        .{ .name = "b", .data_type = &bool_type, .nullable = true },
    };

    var child_a = try makeBoolArray(allocator, &[_]?bool{true});
    defer child_a.release();
    var child_b = try makeBoolArray(allocator, &[_]?bool{false});
    defer child_b.release();

    var struct_builder = array_mod.StructBuilder.init(allocator, fields[0..]);
    defer struct_builder.deinit();
    try struct_builder.appendValid();
    var struct_array = try struct_builder.finish(&[_]ArrayRef{ child_a, child_b });
    defer struct_array.release();

    var struct_scalar_datum = Datum.fromScalar(try Scalar.initNested(struct_array.data().data_type, struct_array));
    defer struct_scalar_datum.release();

    var field0 = try datumStructField(struct_scalar_datum, 0);
    defer field0.release();
    try std.testing.expect(field0.isScalar());
    try std.testing.expect(field0.scalar.data_type == .bool);
    try std.testing.expectEqual(true, field0.scalar.value.bool);

    var field1 = try datumStructField(struct_scalar_datum, 1);
    defer field1.release();
    try std.testing.expect(field1.isScalar());
    try std.testing.expectEqual(false, field1.scalar.value.bool);

    var struct_array_datum = Datum.fromArray(struct_array.retain());
    defer struct_array_datum.release();
    var field0_array = try datumStructField(struct_array_datum, 0);
    defer field0_array.release();
    try std.testing.expect(field0_array.isArray());
    const bool_view = array_mod.BooleanArray{ .data = field0_array.array.data() };
    try std.testing.expectEqual(@as(usize, 1), bool_view.len());
    try std.testing.expectEqual(true, bool_view.value(0));

    var null_child_a = try makeBoolArray(allocator, &[_]?bool{true});
    defer null_child_a.release();
    var null_child_b = try makeBoolArray(allocator, &[_]?bool{false});
    defer null_child_b.release();
    var null_struct_builder = array_mod.StructBuilder.init(allocator, fields[0..]);
    defer null_struct_builder.deinit();
    try null_struct_builder.appendNull();
    var null_struct_array = try null_struct_builder.finish(&[_]ArrayRef{ null_child_a, null_child_b });
    defer null_struct_array.release();

    var null_struct_scalar = Datum.fromScalar(try Scalar.initNested(null_struct_array.data().data_type, null_struct_array));
    defer null_struct_scalar.release();
    var null_field = try datumStructField(null_struct_scalar, 0);
    defer null_field.release();
    try std.testing.expect(null_field.isScalar());
    try std.testing.expect(null_field.scalar.isNull());
}

test "compute datumBuildEmptyLike and datumSliceEmpty preserve nested layout" {
    const allocator = std.testing.allocator;

    const int32_type = DataType{ .int32 = {} };
    const bool_type = DataType{ .bool = {} };
    const list_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };
    const struct_fields = [_]datatype.Field{
        .{ .name = "x", .data_type = &int32_type, .nullable = true },
        .{ .name = "flag", .data_type = &bool_type, .nullable = true },
    };

    const list_type = DataType{ .list = .{ .value_field = list_field } };
    const fixed_size_list_type = DataType{ .fixed_size_list = .{ .list_size = 2, .value_field = list_field } };
    const struct_type = DataType{ .struct_ = .{ .fields = struct_fields[0..] } };

    var empty_list = try datumBuildEmptyLike(list_type);
    defer empty_list.release();
    try std.testing.expect(empty_list.isArray());
    try std.testing.expectEqual(@as(usize, 0), empty_list.array.data().length);
    try std.testing.expect(empty_list.array.data().data_type.eql(list_type));
    try empty_list.array.data().validateLayout();

    var empty_fsl = try datumBuildEmptyLike(fixed_size_list_type);
    defer empty_fsl.release();
    try std.testing.expect(empty_fsl.isArray());
    try std.testing.expectEqual(@as(usize, 0), empty_fsl.array.data().length);
    try std.testing.expect(empty_fsl.array.data().data_type.eql(fixed_size_list_type));
    try empty_fsl.array.data().validateLayout();

    var empty_struct = try datumBuildEmptyLike(struct_type);
    defer empty_struct.release();
    try std.testing.expect(empty_struct.isArray());
    try std.testing.expectEqual(@as(usize, 0), empty_struct.array.data().length);
    try std.testing.expect(empty_struct.array.data().data_type.eql(struct_type));
    try empty_struct.array.data().validateLayout();

    var values = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer values.release();
    var list_builder = try array_mod.ListBuilder.init(allocator, 1, list_field);
    defer list_builder.deinit();
    try list_builder.appendLen(2);
    var list_one = try list_builder.finish(values);
    defer list_one.release();

    var scalar_list = Datum.fromScalar(try Scalar.initNested(list_type, list_one));
    defer scalar_list.release();
    var sliced_empty = try datumSliceEmpty(scalar_list);
    defer sliced_empty.release();
    try std.testing.expect(sliced_empty.isArray());
    try std.testing.expectEqual(@as(usize, 0), sliced_empty.array.data().length);
    try std.testing.expect(sliced_empty.array.data().data_type.eql(list_type));
    try sliced_empty.array.data().validateLayout();
}

test "compute chunkedResolveLogicalIndices maps chunk-local coordinates and validates bounds" {
    const allocator = std.testing.allocator;

    var c0 = try makeInt32Array(allocator, &[_]?i32{ 10, 20 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ 30, 40, 50 });
    defer c1.release();
    var chunks = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunks.release();

    const mapped = try chunkedResolveLogicalIndices(allocator, chunks, &[_]usize{ 0, 1, 2, 4 });
    defer allocator.free(mapped);

    try std.testing.expectEqual(@as(usize, 4), mapped.len);
    try std.testing.expectEqual(@as(usize, 0), mapped[0].chunk_index);
    try std.testing.expectEqual(@as(usize, 0), mapped[0].index_in_chunk);
    try std.testing.expectEqual(@as(usize, 0), mapped[1].chunk_index);
    try std.testing.expectEqual(@as(usize, 1), mapped[1].index_in_chunk);
    try std.testing.expectEqual(@as(usize, 1), mapped[2].chunk_index);
    try std.testing.expectEqual(@as(usize, 0), mapped[2].index_in_chunk);
    try std.testing.expectEqual(@as(usize, 1), mapped[3].chunk_index);
    try std.testing.expectEqual(@as(usize, 2), mapped[3].index_in_chunk);

    try std.testing.expectError(
        error.InvalidInput,
        chunkedResolveLogicalIndices(allocator, chunks, &[_]usize{5}),
    );
}

test "compute datumTake keeps chunked output and matches array logical result on misaligned boundaries" {
    const allocator = std.testing.allocator;

    var base = try makeInt32Array(allocator, &[_]?i32{ 10, 20, 30, 40, 50 });
    defer base.release();

    var c0 = try makeInt32Array(allocator, &[_]?i32{ 10, 20 });
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{30});
    defer c1.release();
    var c2 = try makeInt32Array(allocator, &[_]?i32{ 40, 50 });
    defer c2.release();
    var chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1, c2 });
    defer chunked.release();

    var array_datum = Datum.fromArray(base.retain());
    defer array_datum.release();
    var chunked_datum = Datum.fromChunked(chunked.retain());
    defer chunked_datum.release();

    const indices = [_]usize{ 1, 2, 4, 0 };

    var out_array = try datumTake(array_datum, indices[0..]);
    defer out_array.release();
    var out_chunked = try datumTake(chunked_datum, indices[0..]);
    defer out_chunked.release();

    try std.testing.expect(out_array.isArray());
    try std.testing.expect(out_chunked.isChunked());
    try std.testing.expectEqual(@as(usize, 4), out_chunked.chunked.len());

    const array_values = try collectInt32ValuesFromDatum(allocator, out_array);
    defer allocator.free(array_values);
    const chunked_values = try collectInt32ValuesFromDatum(allocator, out_chunked);
    defer allocator.free(chunked_values);

    try std.testing.expectEqual(@as(usize, array_values.len), chunked_values.len);
    var i: usize = 0;
    while (i < array_values.len) : (i += 1) {
        try std.testing.expectEqual(array_values[i], chunked_values[i]);
    }
    try std.testing.expectEqual(@as(?i32, 20), chunked_values[0]);
    try std.testing.expectEqual(@as(?i32, 30), chunked_values[1]);
    try std.testing.expectEqual(@as(?i32, 50), chunked_values[2]);
    try std.testing.expectEqual(@as(?i32, 10), chunked_values[3]);
}

test "compute datumFilterSelectionIndices and datumFilterChunkAware stay consistent for array and chunked" {
    const allocator = std.testing.allocator;

    var values_array = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3, 4, 5 });
    defer values_array.release();
    var pred_array = try makeBoolArray(allocator, &[_]?bool{ true, false, null, true, true });
    defer pred_array.release();

    var v0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer v0.release();
    var v1 = try makeInt32Array(allocator, &[_]?i32{ 3, 4, 5 });
    defer v1.release();
    var values_chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ v0, v1 });
    defer values_chunked.release();

    var p0 = try makeBoolArray(allocator, &[_]?bool{true});
    defer p0.release();
    var p1 = try makeBoolArray(allocator, &[_]?bool{ false, null, true });
    defer p1.release();
    var p2 = try makeBoolArray(allocator, &[_]?bool{true});
    defer p2.release();
    var pred_chunked = try ChunkedArray.init(allocator, .{ .bool = {} }, &[_]ArrayRef{ p0, p1, p2 });
    defer pred_chunked.release();

    var pred_array_datum = Datum.fromArray(pred_array.retain());
    defer pred_array_datum.release();
    const selections = try datumFilterSelectionIndices(allocator, pred_array_datum, 5, .{ .drop_nulls = false });
    defer allocator.free(selections);
    try std.testing.expectEqual(@as(usize, 4), selections.len);
    try std.testing.expectEqual(@as(?usize, 0), selections[0]);
    try std.testing.expectEqual(@as(?usize, null), selections[1]);
    try std.testing.expectEqual(@as(?usize, 3), selections[2]);
    try std.testing.expectEqual(@as(?usize, 4), selections[3]);

    var array_datum = Datum.fromArray(values_array.retain());
    defer array_datum.release();
    var chunked_datum = Datum.fromChunked(values_chunked.retain());
    defer chunked_datum.release();
    var pred_chunked_datum = Datum.fromChunked(pred_chunked.retain());
    defer pred_chunked_datum.release();

    var filtered_array = try datumFilterChunkAware(array_datum, pred_array_datum, .{ .drop_nulls = false });
    defer filtered_array.release();
    var filtered_chunked = try datumFilterChunkAware(chunked_datum, pred_chunked_datum, .{ .drop_nulls = false });
    defer filtered_chunked.release();

    try std.testing.expect(filtered_array.isArray());
    try std.testing.expect(filtered_chunked.isChunked());

    const expected = try collectInt32ValuesFromDatum(allocator, filtered_array);
    defer allocator.free(expected);
    const actual = try collectInt32ValuesFromDatum(allocator, filtered_chunked);
    defer allocator.free(actual);

    try std.testing.expectEqual(@as(usize, expected.len), actual.len);
    var i: usize = 0;
    while (i < expected.len) : (i += 1) {
        try std.testing.expectEqual(expected[i], actual[i]);
    }
}

test "compute datumFilter keeps compatibility array output shape for chunked inputs" {
    const allocator = std.testing.allocator;

    var v0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2 });
    defer v0.release();
    var v1 = try makeInt32Array(allocator, &[_]?i32{ 3, 4 });
    defer v1.release();
    var values_chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ v0, v1 });
    defer values_chunked.release();

    var p0 = try makeBoolArray(allocator, &[_]?bool{ true, false, true, true });
    defer p0.release();
    var values_datum = Datum.fromChunked(values_chunked.retain());
    defer values_datum.release();
    var pred_datum = Datum.fromArray(p0.retain());
    defer pred_datum.release();

    var filtered = try datumFilter(values_datum, pred_datum, .{ .drop_nulls = true });
    defer filtered.release();
    try std.testing.expect(filtered.isArray());
}

test "compute datumFilter supports scalar array chunked and fixed_size_list null alignment" {
    const allocator = std.testing.allocator;
    const int32_array = array_mod.Int32Array;

    const int32_type = DataType{ .int32 = {} };
    const list_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };
    const list_type = DataType{ .list = .{ .value_field = list_field } };

    var scalar_values_child = try makeInt32Array(allocator, &[_]?i32{ 7, 8 });
    defer scalar_values_child.release();
    var scalar_list_builder = try array_mod.ListBuilder.init(allocator, 1, list_field);
    defer scalar_list_builder.deinit();
    try scalar_list_builder.appendLen(2);
    var scalar_list_payload = try scalar_list_builder.finish(scalar_values_child);
    defer scalar_list_payload.release();

    var scalar_datum = Datum.fromScalar(try Scalar.initNested(list_type, scalar_list_payload));
    defer scalar_datum.release();

    var pred_array = try makeBoolArray(allocator, &[_]?bool{ true, false, true });
    defer pred_array.release();
    var pred_datum = Datum.fromArray(pred_array.retain());
    defer pred_datum.release();

    var filtered_scalar = try datumFilter(scalar_datum, pred_datum, .{ .drop_nulls = true });
    defer filtered_scalar.release();
    try std.testing.expect(filtered_scalar.isArray());
    try std.testing.expectEqual(@as(usize, 2), filtered_scalar.array.data().length);
    try filtered_scalar.array.data().validateLayout();
    const filtered_scalar_list = array_mod.ListArray{ .data = filtered_scalar.array.data() };
    var fs0 = try filtered_scalar_list.value(0);
    defer fs0.release();
    const fs0_values = int32_array{ .data = fs0.data() };
    try std.testing.expectEqual(@as(i32, 7), fs0_values.value(0));
    try std.testing.expectEqual(@as(i32, 8), fs0_values.value(1));

    var list_values = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3 });
    defer list_values.release();
    var list_builder = try array_mod.ListBuilder.init(allocator, 3, list_field);
    defer list_builder.deinit();
    try list_builder.appendLen(1);
    try list_builder.appendLen(1);
    try list_builder.appendLen(1);
    var list_array = try list_builder.finish(list_values);
    defer list_array.release();

    var pred_chunk0 = try makeBoolArray(allocator, &[_]?bool{ true, null });
    defer pred_chunk0.release();
    var pred_chunk1 = try makeBoolArray(allocator, &[_]?bool{false});
    defer pred_chunk1.release();
    var pred_chunked = try ChunkedArray.init(allocator, .{ .bool = {} }, &[_]ArrayRef{ pred_chunk0, pred_chunk1 });
    defer pred_chunked.release();

    var list_datum = Datum.fromArray(list_array.retain());
    defer list_datum.release();
    var pred_chunked_datum = Datum.fromChunked(pred_chunked.retain());
    defer pred_chunked_datum.release();

    var filtered_drop_nulls = try datumFilter(list_datum, pred_chunked_datum, .{ .drop_nulls = true });
    defer filtered_drop_nulls.release();
    try std.testing.expectEqual(@as(usize, 1), filtered_drop_nulls.array.data().length);

    var filtered_keep_nulls = try datumFilter(list_datum, pred_chunked_datum, .{ .drop_nulls = false });
    defer filtered_keep_nulls.release();
    try std.testing.expectEqual(@as(usize, 2), filtered_keep_nulls.array.data().length);
    try filtered_keep_nulls.array.data().validateLayout();
    const filtered_keep_view = array_mod.ListArray{ .data = filtered_keep_nulls.array.data() };
    try std.testing.expect(!filtered_keep_view.isNull(0));
    try std.testing.expect(filtered_keep_view.isNull(1));

    var fsl_values = try makeInt32Array(allocator, &[_]?i32{ 10, 11, 20, 21, 30, 31 });
    defer fsl_values.release();
    var fsl_builder = try array_mod.FixedSizeListBuilder.init(allocator, list_field, 2);
    defer fsl_builder.deinit();
    try fsl_builder.appendValid();
    try fsl_builder.appendNull();
    try fsl_builder.appendValid();
    var fsl_array = try fsl_builder.finish(fsl_values);
    defer fsl_array.release();

    var fsl_pred = try makeBoolArray(allocator, &[_]?bool{ false, true, true });
    defer fsl_pred.release();
    var fsl_datum = Datum.fromArray(fsl_array.retain());
    defer fsl_datum.release();
    var fsl_pred_datum = Datum.fromArray(fsl_pred.retain());
    defer fsl_pred_datum.release();

    var filtered_fsl = try datumFilter(fsl_datum, fsl_pred_datum, .{ .drop_nulls = true });
    defer filtered_fsl.release();
    try std.testing.expect(filtered_fsl.isArray());
    try std.testing.expectEqual(@as(usize, 2), filtered_fsl.array.data().length);
    try filtered_fsl.array.data().validateLayout();

    const fsl_view = array_mod.FixedSizeListArray{ .data = filtered_fsl.array.data() };
    try std.testing.expect(fsl_view.isNull(0));
    try std.testing.expectEqual(@as(usize, 4), fsl_view.valuesRef().data().length);
}

test "compute datumSelect supports mixed candidate datum forms" {
    const allocator = std.testing.allocator;
    const int32_array = array_mod.Int32Array;

    var a = try makeInt32Array(allocator, &[_]?i32{ 10, 20, 30 });
    defer a.release();
    var c0 = try makeInt32Array(allocator, &[_]?i32{100});
    defer c0.release();
    var c1 = try makeInt32Array(allocator, &[_]?i32{ 200, 300 });
    defer c1.release();
    var chunked = try ChunkedArray.init(allocator, .{ .int32 = {} }, &[_]ArrayRef{ c0, c1 });
    defer chunked.release();

    const values = [_]Datum{
        Datum.fromArray(a.retain()),
        Datum.fromScalar(Scalar.init(.{ .int32 = {} }, .{ .i32 = 99 })),
        Datum.fromChunked(chunked.retain()),
    };
    defer {
        var i: usize = 0;
        while (i < values.len) : (i += 1) {
            var d = values[i];
            d.release();
        }
    }

    var out = try datumSelect(&[_]usize{ 0, 1, 2 }, values[0..]);
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 3), out.array.data().length);
    const view = int32_array{ .data = out.array.data() };
    try std.testing.expectEqual(@as(i32, 10), view.value(0));
    try std.testing.expectEqual(@as(i32, 99), view.value(1));
    try std.testing.expectEqual(@as(i32, 300), view.value(2));
}

test "compute datumSelectNullable and datumBuildNullLikeWithAllocator support null output rows" {
    const allocator = std.testing.allocator;
    const int32_array = array_mod.Int32Array;
    const int32_type = DataType{ .int32 = {} };

    var base = try makeInt32Array(allocator, &[_]?i32{ 10, 20, 30 });
    defer base.release();

    const values = [_]Datum{
        Datum.fromArray(base.retain()),
        Datum.fromScalar(Scalar.init(.{ .int32 = {} }, .{ .i32 = 88 })),
    };
    defer {
        var i: usize = 0;
        while (i < values.len) : (i += 1) {
            var d = values[i];
            d.release();
        }
    }

    var out = try datumSelectNullable(&[_]?usize{ 0, null, 1 }, values[0..]);
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 3), out.array.data().length);
    try out.array.data().validateLayout();
    const view = int32_array{ .data = out.array.data() };
    try std.testing.expectEqual(@as(i32, 10), view.value(0));
    try std.testing.expect(view.isNull(1));
    try std.testing.expectEqual(@as(i32, 88), view.value(2));

    var nulls = try datumBuildNullLikeWithAllocator(allocator, .{ .int32 = {} }, 3);
    defer nulls.release();
    try std.testing.expect(nulls.isArray());
    try std.testing.expectEqual(@as(usize, 3), nulls.array.data().length);
    const null_view = int32_array{ .data = nulls.array.data() };
    try std.testing.expect(null_view.isNull(0));
    try std.testing.expect(null_view.isNull(1));
    try std.testing.expect(null_view.isNull(2));

    const item_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };
    const fixed_size_list_type = DataType{
        .fixed_size_list = .{
            .list_size = 2,
            .value_field = item_field,
        },
    };
    var fsl_nulls = try datumBuildNullLikeWithAllocator(allocator, fixed_size_list_type, 3);
    defer fsl_nulls.release();
    try std.testing.expect(fsl_nulls.isArray());
    try std.testing.expectEqual(@as(usize, 3), fsl_nulls.array.data().length);
    try fsl_nulls.array.data().validateLayout();
    const fsl_view = array_mod.FixedSizeListArray{ .data = fsl_nulls.array.data() };
    try std.testing.expect(fsl_view.isNull(0));
    try std.testing.expect(fsl_view.isNull(1));
    try std.testing.expect(fsl_view.isNull(2));
    try std.testing.expectEqual(@as(usize, 6), fsl_view.valuesRef().data().length);
}

test "compute datumSelectNullable emits fixed_size_list null rows without child mismatch" {
    const allocator = std.testing.allocator;
    const int32_type = DataType{ .int32 = {} };
    const int32_array = array_mod.Int32Array;
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };

    var values0 = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3, 4, 5, 6 });
    defer values0.release();
    var builder0 = try array_mod.FixedSizeListBuilder.init(allocator, item_field, 2);
    defer builder0.deinit();
    try builder0.appendValid();
    try builder0.appendValid();
    try builder0.appendValid();
    var fsl0 = try builder0.finish(values0);
    defer fsl0.release();

    var values1 = try makeInt32Array(allocator, &[_]?i32{ 10, 11, 20, 21, 30, 31 });
    defer values1.release();
    var builder1 = try array_mod.FixedSizeListBuilder.init(allocator, item_field, 2);
    defer builder1.deinit();
    try builder1.appendValid();
    try builder1.appendValid();
    try builder1.appendValid();
    var fsl1 = try builder1.finish(values1);
    defer fsl1.release();

    const values = [_]Datum{
        Datum.fromArray(fsl0.retain()),
        Datum.fromArray(fsl1.retain()),
    };
    defer {
        var i: usize = 0;
        while (i < values.len) : (i += 1) {
            var d = values[i];
            d.release();
        }
    }

    var out = try datumSelectNullable(&[_]?usize{ 0, null, 1 }, values[0..]);
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 3), out.array.data().length);
    try out.array.data().validateLayout();

    const fsl_view = array_mod.FixedSizeListArray{ .data = out.array.data() };
    try std.testing.expect(!fsl_view.isNull(0));
    try std.testing.expect(fsl_view.isNull(1));
    try std.testing.expect(!fsl_view.isNull(2));
    try std.testing.expectEqual(@as(usize, 6), fsl_view.valuesRef().data().length);

    const child = int32_array{ .data = fsl_view.valuesRef().data() };
    try std.testing.expectEqual(@as(i32, 1), child.value(0));
    try std.testing.expectEqual(@as(i32, 2), child.value(1));
    try std.testing.expect(child.isNull(2));
    try std.testing.expect(child.isNull(3));
    try std.testing.expectEqual(@as(i32, 30), child.value(4));
    try std.testing.expectEqual(@as(i32, 31), child.value(5));
}

test "compute datumFilter emits fixed_size_list null rows when predicate nulls are kept" {
    const allocator = std.testing.allocator;
    const int32_type = DataType{ .int32 = {} };
    const int32_array = array_mod.Int32Array;
    const item_field = datatype.Field{
        .name = "item",
        .data_type = &int32_type,
        .nullable = true,
    };

    var values = try makeInt32Array(allocator, &[_]?i32{ 1, 2, 3, 4, 5, 6 });
    defer values.release();
    var builder = try array_mod.FixedSizeListBuilder.init(allocator, item_field, 2);
    defer builder.deinit();
    try builder.appendValid();
    try builder.appendValid();
    try builder.appendValid();
    var fsl = try builder.finish(values);
    defer fsl.release();

    var pred = try makeBoolArray(allocator, &[_]?bool{ true, null, false });
    defer pred.release();

    var in_datum = Datum.fromArray(fsl.retain());
    defer in_datum.release();
    var pred_datum = Datum.fromArray(pred.retain());
    defer pred_datum.release();

    var out = try datumFilter(in_datum, pred_datum, .{ .drop_nulls = false });
    defer out.release();
    try std.testing.expect(out.isArray());
    try std.testing.expectEqual(@as(usize, 2), out.array.data().length);
    try out.array.data().validateLayout();

    const fsl_view = array_mod.FixedSizeListArray{ .data = out.array.data() };
    try std.testing.expect(!fsl_view.isNull(0));
    try std.testing.expect(fsl_view.isNull(1));
    try std.testing.expectEqual(@as(usize, 4), fsl_view.valuesRef().data().length);

    const child = int32_array{ .data = fsl_view.valuesRef().data() };
    try std.testing.expectEqual(@as(i32, 1), child.value(0));
    try std.testing.expectEqual(@as(i32, 2), child.value(1));
    try std.testing.expect(child.isNull(2));
    try std.testing.expect(child.isNull(3));
}
