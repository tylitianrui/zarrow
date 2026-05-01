const std = @import("std");
const datatype = @import("../datatype.zig");
const array_ref_mod = @import("../array/array_ref.zig");
const array_mod = @import("../array/array.zig");
const chunked_array_mod = @import("../chunked_array.zig");
const concat_array_refs = @import("../concat_array_refs.zig");

pub const DataType = datatype.DataType;
pub const ArrayRef = array_ref_mod.ArrayRef;
pub const ChunkedArray = chunked_array_mod.ChunkedArray;

pub const FunctionKind = enum {
    scalar,
    vector,
    aggregate,
};

pub const ScalarValue = union(enum) {
    null,
    bool: bool,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    f16: f16,
    f32: f32,
    f64: f64,
    date32: i32,
    date64: i64,
    time32: i32,
    time64: i64,
    timestamp: i64,
    duration: i64,
    interval_months: i32,
    interval_day_time: i64,
    interval_month_day_nano: i128,
    decimal32: i32,
    decimal64: i64,
    decimal128: i128,
    decimal256: i256,
    /// Borrowed UTF-8 bytes. Caller (or ExecContext arena allocator) owns memory.
    string: []const u8,
    /// Borrowed raw bytes. Caller (or ExecContext arena allocator) owns memory.
    binary: []const u8,
    /// Non-null nested list value carried by Scalar.payload (len == 1).
    list,
    /// Non-null nested large_list value carried by Scalar.payload (len == 1).
    large_list,
    /// Non-null nested fixed_size_list value carried by Scalar.payload (len == 1).
    fixed_size_list,
    /// Non-null nested struct value carried by Scalar.payload (len == 1).
    struct_,
};

pub const Scalar = struct {
    data_type: DataType,
    value: ScalarValue,
    /// Optional owned array payload used for nested scalar values.
    ///
    /// For nested scalars this is expected to be length=1 and type-equal to
    /// `data_type`; reference counting is managed by retain/release.
    payload: ?ArrayRef = null,

    fn nestedValueTag(data_type: DataType) ?ScalarValue {
        return switch (data_type) {
            .list => .list,
            .large_list => .large_list,
            .fixed_size_list => .fixed_size_list,
            .struct_ => .struct_,
            else => null,
        };
    }

    pub fn init(data_type: DataType, value: ScalarValue) Scalar {
        return .{
            .data_type = data_type,
            .value = value,
            .payload = null,
        };
    }

    /// Build a nested scalar from a 1-element payload array.
    ///
    /// The payload is retained and owned by the resulting Scalar.
    pub fn initNested(data_type: DataType, payload: ArrayRef) KernelError!Scalar {
        const tag = nestedValueTag(data_type) orelse return error.UnsupportedType;
        const payload_data = payload.data();
        if (payload_data.length != 1) return error.InvalidInput;
        if (!payload_data.data_type.eql(data_type)) return error.InvalidInput;

        return .{
            .data_type = data_type,
            .value = if (payload_data.isNull(0)) ScalarValue.null else tag,
            .payload = payload.retain(),
        };
    }

    pub fn retain(self: Scalar) Scalar {
        return .{
            .data_type = self.data_type,
            .value = self.value,
            .payload = if (self.payload) |payload| payload.retain() else null,
        };
    }

    pub fn release(self: *Scalar) void {
        if (self.payload) |*payload| payload.release();
        self.* = undefined;
    }

    pub fn isNull(self: Scalar) bool {
        return switch (self.value) {
            .null => true,
            else => false,
        };
    }

    pub fn payloadArray(self: Scalar) KernelError!ArrayRef {
        return (self.payload orelse return error.InvalidInput).retain();
    }
};

pub const Datum = union(enum) {
    array: ArrayRef,
    chunked: ChunkedArray,
    scalar: Scalar,

    pub fn fromArray(arr: ArrayRef) Datum {
        return .{ .array = arr };
    }

    pub fn fromChunked(chunks: ChunkedArray) Datum {
        return .{ .chunked = chunks };
    }

    pub fn fromScalar(s: Scalar) Datum {
        return .{ .scalar = s };
    }

    pub inline fn isArray(self: Datum) bool {
        return self == .array;
    }

    pub inline fn isChunked(self: Datum) bool {
        return self == .chunked;
    }

    pub inline fn isScalar(self: Datum) bool {
        return self == .scalar;
    }

    pub fn asArray(self: Datum) ?ArrayRef {
        return switch (self) {
            .array => |arr| arr,
            else => null,
        };
    }

    pub fn asChunked(self: Datum) ?ChunkedArray {
        return switch (self) {
            .chunked => |chunks| chunks,
            else => null,
        };
    }

    pub fn asScalar(self: Datum) ?Scalar {
        return switch (self) {
            .scalar => |s| s,
            else => null,
        };
    }

    pub fn retain(self: Datum) Datum {
        return switch (self) {
            .array => |arr| .{ .array = arr.retain() },
            .chunked => |chunks| .{ .chunked = chunks.retain() },
            .scalar => |s| .{ .scalar = s.retain() },
        };
    }

    pub fn release(self: *Datum) void {
        switch (self.*) {
            .array => |*arr| arr.release(),
            .chunked => |*chunks| chunks.release(),
            .scalar => |*s| s.release(),
        }
        self.* = undefined;
    }

    pub fn dataType(self: Datum) DataType {
        return switch (self) {
            .array => |arr| arr.data().data_type,
            .chunked => |chunks| chunks.dataType(),
            .scalar => |s| s.data_type,
        };
    }
};

pub const KernelError = error{
    OutOfMemory,
    FunctionNotFound,
    InvalidArity,
    InvalidOptions,
    InvalidInput,
    Overflow,
    DivideByZero,
    InvalidCast,
    UnsupportedType,
    NotImplemented,
    MissingLifecycle,
    AggregateStateMismatch,
    NoMatchingKernel,
};

pub const TypeCheckFn = *const fn (args: []const Datum) bool;
/// Well-known options payload tags used by compute kernels.
pub const OptionsTag = enum {
    /// Kernel does not require options.
    none,
    /// Type conversion / cast behavior.
    cast,
    /// Arithmetic behavior (overflow, divide-by-zero, etc.).
    arithmetic,
    /// Filter behavior.
    filter,
    /// Sort behavior (order, null placement, NaN handling, stability).
    sort,
    /// Escape hatch for downstream custom kernels.
    custom,
};

/// Common options for cast-like kernels.
pub const CastOptions = struct {
    /// If true, conversion must fail on lossy/overflowing cast.
    safe: bool = true,
    /// Optional target type override used by cast kernels.
    to_type: ?DataType = null,
};

/// Common options for arithmetic kernels.
pub const ArithmeticOptions = struct {
    /// If true, overflow should produce an error instead of wrapping.
    check_overflow: bool = true,
    /// If true, division by zero should return an error.
    divide_by_zero_is_error: bool = true,
};

/// Common options for filter-like kernels.
pub const FilterOptions = struct {
    /// If true, nulls in predicate are treated as false and dropped.
    drop_nulls: bool = true,
};

/// Sort direction for sort-like kernels.
pub const SortOrder = enum {
    ascending,
    descending,
};

/// Null placement strategy for sort-like kernels.
pub const SortNullPlacement = enum {
    at_start,
    at_end,
};

/// NaN placement strategy for floating-point sort-like kernels.
pub const SortNaNPlacement = enum {
    at_start,
    at_end,
};

/// Common options for sort-like kernels.
pub const SortOptions = struct {
    /// Ordering direction.
    order: SortOrder = .ascending,
    /// Null placement behavior.
    null_placement: SortNullPlacement = .at_end,
    /// Optional NaN placement policy for floating-point inputs.
    nan_placement: ?SortNaNPlacement = null,
    /// If true, equal-key ordering should be stable.
    stable: bool = false,
};

/// Custom untyped options hook for downstream extension kernels.
pub const CustomOptions = struct {
    /// Caller-defined discriminator.
    tag: []const u8,
    /// Optional opaque payload owned by caller.
    payload: ?*const anyopaque = null,
};

/// Type-safe options payload passed to all kernel signatures and executors.
pub const Options = union(OptionsTag) {
    none: void,
    cast: CastOptions,
    arithmetic: ArithmeticOptions,
    filter: FilterOptions,
    sort: SortOptions,
    custom: CustomOptions,

    pub fn noneValue() Options {
        return .{ .none = {} };
    }

    pub fn tag(self: Options) OptionsTag {
        return std.meta.activeTag(self);
    }
};

pub const OptionsCheckFn = *const fn (options: Options) bool;
pub const ResultTypeFn = *const fn (args: []const Datum, options: Options) KernelError!DataType;
pub const KernelExecFn = *const fn (ctx: *ExecContext, args: []const Datum, options: Options) KernelError!Datum;
pub const AggregateInitFn = *const fn (ctx: *ExecContext, options: Options) KernelError!*anyopaque;
pub const AggregateUpdateFn = *const fn (ctx: *ExecContext, state: *anyopaque, args: []const Datum, options: Options) KernelError!void;
pub const AggregateMergeFn = *const fn (ctx: *ExecContext, state: *anyopaque, other_state: *anyopaque, options: Options) KernelError!void;
pub const AggregateFinalizeFn = *const fn (ctx: *ExecContext, state: *anyopaque, options: Options) KernelError!Datum;
pub const AggregateDeinitFn = *const fn (ctx: *ExecContext, state: *anyopaque) void;

/// Stateful aggregate lifecycle callbacks used for incremental/grouped aggregation.
pub const AggregateLifecycle = struct {
    init: AggregateInitFn,
    update: AggregateUpdateFn,
    merge: AggregateMergeFn,
    finalize: AggregateFinalizeFn,
    deinit: AggregateDeinitFn,
};

/// Kernel signature metadata used for dispatch and type inference.
pub const KernelSignature = struct {
    /// Required argument count (exact arity or minimum arity for variadic kernels).
    arity: usize,
    /// Whether the signature accepts variadic arguments.
    variadic: bool = false,
    /// Optional maximum arity for bounded variadic signatures.
    max_arity: ?usize = null,
    /// Optional argument validator for logical type matching.
    type_check: ?TypeCheckFn = null,
    /// Optional options validator for type-safe options enforcement.
    options_check: ?OptionsCheckFn = null,
    /// Optional result type inference callback.
    result_type_fn: ?ResultTypeFn = null,

    pub fn any(arity: usize) KernelSignature {
        return .{
            .arity = arity,
            .variadic = false,
            .max_arity = null,
            .type_check = null,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn atLeast(min_arity: usize) KernelSignature {
        return .{
            .arity = min_arity,
            .variadic = true,
            .max_arity = null,
            .type_check = null,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn range(min_arity: usize, max_arity: usize) KernelSignature {
        std.debug.assert(max_arity >= min_arity);
        return .{
            .arity = min_arity,
            .variadic = true,
            .max_arity = max_arity,
            .type_check = null,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn unary(type_check: ?TypeCheckFn) KernelSignature {
        return .{
            .arity = 1,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn binary(type_check: ?TypeCheckFn) KernelSignature {
        return .{
            .arity = 2,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = null,
        };
    }

    pub fn unaryWithResult(type_check: ?TypeCheckFn, result_type_fn: ResultTypeFn) KernelSignature {
        return .{
            .arity = 1,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = result_type_fn,
        };
    }

    pub fn binaryWithResult(type_check: ?TypeCheckFn, result_type_fn: ResultTypeFn) KernelSignature {
        return .{
            .arity = 2,
            .variadic = false,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = result_type_fn,
        };
    }

    pub fn variadicWithResult(min_arity: usize, type_check: ?TypeCheckFn, result_type_fn: ResultTypeFn) KernelSignature {
        return .{
            .arity = min_arity,
            .variadic = true,
            .max_arity = null,
            .type_check = type_check,
            .options_check = null,
            .result_type_fn = result_type_fn,
        };
    }

    const ArityModel = enum {
        exact,
        at_least,
        range,
    };

    fn arityModel(self: KernelSignature) ArityModel {
        if (!self.variadic) return .exact;
        if (self.max_arity != null) return .range;
        return .at_least;
    }

    fn aritySpecificityRank(self: KernelSignature) u8 {
        return switch (self.arityModel()) {
            .exact => 3,
            .range => 2,
            .at_least => 1,
        };
    }

    fn hasValidArityModel(self: KernelSignature) bool {
        if (!self.variadic) return self.max_arity == null;
        if (self.max_arity) |max| return max >= self.arity;
        return true;
    }

    pub fn isValidArityModel(self: KernelSignature) bool {
        return self.hasValidArityModel();
    }

    pub fn matchesArity(self: KernelSignature, arg_count: usize) bool {
        std.debug.assert(self.hasValidArityModel());
        if (!self.hasValidArityModel()) return false;
        return switch (self.arityModel()) {
            .exact => arg_count == self.arity,
            .at_least => arg_count >= self.arity,
            .range => arg_count >= self.arity and arg_count <= self.max_arity.?,
        };
    }

    pub fn matches(self: KernelSignature, args: []const Datum) bool {
        if (!self.matchesArity(args.len)) return false;
        if (self.type_check) |check| return check(args);
        return true;
    }

    pub fn matchesOptions(self: KernelSignature, options: Options) bool {
        if (self.options_check) |check| return check(options);
        return true;
    }

    pub fn accepts(self: KernelSignature, args: []const Datum, options: Options) bool {
        return self.matches(args) and self.matchesOptions(options);
    }

    /// Human-readable mismatch reason for diagnostics and debugging.
    pub fn explainMismatch(self: KernelSignature, args: []const Datum, options: Options) []const u8 {
        if (!self.matchesArity(args.len)) {
            return switch (self.arityModel()) {
                .exact => "arity mismatch: argument count does not match exact kernel arity",
                .at_least => "arity mismatch: argument count is below minimum kernel arity",
                .range => "arity mismatch: argument count is outside kernel arity range",
            };
        }
        if (self.type_check) |check| {
            if (!check(args)) return "type mismatch: arguments did not satisfy kernel type_check";
        }
        if (self.options_check) |check| {
            if (!check(options)) return "options mismatch: options did not satisfy kernel options_check";
        }
        return "signature accepted";
    }

    pub fn inferResultType(self: KernelSignature, args: []const Datum, options: Options) KernelError!DataType {
        if (!self.matchesArity(args.len)) return error.InvalidArity;
        if (self.type_check) |check| {
            if (!check(args)) return error.NoMatchingKernel;
        }
        if (self.options_check) |check| {
            if (!check(options)) return error.InvalidOptions;
        }
        if (self.result_type_fn) |infer| {
            return infer(args, options);
        }
        if (args.len == 0) return error.InvalidInput;
        return args[0].dataType();
    }

    /// Human-readable reason for result-type inference failure.
    pub fn explainInferResultTypeFailure(self: KernelSignature, args: []const Datum, options: Options) []const u8 {
        if (!self.matchesArity(args.len)) return "cannot infer result type: invalid arity";
        if (self.type_check) |check| {
            if (!check(args)) return "cannot infer result type: argument type_check failed";
        }
        if (self.options_check) |check| {
            if (!check(options)) return "cannot infer result type: options_check failed";
        }
        if (self.result_type_fn == null and args.len == 0) return "cannot infer result type: no arguments and no result_type_fn";
        return "result type inference should succeed";
    }
};

pub const Kernel = struct {
    signature: KernelSignature,
    exec: KernelExecFn,
    aggregate_lifecycle: ?AggregateLifecycle = null,

    pub fn supportsAggregateLifecycle(self: Kernel) bool {
        return self.aggregate_lifecycle != null;
    }
};

/// Live aggregate state handle created from an aggregate kernel lifecycle.
pub const AggregateSession = struct {
    ctx: *ExecContext,
    kernel: *const Kernel,
    lifecycle: AggregateLifecycle,
    options: Options,
    state: *anyopaque,

    pub fn update(self: *AggregateSession, args: []const Datum) KernelError!void {
        if (!self.kernel.signature.matches(args)) return error.NoMatchingKernel;
        if (!self.kernel.signature.matchesOptions(self.options)) return error.InvalidOptions;
        return self.lifecycle.update(self.ctx, self.state, args, self.options);
    }

    pub fn merge(self: *AggregateSession, other: *AggregateSession) KernelError!void {
        if (self.kernel != other.kernel) return error.AggregateStateMismatch;
        return self.lifecycle.merge(self.ctx, self.state, other.state, self.options);
    }

    pub fn finalize(self: *AggregateSession) KernelError!Datum {
        return self.lifecycle.finalize(self.ctx, self.state, self.options);
    }

    pub fn deinit(self: *AggregateSession) void {
        self.lifecycle.deinit(self.ctx, self.state);
        self.* = undefined;
    }
};

const function_kind_count = @typeInfo(FunctionKind).@"enum".fields.len;
const FunctionIndexMap = std.StringHashMap(usize);

pub const Function = struct {
    allocator: std.mem.Allocator,
    name: []u8,
    kind: FunctionKind,
    kernels: std.ArrayList(Kernel),

    pub fn kernelCount(self: *const Function) usize {
        return self.kernels.items.len;
    }

    pub fn kernelsSlice(self: *const Function) []const Kernel {
        return self.kernels.items;
    }

    fn deinit(self: *Function) void {
        self.kernels.deinit(self.allocator);
        self.allocator.free(self.name);
    }
};

pub const FunctionRegistry = struct {
    allocator: std.mem.Allocator,
    functions: std.ArrayList(Function),
    function_index_by_kind: [function_kind_count]FunctionIndexMap,

    pub fn init(allocator: std.mem.Allocator) FunctionRegistry {
        return .{
            .allocator = allocator,
            .functions = .{},
            .function_index_by_kind = initFunctionIndexMaps(allocator),
        };
    }

    pub fn deinit(self: *FunctionRegistry) void {
        for (&self.function_index_by_kind) |*index_map| {
            index_map.deinit();
        }
        for (self.functions.items) |*entry| {
            entry.deinit();
        }
        self.functions.deinit(self.allocator);
    }

    pub fn registerKernel(
        self: *FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        kernel: Kernel,
    ) KernelError!void {
        if (!kernel.signature.isValidArityModel()) return error.InvalidInput;
        if (self.findFunctionIndex(name, kind)) |idx| {
            try self.functions.items[idx].kernels.append(self.allocator, kernel);
            return;
        }

        var entry = Function{
            .allocator = self.allocator,
            .name = try self.allocator.dupe(u8, name),
            .kind = kind,
            .kernels = .{},
        };
        errdefer self.allocator.free(entry.name);
        try entry.kernels.append(self.allocator, kernel);
        try self.functions.append(self.allocator, entry);
        errdefer {
            var popped = self.functions.pop().?;
            popped.deinit();
        }
        const new_idx = self.functions.items.len - 1;
        try self.getIndexMap(kind).put(self.functions.items[new_idx].name, new_idx);
    }

    pub fn registerScalarKernel(self: *FunctionRegistry, name: []const u8, kernel: Kernel) KernelError!void {
        return self.registerKernel(name, .scalar, kernel);
    }

    pub fn registerVectorKernel(self: *FunctionRegistry, name: []const u8, kernel: Kernel) KernelError!void {
        return self.registerKernel(name, .vector, kernel);
    }

    pub fn registerAggregateKernel(self: *FunctionRegistry, name: []const u8, kernel: Kernel) KernelError!void {
        return self.registerKernel(name, .aggregate, kernel);
    }

    pub fn containsFunction(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) bool {
        return self.findFunction(name, kind) != null;
    }

    pub fn functionCount(self: *const FunctionRegistry) usize {
        return self.functions.items.len;
    }

    pub fn functionAt(self: *const FunctionRegistry, index: usize) ?*const Function {
        if (index >= self.functions.items.len) return null;
        return &self.functions.items[index];
    }

    pub fn kernelCount(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) usize {
        const function = self.findFunction(name, kind) orelse return 0;
        return function.kernels.items.len;
    }

    pub fn findFunction(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) ?*const Function {
        const idx = self.findFunctionIndex(name, kind) orelse return null;
        return &self.functions.items[idx];
    }

    pub fn resolveKernel(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!*const Kernel {
        const function = self.findFunction(name, kind) orelse return error.FunctionNotFound;
        if (function.kernels.items.len == 0) return error.NoMatchingKernel;

        var saw_matching_arity = false;
        var saw_matching_type = false;
        var best_kernel: ?*const Kernel = null;
        var best_specificity: u8 = 0;
        for (function.kernels.items) |*kernel| {
            if (!kernel.signature.matchesArity(args.len)) continue;
            saw_matching_arity = true;
            if (!kernel.signature.matches(args)) continue;
            saw_matching_type = true;
            if (!kernel.signature.matchesOptions(options)) continue;

            const specificity = kernel.signature.aritySpecificityRank();
            if (best_kernel == null or specificity > best_specificity) {
                best_kernel = kernel;
                best_specificity = specificity;
            }
        }
        if (best_kernel) |kernel| return kernel;
        if (!saw_matching_arity) return error.InvalidArity;
        if (saw_matching_type) return error.InvalidOptions;
        return error.NoMatchingKernel;
    }

    /// Explain why kernel resolution would fail for the given call site.
    pub fn explainResolveKernelFailure(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) []const u8 {
        const function = self.findFunction(name, kind) orelse return "function not found";
        if (function.kernels.items.len == 0) return "function has no registered kernels";

        var saw_matching_arity = false;
        var saw_matching_type = false;
        var best_specificity: u8 = 0;
        var saw_matching_kernel = false;
        var has_exact = false;
        var has_range = false;
        var has_at_least = false;
        var any_min_gt_arg = false;
        var any_range_excludes_arg = false;
        for (function.kernels.items) |*kernel| {
            switch (kernel.signature.arityModel()) {
                .exact => has_exact = true,
                .range => has_range = true,
                .at_least => has_at_least = true,
            }
            if (args.len < kernel.signature.arity) any_min_gt_arg = true;
            if (kernel.signature.arityModel() == .range and !kernel.signature.matchesArity(args.len)) {
                any_range_excludes_arg = true;
            }

            if (!kernel.signature.matchesArity(args.len)) continue;
            saw_matching_arity = true;
            if (!kernel.signature.matches(args)) continue;
            saw_matching_type = true;
            if (!kernel.signature.matchesOptions(options)) continue;
            const specificity = kernel.signature.aritySpecificityRank();
            if (!saw_matching_kernel or specificity > best_specificity) {
                best_specificity = specificity;
                saw_matching_kernel = true;
            }
        }
        if (saw_matching_kernel) return "kernel resolution should succeed";
        if (!saw_matching_arity) {
            if (has_at_least and any_min_gt_arg) return "no kernel matched minimum arity";
            if (has_range and any_range_excludes_arg) return "no kernel matched arity range";
            if (has_exact) return "no kernel matched exact arity";
            return "no kernel matched arity";
        }
        if (saw_matching_type) return "kernel matched args but options were invalid";
        return "no kernel matched argument types";
    }

    pub fn resolveResultType(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!DataType {
        const kernel = try self.resolveKernel(name, kind, args, options);
        return kernel.signature.inferResultType(args, options);
    }

    /// Explain why result-type inference would fail for the given call site.
    pub fn explainResolveResultTypeFailure(
        self: *const FunctionRegistry,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) []const u8 {
        const function = self.findFunction(name, kind) orelse return "cannot infer result type: function not found";
        if (function.kernels.items.len == 0) return "cannot infer result type: function has no kernels";
        for (function.kernels.items) |*kernel| {
            if (!kernel.signature.accepts(args, options)) continue;
            return kernel.signature.explainInferResultTypeFailure(args, options);
        }
        return self.explainResolveKernelFailure(name, kind, args, options);
    }

    pub fn invoke(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        const kernel = try self.resolveKernel(name, kind, args, options);
        _ = try kernel.signature.inferResultType(args, options);
        return kernel.exec(ctx, args, options);
    }

    pub fn invokeScalar(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .scalar, args, options);
    }

    pub fn invokeVector(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .vector, args, options);
    }

    pub fn invokeAggregate(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.invoke(ctx, name, .aggregate, args, options);
    }

    /// Create a stateful aggregate session from an aggregate kernel lifecycle.
    pub fn beginAggregate(
        self: *const FunctionRegistry,
        ctx: *ExecContext,
        name: []const u8,
        prototype_args: []const Datum,
        options: Options,
    ) KernelError!AggregateSession {
        const kernel = try self.resolveKernel(name, .aggregate, prototype_args, options);
        const lifecycle = kernel.aggregate_lifecycle orelse return error.MissingLifecycle;
        const state = try lifecycle.init(ctx, options);
        return .{
            .ctx = ctx,
            .kernel = kernel,
            .lifecycle = lifecycle,
            .options = options,
            .state = state,
        };
    }

    fn findFunctionIndex(self: *const FunctionRegistry, name: []const u8, kind: FunctionKind) ?usize {
        const idx = self.getIndexMapConst(kind).get(name) orelse return null;
        if (idx >= self.functions.items.len) return null;
        return idx;
    }

    fn getIndexMap(self: *FunctionRegistry, kind: FunctionKind) *FunctionIndexMap {
        return &self.function_index_by_kind[@intFromEnum(kind)];
    }

    fn getIndexMapConst(self: *const FunctionRegistry, kind: FunctionKind) *const FunctionIndexMap {
        return &self.function_index_by_kind[@intFromEnum(kind)];
    }
};

fn initFunctionIndexMaps(allocator: std.mem.Allocator) [function_kind_count]FunctionIndexMap {
    var maps: [function_kind_count]FunctionIndexMap = undefined;
    inline for (0..function_kind_count) |i| {
        maps[i] = FunctionIndexMap.init(allocator);
    }
    return maps;
}

/// Overflow policy for arithmetic kernels.
pub const OverflowMode = enum {
    checked,
    wrapping,
    saturating,
};

/// Execution configuration shared by all kernel invocations in a context.
pub const ExecConfig = struct {
    /// Safe cast mode for cast kernels (fail on lossy casts when true).
    safe_cast: bool = true,
    /// Overflow policy for arithmetic kernels.
    overflow_mode: OverflowMode = .checked,
    /// Preferred thread count for vector/aggregate execution.
    threads: usize = 1,
    /// Optional arena-like allocator used for temporary/borrowed scalar payloads.
    arena_allocator: ?std.mem.Allocator = null,
};

pub const ExecContext = struct {
    allocator: std.mem.Allocator,
    registry: *const FunctionRegistry,
    config: ExecConfig,

    pub fn init(allocator: std.mem.Allocator, registry: *const FunctionRegistry) ExecContext {
        return initWithConfig(allocator, registry, .{});
    }

    pub fn initWithConfig(allocator: std.mem.Allocator, registry: *const FunctionRegistry, config: ExecConfig) ExecContext {
        var normalized = config;
        if (normalized.threads == 0) normalized.threads = 1;
        return .{
            .allocator = allocator,
            .registry = registry,
            .config = normalized,
        };
    }

    pub fn tempAllocator(self: *const ExecContext) std.mem.Allocator {
        return self.config.arena_allocator orelse self.allocator;
    }

    /// Duplicate UTF-8 bytes into the context temp allocator for scalar string payloads.
    pub fn dupScalarString(self: *const ExecContext, value: []const u8) KernelError![]const u8 {
        return self.tempAllocator().dupe(u8, value) catch error.OutOfMemory;
    }

    /// Duplicate raw bytes into the context temp allocator for scalar binary payloads.
    pub fn dupScalarBinary(self: *const ExecContext, value: []const u8) KernelError![]const u8 {
        return self.tempAllocator().dupe(u8, value) catch error.OutOfMemory;
    }

    pub fn safeCastEnabled(self: *const ExecContext) bool {
        return self.config.safe_cast;
    }

    pub fn overflowMode(self: *const ExecContext) OverflowMode {
        return self.config.overflow_mode;
    }

    pub fn threads(self: *const ExecContext) usize {
        return self.config.threads;
    }

    pub fn invoke(
        self: *ExecContext,
        name: []const u8,
        kind: FunctionKind,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invoke(self, name, kind, args, options);
    }

    pub fn invokeScalar(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invokeScalar(self, name, args, options);
    }

    pub fn invokeVector(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invokeVector(self, name, args, options);
    }

    pub fn invokeAggregate(
        self: *ExecContext,
        name: []const u8,
        args: []const Datum,
        options: Options,
    ) KernelError!Datum {
        return self.registry.invokeAggregate(self, name, args, options);
    }

    pub fn beginAggregate(
        self: *ExecContext,
        name: []const u8,
        prototype_args: []const Datum,
        options: Options,
    ) KernelError!AggregateSession {
        return self.registry.beginAggregate(self, name, prototype_args, options);
    }
};

pub const ExecChunkValue = union(enum) {
    array: ArrayRef,
    scalar: Scalar,

    pub fn dataType(self: ExecChunkValue) DataType {
        return switch (self) {
            .array => |arr| arr.data().data_type,
            .scalar => |s| s.data_type,
        };
    }

    pub fn isNullAt(self: ExecChunkValue, logical_index: usize) bool {
        return switch (self) {
            .array => |arr| blk: {
                std.debug.assert(logical_index < arr.data().length);
                break :blk arr.data().isNull(logical_index);
            },
            .scalar => |s| s.isNull(),
        };
    }

    pub fn release(self: *ExecChunkValue) void {
        switch (self.*) {
            .array => |*arr| arr.release(),
            .scalar => |*s| s.release(),
        }
        self.* = undefined;
    }
};

/// Common null propagation helper for unary kernels.
pub fn unaryNullPropagates(input: ExecChunkValue, logical_index: usize) bool {
    return input.isNullAt(logical_index);
}

/// Common null propagation helper for binary kernels.
pub fn binaryNullPropagates(lhs: ExecChunkValue, rhs: ExecChunkValue, logical_index: usize) bool {
    return lhs.isNullAt(logical_index) or rhs.isNullAt(logical_index);
}

/// Common null propagation helper for n-ary kernels.
pub fn naryNullPropagates(values: []const ExecChunkValue, logical_index: usize) bool {
    for (values) |value| {
        if (value.isNullAt(logical_index)) return true;
    }
    return false;
}

pub const UnaryExecChunk = struct {
    values: ExecChunkValue,
    len: usize,

    pub fn unaryNullAt(self: UnaryExecChunk, logical_index: usize) bool {
        std.debug.assert(logical_index < self.len);
        return unaryNullPropagates(self.values, logical_index);
    }

    pub fn deinit(self: *UnaryExecChunk) void {
        self.values.release();
        self.* = undefined;
    }
};

pub const BinaryExecChunk = struct {
    lhs: ExecChunkValue,
    rhs: ExecChunkValue,
    len: usize,

    pub fn binaryNullAt(self: BinaryExecChunk, logical_index: usize) bool {
        std.debug.assert(logical_index < self.len);
        return binaryNullPropagates(self.lhs, self.rhs, logical_index);
    }

    pub fn deinit(self: *BinaryExecChunk) void {
        self.lhs.release();
        self.rhs.release();
        self.* = undefined;
    }
};

pub const NaryExecChunk = struct {
    allocator: std.mem.Allocator,
    values: []ExecChunkValue,
    len: usize,

    pub fn naryNullAt(self: NaryExecChunk, logical_index: usize) bool {
        std.debug.assert(logical_index < self.len);
        return naryNullPropagates(self.values, logical_index);
    }

    pub fn deinit(self: *NaryExecChunk) void {
        for (self.values) |*value| {
            value.release();
        }
        self.allocator.free(self.values);
        self.* = undefined;
    }
};

fn datumArrayLikeLen(datum: Datum) ?usize {
    return switch (datum) {
        .array => |arr| arr.data().length,
        .chunked => |chunks| chunks.len(),
        .scalar => null,
    };
}

/// Infer binary execution length with scalar-broadcast semantics.
pub fn inferBinaryExecLen(lhs: Datum, rhs: Datum) KernelError!usize {
    const lhs_len = datumArrayLikeLen(lhs);
    const rhs_len = datumArrayLikeLen(rhs);

    if (lhs_len == null and rhs_len == null) return 1;
    if (lhs_len == null) return rhs_len.?;
    if (rhs_len == null) return lhs_len.?;
    if (lhs_len.? != rhs_len.?) return error.InvalidInput;
    return lhs_len.?;
}

/// Infer n-ary execution length with scalar-broadcast semantics.
pub fn inferNaryExecLen(args: []const Datum) KernelError!usize {
    if (args.len == 0) return error.InvalidArity;

    var array_like_len: ?usize = null;
    for (args) |arg| {
        const current_len = datumArrayLikeLen(arg);
        if (current_len == null) continue;
        if (array_like_len == null) {
            array_like_len = current_len.?;
            continue;
        }
        if (array_like_len.? != current_len.?) return error.InvalidInput;
    }
    return array_like_len orelse 1;
}

fn mapArrayReadError(err: anyerror) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidInput,
    };
}

fn mapChunkedError(err: chunked_array_mod.Error) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.LengthOverflow => error.Overflow,
        else => error.InvalidInput,
    };
}

const ChunkLookup = struct {
    chunk: *const ArrayRef,
    local_index: usize,
};

/// Chunk-local coordinate for a logical row in a chunked datum.
pub const ChunkLocalIndex = struct {
    chunk_index: usize,
    index_in_chunk: usize,
};

const ChunkIndexResolver = struct {
    offsets: []usize,

    fn init(allocator: std.mem.Allocator, chunks: ChunkedArray) KernelError!ChunkIndexResolver {
        var offsets = allocator.alloc(usize, chunks.numChunks() + 1) catch return error.OutOfMemory;
        errdefer allocator.free(offsets);

        offsets[0] = 0;
        var total: usize = 0;
        var i: usize = 0;
        while (i < chunks.numChunks()) : (i += 1) {
            total = std.math.add(usize, total, chunks.chunk(i).data().length) catch return error.Overflow;
            offsets[i + 1] = total;
        }
        return .{ .offsets = offsets };
    }

    fn deinit(self: *ChunkIndexResolver, allocator: std.mem.Allocator) void {
        allocator.free(self.offsets);
        self.* = undefined;
    }

    fn locate(self: *const ChunkIndexResolver, chunks: ChunkedArray, logical_index: usize) KernelError!ChunkLocalIndex {
        if (logical_index >= chunks.len()) return error.InvalidInput;
        if (chunks.numChunks() == 0) return error.InvalidInput;

        var lo: usize = 0;
        var hi: usize = chunks.numChunks();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.offsets[mid + 1] <= logical_index) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if (lo >= chunks.numChunks()) return error.InvalidInput;
        return .{
            .chunk_index = lo,
            .index_in_chunk = logical_index - self.offsets[lo],
        };
    }
};

/// Resolve logical row indices into chunk-local coordinates.
///
/// This is a building block for permutation-producing kernels (e.g. sort_indices)
/// that need to bridge global logical indices back to chunk-local positions.
pub fn chunkedResolveLogicalIndices(
    allocator: std.mem.Allocator,
    chunks: ChunkedArray,
    logical_indices: []const usize,
) KernelError![]ChunkLocalIndex {
    var out = allocator.alloc(ChunkLocalIndex, logical_indices.len) catch return error.OutOfMemory;
    errdefer allocator.free(out);

    var resolver = try ChunkIndexResolver.init(allocator, chunks);
    defer resolver.deinit(allocator);

    for (logical_indices, 0..) |logical_index, i| {
        out[i] = try resolver.locate(chunks, logical_index);
    }
    return out;
}

fn lookupChunkAt(chunks: ChunkedArray, logical_index: usize) ?ChunkLookup {
    if (logical_index >= chunks.len()) return null;

    var remaining = logical_index;
    var chunk_index: usize = 0;
    while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
        const chunk_ref = chunks.chunk(chunk_index);
        const chunk_len = chunk_ref.data().length;
        if (remaining < chunk_len) {
            return .{
                .chunk = chunk_ref,
                .local_index = remaining,
            };
        }
        remaining -= chunk_len;
    }
    return null;
}

fn scalarFromSingleArrayRef(value: ArrayRef) KernelError!Scalar {
    const data = value.data();
    if (data.length != 1) return error.InvalidInput;
    if (data.isNull(0)) return Scalar.init(data.data_type, .null);

    const pValue = struct {
        fn get(comptime T: type, arr: anytype) KernelError!T {
            return arr.value(0) catch |err| return mapArrayReadError(err);
        }
    }.get;

    return switch (data.data_type) {
        .bool => Scalar.init(data.data_type, .{ .bool = (array_mod.BooleanArray{ .data = data }).value(0) }),
        .int8 => Scalar.init(data.data_type, .{ .i8 = try pValue(i8, array_mod.Int8Array{ .data = data }) }),
        .int16 => Scalar.init(data.data_type, .{ .i16 = try pValue(i16, array_mod.Int16Array{ .data = data }) }),
        .int32 => Scalar.init(data.data_type, .{ .i32 = try pValue(i32, array_mod.Int32Array{ .data = data }) }),
        .int64 => Scalar.init(data.data_type, .{ .i64 = try pValue(i64, array_mod.Int64Array{ .data = data }) }),
        .uint8 => Scalar.init(data.data_type, .{ .u8 = try pValue(u8, array_mod.UInt8Array{ .data = data }) }),
        .uint16 => Scalar.init(data.data_type, .{ .u16 = try pValue(u16, array_mod.UInt16Array{ .data = data }) }),
        .uint32 => Scalar.init(data.data_type, .{ .u32 = try pValue(u32, array_mod.UInt32Array{ .data = data }) }),
        .uint64 => Scalar.init(data.data_type, .{ .u64 = try pValue(u64, array_mod.UInt64Array{ .data = data }) }),
        .half_float => Scalar.init(data.data_type, .{ .f16 = try pValue(f16, array_mod.HalfFloatArray{ .data = data }) }),
        .float => Scalar.init(data.data_type, .{ .f32 = try pValue(f32, array_mod.Float32Array{ .data = data }) }),
        .double => Scalar.init(data.data_type, .{ .f64 = try pValue(f64, array_mod.Float64Array{ .data = data }) }),
        .date32 => Scalar.init(data.data_type, .{ .date32 = try pValue(i32, array_mod.Date32Array{ .data = data }) }),
        .date64 => Scalar.init(data.data_type, .{ .date64 = try pValue(i64, array_mod.Date64Array{ .data = data }) }),
        .time32 => Scalar.init(data.data_type, .{ .time32 = try pValue(i32, array_mod.Time32Array{ .data = data }) }),
        .time64 => Scalar.init(data.data_type, .{ .time64 = try pValue(i64, array_mod.Time64Array{ .data = data }) }),
        .timestamp => Scalar.init(data.data_type, .{ .timestamp = try pValue(i64, array_mod.TimestampArray{ .data = data }) }),
        .duration => Scalar.init(data.data_type, .{ .duration = try pValue(i64, array_mod.DurationArray{ .data = data }) }),
        .interval_months => Scalar.init(data.data_type, .{ .interval_months = try pValue(i32, array_mod.IntervalMonthsArray{ .data = data }) }),
        .interval_day_time => Scalar.init(data.data_type, .{ .interval_day_time = try pValue(i64, array_mod.IntervalDayTimeArray{ .data = data }) }),
        .interval_month_day_nano => Scalar.init(data.data_type, .{ .interval_month_day_nano = try pValue(i128, array_mod.IntervalMonthDayNanoArray{ .data = data }) }),
        .decimal32 => Scalar.init(data.data_type, .{ .decimal32 = try pValue(i32, array_mod.Decimal32Array{ .data = data }) }),
        .decimal64 => Scalar.init(data.data_type, .{ .decimal64 = try pValue(i64, array_mod.Decimal64Array{ .data = data }) }),
        .decimal128 => Scalar.init(data.data_type, .{ .decimal128 = try pValue(i128, array_mod.Decimal128Array{ .data = data }) }),
        .decimal256 => Scalar.init(data.data_type, .{ .decimal256 = try pValue(i256, array_mod.Decimal256Array{ .data = data }) }),
        .string => .{
            .data_type = data.data_type,
            .value = .{ .string = (array_mod.StringArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .large_string => .{
            .data_type = data.data_type,
            .value = .{ .string = (array_mod.LargeStringArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .binary => .{
            .data_type = data.data_type,
            .value = .{ .binary = (array_mod.BinaryArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .large_binary => .{
            .data_type = data.data_type,
            .value = .{ .binary = (array_mod.LargeBinaryArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .fixed_size_binary => .{
            .data_type = data.data_type,
            .value = .{ .binary = (array_mod.FixedSizeBinaryArray{ .data = data }).value(0) },
            .payload = value.retain(),
        },
        .list, .large_list, .fixed_size_list, .struct_ => Scalar.initNested(data.data_type, value),
        else => error.UnsupportedType,
    };
}

/// Extract one logical list value from list-like datums (array/chunked/scalar).
pub fn datumListValueAt(datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .list) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            const view = array_mod.ListArray{ .data = arr.data() };
            break :blk view.value(logical_index) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .list) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            const view = array_mod.ListArray{ .data = located.chunk.data() };
            break :blk view.value(located.local_index) catch |err| mapArrayReadError(err);
        },
        .scalar => |s| blk: {
            if (s.data_type != .list) break :blk error.InvalidInput;
            if (s.isNull()) break :blk error.InvalidInput;
            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .list or payload.data().length != 1) break :blk error.InvalidInput;
            const view = array_mod.ListArray{ .data = payload.data() };
            break :blk view.value(0) catch |err| mapArrayReadError(err);
        },
    };
}

/// Extract one logical large_list value from list-like datums (array/chunked/scalar).
pub fn datumLargeListValueAt(datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .large_list) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            const view = array_mod.LargeListArray{ .data = arr.data() };
            break :blk view.value(logical_index) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .large_list) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            const view = array_mod.LargeListArray{ .data = located.chunk.data() };
            break :blk view.value(located.local_index) catch |err| mapArrayReadError(err);
        },
        .scalar => |s| blk: {
            if (s.data_type != .large_list) break :blk error.InvalidInput;
            if (s.isNull()) break :blk error.InvalidInput;
            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .large_list or payload.data().length != 1) break :blk error.InvalidInput;
            const view = array_mod.LargeListArray{ .data = payload.data() };
            break :blk view.value(0) catch |err| mapArrayReadError(err);
        },
    };
}

/// Extract one logical fixed_size_list value from list-like datums (array/chunked/scalar).
pub fn datumFixedSizeListValueAt(datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (arr.data().data_type != .fixed_size_list) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            const view = array_mod.FixedSizeListArray{ .data = arr.data() };
            break :blk view.value(logical_index) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .fixed_size_list) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            const view = array_mod.FixedSizeListArray{ .data = located.chunk.data() };
            break :blk view.value(located.local_index) catch |err| mapArrayReadError(err);
        },
        .scalar => |s| blk: {
            if (s.data_type != .fixed_size_list) break :blk error.InvalidInput;
            if (s.isNull()) break :blk error.InvalidInput;
            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .fixed_size_list or payload.data().length != 1) break :blk error.InvalidInput;
            const view = array_mod.FixedSizeListArray{ .data = payload.data() };
            break :blk view.value(0) catch |err| mapArrayReadError(err);
        },
    };
}

/// Extract one struct field while preserving array-like/scalar-like semantics.
///
/// - `array(struct)` -> `array(field)`
/// - `chunked(struct)` -> `chunked(field)`
/// - `scalar(struct)` -> `scalar(field)`
pub fn datumStructField(datum: Datum, field_index: usize) KernelError!Datum {
    return switch (datum) {
        .array => |arr| blk: {
            const dt = arr.data().data_type;
            if (dt != .struct_) break :blk error.InvalidInput;
            if (field_index >= dt.struct_.fields.len) break :blk error.InvalidInput;
            const view = array_mod.StructArray{ .data = arr.data() };
            const field = view.field(field_index) catch |err| break :blk mapArrayReadError(err);
            break :blk Datum.fromArray(field);
        },
        .chunked => |chunks| blk: {
            const struct_dt = chunks.dataType();
            if (struct_dt != .struct_) break :blk error.InvalidInput;
            if (field_index >= struct_dt.struct_.fields.len) break :blk error.InvalidInput;
            const field_dt = struct_dt.struct_.fields[field_index].data_type.*;
            const allocator = chunks.node.allocator;

            var fields = allocator.alloc(ArrayRef, chunks.numChunks()) catch break :blk error.OutOfMemory;
            var field_count: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < field_count) : (i += 1) fields[i].release();
                allocator.free(fields);
            }

            var chunk_index: usize = 0;
            while (chunk_index < chunks.numChunks()) : (chunk_index += 1) {
                const chunk = chunks.chunk(chunk_index);
                const chunk_dt = chunk.data().data_type;
                if (chunk_dt != .struct_ or field_index >= chunk_dt.struct_.fields.len) break :blk error.InvalidInput;
                const view = array_mod.StructArray{ .data = chunk.data() };
                fields[field_count] = view.field(field_index) catch |err| break :blk mapArrayReadError(err);
                field_count += 1;
            }

            const out = ChunkedArray.init(allocator, field_dt, fields[0..field_count]) catch |err| break :blk mapChunkedError(err);
            var i: usize = 0;
            while (i < field_count) : (i += 1) fields[i].release();
            allocator.free(fields);

            break :blk Datum.fromChunked(out);
        },
        .scalar => |s| blk: {
            const dt = s.data_type;
            if (dt != .struct_) break :blk error.InvalidInput;
            if (field_index >= dt.struct_.fields.len) break :blk error.InvalidInput;
            const field_dt = dt.struct_.fields[field_index].data_type.*;

            if (s.isNull()) {
                break :blk Datum.fromScalar(Scalar.init(field_dt, .null));
            }

            var payload = try s.payloadArray();
            defer payload.release();
            if (payload.data().data_type != .struct_ or payload.data().length != 1) break :blk error.InvalidInput;

            const view = array_mod.StructArray{ .data = payload.data() };
            var field_array = view.field(field_index) catch |err| break :blk mapArrayReadError(err);
            defer field_array.release();

            const field_scalar = try scalarFromSingleArrayRef(field_array);
            break :blk Datum.fromScalar(field_scalar);
        },
    };
}

fn mapConcatError(err: anyerror) KernelError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.UnsupportedType => error.UnsupportedType,
        else => error.InvalidInput,
    };
}

fn inferDatumAllocator(datum: Datum) ?std.mem.Allocator {
    return switch (datum) {
        .array => |arr| arr.node.allocator,
        .chunked => |chunks| chunks.node.allocator,
        .scalar => |s| if (s.payload) |payload| payload.node.allocator else null,
    };
}

fn inferDatumsAllocator(datums: []const Datum) std.mem.Allocator {
    for (datums) |datum| {
        if (inferDatumAllocator(datum)) |allocator| return allocator;
    }
    return std.heap.page_allocator;
}

fn makeNullArray(allocator: std.mem.Allocator, len: usize) KernelError!ArrayRef {
    var builder = try array_mod.NullBuilder.init(allocator, len);
    defer builder.deinit();
    builder.appendNulls(len) catch |err| return mapArrayReadError(err);
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeBoolArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?bool) KernelError!ArrayRef {
    var builder = try array_mod.BooleanBuilder.init(allocator, len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeInt32ArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?i32) KernelError!ArrayRef {
    var builder = try array_mod.Int32Builder.init(allocator, len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeInt64ArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?i64) KernelError!ArrayRef {
    var builder = try array_mod.Int64Builder.init(allocator, len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeStringArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.StringBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeLargeStringArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.LargeStringBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeBinaryArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.BinaryBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeLargeBinaryArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.LargeBinaryBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeStringViewArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.StringViewBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn makeBinaryViewArrayFilled(allocator: std.mem.Allocator, len: usize, value: ?[]const u8) KernelError!ArrayRef {
    const bytes_len = if (value) |v| v.len * len else 0;
    var builder = try array_mod.BinaryViewBuilder.init(allocator, len, bytes_len);
    defer builder.deinit();
    var i: usize = 0;
    while (i < len) : (i += 1) {
        if (value) |v| {
            builder.append(v) catch |err| return mapArrayReadError(err);
        } else {
            builder.appendNull() catch |err| return mapArrayReadError(err);
        }
    }
    return builder.finish() catch |err| return mapArrayReadError(err);
}

fn repeatSingleArray(allocator: std.mem.Allocator, single: ArrayRef, count: usize) KernelError!ArrayRef {
    if (single.data().length != 1) return error.InvalidInput;
    if (count == 0) {
        return single.slice(0, 0) catch |err| mapArrayReadError(err);
    }
    if (count == 1) return single.retain();

    var parts = allocator.alloc(ArrayRef, count) catch return error.OutOfMemory;
    defer allocator.free(parts);

    var parts_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < parts_count) : (i += 1) parts[i].release();
    }

    var i: usize = 0;
    while (i < count) : (i += 1) {
        parts[i] = single.retain();
        parts_count += 1;
    }

    const out = concat_array_refs.concatArrayRefs(allocator, single.data().data_type, parts) catch |err| return mapConcatError(err);
    return out;
}

fn buildNullLikeArray(allocator: std.mem.Allocator, data_type: DataType, len: usize) KernelError!ArrayRef {
    return switch (data_type) {
        .null => makeNullArray(allocator, len),
        .bool => makeBoolArrayFilled(allocator, len, null),
        .int32 => makeInt32ArrayFilled(allocator, len, null),
        .int64 => makeInt64ArrayFilled(allocator, len, null),
        .string => makeStringArrayFilled(allocator, len, null),
        .large_string => makeLargeStringArrayFilled(allocator, len, null),
        .binary => makeBinaryArrayFilled(allocator, len, null),
        .large_binary => makeLargeBinaryArrayFilled(allocator, len, null),
        .string_view => makeStringViewArrayFilled(allocator, len, null),
        .binary_view => makeBinaryViewArrayFilled(allocator, len, null),
        .list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.ListBuilder.init(allocator, len, list_ty.value_field);
            defer builder.deinit();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }
            break :blk builder.finish(values.array) catch |err| mapArrayReadError(err);
        },
        .large_list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.LargeListBuilder.init(allocator, len, list_ty.value_field);
            defer builder.deinit();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }
            break :blk builder.finish(values.array) catch |err| mapArrayReadError(err);
        },
        .fixed_size_list => |list_ty| blk: {
            const list_size = std.math.cast(usize, list_ty.list_size) orelse return error.InvalidInput;
            const values_len = std.math.mul(usize, len, list_size) catch return error.Overflow;
            var values = try buildNullLikeArray(allocator, list_ty.value_field.data_type.*, values_len);
            defer values.release();

            var builder = array_mod.FixedSizeListBuilder.init(allocator, list_ty.value_field, list_size) catch |err| return mapArrayReadError(err);
            defer builder.deinit();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }
            break :blk builder.finish(values) catch |err| mapArrayReadError(err);
        },
        .struct_ => |struct_ty| blk: {
            var children = allocator.alloc(ArrayRef, struct_ty.fields.len) catch return error.OutOfMemory;
            var child_count: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < child_count) : (i += 1) children[i].release();
                allocator.free(children);
            }

            for (struct_ty.fields, 0..) |field, i| {
                children[i] = try buildNullLikeArray(allocator, field.data_type.*, len);
                child_count += 1;
            }

            var builder = array_mod.StructBuilder.init(allocator, struct_ty.fields);
            defer builder.deinit();
            var i: usize = 0;
            while (i < len) : (i += 1) {
                builder.appendNull() catch |err| return mapArrayReadError(err);
            }

            const out = builder.finish(children[0..child_count]) catch |err| return mapArrayReadError(err);
            var i_release: usize = 0;
            while (i_release < child_count) : (i_release += 1) children[i_release].release();
            allocator.free(children);
            break :blk out;
        },
        else => error.UnsupportedType,
    };
}

fn scalarToSingleArrayRef(allocator: std.mem.Allocator, scalar: Scalar) KernelError!ArrayRef {
    if (scalar.payload) |payload| {
        if (payload.data().length != 1) return error.InvalidInput;
        return payload.retain();
    }

    return switch (scalar.data_type) {
        .null => makeNullArray(allocator, 1),
        .bool => makeBoolArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.bool),
        .int32 => makeInt32ArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.i32),
        .int64 => makeInt64ArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.i64),
        .string => makeStringArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.string),
        .large_string => makeLargeStringArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.string),
        .binary => makeBinaryArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.binary),
        .large_binary => makeLargeBinaryArrayFilled(allocator, 1, if (scalar.isNull()) null else scalar.value.binary),
        .list, .large_list, .fixed_size_list, .struct_ => {
            if (!scalar.isNull()) return error.InvalidInput;
            return buildNullLikeArray(allocator, scalar.data_type, 1);
        },
        else => error.UnsupportedType,
    };
}

fn datumElementArrayAt(allocator: std.mem.Allocator, datum: Datum, logical_index: usize) KernelError!ArrayRef {
    return switch (datum) {
        .array => |arr| blk: {
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            break :blk arr.slice(logical_index, 1) catch |err| mapArrayReadError(err);
        },
        .chunked => |chunks| blk: {
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            break :blk located.chunk.slice(located.local_index, 1) catch |err| mapArrayReadError(err);
        },
        .scalar => |scalar| scalarToSingleArrayRef(allocator, scalar),
    };
}

const PredicateDecision = enum {
    keep,
    drop,
    emit_null,
};

fn appendOwnedArrayRef(
    allocator: std.mem.Allocator,
    pieces: *std.ArrayList(ArrayRef),
    piece: ArrayRef,
) KernelError!void {
    pieces.append(allocator, piece) catch {
        var owned = piece;
        owned.release();
        return error.OutOfMemory;
    };
}

fn flushTakeContiguousRun(
    allocator: std.mem.Allocator,
    out_chunks: *std.ArrayList(ArrayRef),
    chunks: ChunkedArray,
    chunk_index: usize,
    run_start_local: usize,
    run_len: usize,
) KernelError!void {
    if (run_len == 0) return;
    const source = chunks.chunk(chunk_index).*;
    const source_len = source.data().length;
    const piece = if (run_start_local == 0 and run_len == source_len)
        source.retain()
    else
        source.slice(run_start_local, run_len) catch |err| return mapArrayReadError(err);
    try appendOwnedArrayRef(allocator, out_chunks, piece);
}

fn flushTakeNullRun(
    allocator: std.mem.Allocator,
    out_chunks: *std.ArrayList(ArrayRef),
    out_type: DataType,
    null_run_len: usize,
) KernelError!void {
    if (null_run_len == 0) return;
    const nulls = try buildNullLikeArray(allocator, out_type, null_run_len);
    try appendOwnedArrayRef(allocator, out_chunks, nulls);
}

fn datumTakeChunkedNullable(
    allocator: std.mem.Allocator,
    chunks: ChunkedArray,
    out_type: DataType,
    indices: []const ?usize,
) KernelError!Datum {
    if (indices.len == 0) {
        return Datum.fromChunked(chunks.slice(allocator, 0, 0) catch |err| return mapChunkedError(err));
    }

    var out_chunks: std.ArrayList(ArrayRef) = .{};
    defer {
        for (out_chunks.items) |*chunk| chunk.release();
        out_chunks.deinit(allocator);
    }

    var resolver = try ChunkIndexResolver.init(allocator, chunks);
    defer resolver.deinit(allocator);

    var run_active = false;
    var run_chunk_index: usize = 0;
    var run_start_local: usize = 0;
    var run_len: usize = 0;
    var null_run_len: usize = 0;

    for (indices) |maybe_index| {
        if (maybe_index == null) {
            if (run_active) {
                try flushTakeContiguousRun(allocator, &out_chunks, chunks, run_chunk_index, run_start_local, run_len);
                run_active = false;
                run_len = 0;
            }
            null_run_len = std.math.add(usize, null_run_len, 1) catch return error.Overflow;
            continue;
        }

        if (null_run_len > 0) {
            try flushTakeNullRun(allocator, &out_chunks, out_type, null_run_len);
            null_run_len = 0;
        }

        const located = try resolver.locate(chunks, maybe_index.?);
        if (!run_active) {
            run_active = true;
            run_chunk_index = located.chunk_index;
            run_start_local = located.index_in_chunk;
            run_len = 1;
            continue;
        }

        const expected_next = run_start_local + run_len;
        if (located.chunk_index == run_chunk_index and located.index_in_chunk == expected_next) {
            run_len = std.math.add(usize, run_len, 1) catch return error.Overflow;
            continue;
        }

        try flushTakeContiguousRun(allocator, &out_chunks, chunks, run_chunk_index, run_start_local, run_len);
        run_chunk_index = located.chunk_index;
        run_start_local = located.index_in_chunk;
        run_len = 1;
    }

    if (run_active) {
        try flushTakeContiguousRun(allocator, &out_chunks, chunks, run_chunk_index, run_start_local, run_len);
    }
    if (null_run_len > 0) {
        try flushTakeNullRun(allocator, &out_chunks, out_type, null_run_len);
    }

    const out = ChunkedArray.init(allocator, out_type, out_chunks.items) catch |err| return mapChunkedError(err);
    return Datum.fromChunked(out);
}

fn datumTakeArrayLikeNullable(
    allocator: std.mem.Allocator,
    datum: Datum,
    out_type: DataType,
    indices: []const ?usize,
) KernelError!Datum {
    if (indices.len == 0) return datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }
    try pieces.ensureTotalCapacity(allocator, indices.len);

    for (indices) |choice| {
        const piece = if (choice) |idx|
            try datumElementArrayAt(allocator, datum, idx)
        else
            try buildNullLikeArray(allocator, out_type, 1);
        pieces.appendAssumeCapacity(piece);
    }

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return mapConcatError(err);
    return Datum.fromArray(out);
}

fn predicateDecisionAt(predicate: Datum, logical_index: usize, options: FilterOptions) KernelError!PredicateDecision {
    return switch (predicate) {
        .scalar => |s| blk: {
            if (s.data_type != .bool) break :blk error.InvalidInput;
            if (s.isNull()) break :blk if (options.drop_nulls) .drop else .emit_null;
            break :blk if (s.value.bool) .keep else .drop;
        },
        .array => |arr| blk: {
            if (arr.data().data_type != .bool) break :blk error.InvalidInput;
            if (logical_index >= arr.data().length) break :blk error.InvalidInput;
            if (arr.data().isNull(logical_index)) break :blk if (options.drop_nulls) .drop else .emit_null;
            const bool_array = array_mod.BooleanArray{ .data = arr.data() };
            break :blk if (bool_array.value(logical_index)) .keep else .drop;
        },
        .chunked => |chunks| blk: {
            if (chunks.dataType() != .bool) break :blk error.InvalidInput;
            const located = lookupChunkAt(chunks, logical_index) orelse break :blk error.InvalidInput;
            if (located.chunk.data().isNull(located.local_index)) break :blk if (options.drop_nulls) .drop else .emit_null;
            const bool_array = array_mod.BooleanArray{ .data = located.chunk.data() };
            break :blk if (bool_array.value(located.local_index)) .keep else .drop;
        },
    };
}

/// Build nullable selection indices from a boolean predicate.
///
/// `keep` emits the logical input index, `drop` emits nothing, and `emit_null`
/// emits `null` to preserve output row count for null-predicate semantics.
pub fn datumFilterSelectionIndices(
    allocator: std.mem.Allocator,
    predicate: Datum,
    logical_len: usize,
    options: FilterOptions,
) KernelError![]?usize {
    var selections: std.ArrayList(?usize) = .{};
    defer selections.deinit(allocator);

    var i: usize = 0;
    while (i < logical_len) : (i += 1) {
        switch (try predicateDecisionAt(predicate, i, options)) {
            .drop => {},
            .keep => try selections.append(allocator, i),
            .emit_null => try selections.append(allocator, null),
        }
    }
    return selections.toOwnedSlice(allocator);
}

/// Build an all-null datum for the requested logical type and length.
pub fn datumBuildNullLike(data_type: DataType, len: usize) KernelError!Datum {
    return datumBuildNullLikeWithAllocator(std.heap.page_allocator, data_type, len);
}

/// Build an all-null datum for the requested logical type and length.
pub fn datumBuildNullLikeWithAllocator(allocator: std.mem.Allocator, data_type: DataType, len: usize) KernelError!Datum {
    return Datum.fromArray(try buildNullLikeArray(allocator, data_type, len));
}

/// Build an empty datum preserving the requested logical type and nested layout.
pub fn datumBuildEmptyLike(data_type: DataType) KernelError!Datum {
    return datumBuildEmptyLikeWithAllocator(std.heap.page_allocator, data_type);
}

/// Build an empty datum preserving the requested logical type and nested layout.
pub fn datumBuildEmptyLikeWithAllocator(allocator: std.mem.Allocator, data_type: DataType) KernelError!Datum {
    const out = switch (data_type) {
        .null => try makeNullArray(allocator, 0),
        .bool => try makeBoolArrayFilled(allocator, 0, null),
        .int32 => try makeInt32ArrayFilled(allocator, 0, null),
        .int64 => try makeInt64ArrayFilled(allocator, 0, null),
        .string => try makeStringArrayFilled(allocator, 0, null),
        .large_string => try makeLargeStringArrayFilled(allocator, 0, null),
        .binary => try makeBinaryArrayFilled(allocator, 0, null),
        .large_binary => try makeLargeBinaryArrayFilled(allocator, 0, null),
        .string_view => try makeStringViewArrayFilled(allocator, 0, null),
        .binary_view => try makeBinaryViewArrayFilled(allocator, 0, null),
        .list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.ListBuilder.init(allocator, 0, list_ty.value_field);
            defer builder.deinit();
            break :blk builder.finish(values.array) catch |err| return mapArrayReadError(err);
        },
        .large_list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            var builder = try array_mod.LargeListBuilder.init(allocator, 0, list_ty.value_field);
            defer builder.deinit();
            break :blk builder.finish(values.array) catch |err| return mapArrayReadError(err);
        },
        .fixed_size_list => |list_ty| blk: {
            var values = try datumBuildEmptyLikeWithAllocator(allocator, list_ty.value_field.data_type.*);
            defer values.release();
            std.debug.assert(values == .array);

            const list_size = std.math.cast(usize, list_ty.list_size) orelse return error.InvalidInput;
            var builder = array_mod.FixedSizeListBuilder.init(allocator, list_ty.value_field, list_size) catch |err| return mapArrayReadError(err);
            defer builder.deinit();
            break :blk builder.finish(values.array) catch |err| return mapArrayReadError(err);
        },
        .struct_ => |struct_ty| blk: {
            var children = allocator.alloc(ArrayRef, struct_ty.fields.len) catch return error.OutOfMemory;
            var child_count: usize = 0;
            errdefer {
                var i: usize = 0;
                while (i < child_count) : (i += 1) children[i].release();
                allocator.free(children);
            }

            for (struct_ty.fields, 0..) |field, i| {
                const child_empty = try datumBuildEmptyLikeWithAllocator(allocator, field.data_type.*);
                if (child_empty != .array) return error.InvalidInput;
                children[i] = child_empty.array;
                child_count += 1;
            }

            var builder = array_mod.StructBuilder.init(allocator, struct_ty.fields);
            defer builder.deinit();
            const out = builder.finish(children[0..child_count]) catch |err| return mapArrayReadError(err);

            var i_release: usize = 0;
            while (i_release < child_count) : (i_release += 1) children[i_release].release();
            allocator.free(children);
            break :blk out;
        },
        else => return error.UnsupportedType,
    };

    return Datum.fromArray(out);
}

/// Create an empty datum preserving the input datum's logical type.
pub fn datumSliceEmpty(datum: Datum) KernelError!Datum {
    return switch (datum) {
        .array => |arr| Datum.fromArray(arr.slice(0, 0) catch |err| return mapArrayReadError(err)),
        .chunked => |chunks| Datum.fromChunked(chunks.slice(chunks.node.allocator, 0, 0) catch |err| return mapChunkedError(err)),
        .scalar => |s| datumBuildEmptyLikeWithAllocator(inferDatumAllocator(datum) orelse std.heap.page_allocator, s.data_type),
    };
}

/// Gather/take rows from a datum according to logical indices.
///
/// For chunked inputs this helper preserves chunked output and avoids forcing
/// concat pre-normalization.
pub fn datumTake(datum: Datum, indices: []const usize) KernelError!Datum {
    const allocator = inferDatumAllocator(datum) orelse std.heap.page_allocator;
    const out_type = datum.dataType();

    if (indices.len == 0) {
        return switch (datum) {
            .chunked => |chunks| Datum.fromChunked(chunks.slice(allocator, 0, 0) catch |err| return mapChunkedError(err)),
            else => datumBuildEmptyLikeWithAllocator(allocator, out_type),
        };
    }

    return switch (datum) {
        .chunked => |chunks| blk: {
            var nullable = allocator.alloc(?usize, indices.len) catch break :blk error.OutOfMemory;
            defer allocator.free(nullable);
            for (indices, 0..) |index, i| nullable[i] = index;
            break :blk try datumTakeChunkedNullable(allocator, chunks, out_type, nullable);
        },
        else => blk: {
            var nullable = allocator.alloc(?usize, indices.len) catch break :blk error.OutOfMemory;
            defer allocator.free(nullable);
            for (indices, 0..) |index, i| nullable[i] = index;
            break :blk try datumTakeArrayLikeNullable(allocator, datum, out_type, nullable);
        },
    };
}

/// Gather/take rows with nullable indices where `null` emits an all-null row.
///
/// For chunked inputs this helper preserves chunked output and avoids forcing
/// concat pre-normalization.
pub fn datumTakeNullable(datum: Datum, indices: []const ?usize) KernelError!Datum {
    const allocator = inferDatumAllocator(datum) orelse std.heap.page_allocator;
    const out_type = datum.dataType();
    return switch (datum) {
        .chunked => |chunks| datumTakeChunkedNullable(allocator, chunks, out_type, indices),
        else => datumTakeArrayLikeNullable(allocator, datum, out_type, indices),
    };
}

/// Row-wise selection primitive shared by choose/case_when/filter style operators.
///
/// `indices[i]` selects which entry in `values` contributes output row `i`.
/// Each candidate value may be array/chunked/scalar (scalar broadcasts).
pub fn datumSelect(indices: []const usize, values: []const Datum) KernelError!Datum {
    if (values.len == 0) return error.InvalidArity;
    if (!sameDataTypes(values)) return error.InvalidInput;

    const output_len = try inferNaryExecLen(values);
    if (indices.len != output_len) return error.InvalidInput;

    const allocator = inferDatumsAllocator(values);
    const out_type = values[0].dataType();
    if (indices.len == 0) return datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }
    try pieces.ensureTotalCapacity(allocator, indices.len);

    for (indices, 0..) |choice, logical_index| {
        if (choice >= values.len) return error.InvalidInput;
        const piece = try datumElementArrayAt(allocator, values[choice], logical_index);
        pieces.appendAssumeCapacity(piece);
    }

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return mapConcatError(err);
    return Datum.fromArray(out);
}

/// Row-wise selection primitive with nullable index support.
///
/// `indices[i] == null` emits a null row at output position `i`.
pub fn datumSelectNullable(indices: []const ?usize, values: []const Datum) KernelError!Datum {
    if (values.len == 0) return error.InvalidArity;
    if (!sameDataTypes(values)) return error.InvalidInput;

    const output_len = try inferNaryExecLen(values);
    if (indices.len != output_len) return error.InvalidInput;

    const allocator = inferDatumsAllocator(values);
    const out_type = values[0].dataType();
    if (indices.len == 0) return datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }
    try pieces.ensureTotalCapacity(allocator, indices.len);

    for (indices, 0..) |choice, logical_index| {
        const piece = if (choice) |idx| blk: {
            if (idx >= values.len) return error.InvalidInput;
            break :blk try datumElementArrayAt(allocator, values[idx], logical_index);
        } else try buildNullLikeArray(allocator, out_type, 1);
        pieces.appendAssumeCapacity(piece);
    }

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return mapConcatError(err);
    return Datum.fromArray(out);
}

/// Chunk-aware filter helper for array/chunked/scalar datums with bool predicates.
///
/// This helper composes `datumFilterSelectionIndices` + `datumTakeNullable`,
/// preserving chunked output when the input datum is chunked.
pub fn datumFilterChunkAware(datum: Datum, predicate: Datum, options: FilterOptions) KernelError!Datum {
    const input_len = try inferBinaryExecLen(datum, predicate);
    const allocator = inferDatumsAllocator(&[_]Datum{ datum, predicate });
    const selections = try datumFilterSelectionIndices(allocator, predicate, input_len, options);
    defer allocator.free(selections);
    return datumTakeNullable(datum, selections);
}

/// Generic filter primitive for array/chunked/scalar datums with bool predicates.
///
/// This preserves the historical array-output shape for compatibility.
pub fn datumFilter(datum: Datum, predicate: Datum, options: FilterOptions) KernelError!Datum {
    const output_len = try inferBinaryExecLen(datum, predicate);
    const allocator = inferDatumsAllocator(&[_]Datum{ datum, predicate });
    const out_type = datum.dataType();

    if (output_len == 0) return datumBuildEmptyLikeWithAllocator(allocator, out_type);

    var pieces: std.ArrayList(ArrayRef) = .{};
    defer {
        for (pieces.items) |*piece| piece.release();
        pieces.deinit(allocator);
    }

    var i: usize = 0;
    while (i < output_len) : (i += 1) {
        const decision = try predicateDecisionAt(predicate, i, options);
        const piece = switch (decision) {
            .drop => continue,
            .keep => try datumElementArrayAt(allocator, datum, i),
            .emit_null => try buildNullLikeArray(allocator, out_type, 1),
        };
        try appendOwnedArrayRef(allocator, &pieces, piece);
    }

    if (pieces.items.len == 0) return datumBuildEmptyLikeWithAllocator(allocator, out_type);

    const out = concat_array_refs.concatArrayRefs(allocator, out_type, pieces.items) catch |err| return mapConcatError(err);
    return Datum.fromArray(out);
}

const ExecDatumCursor = union(enum) {
    array: struct {
        array: ArrayRef,
        offset: usize = 0,
    },
    chunked: struct {
        chunked: ChunkedArray,
        chunk_index: usize = 0,
        offset: usize = 0,
    },
    scalar: struct {
        scalar: Scalar,
    },

    fn init(datum: Datum) ExecDatumCursor {
        return switch (datum) {
            .array => |arr| .{ .array = .{ .array = arr } },
            .chunked => |chunks| .{ .chunked = .{ .chunked = chunks } },
            .scalar => |s| .{ .scalar = .{ .scalar = s } },
        };
    }

    fn normalize(self: *ExecDatumCursor) void {
        switch (self.*) {
            .array => {},
            .chunked => |*s| {
                while (s.chunk_index < s.chunked.numChunks()) {
                    const chunk_len = s.chunked.chunk(s.chunk_index).data().length;
                    if (chunk_len == 0 or s.offset == chunk_len) {
                        s.chunk_index += 1;
                        s.offset = 0;
                        continue;
                    }
                    break;
                }
            },
            .scalar => {},
        }
    }

    fn remainingCurrent(self: *ExecDatumCursor) usize {
        self.normalize();
        return switch (self.*) {
            .array => |*s| s.array.data().length - s.offset,
            .chunked => |*s| blk: {
                if (s.chunk_index >= s.chunked.numChunks()) break :blk 0;
                const chunk_len = s.chunked.chunk(s.chunk_index).data().length;
                break :blk chunk_len - s.offset;
            },
            .scalar => std.math.maxInt(usize),
        };
    }

    fn take(self: *ExecDatumCursor, len: usize) KernelError!ExecChunkValue {
        self.normalize();
        return switch (self.*) {
            .array => |*s| blk: {
                const arr_len = s.array.data().length;
                if (len > arr_len - s.offset) return error.InvalidInput;
                const out = if (s.offset == 0 and len == arr_len)
                    s.array.retain()
                else
                    s.array.slice(s.offset, len) catch return error.OutOfMemory;
                s.offset += len;
                break :blk .{ .array = out };
            },
            .chunked => |*s| blk: {
                if (s.chunk_index >= s.chunked.numChunks()) return error.InvalidInput;
                const chunk_ref = s.chunked.chunk(s.chunk_index).*;
                const chunk_len = chunk_ref.data().length;
                if (len > chunk_len - s.offset) return error.InvalidInput;

                const out = if (s.offset == 0 and len == chunk_len)
                    chunk_ref.retain()
                else
                    chunk_ref.slice(s.offset, len) catch return error.OutOfMemory;

                s.offset += len;
                if (s.offset == chunk_len) {
                    s.chunk_index += 1;
                    s.offset = 0;
                }
                break :blk .{ .array = out };
            },
            .scalar => |s| .{ .scalar = s.scalar.retain() },
        };
    }
};

pub const UnaryExecChunkIterator = struct {
    cursor: ExecDatumCursor,
    total_len: usize,
    consumed: usize = 0,

    pub fn init(datum: Datum) UnaryExecChunkIterator {
        return .{
            .cursor = ExecDatumCursor.init(datum),
            .total_len = switch (datum) {
                .array => |arr| arr.data().length,
                .chunked => |chunks| chunks.len(),
                .scalar => 1,
            },
        };
    }

    pub fn next(self: *UnaryExecChunkIterator) KernelError!?UnaryExecChunk {
        if (self.consumed >= self.total_len) return null;
        const remaining_total = self.total_len - self.consumed;
        const current_remaining = self.cursor.remainingCurrent();
        if (current_remaining == 0) return error.InvalidInput;
        const run_len = @min(remaining_total, current_remaining);
        var values = try self.cursor.take(run_len);
        errdefer values.release();
        self.consumed += run_len;
        return .{
            .values = values,
            .len = run_len,
        };
    }
};

pub const BinaryExecChunkIterator = struct {
    lhs_cursor: ExecDatumCursor,
    rhs_cursor: ExecDatumCursor,
    total_len: usize,
    consumed: usize = 0,

    pub fn init(lhs: Datum, rhs: Datum) KernelError!BinaryExecChunkIterator {
        return .{
            .lhs_cursor = ExecDatumCursor.init(lhs),
            .rhs_cursor = ExecDatumCursor.init(rhs),
            .total_len = try inferBinaryExecLen(lhs, rhs),
        };
    }

    pub fn next(self: *BinaryExecChunkIterator) KernelError!?BinaryExecChunk {
        if (self.consumed >= self.total_len) return null;
        const remaining_total = self.total_len - self.consumed;

        const lhs_remaining = self.lhs_cursor.remainingCurrent();
        const rhs_remaining = self.rhs_cursor.remainingCurrent();
        if (lhs_remaining == 0 or rhs_remaining == 0) return error.InvalidInput;

        const run_len = @min(remaining_total, @min(lhs_remaining, rhs_remaining));
        if (run_len == 0) return error.InvalidInput;

        var lhs = try self.lhs_cursor.take(run_len);
        errdefer lhs.release();
        var rhs = try self.rhs_cursor.take(run_len);
        errdefer rhs.release();

        self.consumed += run_len;
        return .{
            .lhs = lhs,
            .rhs = rhs,
            .len = run_len,
        };
    }
};

pub const NaryExecChunkIterator = struct {
    allocator: std.mem.Allocator,
    cursors: []ExecDatumCursor,
    total_len: usize,
    consumed: usize = 0,

    pub fn init(allocator: std.mem.Allocator, args: []const Datum) KernelError!NaryExecChunkIterator {
        if (args.len == 0) return error.InvalidArity;

        var cursors = allocator.alloc(ExecDatumCursor, args.len) catch return error.OutOfMemory;
        errdefer allocator.free(cursors);
        for (args, 0..) |arg, idx| {
            cursors[idx] = ExecDatumCursor.init(arg);
        }
        return .{
            .allocator = allocator,
            .cursors = cursors,
            .total_len = try inferNaryExecLen(args),
        };
    }

    pub fn deinit(self: *NaryExecChunkIterator) void {
        self.allocator.free(self.cursors);
        self.* = undefined;
    }

    pub fn next(self: *NaryExecChunkIterator) KernelError!?NaryExecChunk {
        if (self.consumed >= self.total_len) return null;
        const remaining_total = self.total_len - self.consumed;

        var run_len = remaining_total;
        for (self.cursors) |*cursor| {
            const remaining = cursor.remainingCurrent();
            if (remaining == 0) return error.InvalidInput;
            run_len = @min(run_len, remaining);
        }
        if (run_len == 0) return error.InvalidInput;

        var values = self.allocator.alloc(ExecChunkValue, self.cursors.len) catch return error.OutOfMemory;
        var taken: usize = 0;
        errdefer {
            while (taken > 0) {
                taken -= 1;
                values[taken].release();
            }
            self.allocator.free(values);
        }
        for (self.cursors, 0..) |*cursor, idx| {
            values[idx] = try cursor.take(run_len);
            taken += 1;
        }

        self.consumed += run_len;
        return .{
            .allocator = self.allocator,
            .values = values,
            .len = run_len,
        };
    }
};

/// Convert integer-like values using a standardized InvalidCast error path.
pub fn intCastOrInvalidCast(comptime T: type, value: anytype) KernelError!T {
    return std.math.cast(T, value) orelse error.InvalidCast;
}

/// i64 division helper with standardized DivideByZero / Overflow behavior.
pub fn arithmeticDivI64(lhs: i64, rhs: i64, options: ArithmeticOptions) KernelError!i64 {
    if (rhs == 0) {
        if (options.divide_by_zero_is_error) return error.DivideByZero;
        return 0;
    }
    if (options.check_overflow and lhs == std.math.minInt(i64) and rhs == -1) {
        return error.Overflow;
    }
    return @divTrunc(lhs, rhs);
}

pub fn hasArity(args: []const Datum, expected_arity: usize) bool {
    return args.len == expected_arity;
}

pub fn unaryArray(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].isArray();
}

pub fn unaryChunked(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].isChunked();
}

pub fn unaryScalar(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].isScalar();
}

pub fn sameDataTypes(args: []const Datum) bool {
    if (args.len <= 1) return true;
    const first = args[0].dataType();
    for (args[1..]) |arg| {
        if (!first.eql(arg.dataType())) return false;
    }
    return true;
}

pub fn allNumeric(args: []const Datum) bool {
    for (args) |arg| {
        if (!arg.dataType().isNumeric()) return false;
    }
    return true;
}

pub fn unaryNumeric(args: []const Datum) bool {
    return hasArity(args, 1) and args[0].dataType().isNumeric();
}

pub fn binarySameNumeric(args: []const Datum) bool {
    return hasArity(args, 2) and sameDataTypes(args) and allNumeric(args);
}

test {
    _ = @import("core_test.zig");
}
