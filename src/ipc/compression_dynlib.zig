const std = @import("std");

pub const ZstdSymbols = struct {
    compress_bound: *const fn (usize) callconv(.c) usize,
    compress: *const fn (?*anyopaque, usize, ?*const anyopaque, usize, c_int) callconv(.c) usize,
    is_error: *const fn (usize) callconv(.c) c_uint,
};

pub const Lz4Symbols = struct {
    create_decompression_context: *const fn (*?*anyopaque, c_uint) callconv(.c) usize,
    free_decompression_context: *const fn (?*anyopaque) callconv(.c) usize,
    decompress: *const fn (?*anyopaque, ?*anyopaque, *usize, ?*const anyopaque, *usize, ?*const anyopaque) callconv(.c) usize,
    is_error: *const fn (usize) callconv(.c) c_uint,
    compress_frame_bound: *const fn (usize, ?*const anyopaque) callconv(.c) usize,
    compress_frame: *const fn (?*anyopaque, usize, ?*const anyopaque, usize, ?*const anyopaque) callconv(.c) usize,
};

extern fn ZSTD_compressBound(src_size: usize) callconv(.c) usize;
extern fn ZSTD_compress(dst: ?*anyopaque, dst_capacity: usize, src: ?*const anyopaque, src_size: usize, compression_level: c_int) callconv(.c) usize;
extern fn ZSTD_isError(code: usize) callconv(.c) c_uint;

extern fn LZ4F_createDecompressionContext(ctx_ptr: *?*anyopaque, version: c_uint) callconv(.c) usize;
extern fn LZ4F_freeDecompressionContext(ctx: ?*anyopaque) callconv(.c) usize;
extern fn LZ4F_decompress(
    ctx: ?*anyopaque,
    dst: ?*anyopaque,
    dst_size_ptr: *usize,
    src: ?*const anyopaque,
    src_size_ptr: *usize,
    options_ptr: ?*const anyopaque,
) callconv(.c) usize;
extern fn LZ4F_isError(code: usize) callconv(.c) c_uint;
extern fn LZ4F_compressFrameBound(src_size: usize, prefs_ptr: ?*const anyopaque) callconv(.c) usize;
extern fn LZ4F_compressFrame(
    dst: ?*anyopaque,
    dst_capacity: usize,
    src: ?*const anyopaque,
    src_size: usize,
    prefs_ptr: ?*const anyopaque,
) callconv(.c) usize;

var linked_zstd_symbols: ZstdSymbols = .{
    .compress_bound = ZSTD_compressBound,
    .compress = ZSTD_compress,
    .is_error = ZSTD_isError,
};

var linked_lz4_symbols: Lz4Symbols = .{
    .create_decompression_context = LZ4F_createDecompressionContext,
    .free_decompression_context = LZ4F_freeDecompressionContext,
    .decompress = LZ4F_decompress,
    .is_error = LZ4F_isError,
    .compress_frame_bound = LZ4F_compressFrameBound,
    .compress_frame = LZ4F_compressFrame,
};

pub fn loadZstdSymbols() !*const ZstdSymbols {
    return &zstd_symbols;
}

pub fn loadLz4Symbols() !*const Lz4Symbols {
    return &lz4_symbols;
}
