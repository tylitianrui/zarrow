const std = @import("std");

pub const StreamError = error{
    InvalidMessage,
    UnsupportedType,
    SchemaNotRead,
    InvalidMetadata,
    InvalidBody,
};

pub const Alignment: usize = 8;
pub const ContinuationMarker: u32 = 0xFFFF_FFFF;

pub fn paddedLen(len: usize) usize {
    return len + padLen(len);
}

pub fn padLen(len: usize) usize {
    const rem = len % Alignment;
    return if (rem == 0) 0 else Alignment - rem;
}

pub fn writeMessageLength(writer: anytype, len: u32) !void {
    try writeInt(writer, u32, ContinuationMarker);
    try writeInt(writer, u32, len);
}

pub fn readMessageLength(reader: anytype) !?u32 {
    const first = try readInt(reader, u32);
    if (first == 0) return null;
    if (first == ContinuationMarker) {
        const len = try readInt(reader, u32);
        if (len == 0) return null;
        return len;
    }
    return first;
}

pub fn writePadding(writer: anytype, pad_len: usize) !void {
    if (pad_len == 0) return;
    var zeros: [Alignment]u8 = [_]u8{0} ** Alignment;
    var remaining = pad_len;
    while (remaining > 0) {
        const chunk = @min(remaining, zeros.len);
        try writer.writeAll(zeros[0..chunk]);
        remaining -= chunk;
    }
}

pub fn skipPadding(reader: anytype, pad_len: usize) !void {
    if (pad_len == 0) return;
    var buf: [Alignment]u8 = undefined;
    var remaining = pad_len;
    while (remaining > 0) {
        const chunk = @min(remaining, buf.len);
        try reader.readNoEof(buf[0..chunk]);
        remaining -= chunk;
    }
}

pub fn writeInt(writer: anytype, comptime T: type, value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try writer.writeAll(&buf);
}

pub fn readInt(reader: anytype, comptime T: type) !T {
    var buf: [@sizeOf(T)]u8 = undefined;
    try reader.readNoEof(&buf);
    return std.mem.readInt(T, &buf, .little);
}
