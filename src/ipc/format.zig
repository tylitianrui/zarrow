const std = @import("std");

pub const StreamError = error{
    InvalidMagic,
    InvalidVersion,
    InvalidMessage,
    UnsupportedType,
    SchemaNotRead,
    InvalidMetadata,
    InvalidBody,
};

pub const MAGIC = "ZARW";
pub const VERSION: u16 = 1;

pub const MessageType = enum(u8) {
    schema = 1,
    record_batch = 2,
    end = 3,
};

pub fn writeStreamHeader(writer: anytype) !void {
    try writer.writeAll(MAGIC);
    try writeInt(writer, u16, VERSION);
    try writeInt(writer, u16, 0);
}

pub fn readStreamHeader(reader: anytype) !void {
    var magic: [4]u8 = undefined;
    try reader.readNoEof(&magic);
    if (!std.mem.eql(u8, magic[0..], MAGIC)) return StreamError.InvalidMagic;
    const version = try readInt(reader, u16);
    if (version != VERSION) return StreamError.InvalidVersion;
    _ = try readInt(reader, u16);
}

pub fn writeMessageHeader(writer: anytype, msg_type: MessageType, meta_len: u32, body_len: u32) !void {
    try writeInt(writer, u8, @intFromEnum(msg_type));
    try writeInt(writer, u32, meta_len);
    try writeInt(writer, u32, body_len);
}

pub fn readMessageHeader(reader: anytype) !struct { msg_type: MessageType, meta_len: u32, body_len: u32 } {
    const raw_type = try readInt(reader, u8);
    const msg_type = std.meta.intToEnum(MessageType, raw_type) catch return StreamError.InvalidMessage;
    const meta_len = try readInt(reader, u32);
    const body_len = try readInt(reader, u32);
    return .{ .msg_type = msg_type, .meta_len = meta_len, .body_len = body_len };
}

pub fn writeInt(writer: anytype, comptime T: type, value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeIntLittle(T, &buf, value);
    try writer.writeAll(&buf);
}

pub fn readInt(reader: anytype, comptime T: type) !T {
    var buf: [@sizeOf(T)]u8 = undefined;
    try reader.readNoEof(&buf);
    return std.mem.readIntLittle(T, &buf);
}
