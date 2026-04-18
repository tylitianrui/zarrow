const std = @import("std");
const reader = @import("reader.zig");

pub const Error = error{
    InvalidMetadata,
};

pub const MessageEnvelope = struct {
    version: i16,
    header_type: u8,
    body_length: i64,
};

/// Fast structural sanity check for any FlatBuffer table blob.
pub fn isSaneTable(buf: []const u8) bool {
    _ = reader.rootTable(buf) catch return false;
    return true;
}

/// Parse + validate Arrow IPC Message envelope fields without full unpack.
///
/// Message.fbs field ids:
/// 0 = version (enum MetadataVersion: short)
/// 1 = header_type (union discriminator)
/// 3 = bodyLength (long)
pub fn parseArrowMessageEnvelope(buf: []const u8) Error!MessageEnvelope {
    const table = reader.rootTable(buf) catch return error.InvalidMetadata;

    const version = table.readFieldI16(0) orelse 0;
    if (version < 0 or version > 4) return error.InvalidMetadata;

    const header_type = table.readFieldU8(1) orelse return error.InvalidMetadata;
    if (header_type == 0) return error.InvalidMetadata;

    const body_length = table.readFieldI64(3) orelse 0;

    return .{
        .version = version,
        .header_type = header_type,
        .body_length = body_length,
    };
}

test "verify rejects malformed table bytes" {
    const malformed = [_]u8{
        0x10, 0x00, 0x00, 0x00,
        0xff, 0xff, 0xff, 0xff,
    };
    try std.testing.expect(!isSaneTable(malformed[0..]));
    try std.testing.expectError(error.InvalidMetadata, parseArrowMessageEnvelope(malformed[0..]));
}
