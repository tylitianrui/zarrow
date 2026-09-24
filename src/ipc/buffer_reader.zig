const std = @import("std");

/// A seekable, borrowed byte slice with the same reader interface as std.Io.File.Reader.
pub const BufferReader = struct {
    interface: std.Io.Reader,

    pub fn init(bytes: []const u8) BufferReader {
        return .{ .interface = .fixed(bytes) };
    }

    pub fn getSize(self: *const BufferReader) error{}!u64 {
        return self.interface.end;
    }

    pub fn seekTo(self: *BufferReader, offset: u64) error{EndOfStream}!void {
        if (offset > self.interface.end) return error.EndOfStream;
        self.interface.seek = @intCast(offset);
    }
};

test "buffer reader seeks without reading beyond its borrowed slice" {
    var reader = BufferReader.init("abcd");
    try std.testing.expectEqual(@as(u64, 4), try reader.getSize());
    try reader.seekTo(2);
    var bytes: [2]u8 = undefined;
    try reader.interface.readSliceAll(&bytes);
    try std.testing.expectEqualStrings("cd", &bytes);
    try std.testing.expectError(error.EndOfStream, reader.interface.readSliceAll(&bytes));
    try std.testing.expectError(error.EndOfStream, reader.seekTo(5));
    try reader.seekTo(0);
    try reader.interface.readSliceAll(&bytes);
    try std.testing.expectEqualStrings("ab", &bytes);
}
