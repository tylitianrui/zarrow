// Re-export the public buffer types from the package root.
const buffer = @import("buffer.zig");
const bitmap = @import("bitmap.zig");

pub const Buffer = buffer.Buffer;
pub const MutableBuffer = buffer.MutableBuffer;
pub const ValidityBitmap = bitmap.ValidityBitmap;
pub const MutableValidityBitmap = bitmap.MutableValidityBitmap;

// Pull buffer tests into the root test target.
test {
    _ = @import("buffer.zig");
    _ = @import("bitmap.zig");
}
