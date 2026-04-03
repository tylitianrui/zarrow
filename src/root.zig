// Re-export the public buffer types from the package root.
const buffer = @import("buffer.zig");

pub const Buffer = buffer.Buffer;
pub const MutableBuffer = buffer.MutableBuffer;

// Pull buffer tests into the root test target.
test {
    _ = @import("buffer.zig");
}
