//! FlatBuffers runtime vendored from archaistvolts/flatbufferz @ 315ed95.
//! Only the runtime subset needed by the pre-generated src/arrow_fbs/ code
//! is included here.  Code-generation helpers (codegen, idl, reflection) are
//! intentionally omitted.

pub const Builder = @import("Builder.zig");
pub const Table = @import("Table.zig");
pub const Struct = Table.Struct;
pub const encode = @import("encode.zig");
pub const common = @import("common.zig");

const fb_helpers = @import("flatbuffers.zig");
pub const GetRootAs = fb_helpers.GetRootAs;
pub const GetSizePrefixedRootAs = fb_helpers.GetSizePrefixedRootAs;
pub const GetSizePrefix = fb_helpers.GetSizePrefix;
pub const GetIndirectOffset = fb_helpers.GetIndirectOffset;
pub const GetBufferIdentifier = fb_helpers.GetBufferIdentifier;
pub const BufferHasIdentifier = fb_helpers.BufferHasIdentifier;
