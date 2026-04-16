const datatype = @import("../datatype.zig");

pub const DataType = datatype.DataType;
pub const IntType = datatype.IntType;

pub const BufferRegion = struct {
    offset: usize,
    length: usize,

    pub fn bytes(self: BufferRegion, body: []const u8) []const u8 {
        return body[self.offset .. self.offset + self.length];
    }
};

pub const TensorDim = struct {
    size: i64,
    name: ?[]const u8,
};

pub const TensorMetadata = struct {
    value_type: DataType,
    shape: []const TensorDim,
    strides: ?[]const i64,
    data: BufferRegion,
};

pub const SparseMatrixAxis = enum {
    row,
    column,
};

pub const SparseTensorIndexMetadata = union(enum) {
    coo: struct {
        indices_type: IntType,
        indices_strides: ?[]const i64,
        indices: BufferRegion,
        is_canonical: bool,
    },
    csx: struct {
        compressed_axis: SparseMatrixAxis,
        indptr_type: IntType,
        indptr: BufferRegion,
        indices_type: IntType,
        indices: BufferRegion,
    },
    csf: struct {
        indptr_type: IntType,
        indptr_buffers: []const BufferRegion,
        indices_type: IntType,
        indices_buffers: []const BufferRegion,
        axis_order: []const i32,
    },
};

pub const SparseTensorMetadata = struct {
    value_type: DataType,
    shape: []const TensorDim,
    non_zero_length: usize,
    sparse_index: SparseTensorIndexMetadata,
    data: BufferRegion,
};

pub const TensorLikeMetadata = union(enum) {
    tensor: TensorMetadata,
    sparse_tensor: SparseTensorMetadata,
};
