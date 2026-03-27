pub const DataType = enum(u8) {
    // NULL type having no physical storage
    NULL,
    // BOOL is a 1 bit, LSB bit-packed ordering
    BOOL,
    // UINT8 is an Unsigned 8-bit little-endian integer
    UINT8,
    // INT8 is a Signed 8-bit little-endian integer
    INT8,
    // UINT16 is an Unsigned 16-bit little-endian integer
    UINT16,
    // INT16 is a Signed 16-bit little-endian integer
    INT16,
    // UINT32 is an Unsigned 32-bit little-endian integer
    UINT32,
    // INT32 is a Signed 32-bit little-endian integer
    INT32,
    // UINT64 is an Unsigned 64-bit little-endian integer
    UINT64,
    // INT64 is a Signed 64-bit little-endian integer
    INT64,
    // FLOAT16 is a 2-byte floating point value
    FLOAT16,
    // FLOAT32 is a 4-byte floating point value
    FLOAT32,
    // FLOAT64 is an 8-byte floating point value
    FLOAT64,
    // STRING is a UTF8 variable-length string
    STRING,
    // BINARY is a Variable-length byte type (no guarantee of UTF8-ness)
    BINARY,
    // FIXED_SIZE_BINARY is a binary where each value occupies the same number of bytes
    FIXED_SIZE_BINARY,
    // DATE32 is int32 days since the UNIX epoch
    DATE32,
    // DATE64 is int64 milliseconds since the UNIX epoch
    DATE64,
    // TIMESTAMP is an exact timestamp encoded with int64 since UNIX epoch
    // Default unit millisecond
    TIMESTAMP,
    // TIME32 is a signed 32-bit integer, representing either seconds or
    // milliseconds since midnight
    TIME32,
    // TIME64 is a signed 64-bit integer, representing either microseconds or
    // nanoseconds since midnight
    TIME64,
    // INTERVAL_MONTHS is YEAR_MONTH interval in SQL style
    INTERVAL_MONTHS,
    // INTERVAL_DAY_TIME is DAY_TIME in SQL Style
    INTERVAL_DAY_TIME,
    // DECIMAL128 is a precision- and scale-based decimal type.
    DECIMAL128,
    // DECIMAL256 is a precision and scale based decimal type, with 256 bit max.
    DECIMAL256,
    // LIST is a list of some logical data type
    LIST,
    // STRUCT of logical types
    STRUCT,
    // SPARSE_UNION of logical types
    SPARSE_UNION,
    // DENSE_UNION of logical types
    DENSE_UNION,
    // DICTIONARY aka Category type
    DICTIONARY,
    // MAP is a repeated struct logical type
    MAP,
    // Custom data type, implemented by user
    EXTENSION,
    // Fixed size list of some logical type
    FIXED_SIZE_LIST,
    // Measure of elapsed time in either seconds, milliseconds, microseconds
    // or nanoseconds.
    DURATION,
    // like STRING, but 64-bit offsets
    LARGE_STRING,
    // like BINARY but with 64-bit offsets
    LARGE_BINARY,
    // like LIST but with 64-bit offsets
    LARGE_LIST,
    // calendar interval with three fields
    INTERVAL_MONTH_DAY_NANO,

    // Run-end encoded type
    RUN_END_ENCODED,

    // String (UTF8) view type with 4-byte prefix and inline
    // small string optimizations
    STRING_VIEW,

    // Bytes view with 4-byte prefix and inline small byte arrays optimization
    BINARY_VIEW,

    // LIST_VIEW is a list of some logical data type represented with offsets and sizes
    LIST_VIEW,

    // like LIST but with 64-bit offsets
    LARGE_LIST_VIEW,

    // Decimal value with 32-bit representation
    DECIMAL32,

    // Decimal value with 64-bit representation
    DECIMAL64,
};

// Alias to ensure we do not break any consumers
pub const DECIMAL = DataType.DECIMAL128;
