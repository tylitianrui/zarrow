Vendored IPC compression libraries

This directory vendors C sources used by Arrow IPC BodyCompression support
(ZSTD and LZ4_FRAME). zarrow builds these sources directly via Zig build.

Pinned upstream versions:
- zstd: v1.5.7
- lz4: v1.9.4

Included files:
- vendor/zstd/zstd.c (generated amalgamation)
- vendor/zstd/zstd.h
- vendor/zstd/zstd_errors.h
- vendor/zstd.h (copy for zstd.c relative include "../zstd.h")
- vendor/zstd_errors.h (copy for vendor/zstd.h include "zstd_errors.h")
- vendor/lz4/lz4_all.c (aggregation unit compiled by Zig)
- vendor/lz4/lz4.c
- vendor/lz4/lz4.h
- vendor/lz4/lz4hc.c
- vendor/lz4/lz4hc.h
- vendor/lz4/lz4frame.c
- vendor/lz4/lz4frame.h
- vendor/lz4/xxhash.c
- vendor/lz4/xxhash.h

How zstd.c was generated:
1. clone zstd v1.5.7
2. run in zstd/build/single_file_libs:
   python3 combine.py -r ../../lib -x legacy/zstd_legacy.h -k zstd.h -o <repo>/vendor/zstd/zstd.c zstd-in.c
3. copy lib/zstd.h to vendor/zstd/zstd.h
4. copy vendor/zstd/zstd.h to vendor/zstd.h
5. copy lib/zstd_errors.h to vendor/zstd/zstd_errors.h and vendor/zstd_errors.h

License files are copied to vendor/zstd/LICENSE and vendor/lz4/LICENSE.

Build integration:
- Zig compiles exactly two compression C translation units:
  - vendor/zstd/zstd.c
  - vendor/lz4/lz4_all.c
