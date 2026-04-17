# Apache Arrow 规范与 IPC：C / Rust 实现原理（面向实现）

> 更新时间：2026-04-16  
> 目标：给实现者一份可直接落地的“规范 + 实现”说明，重点覆盖 Arrow 列式内存规范、IPC 线协议、C 接口实现方式与 Rust（arrow-rs）分层实现。

## 1. Arrow 列式规范（必须掌握）

### 1.1 统一数据模型

Arrow 数组在规范上由以下元素组成：

- `data_type`
- `buffers[]`
- `length`
- `null_count`
- `children[]`（嵌套类型）
- `dictionary`（字典编码）

实现上可以把任意列看成一棵 `ArrayData tree`：父节点描述本列，子节点描述 nested 子列。

### 1.2 对齐与填充

- 内存分配推荐使用 `8` 或 `64` 字节对齐与 padding。
- 在 IPC 序列化时，这些对齐与 padding 要求会被强制执行。
- 社区建议优先使用 `64` 字节（更利于 SIMD / cache line 行为）。

### 1.3 空值位图（Validity Bitmap）

- 大多数类型使用独立 validity bitmap。
- 位语义：`1 = 非空`，`0 = null`。
- 位序：`LSB-first`（`bitmap[j / 8] & (1 << (j % 8))`）。
- `null_count == 0` 时，bitmap 可以省略（实现可自行决定是否仍分配）。

### 1.4 变长布局核心约束（Binary/List）

- 通过 `offsets` + `data` 表示变长值。
- `offsets` 必须单调不减。
- null 槽位可以对应非空物理区间（语义由 validity 决定）。

## 2. IPC 规范（Stream / File）

### 2.1 IPC 消息类型与结构

Arrow IPC 传输单元是消息流，核心消息：

- `Schema`
- `RecordBatch`
- `DictionaryBatch`

#### 2.1.1 Schema / RecordBatch / DictionaryBatch 结构

以下结构来自 Arrow IPC FlatBuffers 定义（`src/format/Schema.fbs`、`src/format/Message.fbs`）。

#### Schema（逻辑结构定义）

`Schema` 关注“列长什么样”，不包含具体列数据：

```text
Schema {
  endianness: Endianness = Little
  fields: [Field]
  custom_metadata: [KeyValue]
  features: [Feature]
}
```

`Field` 递归描述每一列：

```text
Field {
  name: string
  nullable: bool
  type: Type
  dictionary: DictionaryEncoding?   // 字典编码时存在
  children: [Field]                 // List/Struct/Union 等嵌套类型
  custom_metadata: [KeyValue]
}
```

`DictionaryEncoding` 关键字段：

```text
DictionaryEncoding {
  id: long
  indexType: Int
  isOrdered: bool
}
```

实现要点：

- `Schema` 决定后续 `RecordBatch` 的 `nodes/buffers` 扁平化顺序。
- dictionary 列的索引类型在 `Field.dictionary.indexType` 固定，实际字典值由后续 `DictionaryBatch` 提供。

#### RecordBatch（一批列数据）

`RecordBatch` 描述“这一批行的数据布局和 body 映射关系”：

```text
RecordBatch {
  length: long                 // 行数
  nodes: [FieldNode]           // 预序遍历后的逻辑节点
  buffers: [Buffer]            // 预序遍历后的物理 buffer 区间
  compression: BodyCompression?
  variadicBufferCounts: [long] // BinaryView/Utf8View 等可变 buffer 计数
}

FieldNode {
  length: long
  null_count: long
}

Buffer {
  offset: long
  length: long
}
```

实现要点：

- `nodes` 与 `buffers` 都是按 schema 树“前序展开（pre-order flatten）”。
- `Buffer.offset/length` 是相对本条消息 body 的区间。
- `compression` 若开启，buffer 级别压缩并按规范附带原始长度头。

#### DictionaryBatch（字典载荷）

`DictionaryBatch` 是“给某个 dictionary id 发送字典值”的专用消息：

```text
DictionaryBatch {
  id: long
  data: RecordBatch
  isDelta: bool = false
}
```

语义：

- `id`：对应 `Field.dictionary.id`。
- `data`：字典值本身（也是一个 `RecordBatch`）。
- `isDelta=false`：替换现有字典。
- `isDelta=true`：把本批字典值 append 到已有字典末尾。

实现要点：

- Writer 需要维护 `dictionary id -> last dictionary` 状态，决定发 full 还是 delta。
- Reader 必须先消费/更新字典，再解码后续使用该 id 的 `RecordBatch`。

### 2.2 Encapsulated Message 格式

每条 IPC 消息（stream/file）按下列结构封包：

1. 32-bit continuation indicator（有效值 `0xFFFFFFFF`）
2. 32-bit little-endian metadata length
3. FlatBuffers `Message` 元数据
4. padding（对齐到 8 字节边界）
5. body（长度需为 8 字节倍数）

这保证了消息可重定位、可零拷贝重建结构。

### 2.3 Streaming Format

流格式顺序：

1. `Schema`
2. 若干 `DictionaryBatch`
3. 若干 `RecordBatch`
4. 可选 `EOS`：`0xFFFFFFFF 0x00000000`

关键规则：

- `DictionaryBatch` 与 `RecordBatch` 可交错；
- 但在 stream 中，某字典 key 被使用前，通常应先出现对应 dictionary；
- 写端可通过 EOS 或关闭流来结束。

### 2.4 File Format（随机访问）

文件格式 = stream 格式 + footer：

- 文件头魔数：`ARROW1`
- 文件尾魔数：`ARROW1`
- 尾部有 footer（包含 schema 副本与各数据块偏移/长度），用于随机访问 RecordBatch

补充：

- 文件中对 dictionary 的出现顺序比 stream 更宽松（只要在文件中定义过即可）；
- 但同一 dictionary id 不能有多个“非 delta”替换批次。

## 3. C 实现原理（推荐路径）

### 3.1 以 C Data Interface 作为 ABI 核心

C 侧最小可互操作单元是两个结构体：

- `ArrowSchema`：描述类型与元数据
- `ArrowArray`：描述数据本体（buffers/children/dictionary）

重点不是“调用某个大库 API”，而是严格遵循这两个结构体的字段语义和生命周期契约。

### 3.2 生命周期与所有权模型

C Data Interface 的核心机制：

- 生产者填充结构体 + 设置 `release` 回调；
- 消费者使用后调用 `release`；
- 结构体“释放态”由 `release == NULL` 表示；
- move 语义通过“浅拷贝后将源标记为 released”实现。

实现结论：C 实现的稳定性关键在于 **release 回调递归释放 children/dictionary/private_data** 是否严格正确。

### 3.3 多批次传输：C Stream Interface

`ArrowArrayStream` 通过回调提供 pull 模式：

- `get_schema`
- `get_next`
- `get_last_error`
- `release`

`get_next` 返回 released array 表示 EOS。  
这层接口可直接承载数据库/执行引擎批处理输出，不需要 IPC 反序列化步骤。

### 3.4 实践建议：nanoarrow

如果目标是“轻量 C/C++ Arrow API”，可优先 nanoarrow：

- 直接围绕 C Data Interface 实现；
- 体积小、易 vendoring；
- 适合做 C ABI 边界层（而非完整分析引擎）。

## 4. Rust（arrow-rs）实现原理

### 4.1 分层架构（最关键）

arrow-rs 不是一个单 crate，而是分层：

- `arrow-schema`：逻辑类型（`DataType` / `Field` / `Schema`）
- `arrow-buffer`：底层共享内存与 bit/byte buffer 抽象
- `arrow-data`：`ArrayData` 与 layout 校验、低层构造
- `arrow-array`：强类型数组与 builder、`dyn Array` 类型擦除
- `arrow-ipc`：Stream/File 读写与 IPC 编解码

实现建议：先用 `arrow-data` 心智模型（buffers/children/layout），再上 `arrow-array` API。

### 4.2 IPC 在 Rust 里的对应

`arrow-ipc` 明确区分两类：

1. Streaming：`StreamReader` / `StreamWriter`
2. File：`FileReader` / `FileWriter`

语义上和规范一一对应：

- stream 面向连续读取；
- file 依赖 footer 支持随机访问；
- writer 结束阶段负责写 termination/footer（例如 `finish` / `close`）。

### 4.3 Rust 与 C ABI 互操作

arrow-rs 在 `ffi` / `ffi_stream` 模块提供 C 接口桥接：

- C Data Interface：`ArrowSchema` / `ArrowArray` 对应 Rust FFI 结构；
- C Stream Interface：`ArrowArrayStream` 对应 `FFI_ArrowArrayStream`；
- 可将 C 侧批流导入为 Rust `RecordBatchReader`，也可反向导出。

这使 Rust 可作为“计算内核”，C 侧作为“系统边界层”。

## 5. C 与 Rust 的落地分工建议

推荐分层：

1. C 层：进程内 ABI 边界（C Data/C Stream，负责生命周期与零拷贝交接）
2. Rust 层：类型安全构造、校验、计算与 IPC 编解码
3. 传输层：跨进程/落盘时使用 IPC Stream/File

一句话总结：

- **进程内**优先 C Data/C Stream（无 FlatBuffers 依赖、零拷贝）。
- **进程间/持久化**使用 IPC（可流式、可随机访问、可压缩）。

## 6. 与本仓库（zarrow）的对应点

本项目已覆盖上述关键路径，可重点对照：

- IPC：`src/ipc/`
- FlatBuffers 协议定义：`src/format/*.fbs`
- C Data Interface 相关：`src/ffi/c_data.zig`
- ArrayData 与布局校验：`src/array/array_data.zig`

## 7. 实例（可直接运行/改写）

### 7.1 Zig：IPC Stream 写读回环（zarrow）

仓库现成示例：`examples/ipc_stream.zig`  
运行：

```sh
zig build example-ipc_stream
```

核心流程代码（和示例一致）：

```zig
const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const id_type = zarrow.DataType{ .int32 = {} };
    const name_type = zarrow.DataType{ .string = {} };
    const fields = [_]zarrow.Field{
        .{ .name = "id", .data_type = &id_type, .nullable = false },
        .{ .name = "name", .data_type = &name_type, .nullable = true },
    };
    const schema = zarrow.Schema{ .fields = fields[0..] };

    var id_builder = try zarrow.Int32Builder.init(allocator, 3);
    defer id_builder.deinit();
    try id_builder.append(1);
    try id_builder.append(2);
    try id_builder.append(3);
    var id_ref = try id_builder.finish();
    defer id_ref.release();

    var name_builder = try zarrow.StringBuilder.init(allocator, 3, 12);
    defer name_builder.deinit();
    try name_builder.append("alice");
    try name_builder.appendNull();
    try name_builder.append("bob");
    var name_ref = try name_builder.finish();
    defer name_ref.release();

    var batch = try zarrow.RecordBatch.initBorrowed(allocator, schema, &[_]zarrow.ArrayRef{ id_ref, name_ref });
    defer batch.deinit();

    var out = std.array_list.Managed(u8).init(allocator);
    defer out.deinit();

    var writer = zarrow.IpcStreamWriter(@TypeOf(out.writer())).init(allocator, out.writer());
    defer writer.deinit();
    try writer.writeSchema(schema);
    try writer.writeRecordBatch(batch);
    try writer.writeEnd();

    var in = std.io.fixedBufferStream(out.items);
    var reader = zarrow.IpcStreamReader(@TypeOf(in.reader())).init(allocator, in.reader());
    defer reader.deinit();

    _ = try reader.readSchema();
    while (try reader.nextRecordBatch()) |*rb| {
        var one = rb.*;
        defer one.deinit();
        std.debug.print("rows={d}\n", .{one.numRows()});
    }
}
```

### 7.2 Zig：C Data Interface 导出/导入示例

这个例子展示“进程内零拷贝交接”最核心路径：

```zig
const std = @import("std");
const zarrow = @import("zarrow");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const id_type = zarrow.DataType{ .int32 = {} };

    var b = try zarrow.Int32Builder.init(allocator, 3);
    defer b.deinit();
    try b.append(10);
    try b.appendNull();
    try b.append(30);
    var arr = try b.finish();
    defer arr.release();

    // Export
    var c_arr = try zarrow.exportArrayToC(allocator, arr);
    defer if (c_arr.release) |rel| rel(&c_arr);

    // Import (ownership transferred to importer)
    var imported = try zarrow.importArrayFromC(allocator, &id_type, &c_arr);
    defer imported.release();

    const view = zarrow.Int32Array{ .data = imported.data() };
    std.debug.print("len={d}, v0={d}, isNull1={any}\n", .{
        view.len(),
        view.value(0),
        view.isNull(1),
    });
}
```

### 7.3 Rust：arrow-rs IPC Stream 示例

```rust
use std::sync::Arc;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_ipc::writer::StreamWriter;
use arrow_ipc::reader::StreamReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")])),
        ],
    )?;

    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema)?;
        w.write(&batch)?;
        w.finish()?;
    }

    let mut r = StreamReader::try_new(std::io::Cursor::new(buf), None)?;
    while let Some(batch) = r.next() {
        let b = batch?;
        println!("rows={}", b.num_rows());
    }
    Ok(())
}
```

## 8. 代码逻辑实现（zarrow 源码级）

### 8.1 IPC Writer 逻辑链

对应文件：`src/ipc/stream_writer.zig`

1. `writeSchema()`：构建 Schema FlatBuffers 并写消息头  
   入口：`writeSchema`（:186）  
   核心：`buildSchemaT`（:556） -> `writeMessage`（:328）
2. `writeRecordBatch()`：扫描字段、处理 dictionary、构建 RecordBatch 元数据与 body  
   入口：`writeRecordBatch`（:216）  
   关键动作：字典状态缓存（`dictionary_values`）+ body compression 选择
3. `writeEnd()`：输出流结束标记（EOS）  
   入口：`writeEnd`（:321）

一句话：Writer 的职责是“把 `ArrayData tree` 线性化为 IPC 消息序列，并维持字典状态机”。

### 8.2 IPC Reader 逻辑链

对应文件：`src/ipc/stream_reader.zig`

1. `readSchema()`：读取首个 Schema 消息并建立 `SchemaRef`  
   入口：`readSchema`（:174）  
   核心：`readMessage`（:352） -> `buildSchemaFromFlatbufWithEndiannessMode`（:687）
2. `nextRecordBatch()`：循环取消息，先吞掉 dictionary，再产出 batch  
   入口：`nextRecordBatch`（:193）  
   核心：`readMessageOptional`（:281）  
   Dictionary 分支：`ingestDictionaryBatchWithMap`（:1702）  
   RecordBatch 分支：`buildRecordBatchFromFlatbuf`（:1119）

一句话：Reader 的职责是“按消息顺序恢复 schema + dictionary 上下文，再重建列数据”。

### 8.3 C Data 导入导出逻辑链

对应文件：`src/ffi/c_data.zig`

1. Schema：
- 导出：`exportSchema`（:120） -> `releaseExportedSchema`（:338）
- 导入：`importSchemaOwned`（:126）
2. Array：
- 导出：`exportArray`（:153） -> `exportArrayRecursive`（:368） -> `releaseExportedArray`（:426）
- 导入：`importArray`（:157） -> `importArrayRecursive`（:671）
3. 生命周期关键点：
- importer 接管所有权后，会将输入 `c_array.release = null`（避免双释放）
- `ownerRelease`（:108）负责真正触发 C 侧 release 并销毁 owner

一句话：FFI 的核心不是“拷贝数据”，而是“严格转移所有权并保证 release 回调可重入、可递归”。

## 9. 官方来源（Primary Sources）

- Arrow Columnar Format（规范主文档，Version 1.5）：  
  https://arrow.apache.org/docs/format/Columnar.html
- Arrow C Data Interface：  
  https://arrow.apache.org/docs/format/CDataInterface.html
- Arrow C Stream Interface：  
  https://arrow.apache.org/docs/format/CStreamInterface.html
- Arrow C++ IPC API：  
  https://arrow.apache.org/docs/cpp/api/ipc.html
- Arrow C++ Memory Management：  
  https://arrow.apache.org/docs/cpp/memory.html
- Arrow C++ C Interfaces（Import/Export C ABI）：  
  https://arrow.apache.org/docs/cpp/api/c_abi.html
- arrow-rs crate docs（总体）：  
  https://docs.rs/arrow/latest/arrow/
- arrow-ipc crate docs：  
  https://docs.rs/arrow-ipc/latest/arrow_ipc/
- arrow-array crate docs：  
  https://docs.rs/arrow-array/latest/arrow_array/
- arrow-data crate docs：  
  https://docs.rs/arrow-data/latest/arrow_data/
- arrow-buffer crate docs：  
  https://docs.rs/arrow-buffer/latest/arrow_buffer/
- nanoarrow 文档（轻量 C/C++ 实践）：  
  https://arrow.apache.org/nanoarrow/latest/getting-started/cpp.html
