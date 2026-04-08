#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

std::shared_ptr<arrow::Schema> CanonicalSchema() {
  return arrow::schema({
      arrow::field("id", arrow::int32(), false),
      arrow::field("name", arrow::utf8(), true),
  });
}

arrow::Status Generate(const std::string& path) {
  auto schema = CanonicalSchema();

  arrow::Int32Builder id_builder;
  ARROW_RETURN_NOT_OK(id_builder.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> ids;
  ARROW_RETURN_NOT_OK(id_builder.Finish(&ids));

  arrow::StringBuilder name_builder;
  ARROW_RETURN_NOT_OK(name_builder.Append("alice"));
  ARROW_RETURN_NOT_OK(name_builder.AppendNull());
  ARROW_RETURN_NOT_OK(name_builder.Append("bob"));
  std::shared_ptr<arrow::Array> names;
  ARROW_RETURN_NOT_OK(name_builder.Finish(&names));

  auto batch = arrow::RecordBatch::Make(schema, 3, {ids, names});
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::FileOutputStream::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out.get(), schema));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Close();
}

arrow::Status Validate(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto in, arrow::io::ReadableFile::Open(path));
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(in));
  auto schema = reader->schema();
  if (schema->num_fields() != 2) return arrow::Status::Invalid("invalid field count");
  if (schema->field(0)->name() != "id" || schema->field(0)->type()->id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("invalid id field");
  }
  if (schema->field(1)->name() != "name" || schema->field(1)->type()->id() != arrow::Type::STRING) {
    return arrow::Status::Invalid("invalid name field");
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
  if (!batch) return arrow::Status::Invalid("missing batch");
  if (batch->num_rows() != 3) return arrow::Status::Invalid("invalid row count");

  std::shared_ptr<arrow::RecordBatch> extra;
  ARROW_RETURN_NOT_OK(reader->ReadNext(&extra));
  if (extra) return arrow::Status::Invalid("unexpected extra batch");

  auto ids = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
  auto names = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

  if (ids->Value(0) != 1 || ids->Value(1) != 2 || ids->Value(2) != 3) {
    return arrow::Status::Invalid("invalid id values");
  }
  if (names->GetString(0) != "alice" || !names->IsNull(1) || names->GetString(2) != "bob") {
    return arrow::Status::Invalid("invalid name values");
  }
  return arrow::Status::OK();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: interop_cpp <generate|validate> <path.arrow>\n";
    return 2;
  }
  const std::string mode = argv[1];
  const std::string path = argv[2];

  arrow::Status st = arrow::Status::Invalid("mode must be generate or validate");
  if (mode == "generate") st = Generate(path);
  if (mode == "validate") st = Validate(path);
  if (!st.ok()) {
    std::cerr << st.ToString() << "\n";
    return 1;
  }
  return 0;
}

