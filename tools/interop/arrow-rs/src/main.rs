use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

fn canonical_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn generate(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = canonical_schema();
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names])?;

    let file = File::create(path)?;
    let mut writer = StreamWriter::try_new(file, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

fn validate(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = StreamReader::try_new(file, None)?;
    let schema = reader.schema();
    if schema.fields().len() != 2 {
        return Err("invalid schema field count".into());
    }
    if schema.field(0).name() != "id" || schema.field(0).data_type() != &DataType::Int32 {
        return Err("invalid id field".into());
    }
    if schema.field(1).name() != "name" || schema.field(1).data_type() != &DataType::Utf8 {
        return Err("invalid name field".into());
    }

    let batch = reader.next().ok_or("missing batch")??;
    if batch.num_rows() != 3 {
        return Err("invalid row count".into());
    }
    if reader.next().is_some() {
        return Err("unexpected extra batch".into());
    }

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("id downcast failed")?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("name downcast failed")?;

    if ids.value(0) != 1 || ids.value(1) != 2 || ids.value(2) != 3 {
        return Err("invalid id values".into());
    }
    if names.value(0) != "alice" || !names.is_null(1) || names.value(2) != "bob" {
        return Err("invalid name values".into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _exe = args.next();
    let mode = args.next().ok_or("usage: <generate|validate> <path.arrow>")?;
    let path = args.next().ok_or("usage: <generate|validate> <path.arrow>")?;
    let path = Path::new(&path);

    match mode.as_str() {
        "generate" => generate(path),
        "validate" => validate(path),
        _ => Err("mode must be generate or validate".into()),
    }
}

