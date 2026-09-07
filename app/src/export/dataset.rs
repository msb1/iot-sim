use std::fs::File;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use parquet::basic::Compression;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use tokio_util::sync::CancellationToken;

use crate::config::DatasetConfig;
use crate::export::{ExportError, TelemetryExporter};
use crate::telemetry::SensorReading;

const DATASET_SCHEMA: &str = "message iot_telemetry {\
    REQUIRED BINARY entity_type (UTF8);\
    REQUIRED BINARY entity_id (UTF8);\
    REQUIRED BINARY sensor_id (UTF8);\
    REQUIRED BINARY sensor_type (UTF8);\
    REQUIRED INT64 timestamp_ms;\
    REQUIRED INT64 interval_ms;\
    REQUIRED INT64 sequence;\
    REQUIRED BINARY metric (UTF8);\
    REQUIRED DOUBLE value;\
}";

// Keep row-group counts bounded for long historical dataset runs. The Parquet
// format permits at most 32,767 row groups in one file; batching scheduler
// callbacks avoids creating one group for every callback.
const ROW_GROUP_SIZE: usize = 100_000;

#[derive(Debug)]
struct DatasetRow {
    entity_type: String,
    entity_id: String,
    sensor_id: String,
    sensor_type: String,
    timestamp_ms: i64,
    interval_ms: i64,
    sequence: i64,
    metric: String,
    value: f64,
}

struct DatasetWriter {
    object_key: String,
    local_path: PathBuf,
    writer: SerializedFileWriter<File>,
    row_count: u64,
    buffered_rows: Vec<DatasetRow>,
}

/// Writes a long-form Parquet dataset locally in row groups, then uploads the
/// finalized immutable object to RustFS when generation is complete.
pub struct DatasetExporter {
    config: DatasetConfig,
    dataset_start_iso: String,
    writer: Mutex<Option<DatasetWriter>>,
}

impl DatasetExporter {
    pub fn new(config: DatasetConfig, dataset_started_ms: i64) -> Result<Self, ExportError> {
        let dataset_start_iso = chrono::DateTime::from_timestamp_millis(dataset_started_ms)
            .ok_or_else(|| {
                ExportError::Dataset("dataset start timestamp is not representable".into())
            })?
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        Ok(Self {
            config,
            dataset_start_iso,
            writer: Mutex::new(None),
        })
    }

    fn new_writer(entity_id: &str, dataset_start_iso: &str) -> Result<DatasetWriter, ExportError> {
        let temp = tempfile::NamedTempFile::new()
            .map_err(|error| ExportError::Dataset(error.to_string()))?;
        let local_path = temp
            .into_temp_path()
            .keep()
            .map_err(|error| ExportError::Dataset(error.error.to_string()))?;
        let file =
            File::create(&local_path).map_err(|error| ExportError::Dataset(error.to_string()))?;
        let schema = std::sync::Arc::new(
            parse_message_type(DATASET_SCHEMA)
                .map_err(|error| ExportError::Dataset(error.to_string()))?,
        );
        let properties = std::sync::Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let writer = SerializedFileWriter::new(file, schema, properties)
            .map_err(|error| ExportError::Dataset(error.to_string()))?;
        let object_key = dataset_object_key(entity_id, dataset_start_iso);
        Ok(DatasetWriter {
            object_key,
            local_path,
            writer,
            row_count: 0,
            buffered_rows: Vec::with_capacity(ROW_GROUP_SIZE),
        })
    }

    fn write_rows(writer: &mut DatasetWriter, rows: Vec<DatasetRow>) -> Result<(), ExportError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut group = writer.writer.next_row_group().map_err(dataset_error)?;
        write_bytes(&mut group, rows.iter().map(|row| row.entity_type.as_str()))?;
        write_bytes(&mut group, rows.iter().map(|row| row.entity_id.as_str()))?;
        write_bytes(&mut group, rows.iter().map(|row| row.sensor_id.as_str()))?;
        write_bytes(&mut group, rows.iter().map(|row| row.sensor_type.as_str()))?;
        write_i64(&mut group, rows.iter().map(|row| row.timestamp_ms))?;
        write_i64(&mut group, rows.iter().map(|row| row.interval_ms))?;
        write_i64(&mut group, rows.iter().map(|row| row.sequence))?;
        write_bytes(&mut group, rows.iter().map(|row| row.metric.as_str()))?;
        write_f64(&mut group, rows.iter().map(|row| row.value))?;
        group.close().map_err(dataset_error)?;
        writer.row_count += rows.len() as u64;
        Ok(())
    }

    fn buffer_rows(writer: &mut DatasetWriter, rows: Vec<DatasetRow>) -> Result<(), ExportError> {
        writer.buffered_rows.extend(rows);
        while writer.buffered_rows.len() >= ROW_GROUP_SIZE {
            let remainder = writer.buffered_rows.split_off(ROW_GROUP_SIZE);
            let batch = std::mem::replace(
                &mut writer.buffered_rows,
                Vec::with_capacity(ROW_GROUP_SIZE),
            );
            Self::write_rows(writer, batch)?;
            writer.buffered_rows = remainder;
        }
        Ok(())
    }

    async fn upload(&self, object_key: &str, local_path: &PathBuf) -> Result<(), ExportError> {
        let credentials = aws_sdk_s3::config::Credentials::new(
            self.config.s3_access_key.clone(),
            self.config.s3_secret_key.clone(),
            None,
            None,
            "iot-sim-dataset-config",
        );
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(credentials)
            .endpoint_url(&self.config.s3_endpoint_url)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);
        let body = ByteStream::from_path(local_path)
            .await
            .map_err(|error| ExportError::Dataset(error.to_string()))?;
        client
            .put_object()
            .bucket(&self.config.s3_bucket_name)
            .key(object_key)
            .content_type("application/vnd.apache.parquet")
            .body(body)
            .send()
            .await
            .map_err(|error| ExportError::Dataset(error.to_string()))?;
        tracing::info!(bucket = %self.config.s3_bucket_name, key = %object_key, "dataset Parquet file uploaded");
        Ok(())
    }
}

fn dataset_object_key(entity_id: &str, dataset_start_iso: &str) -> String {
    format!(
        "dataset/{}-{dataset_start_iso}.parquet",
        filename_component(entity_id)
    )
}

fn filename_component(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '_' | '-' => character,
            _ => '_',
        })
        .collect()
}

fn dataset_error(error: parquet::errors::ParquetError) -> ExportError {
    ExportError::Dataset(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::dataset_object_key;

    #[test]
    fn dataset_run_uses_entity_id_object_key() {
        assert_eq!(
            dataset_object_key("data_center_rack_01", "2026-08-01T00:00:00Z"),
            "dataset/data_center_rack_01-2026-08-01T00:00:00Z.parquet"
        );
    }
}

fn write_bytes<'a>(
    group: &mut parquet::file::writer::SerializedRowGroupWriter<'_, File>,
    values: impl Iterator<Item = &'a str>,
) -> Result<(), ExportError> {
    let values: Vec<ByteArray> = values.map(ByteArray::from).collect();
    let mut column = group.next_column().map_err(dataset_error)?.ok_or_else(|| {
        ExportError::Dataset("Parquet schema has fewer columns than expected".into())
    })?;
    match column.untyped() {
        ColumnWriter::ByteArrayColumnWriter(writer) => writer
            .write_batch(&values, None, None)
            .map_err(dataset_error)?,
        _ => {
            return Err(ExportError::Dataset(
                "unexpected Parquet column type".into(),
            ))
        }
    };
    column.close().map_err(dataset_error)
}

fn write_i64(
    group: &mut parquet::file::writer::SerializedRowGroupWriter<'_, File>,
    values: impl Iterator<Item = i64>,
) -> Result<(), ExportError> {
    let values: Vec<i64> = values.collect();
    let mut column = group.next_column().map_err(dataset_error)?.ok_or_else(|| {
        ExportError::Dataset("Parquet schema has fewer columns than expected".into())
    })?;
    match column.untyped() {
        ColumnWriter::Int64ColumnWriter(writer) => writer
            .write_batch(&values, None, None)
            .map_err(dataset_error)?,
        _ => {
            return Err(ExportError::Dataset(
                "unexpected Parquet column type".into(),
            ))
        }
    };
    column.close().map_err(dataset_error)
}

fn write_f64(
    group: &mut parquet::file::writer::SerializedRowGroupWriter<'_, File>,
    values: impl Iterator<Item = f64>,
) -> Result<(), ExportError> {
    let values: Vec<f64> = values.collect();
    let mut column = group.next_column().map_err(dataset_error)?.ok_or_else(|| {
        ExportError::Dataset("Parquet schema has fewer columns than expected".into())
    })?;
    match column.untyped() {
        ColumnWriter::DoubleColumnWriter(writer) => writer
            .write_batch(&values, None, None)
            .map_err(dataset_error)?,
        _ => {
            return Err(ExportError::Dataset(
                "unexpected Parquet column type".into(),
            ))
        }
    };
    column.close().map_err(dataset_error)
}

#[async_trait]
impl TelemetryExporter for DatasetExporter {
    fn name(&self) -> &'static str {
        "dataset"
    }

    async fn start(&self, _cancellation: CancellationToken) -> Result<(), ExportError> {
        tracing::info!(dataset_start = %self.dataset_start_iso, "dataset exporter writing one Parquet file for the dataset run");
        Ok(())
    }

    async fn export(&self, readings: &[SensorReading]) -> Result<(), ExportError> {
        let Some(first_reading) = readings.first() else {
            return Ok(());
        };
        let mut rows = Vec::new();
        for reading in readings {
            for (metric, value) in &reading.metrics {
                rows.push(DatasetRow {
                    entity_type: reading.entity_type.clone(),
                    entity_id: reading.entity_id.clone(),
                    sensor_id: reading.sensor_id.clone(),
                    sensor_type: serde_json::to_string(&reading.sensor_type)
                        .map_err(ExportError::Serialization)?
                        .trim_matches('"')
                        .to_owned(),
                    timestamp_ms: reading.timestamp_ms,
                    interval_ms: reading.interval_ms,
                    sequence: reading.sequence.min(i64::MAX as u64) as i64,
                    metric: metric.clone(),
                    value: *value,
                });
            }
        }

        let mut writer = self
            .writer
            .lock()
            .map_err(|_| ExportError::Dataset("dataset writer lock poisoned".into()))?;
        if writer.is_none() {
            *writer = Some(Self::new_writer(
                &first_reading.entity_id,
                &self.dataset_start_iso,
            )?);
        }
        let writer = writer
            .as_mut()
            .ok_or_else(|| ExportError::Dataset("dataset exporter was already shut down".into()))?;
        Self::buffer_rows(writer, rows)?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ExportError> {
        let writer = {
            let mut writer = self
                .writer
                .lock()
                .map_err(|_| ExportError::Dataset("dataset writer lock poisoned".into()))?;
            writer.take()
        };
        if let Some(mut writer) = writer {
            let buffered_rows = std::mem::take(&mut writer.buffered_rows);
            Self::write_rows(&mut writer, buffered_rows)?;
            let row_count = writer.row_count;
            let object_key = writer.object_key.clone();
            let local_path = writer.local_path.clone();
            writer.writer.close().map_err(dataset_error)?;
            self.upload(&object_key, &local_path).await?;
            let _ = std::fs::remove_file(&local_path);
            tracing::info!(rows = row_count, key = %object_key, "dataset generation complete");
        }
        Ok(())
    }
}
