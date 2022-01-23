use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use rdkafka::error::KafkaError;

pub mod kafka;

pub fn topic_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
    ]))
}

pub struct DataFusionStreamError(pub DataFusionError);

impl From<KafkaError> for DataFusionStreamError {
    fn from(err: KafkaError) -> Self {
        Self(DataFusionError::Execution(err.to_string()))
    }
}

impl From<DataFusionError> for DataFusionStreamError {
    fn from(err: DataFusionError) -> Self {
        DataFusionStreamError(err)
    }
}
