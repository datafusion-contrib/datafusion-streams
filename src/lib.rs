use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use rdkafka::error::KafkaError;

pub mod kafka;
pub mod dataframe;

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



#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use arrow::datatypes::DataType;
    use datafusion::execution::dataframe_impl::DataFrameImpl;
    use datafusion::logical_plan::{DFField, DFSchema, LogicalPlan};
    use datafusion::logical_plan::plan::StreamScan;
    use datafusion::prelude::*;
    use crate::kafka::execution::KafkaExecutionPlan;


    #[tokio::test]
    pub async fn test_exec() -> datafusion::error::Result<()> {
        let ec = ExecutionContext::new();
        let dfs = DFSchema::new(
            vec![
                DFField::new(Some("topic1"), "key", DataType::Binary, false),
                DFField::new(Some("topic1"), "value", DataType::Binary, false),
            ]
        )?;
        let ep = KafkaExecutionPlan {
            time_window: Duration::from_secs(1),
            topic: "topic1".to_string(),
            batch_size: 10,
            conf: Default::default(),
        };
        let lp = LogicalPlan::StreamingScan(StreamScan {
            topic_name: "topic1".to_string(),
            source: Arc::new(ep),
            schema: Arc::new(dfs),
            batch_size: Some(10),
        });

        let df = DataFrameImpl::new(ec.state, &lp);
        let pp = df.limit(5)?;

        pp.show().await?;


        Ok(())
    }
}
