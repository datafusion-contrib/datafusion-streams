use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug};
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::streaming::StreamingProvider;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics};

use crate::kafka::source::KafkaSource;
use crate::DataFusionStreamError;
use crate::topic_schema;

#[derive(Debug)]
pub struct KafkaExecutionPlan {
    pub time_window: Duration,
    pub topic: String,
    pub batch_size: usize,
    pub conf: HashMap<String, String>,
}

#[async_trait]
impl ExecutionPlan for KafkaExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef { topic_schema() }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(&self, _children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let source = KafkaSource::new(self.topic.clone(), self.time_window, self.batch_size, self.conf.clone(), partition)
            .map_err(|err| err.0)?;
        Ok(Box::pin(source))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

#[async_trait]
impl StreamingProvider for KafkaExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        topic_schema()
    }

    async fn recv(&self, batch_size: usize) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(KafkaExecutionPlan {
            time_window: self.time_window,
            topic: self.topic.clone(),
            batch_size,
            conf: self.conf.clone(),
        }))
    }
}
