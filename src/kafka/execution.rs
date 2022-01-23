use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayBuilder, BinaryBuilder};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::streaming::StreamingProvider;
use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use futures::StreamExt;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use tokio::spawn;
use tokio::sync::mpsc::*;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::topic_schema;

#[derive(Clone, Debug)]
pub struct KafkaExecutionPlan {
    pub time_window: Duration,
    pub topic: String,
    pub conf: HashMap<String, String>,
}

#[async_trait]
impl ExecutionPlan for KafkaExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        topic_schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(3)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(&self, partition: usize, runtime: Arc<RuntimeEnv>) -> Result<SendableRecordBatchStream> {
        let (response_tx, response_rx) = channel::<ArrowResult<RecordBatch>>(2);

        let topic = self.topic.clone();
        let batch_size = runtime.batch_size();
        let schema = ExecutionPlan::schema(self);
        let time_window = self.time_window;
        let conf = self.conf.clone();

        let join_handle: JoinHandle<()> = spawn(async move {
            log::trace!("New Handle: {}, #{}", &topic, partition);

            let client: Arc<StreamConsumer> = Arc::new(conf.clone().into_iter().collect::<ClientConfig>().create().unwrap());
            let consumer = client.split_partition_queue(&topic, partition as i32).unwrap();

            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&topic, partition as i32, Offset::Beginning).unwrap();
            client.assign(&tpl).unwrap();
            loop {
                let mut value_array = BinaryBuilder::new(batch_size);
                let mut key_array = BinaryBuilder::new(batch_size);
                let start_time = Instant::now();

                let main = timeout(Duration::from_millis(100), client.stream().next()).await;
                assert!(main.is_err());
                while start_time.elapsed() < time_window && value_array.len() < batch_size {
                    let msg = consumer.recv().await;

                    log::trace!("Msg: {:?}", msg);
                    match msg {
                        Ok(ref m) => Self::msg_to_record(&mut key_array, &mut value_array, m),
                        Err(KafkaError::PartitionEOF(p)) => {
                            log::trace!("Reached end of partition #{} got EOF", p);
                            return;
                        }
                        _ => panic!("IDK")
                    }.unwrap();
                }

                log::trace!("Sending batch: {}", value_array.len());
                let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(key_array.finish()), Arc::new(value_array.finish())]);
                response_tx.send(batch).await.unwrap();
            }
        });

        Ok(RecordBatchReceiverStream::create(
            &topic_schema(),
            response_rx,
            join_handle,
        ))
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

    async fn recv(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.clone()))
    }
}

impl KafkaExecutionPlan {
    pub fn msg_to_record(key_array: &mut BinaryBuilder, value_array: &mut BinaryBuilder, msg: &BorrowedMessage) -> ArrowResult<()> {
        log::trace!("Msg: {:?}", msg);
        let payload = msg.payload().unwrap_or(&[]);
        value_array.append_value(payload)?;

        let key = msg.key().unwrap_or(&[]);
        key_array.append_value(key)
    }
}
