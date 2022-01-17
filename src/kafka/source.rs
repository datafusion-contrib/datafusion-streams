use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::array::{ArrayBuilder, BinaryBuilder};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::RecordBatchStream;
use futures::{FutureExt, StreamExt};
use futures::stream::Stream;
use rdkafka::{ClientConfig, Message};
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::message::BorrowedMessage;

use crate::{DataFusionStreamError, topic_schema};

pub struct KafkaSource {
    time_window: Duration,
    topic: String,
    batch_size: usize,
    partition: usize,
    // client: Arc<StreamConsumer<DefaultConsumerContext>>,
    consumer: StreamPartitionQueue<DefaultConsumerContext>
}

impl Debug for KafkaSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Topic: {}", self.topic)?;
        writeln!(f, "Duration: {:?}", self.time_window)?;
        Ok(())
    }
}

impl KafkaSource {
    pub fn new(topic: String, time_window: Duration, batch_size: usize, client_cfg: HashMap<String, String>, partition: usize) -> Result<Self, DataFusionStreamError> {
        let client: Arc<StreamConsumer<DefaultConsumerContext>> = Arc::new(client_cfg.into_iter()
            .collect::<ClientConfig>()
            .create()?);

        client.subscribe(&[&topic])?;

        let consumer: StreamPartitionQueue<DefaultConsumerContext> = client.split_partition_queue(&topic, partition as i32).unwrap();
        tokio::spawn(async move {
            let message = client.recv().await;
            panic!("main stream consumer queue unexpectedly received message: {:?}", message);
        });
        Ok(Self { topic, batch_size, time_window, partition, consumer })
    }

    fn msg_to_record(key_array: &mut BinaryBuilder, value_array: &mut BinaryBuilder, msg: &BorrowedMessage) -> ArrowResult<()> {
        log::trace!("Msg: {:?}", msg);
        let payload = msg.payload().unwrap_or(&[]);
        value_array.append_value(payload)?;

        let key = msg.key().unwrap_or(&[]);
        key_array.append_value(key)
    }
}

impl Stream for KafkaSource {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let start = Instant::now();
        let mut value_array = BinaryBuilder::new(self.batch_size);
        let mut key_array = BinaryBuilder::new(self.batch_size);
        log::trace!("Batch Size: {}", self.batch_size);
        while start.elapsed() < self.time_window && value_array.len() < self.batch_size {
            if let Poll::Ready(Some(Ok(msg))) = &mut self.consumer.stream().next().poll_unpin(cx) {
                Self::msg_to_record(&mut key_array, &mut value_array, msg)?;
            }
        }
        if value_array.is_empty() {
            log::trace!("Return Pending...");
            return Poll::Pending;
        }
        log::trace!("Returning batch: {}", value_array.len());
        Poll::Ready(Some(RecordBatch::try_new(self.schema(), vec![Arc::new(key_array.finish()), Arc::new(value_array.finish())])))
    }
}

impl RecordBatchStream for KafkaSource {
    fn schema(&self) -> SchemaRef {
        topic_schema()
    }
}
