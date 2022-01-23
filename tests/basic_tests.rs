use arrow::datatypes::DataType;
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::plan::StreamScan;
use datafusion::logical_plan::{DFField, DFSchema, LogicalPlan};
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::util::pretty::print_batches;
use datafusion_streams::kafka::execution::KafkaExecutionPlan;
use futures::StreamExt;
use tokio::time::timeout;

use crate::utils::*;

mod utils;

const TOPIC_NAME: &str = "test_topic";




#[tokio::test]
pub async fn test_exec() -> datafusion::error::Result<()> {

    setup_logger(true, Some("rdkafka=trace,datafusion_streams=trace"));
    populate_topic(TOPIC_NAME, 15, &value_fn, &key_fn, Some(0), None).await;
    populate_topic(TOPIC_NAME, 15, &value_fn, &key_fn, Some(1), None).await;
    populate_topic(TOPIC_NAME, 15, &value_fn, &key_fn, Some(2), None).await;

    let conf = ExecutionConfig::new()
        .with_target_partitions(3)
        .with_batch_size(5);
    let ec = ExecutionContext::with_config(conf);
    let dfs = DFSchema::new(vec![
        DFField::new(None, "key", DataType::Binary, false),
        DFField::new(None, "value", DataType::Binary, false),
    ])?;
    let schema = dfs.clone();

    let ep = KafkaExecutionPlan {
        time_window: Duration::from_millis(200),
        topic: TOPIC_NAME.to_string(),
        conf: consumer_config("0", None),
    };

    let lp = LogicalPlan::StreamingScan(StreamScan {
        topic_name: TOPIC_NAME.to_string(),
        source: Arc::new(ep),
        schema: Arc::new(dfs),
    });

    let key_exp = col("key").cast_to(&DataType::Utf8, &schema)?.alias("key");
    let value_exp = col("value")
        .cast_to(&DataType::Utf8, &schema)?
        .alias("value");
    let df = DataFrameImpl::new(ec.state, &lp).select(vec![
        key_exp,
        value_exp.clone(),
        length(value_exp),
    ])?;

    timeout(Duration::from_secs(10), async move {
        let mut b = df.execute_stream().await.unwrap();
        // for mut p in b {
            while let Some(Ok(batch)) = b.next().await {
                print_batches(&[batch]).unwrap();
            }
        // }
    })
    .await
    .unwrap();

    Ok(())
}
