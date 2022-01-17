use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use arrow::datatypes::{DataType};
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::{DFField, DFSchema, LogicalPlan};
use datafusion::logical_plan::plan::StreamScan;
use datafusion::prelude::*;

use futures::StreamExt;
use tokio::time::timeout;
use datafusion_streams::kafka::execution::KafkaExecutionPlan;

use datafusion_streams::kafka::source::KafkaSource;

use crate::utils::*;

mod utils;

const TOPIC_NAME: &str = "test_topic";

#[tokio::test]
pub async fn client_create() -> Result<(), Box<dyn Error>> {
    setup_logger(true, Some("rdkafka::consumer=trace"));
    create_topic(TOPIC_NAME, 1).await;
    populate_topic(TOPIC_NAME, 5, &value_fn, &key_fn, Some(1), Some(1)).await;
    std::thread::sleep(Duration::from_secs(1));
    let conf = consumer_config("1", None);
    let mut source = KafkaSource::new(TOPIC_NAME.into(), Duration::from_millis(500), 3, conf, 1).map_err(|e| e.0)?;

    timeout(Duration::from_secs(5), async move {
        while let Some(Ok(batch)) = source.next().await {
            dbg!(batch.num_rows(), batch.num_columns());
        }
    }).await?;

    Ok(())
}

use datafusion::arrow::util::pretty::print_batches;


#[tokio::test]
pub async fn test_exec() -> datafusion::error::Result<()> {
    setup_logger(true, Some("rdkafka=warn,datafusion_streams=trace"));
    populate_topic(TOPIC_NAME, 15, &value_fn, &key_fn, Some(0), None).await;

    let conf = ExecutionConfig::new().with_target_partitions(1).with_batch_size(5);
    let ec = ExecutionContext::with_config(conf);
    let dfs = DFSchema::new(vec![DFField::new(None, "key", DataType::Binary, false), DFField::new(None, "value", DataType::Binary, false)])?;
    let schema = dfs.clone();

    let ep = KafkaExecutionPlan {
        time_window: Duration::from_millis(200),
        topic: TOPIC_NAME.to_string(),
        batch_size: 5,
        conf: consumer_config("0", None),
    };
    dbg!(&ep);
    let lp = LogicalPlan::StreamingScan(StreamScan {
        topic_name: TOPIC_NAME.to_string(),
        source: Arc::new(ep),
        schema: Arc::new(dfs),
        batch_size: Some(5),
    });

    let key_exp = col("key").cast_to(&DataType::Utf8, &schema)?.alias("key");
    let value_exp = col("value").cast_to(&DataType::Utf8, &schema)?.alias("value");
    let df = DataFrameImpl::new(ec.state, &lp).select(vec![key_exp, value_exp.clone(), length(value_exp)])?;

    timeout(Duration::from_secs(10), async move {
        let b = df.execute_stream_partitioned().await.unwrap();
        for mut p in b {
            while let Some(Ok(batch)) = p.next().await {
                print_batches(&[batch]).unwrap();
            }
        }
    }).await.unwrap();


    Ok(())
}

