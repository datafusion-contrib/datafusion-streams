use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::{DFSchema, Expr, FunctionRegistry, JoinType, LogicalPlan, Partitioning};
use datafusion::physical_plan::SendableRecordBatchStream;
use async_trait::async_trait;

pub struct StreamingDataFrame {
    inner_df: DataFrameImpl
}

#[async_trait]
impl DataFrame for StreamingDataFrame {
    fn select_columns(&self, columns: &[&str]) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn select(&self, expr: Vec<Expr>) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn filter(&self, expr: Expr) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn limit(&self, n: usize) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn union(&self, dataframe: Arc<dyn DataFrame>) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn distinct(&self) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn sort(&self, expr: Vec<Expr>) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn join(&self, right: Arc<dyn DataFrame>, join_type: JoinType, left_cols: &[&str], right_cols: &[&str]) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn repartition(&self, partitioning_scheme: Partitioning) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    async fn collect(&self) -> datafusion::error::Result<Vec<RecordBatch>> {
        todo!()
    }

    async fn show(&self) -> datafusion::error::Result<()> {
        todo!()
    }

    async fn show_limit(&self, n: usize) -> datafusion::error::Result<()> {
        todo!()
    }

    async fn execute_stream(&self) -> datafusion::error::Result<SendableRecordBatchStream> {
        todo!()
    }

    async fn collect_partitioned(&self) -> datafusion::error::Result<Vec<Vec<RecordBatch>>> {
        todo!()
    }

    async fn execute_stream_partitioned(&self) -> datafusion::error::Result<Vec<SendableRecordBatchStream>> {
        todo!()
    }

    fn schema(&self) -> &DFSchema {
        todo!()
    }

    fn to_logical_plan(&self) -> LogicalPlan {
        todo!()
    }

    fn explain(&self, verbose: bool, analyze: bool) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn registry(&self) -> Arc<dyn FunctionRegistry> {
        todo!()
    }

    fn intersect(&self, dataframe: Arc<dyn DataFrame>) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }

    fn except(&self, dataframe: Arc<dyn DataFrame>) -> datafusion::error::Result<Arc<dyn DataFrame>> {
        todo!()
    }
}
