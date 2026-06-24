use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct StreamLoadResponse {
    #[serde(alias = "TxnId", alias = "txn_id")]
    pub txn_id: Option<i64>,

    #[serde(alias = "Label", alias = "label")]
    pub label: Option<String>,

    #[serde(alias = "Status", alias = "status")]
    pub status: String,

    #[serde(alias = "Message", alias = "message")]
    pub message: Option<String>,

    #[serde(alias = "NumberTotalRows", alias = "number_total_rows")]
    pub number_total_rows: Option<i64>,

    #[serde(alias = "NumberLoadedRows", alias = "number_loaded_rows")]
    pub number_loaded_rows: Option<i64>,

    #[serde(alias = "NumberFilteredRows", alias = "number_filtered_rows")]
    pub number_filtered_rows: Option<i64>,

    #[serde(alias = "NumberUnselectedRows", alias = "number_unselected_rows")]
    pub number_unselected_rows: Option<i64>,

    #[serde(alias = "LoadBytes", alias = "load_bytes")]
    pub load_bytes: Option<i64>,

    #[serde(alias = "LoadTimeMs", alias = "load_time_ms")]
    pub load_time_ms: Option<i64>,

    #[serde(alias = "ErrorLogUrl", alias = "error_log_url")]
    pub error_log_url: Option<String>,

    #[serde(alias = "State", alias = "state")]
    pub state: Option<String>,

    #[serde(alias = "ExistingJobStatus", alias = "existing_job_status")]
    pub existing_job_status: Option<String>,

    #[serde(alias = "BeginTxnTimeMs", alias = "begin_txn_time_ms")]
    pub begin_txn_time_ms: Option<i64>,

    #[serde(alias = "StreamLoadPlanTimeMs", alias = "stream_load_plan_time_ms")]
    pub stream_load_plan_time_ms: Option<i64>,

    #[serde(alias = "ReadDataTimeMs", alias = "read_data_time_ms")]
    pub read_data_time_ms: Option<i64>,

    #[serde(alias = "WriteDataTimeMs", alias = "write_data_time_ms")]
    pub write_data_time_ms: Option<i64>,

    #[serde(alias = "CommitAndPublishTimeMs", alias = "commit_and_publish_time_ms")]
    pub commit_and_publish_time_ms: Option<i64>,
}
