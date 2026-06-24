use criterion::{Criterion, criterion_group, criterion_main};
use starrocks_stream_load::{
    DataFormat, StarRocksHttpClient, StreamLoadConfig, StreamLoadTableProperties, build_headers,
    redact_sensitive_info,
};

fn bench_build_headers(c: &mut Criterion) {
    let props = StreamLoadTableProperties::builder()
        .database("bench_db")
        .table("bench_tbl")
        .format(DataFormat::JSON)
        .column_separator(",")
        .row_delimiter("\n")
        .columns("id,name,value")
        .jsonpaths("$.id,$.name,$.value")
        .strip_outer_array(true)
        .ignore_json_size(true)
        .max_filter_ratio(0.1)
        .strict_mode(true)
        .timeout(1200)
        .compression("gzip")
        .skip_header(1)
        .where_clause("id > 0")
        .partitions("p1,p2")
        .negative(true)
        .timezone("Asia/Shanghai")
        .header("custom-x", "y")
        .build();

    c.bench_function("build_headers", |b| {
        b.iter(|| build_headers(&props).unwrap());
    });
}

fn bench_log_redaction(c: &mut Criterion) {
    let input = "http://admin:secret_password@127.0.0.1:8030/api/db/table/_stream_load failed with Authorization: Basic YWRtaW46c2VjcmV0X3Bhc3M= and password=secret_password";

    c.bench_function("redact_sensitive_info", |b| {
        b.iter(|| redact_sensitive_info(input));
    });
}

fn bench_get_available_fe(c: &mut Criterion) {
    let config = StreamLoadConfig::builder(
        vec![
            "127.0.0.1:8030".to_string(),
            "127.0.0.1:8031".to_string(),
            "127.0.0.1:8032".to_string(),
        ],
        "test_db".to_string(),
        "admin".to_string(),
    )
    .password("password")
    .build();

    let client = StarRocksHttpClient::new(config).unwrap();

    c.bench_function("get_available_fe", |b| {
        b.iter(|| client.get_available_fe());
    });
}

criterion_group!(
    benches,
    bench_build_headers,
    bench_log_redaction,
    bench_get_available_fe
);
criterion_main!(benches);
