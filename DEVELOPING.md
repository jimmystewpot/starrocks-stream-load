# Developing StarRocks Stream Load Rust SDK

This document describes how to set up the development environment, execute tests, run benchmarks, and contribute to the SDK.

## Prerequisite Tools
- Rust toolchain (stable edition recommended)
- Docker (for end-to-end integration tests)
- MySQL CLI client (optional, for inspecting data manually)

## Running Unit and Mock Integration Tests
The unit tests and wiremock-based integration tests do not require a live StarRocks instance:
```bash
cargo test
```

## Running End-to-End (E2E) Integration Tests
The SDK includes E2E integration tests that validate the library against a real StarRocks 4.0 cluster.

### 1. Automated Script (Recommended)
You can run the entire lifecycle (spin up Docker container, initialize schema, execute E2E tests, and clean up) with the runner script:
```bash
./tests/run_e2e.sh
```

### 2. Manual Testing Workflow
If you wish to keep the StarRocks container running for debugging or iterative test development:

#### Start the StarRocks Docker container:
```bash
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd --name quickstart starrocks/allin1-ubuntu
```

#### Wait for StarRocks to start up and be healthy.
You can monitor container logs or test connection using the MySQL protocol:
```bash
docker exec -it quickstart mysql -P 9030 -h 127.0.0.1 -u root -e "SELECT 1"
```

#### Create the Database and Tables:
Initialize the test schema using the provided SQL script:
```bash
docker exec -i quickstart mysql -P 9030 -h 127.0.0.1 -u root < test_data/test_data.sql
```

#### Run the E2E tests:
Configure the `STARROCKS_E2E` environment variable to signal the test runner to execute the E2E tests:
```bash
STARROCKS_E2E=1 cargo test --test e2e_tests
```

#### Clean up:
Stop and remove the container once finished:
```bash
docker rm -f quickstart
```

## Code Quality Standards
Always ensure that the code is formatted and meets the project clippy guidelines before submitting a pull request:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

## Benchmarks
Run micro-benchmarks to ensure changes do not introduce performance regressions:
```bash
cargo bench
```
