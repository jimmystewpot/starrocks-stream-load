# StarRocks Stream Load Rust SDK: Developer Agent Documentation

This document provides developer agents and future engineers with context, architectural maps, internal design choices, and flow diagrams for the StarRocks Stream Load Rust SDK.

---

## Codebase Architecture & Modules

The SDK is organized cleanly into modular Rust files inside `src/`. Below is the dependency and module relation tree:

```mermaid
graph TD
    lib["src/lib.rs (Public Entry)"]
    config["src/config.rs (Builders & Props)"]
    types["src/types.rs (JSON Data Models)"]
    error["src/error.rs (Redaction & Errors)"]
    http["src/http.rs (Failover HTTP Client)"]
    manager["src/manager.rs (2PC Transaction Manager)"]

    lib --> config
    lib --> types
    lib --> error
    lib --> http
    lib --> manager

    manager --> http
    manager --> error
    manager --> types
    http --> config
    http --> error
```

### Module Responsibilities:
- **`src/config.rs`**: Builder types representing table properties and client settings. Silent allowances for typical builder candidates are structured at the crate root.
- **`src/types.rs`**: Strictly typed deserializers for StarRocks HTTP responses. Captures transaction metadata, loaded row counts, and error log locations.
- **`src/error.rs`**: Crate error aggregation. Handles sensitive string redacting (`redact_sensitive_info`) which automatically removes user authentication details.
- **`src/http.rs`**: Core network communication layer. Controls active node polling, round-robin frontend address rotation, and custom HTTP 307 interception.
- **`src/manager.rs`**: High-level transaction orchestration. Manages Direct Load (V1 API) and 2PC Transaction Load (V2 API).

---

## Custom Redirect Handling (HTTP 307 Interception)

### The Problem
During Stream Load, the Frontend (FE) node acts as a router. When receiving data, it responds with an HTTP `307 Temporary Redirect` specifying a target Backend (BE) node.
By default, standard HTTP clients like `reqwest`:
1. Strip all authentication and payload headers on redirect to prevent information leaks.
2. Strip streamable bodies or prevent multi-part body re-transmission.

### The Solution
We disable default automatic redirects inside `reqwest` and manually handle `307` responses in `src/http.rs`:

```mermaid
sequenceDiagram
    participant Client as SDK Manager
    participant FE as StarRocks Frontend (FE)
    participant BE as StarRocks Backend (BE)

    Client->>FE: POST /api/transaction/load with basic auth & payload
    FE-->>Client: HTTP 307 Temporary Redirect (Location: BE Address)
    Note over Client: Custom Interceptor captures Location & retains Auth Headers
    Client->>BE: POST Location URL with original Auth Headers & payload bytes
    BE-->>Client: HTTP 200 OK (Ingestion Status Payload)
```

By performing the redirect manually, we ensure that authorization headers are securely re-attached and body payloads are safely re-streamed to the target BE.

---

## Two-Phase Commit (2PC) Ingestion Pipeline Flow

The transactional loading flow enables exactly-once processing across multiple tables using a transaction label coordination scheme:

```mermaid
sequenceDiagram
    participant App as Rust Application
    participant Manager as StreamLoadManager
    participant Cluster as StarRocks Cluster

    App->>Manager: begin_transaction(label)
    Manager->>Cluster: POST /api/transaction/begin (Headers: label, db)
    Cluster-->>Manager: Txn ID
    Manager-->>App: Return Txn ID

    loop Write Data
        App->>Manager: load_transaction_data(label, db, table, seq, data)
        Manager->>Cluster: POST /api/transaction/load (Headers: db, table, label, txn_id)
        Cluster-->>Manager: Ingestion status
    end

    alt Commit Ingest
        App->>Manager: prepare_transaction(label)
        Manager->>Cluster: POST /api/transaction/prepare (Headers: label)
        Cluster-->>App: OK (Prepared)
        App->>Manager: commit_transaction(label)
        Manager->>Cluster: POST /api/transaction/commit (Headers: label)
        Cluster-->>App: OK (Committed)
    else Abort Ingest
        App->>Manager: rollback_transaction(label)
        Manager->>Cluster: POST /api/transaction/rollback (Headers: label)
        Cluster-->>App: OK (Aborted)
    end
```

---

## Key Performance & Safety Optimizations

1. **Infallible Header Construction**: Instead of unwrapping conversion results or using panicky constructs, we utilize checked HeaderValue parsing with fallback mapping (`and_then` / functional mapping).
2. **Minimizing Heap Allocations**: In our `build_headers` utility, we insert values conditionally and reference original strings rather than cloning.
3. **Rust Lifetimes and Borrowing**: We borrow properties (`&StreamLoadTableProperties`) instead of cloning to keep memory overhead to a minimum during serialization.
4. **Log Sanitization**: Log messages are passed through `redact_sensitive_info` which uses compiled regex patterns to replace raw credentials with `[REDACTED]` prior to formatting, keeping security leaks out of error payloads.
5. **Node Routing Failover**: Round-robin frontend URL tracking maintains a sequence indicator. When a node failover triggers, the manager increments this index modulo the length of the configured endpoint addresses.
