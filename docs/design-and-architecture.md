# SFTR Mapping Engine: Design and Architecture

What the system does, how it is put together, and why. For the line-by-line flow see [code-flow-and-classes.md](code-flow-and-classes.md); for the mapping rules see [rules-compilation.md](rules-compilation.md); for pictures see [c4-architecture.md](c4-architecture.md).

## 1. Purpose

Trade systems publish one JSON message per trade. The engine turns each message into one row of an **SFTR transaction report** (a CSV with one column per report field) and writes it to a file for that day. Some columns need data from a **Reference Data Service** (for example the reporting counterparty's LEI), so the engine calls it for each message.

| In | Out |
| --- | --- |
| Messages from a Kafka topic, or from a `.txt` (JSON lines) or `.csv` file | `sftr-report-yyyy-mm-dd.csv`, one row per good message |
| Reference-data answers over HTTP | `sftr-report-yyyy-mm-dd.csv.rejects`, one line per bad message |
| Mapping rules in JSON, constants in YAML | Kafka offsets committed once the row is safely on disk |

## 2. Design principles

| Principle | What it means in the code |
| --- | --- |
| **Rules are data, not code** | Each column is a JSON rule. Most columns need no Java at all. Logic that cannot be data is a named lambda. |
| **Fail at startup, not on trade one** | Rules are compiled once. Wrong names, missing constants, bad patterns and missing handlers stop the app with every problem listed. |
| **One message at a time, in order** | One consumer, one thread of processing, one lock in the pipeline. Rows appear in the order messages arrived. |
| **A bad message never stops the run** | It goes to the rejects file with a reason and the next message is processed. |
| **No regulatory data in logs or reasons** | Reject reasons and conversion errors never contain message content. `SftrTransactionReport` has no `toString`. |
| **Never commit ahead of the disk** | A Kafka offset is released only after its row (and every row before it) is written. |
| **Same flow for every door** | Kafka, a file and the REST test endpoint all call `RecordPipeline.handle()`. |

## 3. Architecture at a glance

One Spring Boot application (`sftr-mapping-engine.jar`), no database, no web server unless the test profile is on.

```mermaid
flowchart LR
    subgraph doors["Entry points (any one per run)"]
        K["KafkaIngestRoute<br/>Camel, default"]
        L["KafkaRecordListener<br/>Spring Kafka, PER_RECORD only"]
        F["FileInputRunner<br/>.txt or .csv"]
        R["TestMessageController<br/>rest or both profile"]
    end
    subgraph core["Processing"]
        P["RecordPipeline<br/>one message at a time"]
        T["MessageTransformer<br/>parse, enrich, map"]
        RD["RefDataClient"]
        RP["RecordProcessor<br/>compiled rules"]
    end
    subgraph out["Output"]
        S["ReportSink<br/>PerRecordSink or BatchingSink"]
        W["SftrCsvWriter"]
        J["RejectWriter"]
        O["OffsetCommitter"]
    end
    K --> P
    L --> P
    F --> P
    R --> P
    P --> T
    T --> RD
    T --> RP
    P --> S
    P --> J
    S --> W
    S -.->|row on disk| O
    O -.->|commit| K
```

| Layer | Classes | Responsibility |
| --- | --- | --- |
| Entry points | `KafkaIngestRoute`, `KafkaRecordListener`, `FileInputRunner`, `TestMessageController` | Get a message string and a source reference. Nothing else. |
| Coordination | `RecordPipeline`, `ReportDay` | Take the day once, call the transformer, hand the row to the sink or the reject to the reject writer, run the callback. |
| Transformation | `MessageTransformer`, `RefDataClient`, `RecordProcessor`, `MappingConfigLoader` | Message to row. Rules are compiled by the loader at startup. |
| Output | `ReportSink`, `PerRecordSink`, `BatchingSink`, `SftrCsvWriter`, `RejectWriter`, `OutputFileResolver` | Decide when and where rows and rejects are written. |
| Kafka commit | `OffsetCommitter` | Hold released offsets until the consumer thread commits them. |
| Configuration | `OutputProperties`, `SftrConstants`, `RefDataClient.Properties` | Typed, validated settings. |

## 4. The two phases

1. **Startup.** Spring creates the beans. `RecordProcessor` asks `MappingConfigLoader` to compile the rules (see [rules-compilation.md](rules-compilation.md)). `OutputProperties` validates the output settings. The sink is chosen from `write-mode`. If anything is wrong, the app stops here.
2. **Running.** Messages flow through `RecordPipeline`. Nothing is re-read or re-compiled.

## 5. Key decisions and why

| Decision | Reason | Alternative not taken |
| --- | --- | --- |
| Compile rules to functions at startup | Errors surface at deployment; no per-message parsing or reflection lookup | Interpret the JSON for every message |
| One consumer, one processing thread | Row order must equal arrival order, and the sink owns files | Thread per message or a worker pool (reorders rows, needs a reorder step) |
| Virtual threads **not** used for processing | Work is sequential, so they give no speed-up. Used only for the batch flusher | A virtual thread per message |
| Camel for Kafka ingestion | Requested; small route; `camel-kafka` supports manual commit | Keep Spring Kafka (still available, `PER_RECORD` only) |
| Commit offsets after the write | With batching, rows wait in memory; committing early would lose them on a crash | Commit on receipt |
| At-least-once delivery | A crash between write and commit re-reads the batch; duplicates were accepted | Exactly-once needs a keyed or idempotent output |
| Commit only on the consumer thread | The Kafka consumer is not thread-safe; committing from another thread throws (proved by a spike) | Commit from the flusher thread |
| Pick the day once per message | The row's date and its file cannot disagree around midnight | Read the clock separately in several places |
| New file when the first row of a day arrives | No helper thread, no empty files | A midnight timer that creates files |
| `Asia/Kolkata` day boundary, configurable | Stated requirement; `file-zone` makes it explicit | Server default zone |
| Failed batch write keeps the rows and retries | Dropping a batch silently would lose data | Log and skip |

## 6. Threads

| Thread | Does | Notes |
| --- | --- | --- |
| Camel Kafka consumer (one) | Polls, runs `RecordPipeline`, commits released offsets | A platform thread. The only thread that may commit. |
| `sftr-batch-flusher` (one virtual thread) | Writes a batch when its deadline passes | Only for `COUNT` and `INTERVAL`. Started on the first buffered row. |
| Web request thread | `POST /api/test-message` | Only with the `rest` or `both` profile. `RecordPipeline` serialises it with the consumer. |
| Main thread | Runs `FileInputRunner` in file mode | The app exits when it is done. |

One `ReentrantLock` in `RecordPipeline` serialises messages; one in `BatchingSink` guards its buffer and file, so the consumer and flusher never write together.

## 7. Write modes and files

| Mode | Rows reach the CSV | Offsets released |
| --- | --- | --- |
| `PER_RECORD` (default) | Immediately: open, append, close | Immediately |
| `COUNT` | When `batch-size` rows wait | After the batch |
| `INTERVAL` | When the oldest waiting row is `batch-interval` old | After the batch; committed on the next message |

A batch is also written on a **day change**, at **midnight** in `file-zone`, when `max-buffered-records` is reached, and at shutdown or end of file. A batch never mixes two days.

Files are `<name>-yyyy-mm-dd.csv` and `<name>-yyyy-mm-dd.csv.rejects` (`rollover: DAILY`, default). A day's file is created by its first row and gets its own header; a restart on the same day appends.

## 8. Failure handling

| Situation | What happens | Row | Offset |
| --- | --- | --- | --- |
| Empty message, not JSON, not a JSON object | Reject line `Empty message`, `Malformed JSON` or `Not a JSON object` | none | released after earlier rows are durable |
| Reference data 4xx | Reject `Reference data HTTP <code>` (never retried) | none | as above |
| Reference data 5xx or timeout | Retried (`max-attempts`, growing backoff), then reject `Reference data unavailable` | none | as above |
| A rule throws | Reject `Mapping failed for column <name>` | none | as above |
| Output file cannot be written, `PER_RECORD` | Error logged, message skipped | lost | not released by this message; the next message's commit moves past it |
| Batch cannot be written | Error logged, rows kept, retried after 5 seconds | kept in memory | not released until written |
| Buffer full and unwritable | `append` throws, new row refused | not accepted | not released |
| Crash or restart | Messages after the last committed offset are read again | duplicates possible | |
| Wrong rule or setting | App does not start; all problems listed | | |

## 9. Delivery guarantees

- **Order:** rows are written in arrival order for a single partition. With several partitions the order holds within each partition; use one partition for global order.
- **At least once:** a message is never lost once its offset is committed, and an uncommitted message is read again. In `INTERVAL` mode the commit waits for the next message, so the re-read window lasts until then.
- **No de-duplication:** the CSV has no key; a re-read message writes a second row.

## 10. Configuration map

| Area | Where | Main keys |
| --- | --- | --- |
| Which door | `sftr.ingest`, `sftr.kafka.enabled`, `sftr.input.file`, `sftr.rest.enabled` | `camel` (default) or `spring-kafka`; profiles `kafka`, `rest`, `both`, `file` |
| Output | `sftr.output.*` | `file`, `rejects-file`, `write-mode`, `batch-size`, `batch-interval`, `max-buffered-records`, `rollover`, `file-zone`, `file-date` |
| Reference data | `sftr.refdata.*` | `base-url`, `path`, timeouts, `max-attempts`, `backoff`, `request-fields` |
| Rules | `sftr.mapping.config` + `sftr-field-mappings.json` | one rule per column |
| Constants | `sftr.constants.*` | `values`, `lists`, `lookups` |
| Kafka | `spring.kafka.*`, `sftr.kafka.topic` | brokers, group id, offset reset |

Run modes are profiles: `kafka` (default, Kafka only, no web server), `rest` (test endpoint only, no Kafka), `both` (Kafka and the test endpoint) and `file` (Kafka off; run with `--sftr.input.file=<path>`). Use one of `kafka`, `rest`, `both`. See [running-locally.md](running-locally.md).

## 11. Extending

| To change | Do |
| --- | --- |
| Add or change a column | A rule, plus a constant or a lambda if needed ([rules-compilation.md](rules-compilation.md), section 10) |
| Add an input source | A component that calls `RecordPipeline.handle(json, sourceRef, whenDurable)` |
| Add a write mode | Implement `ReportSink` and select it in `ReportSinkConfig` |
| Change the reference-data call | `RefDataClient` and `sftr.refdata.request-fields` |

## 12. Known limits

- A single process: no clustering of the output file. Run one instance per output file.
- In `INTERVAL` mode with an idle topic, Kafka offsets stay uncommitted until the next message (duplicates after a restart, accepted).
- Pause and resume of the Kafka route when the output is unwritable is not implemented; the sink refuses new rows instead.
- The Camel route and Spring Boot 4 pairing is verified with Camel 4.23.0.

History of how the Camel route and write modes were designed: [design-camel-batch-writer.md](design-camel-batch-writer.md) and [c4-camel-batch-writer.md](c4-camel-batch-writer.md).
