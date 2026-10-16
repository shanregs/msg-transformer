# Design: Camel Kafka route and configurable CSV write modes

Branch: `camel-batch-writer` (created from `file-input`). Status: proposal, no code yet. Decisions confirmed: duplicates after a crash are acceptable, exactly three write modes, newest Camel that supports Spring Boot 4.1 and Java 25, `FileInputRunner` stays as it is.
Diagrams: [c4-camel-batch-writer.md](c4-camel-batch-writer.md). Current design: [code-flow-and-classes.md](code-flow-and-classes.md).

## 1. What is being asked

1. Receive Kafka messages through an Apache Camel route instead of `KafkaRecordListener`.
2. Handle each message with `RecordPipeline` on a virtual thread.
3. Add a flag that controls how rows reach the CSV:
   - **Mode 1, per record:** open the file, write the row, close the file, for every message.
   - **Mode 2, every N minutes:** collect rows in a list and write them as one batch on a timer.
   - **Mode 3, every N records:** collect rows in a list and write them as one batch when the list reaches N.

## 2. Answers to the questions

**Is Camel OK for receiving the Kafka messages?** Yes. The route is a few lines (`from(kafka) -> process`). It adds a dependency. Use **Camel 4.20.0 or newer**: Camel 4.19 was the first release with Spring Boot 4 support (it dropped Spring Boot 3), and the 4.20.0 release notes list Java 25 as supported ([Camel 4.20.0 release notes](https://camel.apache.org/releases/release-4.20.0/)). I did not find a statement about Spring Boot 4.1 specifically, so the step 0 spike confirms it.

**Is one virtual thread for `RecordPipeline` OK?** It is safe, but it gives no speed-up. `RecordPipeline` owns one open output file and rows must come out in message order, so messages have to be handled one after another. One virtual thread and one platform thread do the same work in the same order. The thing that must not happen is *a new virtual thread per message*, because that handles messages concurrently, reorders rows and corrupts the single CSV writer.

Recommendation: keep exactly one consumer (`consumersCount=1`) and run the pipeline on the Camel consumer thread. If you still want a virtual thread, run that consumer loop on a single virtual thread (Java 25 no longer pins virtual threads on `synchronized`, so blocking HTTP and file I/O are fine). Virtual threads would only pay off if reference-data calls were made in parallel, which needs a reorder step before writing. That is out of scope here.

**Does batching change anything else?** Yes, and this is the main design point: **Kafka offsets must be committed only after the rows are on disk.** Today the offset is acknowledged right after each row is written. With batching, rows wait in memory, so committing the offset on receipt would lose them on a crash. Section 5 covers this.

## 3. Target design in one paragraph

Split today's `RecordPipeline` into two parts. A `MessageTransformer` does parse, reference-data fetch and mapping, and returns a report row or throws. A `ReportSink` decides when rows are written. `PerRecordSink` opens, writes and closes for each row. `BatchingSink` holds rows in a list and writes them when the count or the time is reached. A Camel `KafkaIngestRoute` calls the transformer and then the sink. `FileInputRunner` keeps working and calls the same two parts, so there is one code path for both inputs.

## 4. Configuration

```yaml
sftr:
  ingest: camel               # camel | spring-kafka (old listener, kept until the route is proven)
  output:
    file: ./data/sftr-report.csv
    rejects-file:
    write-mode: PER_RECORD    # PER_RECORD | INTERVAL | COUNT
    batch-size: 10            # used by COUNT
    batch-interval: 5m        # used by INTERVAL
    max-buffered-records: 1000   # safety cap, forces a flush
```

| write-mode | Behaviour | Needs |
| --- | --- | --- |
| `PER_RECORD` | Open, append, close per message. Offset committed right after. | nothing |
| `INTERVAL` | Buffer rows. Write them when the oldest buffered row is `batch-interval` old. | `batch-interval` |
| `COUNT` | Buffer rows. Write them when the buffer reaches `batch-size`. | `batch-size` |

Startup validation (same style as the mapping config): a missing or non-positive value for the chosen mode fails the start with a clear message. The old `sftr.output.flush-each-record` flag is superseded and is removed or ignored with a warning.

## 5. Batching and Kafka offsets

Rule: an offset is committed only after every row at or below it has been written to the CSV, or written to the rejects file.

- Every message ends as written or rejected. Both register a "pending commit" (topic, partition, offset) in the buffer.
- A rejected message is written to the rejects file at once, but its offset is **not** committed at once. If rows 5 and 6 are still buffered and 7 is rejected, committing 7 would lose 5 and 6 on a crash.
- When the buffer is flushed (rows written, file closed), the sink commits the highest pending offset per partition.
- In `PER_RECORD` mode the flush happens for every message, so this reduces to today's behaviour.
- Camel setup: `autoCommitEnable=false`, `allowManualCommit=true`, and the route stores the `KafkaManualCommit` header in the sink with each row. Option names must be checked against the Camel version chosen.

Delivery guarantee is **at least once**. If the app crashes after the CSV write but before the commit, the batch is read again and its rows appear twice in the CSV. The CSV has no key, so there is no de-duplication. This was confirmed as acceptable. Document it in the README so downstream readers know.

## 6. Threads and safety

| Thread | Does |
| --- | --- |
| Camel Kafka consumer (one) | Polls, runs `MessageTransformer`, calls `sink.append()`. Size-triggered flushes also run here. |
| Flusher (one virtual thread) | Only in `INTERVAL`. Sleeps until the oldest buffered row reaches its age, then flushes. Needed because the consumer thread is idle when no messages arrive. |

- One `ReentrantLock` in `BatchingSink` guards the buffer and the file, so the consumer and the flusher never write at the same time.
- The interval is measured from the oldest buffered row, not a fixed clock, so no row waits longer than `batch-interval`. The timer starts when the buffer goes from empty to non-empty.
- **Risk to check in a spike:** a Kafka consumer is not thread-safe. If the flusher thread must commit offsets, confirm that `KafkaManualCommit.commit()` is safe from another thread in the chosen Camel version. If it is not, the flusher only writes the CSV and hands the offsets to the consumer thread, which commits them on its next poll. The cost is a slightly longer duplicate window after a crash when the topic is idle.

## 7. Shutdown, errors, rejects

- **Shutdown order:** stop the Camel route (no new messages), flush the buffer, commit the offsets, close the files. Implemented as a `SmartLifecycle` bean ordered after the route. `FileInputRunner` flushes at end of file.
- **Reject path:** unchanged, one line per bad message with source reference and a summary that never contains message content.
- **CSV write failure on a batch:** keep the rows in the buffer, do not commit, log an error, and retry at the next trigger. If the buffer reaches `max-buffered-records`, pause the route until a write succeeds. Today an IOException is logged and the message is skipped, which would silently drop a whole batch, so this behaviour changes on purpose.
- **Header:** `SftrCsvWriter.openAppend` already writes the header only for a new or empty file, so per-record open and close works without change.
- **Timestamps:** `fileDate` still comes from `sftr.output.file-date` or today's date at mapping time.

## 8. Classes

| Class | Package | New / changed | Role |
| --- | --- | --- | --- |
| `KafkaIngestRoute` | `camel` | new | Camel `RouteBuilder`: Kafka endpoint, one consumer, manual commit, calls transformer and sink. |
| `MessageTransformer` | `service` | new (extracted) | Parse, `RefDataClient.fetch`, `RecordProcessor.process`. Returns a row or throws. |
| `ReportSink` | `output` | new interface | `append(row, commitHandle)`, `reject(sourceRef, summary, commitHandle)`, `flush()`, `close()`. |
| `PerRecordSink` | `output` | new | Open, write, close, commit per message. |
| `BatchingSink` | `output` | new | Buffer, lock, count and interval triggers, flusher thread, commit after flush. |
| `RejectWriter` | `output` | new (extracted) | Appends to the rejects file. |
| `OutputProperties` | `output` | new | `@ConfigurationProperties` record with validation for the keys in section 4. |
| `OffsetCommitter` | `camel` | new | Tracks the highest pending offset per partition, commits on flush. |
| `RecordPipeline` | `service` | changed | Becomes a thin facade (transformer + sink) used by `FileInputRunner`, or is removed. |
| `FileInputRunner` | `input` | changed | Calls the same facade, flushes at end of file. |
| `KafkaRecordListener` | `kafka` | kept, off by default | Active only if `sftr.ingest=spring-kafka`. Removed later. |
| `SftrCsvWriter` | `output` | unchanged | `openAppend` already does what `PerRecordSink` needs. |

## 9. Implementation plan

Each step is a separate commit, and the existing tests stay green after every one.

0. **Spike (half a day).** On a scratch branch, import `camel-spring-boot-bom` at 4.20.0 or the newest later release, and add `camel-spring-boot-starter` and `camel-kafka-starter`. Confirm it starts on Spring Boot 4.1.1 and Java 25, and works with the `tools.jackson` (Jackson 3) setup already in use. Pin the version in `pom.xml` as a property. Confirm the manual-commit threading question in section 6. **Stop and re-plan if this fails**; the fallback is the Kafka client directly, with the same sink design.
1. **Refactor, no behaviour change.** Extract `MessageTransformer` and `RejectWriter` from `RecordPipeline`. Introduce `ReportSink` with a first implementation that matches today's behaviour. `RecordPipelineTest`, `GoldenFileTest`, `FileInputRunnerTest` and `KafkaRecordListenerTest` must pass unchanged.
2. **`OutputProperties`.** Typed config with validation and tests for every mode and bad combination.
3. **`PerRecordSink`.** Open, write, close per message. Test that the header is written once and rows survive reopen.
4. **`BatchingSink`.** Count trigger, then interval trigger. Inject a `Clock` and a scheduler so tests run without real waiting. Test: order preserved, flush on shutdown, retry after a write failure, cap behaviour.
5. **`KafkaIngestRoute` and `OffsetCommitter`.** Camel route with one consumer and manual commit. Tests with the Camel test support and a mock endpoint. One integration test with an embedded or containerised Kafka for the commit-after-flush rule.
6. **Wire `sftr.ingest`.** Switch between the new route and the old listener so the change can be rolled back by config.
7. **Golden test across modes.** The same input must give the same CSV rows in `PER_RECORD`, `COUNT` and `INTERVAL`.
8. **Docs.** Update `code-flow-and-classes.md` and the C4 file on this branch, and remove the old listener once the route is proven.

## 10. Decisions and remaining checks

Decided:

1. Duplicate rows after a crash are acceptable (at least once).
2. Exactly three write modes: `PER_RECORD`, `INTERVAL`, `COUNT`.
3. Camel 4.20.0 or newer, whichever is the newest that starts on Spring Boot 4.1 and Java 25.
4. `FileInputRunner` stays as it is and flushes at end of file.

Still to confirm during the work:

- The step 0 spike result for Spring Boot 4.1.
- Whether `KafkaManualCommit.commit()` is safe from the flusher thread (section 6).
- In `INTERVAL` mode, the last rows wait up to `batch-interval` after the topic goes idle. This follows from the mode and is accepted unless you say otherwise.
