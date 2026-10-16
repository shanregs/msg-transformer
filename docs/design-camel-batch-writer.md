# Design: Camel Kafka route and configurable CSV write modes

Branch: `camel-batch-writer` (created from `file-input`). Status: implemented on this branch (steps 0 to 7; step 8 is this documentation). Section 11 lists what was built. Decisions confirmed: duplicates after a crash are acceptable, exactly three write modes, newest Camel that supports Spring Boot 4.1 and Java 25, `FileInputRunner` stays as it is.
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
- When the buffer is flushed (rows written, file closed), the sink marks the highest pending offset per partition as committable. The commit itself runs on the Kafka consumer thread (section 6a): immediately after a count flush, and on the next message after a timer flush.
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
- **Offset commit is bound to the consumer thread.** The spike (section 6a) proved the flusher thread cannot commit. It only writes the CSV and marks offsets committable; the consumer thread commits them.

## 6a. Step 0 spike results

Run on 2026-10-16 against a real single-node Kafka in Docker, with Java 25.0.4 and Spring Boot 4.1.1. Code is on the scratch branch `spike/camel-boot41` (`CamelKafkaSpikeTest`, run with `-Dspike=true` and a broker on `localhost:19092`).

| Question | Result |
| --- | --- |
| Does Camel run on Spring Boot 4.1.1 and Java 25? | **Yes.** Camel **4.23.0** (newest release in Maven Central at the time) starts inside the Spring Boot context. All 78 existing tests still pass with the Camel starters on the classpath, and there is a single `kafka-clients` 4.2.1 (Spring's), so no version clash. |
| Does the Kafka route consume and manual commit work? | **Yes**, with `allowManualCommit=true`, `autoCommitEnable=false`, `consumersCount=1`. |
| Which thread runs the processor? | A **platform** thread named `Camel (camel-1) thread #1 - KafkaConsumer[...]`, not a virtual thread. Consistent with the recommendation: keep one consumer thread. |
| Can another thread call `KafkaManualCommit.commit()`? | **No.** From a virtual thread, and from the main thread, it throws `ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access`, and nothing is committed. |
| Does `commit()` work on the consumer thread? | **Yes.** Committing the last exchange of a batch from inside the route committed offset 4 for 4 messages. |
| Does Camel call the route when the topic is idle (`batching=true`, `batchingIntervalMs`)? | **No.** The route was called once, with the 4 messages, and not again during 6 seconds of idle. There is no idle hook on the consumer thread. |

What this changes in the design:

- `PER_RECORD` and `COUNT`: no change. Both flush on the consumer thread, so the commit follows immediately.
- `INTERVAL`: the timer flush writes the CSV on the flusher thread, but the offsets are committed on the consumer thread when the **next message arrives** (or never, if the topic stays idle). After a crash or restart the last batch is read again and written twice. You accepted duplicates, so this is within the agreed behaviour, but the duplicate window in `INTERVAL` mode is the whole idle period, not just the time between flush and commit.
- Graceful shutdown: the consumer thread is already closing, so the final flush does not commit. The last batch is re-read on the next start. Same trade-off.

Alternative not tested: Spring Kafka (already in the project) queues `Acknowledgment.acknowledge()` calls made from another thread and commits them on its own consumer thread, which would avoid the limitation. If a tight commit after the timer flush matters, that is a reason to keep `KafkaRecordListener` for ingestion and use Camel only elsewhere. Say if you want that checked before step 1.

## 6b. Daily files (step 3b)

A new report file and a new rejects file start each day. Decided with the requester: the day changes at midnight in **Asia/Kolkata**, and the rejects file also rolls over.

- `sftr.output.rollover: DAILY | NONE` (default `DAILY`) and `sftr.output.file-zone` (default `Asia/Kolkata`).
- `OutputFileResolver` adds the date before the extension: `sftr-report.csv` becomes `sftr-report-2026-10-16.csv`, and the default rejects file is `sftr-report-2026-10-16.csv.rejects`. A configured rejects file also gets the date (`rejects-2026-10-16.txt`).
- `ReportDay.today()` reads the clock **once per message** in `RecordPipeline.handle()`. That one date goes into the row (`fileDate`), to the sink and to the reject writer, so a row and its file cannot disagree at midnight.
- There is no helper thread: a day's file is created when its first row arrives (`openAppend` writes the header for a new or empty file), so quiet days leave no empty files. A restart mid-day appends to that day's file.
- `ReportSink.append(report, day)` takes the date. `PerRecordSink` opens the file for that day. `RejectWriter` closes the previous day's file and opens the new one on the first reject of a day.
- A fixed `sftr.output.file-date` replaces today's date in the rows **and** in the file names (useful for replays and tests).
- **For the batching sink (step 4):** a row belongs to the day it arrived, not the day the batch is written. When a row for a new day arrives while older-dated rows are buffered, the sink flushes the old batch first, so a batch never mixes two days. The flusher's deadline is the earlier of the batch interval and the next midnight in `file-zone`, so rows from 23:58 do not wait past midnight.

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

0. **Spike. Done**, see section 6a. Result: Camel 4.23.0 works on Spring Boot 4.1.1 and Java 25; the commit must run on the consumer thread. Pin `camel.version` and import `camel-spring-boot-bom` in step 5.
1. **Refactor, no behaviour change.** Extract `MessageTransformer` and `RejectWriter` from `RecordPipeline`. Introduce `ReportSink` with a first implementation that matches today's behaviour. `RecordPipelineTest`, `GoldenFileTest`, `FileInputRunnerTest` and `KafkaRecordListenerTest` must pass unchanged.
2. **`OutputProperties`.** Typed config with validation and tests for every mode and bad combination.
3. **`PerRecordSink`.** Open, write, close per message. Test that the header is written once and rows survive reopen.
3b. **Daily files. Done**, see section 6b.
4. **`BatchingSink`. Done.** Day-aware: flush on a day change and at midnight (section 6b). Count and timer triggers, flusher virtual thread, failed writes keep the rows and retry after 5 seconds, full buffer refuses the new row. Count trigger, then interval trigger. Inject a `Clock` and a scheduler so tests run without real waiting. Test: order preserved, flush on shutdown, retry after a write failure, cap behaviour.
5. **`KafkaIngestRoute` and `OffsetCommitter`. Done.** Camel route with one consumer and manual commit. `OffsetCommitter` keeps committable offsets from the flusher thread and commits them on the consumer thread (immediately after a count flush, on the next message after a timer flush). Tests with the Camel test support and a mock endpoint. One integration test with an embedded or containerised Kafka for the commit-after-flush rule.
6. **Wire `sftr.ingest`. Done.** Switch between the new route and the old listener so the change can be rolled back by config.
7. **Golden test across modes. Done** (`WriteModesGoldenTest`). The same input must give the same CSV rows in `PER_RECORD`, `COUNT` and `INTERVAL`.
8. **Docs.** Update `code-flow-and-classes.md` and the C4 file on this branch, and remove the old listener once the route is proven.

## 11. What was built

Added on `camel-batch-writer` (commits in order: refactor, `OutputProperties`, `PerRecordSink`, daily files, `BatchingSink`, Camel route).

| Piece | Behaviour |
| --- | --- |
| `sftr.ingest` | `camel` (default) uses `KafkaIngestRoute`. `spring-kafka` keeps the previous `KafkaRecordListener`, and refuses to start with a batch write mode because it acknowledges before the batch is written. `sftr.kafka.enabled=false` turns both off (the `file` profile). |
| `KafkaIngestRoute` | One Camel Kafka consumer (`consumersCount=1`, `maxPollRecords=1`, manual commit, no auto commit). Runs `RecordPipeline` on the consumer thread, then commits whatever `OffsetCommitter` holds. Stops its route before the sink is closed. |
| `RecordPipeline.handle(json, ref, whenDurable)` | The callback runs when the message's outcome is on disk: its row is in the CSV, or its reject line is written and every earlier row is in the CSV. |
| `ReportSink` | `append(report, day, whenDurable)` and `whenDurable(action)`. `PerRecordSink` runs callbacks right after the write. `BatchingSink` runs them, in order, after the batch is written. |
| `OffsetCommitter` | Any thread may release an offset; only the consumer thread commits. Keeps the highest offset per partition. |
| `FileInputRunner` | Calls `pipeline.flush()` at the end of the file, so batch modes write the last rows. |

Verified against a real Kafka (Docker, `apache/kafka`), see the two tests in `src/test/java/.../camel` (run with `-Dkafka.it=true` and a broker on `localhost:19092`):

- `COUNT` of 3: after 2 messages the CSV does not exist and no offset is committed; after the third, the CSV has 3 rows and offset 3 is committed.
- `INTERVAL` of 3 seconds: the timer writes the CSV without any new message, and the offsets are committed only when the next message arrives.

The normal `mvn test` skips those two and needs no broker.

## 10. Decisions and remaining checks

Decided:

1. Duplicate rows after a crash are acceptable (at least once).
2. Exactly three write modes: `PER_RECORD`, `INTERVAL`, `COUNT`.
3. Camel 4.20.0 or newer. Confirmed working: 4.23.0 on Spring Boot 4.1.1 and Java 25.
4. `FileInputRunner` stays as it is and flushes at end of file.

Still to confirm during the work:

- Whether to check Spring Kafka's cross-thread acknowledge before step 1 (end of section 6a).
- In `INTERVAL` mode, the last rows wait up to `batch-interval` after the topic goes idle. This follows from the mode and is accepted unless you say otherwise.
