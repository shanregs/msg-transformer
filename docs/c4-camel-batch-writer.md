# C4 Diagrams: Camel route and configurable CSV write modes

Target design for branch `camel-batch-writer`. Written design: [design-camel-batch-writer.md](design-camel-batch-writer.md). Diagrams are Mermaid.

## Level 1: System context

Unchanged from today: the same neighbours. Only the way the engine receives Kafka messages and writes the CSV changes inside it.

```mermaid
flowchart LR
    op(["Operator<br/>runs the app"]) -->|starts and configures| engine
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot + Apache Camel"] <-->|enrich| ref["Reference Data Service<br/>HTTP, JSON"]
    engine -->|rows| out["Output files<br/>SFTR CSV and .rejects"]
    engine -->|commit offsets after rows are on disk| kafka

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 2: Containers

Still one application. Two things are new inside it: the Camel runtime that consumes Kafka, and a write mode setting that decides when rows are written. The new arrow back to Kafka is the offset commit, which now waits until the CSV write.

```mermaid
flowchart LR
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    engine -->|commit offsets after flush| kafka
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    cfg["Config<br/>sftr-field-mappings.json<br/>application.yml<br/>write-mode, batch-size, batch-interval"] -.->|loaded at startup| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot application<br/>Camel route, one consumer<br/>maps each message to a row<br/>writes per record or in batches"]
    engine <-->|enrich| ref["Reference data<br/>HTTP service, JSON"]
    engine -->|rows| csv["SFTR CSV report<br/>per record or per batch"]
    engine -->|rejects| rej["Rejects file<br/>source and reason"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 3: Components

New components are marked **new**. `RecordPipeline` is split: `MessageTransformer` does the mapping work and `ReportSink` decides when rows are written. Solid arrows run per message; dashed arrows run at startup or on the timer.

```mermaid
flowchart TB
    subgraph engine["SFTR Mapping Engine (Spring Boot application)"]
        route["<b>KafkaIngestRoute</b> (new)<br/>Camel route, one consumer<br/>manual offset commit"]
        file["FileInputRunner<br/>reads .txt or .csv lines<br/>flushes at end of file"]
        tf["<b>MessageTransformer</b> (new, extracted)<br/>parse, enrich, map"]
        ref["RefDataClient<br/>HTTP POST with retries"]
        proc["RecordProcessor<br/>runs the active mappings"]
        sink{{"<b>ReportSink</b> (new interface)<br/>append, reject, flush, close"}}
        per["PerRecordSink (new)<br/>open, write, close<br/>per message"]
        batch["BatchingSink (new)<br/>buffer + lock<br/>flush by count or age"]
        timer["Flusher (new)<br/>one virtual thread<br/>timer for the age trigger"]
        rej["RejectWriter (new, extracted)<br/>appends to the rejects file"]
        csv["SftrCsvWriter<br/>openAppend, write rows"]
        commit["OffsetCommitter (new)<br/>highest pending offset<br/>per partition"]
        props["OutputProperties (new)<br/>write-mode, batch-size,<br/>batch-interval, validated at startup"]

        route -->|message| tf
        file -->|message| tf
        tf -->|fetch| ref
        tf -->|process| proc
        route -->|row or reject| sink
        file -->|row or reject| sink
        sink -.-> per
        sink -.-> batch
        timer -.->|flush when oldest row is old enough| batch
        per --> csv
        batch --> csv
        per --> rej
        batch --> rej
        batch -->|rows written, release offsets| commit
        per -->|row written, release offset| commit
        commit -->|commit| route
        props -.->|selects sink and limits| sink
    end

    style sink fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 4: Code

The sink hierarchy and the objects that carry a row together with the Kafka offset it came from.

```mermaid
classDiagram
    class ReportSink {
        <<interface>>
        append(row, commitHandle)
        reject(sourceRef, summary, commitHandle)
        flush()
        close()
    }
    class PerRecordSink {
        append: open, write, close, commit
    }
    class BatchingSink {
        buffer : rows plus commit handles
        lock : ReentrantLock
        append: add, flush if count reached
        flush: write all, close, commit
        scheduleAgeFlush()
    }
    class OutputProperties {
        <<record>>
        file, rejectsFile
        writeMode
        batchSize
        batchInterval
        maxBufferedRecords
    }
    class WriteMode {
        <<enum>>
        PER_RECORD
        INTERVAL
        COUNT
    }
    class OffsetCommitter {
        register(commitHandle)
        commitUpTo()
    }
    class MessageTransformer {
        transform(json) SftrTransactionReport
    }
    class KafkaIngestRoute {
        <<RouteBuilder>>
        configure()
    }
    class SftrCsvWriter {
        openAppend(file, columns) Session
    }
    class RejectWriter {
        write(sourceRef, summary)
    }

    ReportSink <|.. PerRecordSink
    ReportSink <|.. BatchingSink
    OutputProperties --> WriteMode
    PerRecordSink ..> SftrCsvWriter : opens per row
    BatchingSink ..> SftrCsvWriter : opens per batch
    PerRecordSink ..> RejectWriter
    BatchingSink ..> RejectWriter
    PerRecordSink ..> OffsetCommitter
    BatchingSink ..> OffsetCommitter
    KafkaIngestRoute ..> MessageTransformer : transform
    KafkaIngestRoute ..> ReportSink : append or reject
    OutputProperties ..> ReportSink : chooses implementation
```

## Dynamic view 1: `COUNT` mode, batch of 3

Offsets 10 and 11 stay uncommitted while their rows wait in the buffer. The commit happens only after the write.

```mermaid
sequenceDiagram
    participant K as Kafka
    participant R as KafkaIngestRoute
    participant T as MessageTransformer
    participant S as BatchingSink
    participant F as CSV file
    participant C as OffsetCommitter

    K->>R: message, offset 10
    R->>T: transform
    T-->>R: row
    R->>S: append(row, offset 10)
    K->>R: message, offset 11
    R->>T: transform
    T-->>R: row
    R->>S: append(row, offset 11)
    K->>R: message, offset 12
    R->>T: transform
    T-->>R: row
    R->>S: append(row, offset 12)
    Note over S: buffer reached batch-size 3
    S->>F: open, write 3 rows, close
    S->>C: release offsets 10 to 12
    C->>K: commit offset 12
```

## Dynamic view 2: `INTERVAL` mode with a rejected message

The rejected message goes to the rejects file at once, but its offset waits for the next flush so earlier buffered rows are not lost on a crash.

```mermaid
sequenceDiagram
    participant R as KafkaIngestRoute
    participant S as BatchingSink
    participant J as RejectWriter
    participant X as Flusher (timer)
    participant F as CSV file
    participant C as OffsetCommitter

    R->>S: append(row, offset 5)
    Note over S,X: buffer was empty, timer starts
    R->>S: append(row, offset 6)
    R->>S: reject(offset 7, Malformed JSON)
    S->>J: write rejects line
    Note over S: offset 7 queued behind 5 and 6
    X->>S: batch-interval elapsed, flush
    S->>F: open, write rows 5 and 6, close
    S->>C: release offsets 5 to 7
    C-->>R: commit offset 7
```

## Decision view: when does a write happen?

```mermaid
flowchart TD
    A["Row or reject arrives"] --> B{"write-mode"}
    B -->|PER_RECORD| C["open file, write, close<br/>commit offset"]
    B -->|COUNT| D["add to buffer"] --> E{"buffer size at least batch-size?"}
    B -->|INTERVAL| F["add to buffer<br/>start timer if buffer was empty"]
    E -->|yes| H["flush: write all rows, close, commit"]
    E -->|no| I["wait for next message"]
    F --> I
    T["Timer fires<br/>oldest row is batch-interval old"] --> H
    S["Shutdown"] --> H
    M["Buffer reaches max-buffered-records"] --> H
```
