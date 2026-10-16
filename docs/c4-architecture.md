# SFTR Mapping Engine: C4 Architecture Diagrams

Diagrams are Mermaid, so they render on GitHub and in most IDEs. See [code-flow-and-classes.md](code-flow-and-classes.md) for the written walkthrough and [design-camel-batch-writer.md](design-camel-batch-writer.md) for why it is built this way.

## Level 1: System context

The engine takes messages from Kafka or an input file, asks the Reference Data Service to enrich each one, and writes the SFTR CSV plus a rejects file for each day. It commits Kafka offsets only after the rows are on disk. One source is used per run; Kafka is the default and the `file` profile switches it off.

```mermaid
flowchart LR
    op(["Operator<br/>runs the app"]) -->|starts and configures| engine
    op -->|optional: POST /api/test-message, rest or both profile| engine
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    engine -->|commit offsets after rows are on disk| kafka
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot + Apache Camel<br/>maps messages to rows"] <-->|enrich| ref["Reference Data Service<br/>HTTP, JSON"]
    engine -->|rows| out["Output files<br/>daily SFTR CSV and .rejects"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 2: Containers

One Spring Boot application (`sftr-mapping-engine.jar`) with one Camel Kafka consumer. It reads its mapping config once at startup, then handles messages one at a time.

```mermaid
flowchart LR
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    engine -->|commit offsets after the write| kafka
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    cfg["Config<br/>sftr-field-mappings.json<br/>application.yml<br/>write-mode, batch-size, batch-interval,<br/>rollover, file-zone"] -.->|loaded at startup| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot application<br/>Camel route, one consumer<br/>maps each message to a row<br/>writes per record or in batches<br/>one file per day"]
    engine <-->|enrich| ref["Reference data<br/>HTTP service, JSON"]
    engine -->|rows| csv["SFTR CSV report<br/>sftr-report-yyyy-mm-dd.csv"]
    engine -->|rejects| rej["Rejects file<br/>source and reason, per day"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 3: Components

Solid arrows run per message; dashed arrows run at startup, on the timer, or by configuration.

```mermaid
flowchart TB
    subgraph engine["SFTR Mapping Engine (Spring Boot application)"]
        route["KafkaIngestRoute<br/>Camel route, one consumer<br/>manual offset commit"]
        legacy["KafkaRecordListener<br/>previous listener<br/>PER_RECORD only"]
        file["FileInputRunner<br/>reads .txt or .csv lines<br/>flushes at end of file"]
        rest["TestMessageController<br/>POST /api/test-message<br/>rest or both profile"]
        pipe["<b>RecordPipeline</b><br/>one message at a time<br/>day, transform, append or reject<br/>runs the durable callback"]
        day["ReportDay<br/>one clock reading per message<br/>midnight in Asia/Kolkata"]
        tf["MessageTransformer<br/>parse, enrich, map"]
        ref["RefDataClient<br/>HTTP POST with retries"]
        proc["RecordProcessor<br/>runs the active mappings"]
        sink{{"ReportSink<br/>append, whenDurable, flush, close"}}
        per["PerRecordSink<br/>open, write, close per message"]
        batch["BatchingSink<br/>buffer + lock<br/>flush by count, age, day change, midnight"]
        timer["Flusher<br/>one virtual thread<br/>deadline timer"]
        rej["RejectWriter<br/>appends to the day's rejects file"]
        csv["SftrCsvWriter<br/>openAppend, write rows"]
        files["OutputFileResolver<br/>daily file names"]
        commit["OffsetCommitter<br/>released offsets<br/>commits on the consumer thread"]
        props["OutputProperties<br/>write-mode, batch-size, batch-interval,<br/>rollover, file-zone, validated at startup"]
        loader["MappingConfigLoader<br/>reads and validates JSON<br/>compiles each row"]
        fns["SftrMappingFunctions<br/>named business lambdas<br/>+ RefDataExtractors, DateHelpers"]
        consts["SftrConstants<br/>values, lists, lookups"]

        route -->|handle| pipe
        legacy -->|handle| pipe
        file -->|handle| pipe
        rest -->|handle, then flush| pipe
        pipe -->|today| day
        pipe -->|transform| tf
        tf -->|fetch| ref
        tf -->|process| proc
        consts -->|context| proc
        pipe -->|row| sink
        pipe -->|reject| rej
        sink -.-> per
        sink -.-> batch
        timer -.->|flush when due| batch
        per --> csv
        batch --> csv
        files -.->|file for the day| per
        files -.->|file for the day| batch
        files -.->|file for the day| rej
        sink -->|row is on disk, release offset| commit
        route -->|commit released offsets| commit
        props -.->|selects sink and limits| sink
        loader -.->|compiled| proc
        fns -.->|find by name| loader
    end

    style pipe fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 4: Code

### Config compilation (startup)

At startup `MappingConfigLoader.load()` turns every `FieldMapping` into a `Column`, and every active one also into a `CompiledMapping` that wraps a `MappingFunction`. `RecordProcessor` keeps the resulting `CompiledConfig` and, for each message, creates an `SftrTransactionReport` and fills it by running the compiled mappings. `Column.read` pulls the values back out when the CSV row is written.

```mermaid
classDiagram
    class FieldMapping {
        <<record>>
        outputField, mappingType
        inputField, constantRef
        handler, status, formatter
    }
    class MappingConfigLoader {
        <<component>>
        load() CompiledConfig
        compile(row, field)
    }
    class RecordProcessor {
        <<component>>
        process(message, ctx)
        columnDefs()
    }
    class CompiledConfig {
        <<record>>
        columns, active, skipped
    }
    class CompiledMapping {
        <<record>>
        column, target, fn
        value(message, ctx)
        set(report, value)
    }
    class Column {
        <<record>>
        name, field
        read(report)
    }
    class MappingFunction {
        <<interface>>
        apply(message, ctx) Object
        text(node, field) String
    }
    class SftrTransactionReport {
        one field per CSV column
    }

    MappingConfigLoader ..> FieldMapping : reads
    RecordProcessor ..> MappingConfigLoader : load() at startup
    MappingConfigLoader ..> CompiledConfig : returns
    CompiledConfig o-- CompiledMapping : active
    CompiledConfig o-- Column : columns
    CompiledMapping --> MappingFunction : fn
    RecordProcessor ..> SftrTransactionReport : creates per message
    Column ..> SftrTransactionReport : reads its field
```

### Output (per message)

```mermaid
classDiagram
    class RecordPipeline {
        handle(json, sourceRef, whenDurable) Outcome
        flush()
    }
    class MessageTransformer {
        transform(json, day) SftrTransactionReport
    }
    class ReportSink {
        <<interface>>
        append(row, day, whenDurable)
        whenDurable(action)
        flush()
        close()
    }
    class PerRecordSink {
        open, write, close per row
    }
    class BatchingSink {
        buffer, pending callbacks, lock
        flushIfDue()
        retry after failed write
    }
    class RejectWriter {
        write(sourceRef, summary, day)
        summarize(error)
    }
    class OutputFileResolver {
        report(day) Path
        rejects(day) Path
    }
    class ReportDay {
        today() LocalDate
    }
    class OutputProperties {
        <<record>>
        file, rejectsFile, fileDate
        rollover, fileZone
        writeMode, batchSize
        batchInterval, maxBufferedRecords
    }
    class OffsetCommitter {
        release(partition, offset, commit)
        commitReleased()
    }
    class KafkaIngestRoute {
        <<RouteBuilder>>
        handle(exchange)
    }
    class SftrCsvWriter {
        openAppend(file, columns) Session
    }

    ReportSink <|.. PerRecordSink
    ReportSink <|.. BatchingSink
    RecordPipeline ..> MessageTransformer
    RecordPipeline ..> ReportSink
    RecordPipeline ..> RejectWriter
    RecordPipeline ..> ReportDay
    PerRecordSink ..> SftrCsvWriter
    BatchingSink ..> SftrCsvWriter
    PerRecordSink ..> OutputFileResolver
    BatchingSink ..> OutputFileResolver
    RejectWriter ..> OutputFileResolver
    OutputFileResolver ..> OutputProperties
    ReportDay ..> OutputProperties
    KafkaIngestRoute ..> RecordPipeline
    KafkaIngestRoute ..> OffsetCommitter
```

## Dynamic view 1: `PER_RECORD`, one message

```mermaid
sequenceDiagram
    participant In as KafkaIngestRoute
    participant P as RecordPipeline
    participant T as MessageTransformer
    participant R as RefDataClient
    participant M as RecordProcessor
    participant S as PerRecordSink
    participant C as OffsetCommitter

    In->>P: handle(json, ref, whenDurable)
    P->>T: transform(json, day)
    T->>R: fetch(message)
    R-->>T: ref data JSON
    T->>M: process(message, ctx)
    M-->>T: report object
    T-->>P: row
    P->>S: append(row, day, whenDurable)
    S->>S: open file, write, close
    S-->>C: whenDurable: release(offset)
    In->>C: commitReleased()
```

## Dynamic view 2: `COUNT` mode, batch of 3

Offsets 10 and 11 stay uncommitted while their rows wait in the buffer. The commit happens only after the write.

```mermaid
sequenceDiagram
    participant K as Kafka
    participant R as KafkaIngestRoute
    participant P as RecordPipeline
    participant S as BatchingSink
    participant F as CSV file
    participant C as OffsetCommitter

    K->>R: message, offset 10
    R->>P: handle
    P->>S: append(row, offset 10 callback)
    K->>R: message, offset 11
    R->>P: handle
    P->>S: append(row, offset 11 callback)
    K->>R: message, offset 12
    R->>P: handle
    P->>S: append(row, offset 12 callback)
    Note over S: buffer reached batch-size 3
    S->>F: open, write 3 rows, close
    S-->>C: callbacks release offsets 10 to 12
    R->>C: commitReleased()
    C->>K: commit offset 12
```

## Dynamic view 3: `INTERVAL` mode with a rejected message

The rejected message goes to the rejects file at once, but its offset waits for the batch so earlier buffered rows are not lost on a crash. The timer thread writes the CSV; the commit runs on the consumer thread when the next message arrives.

```mermaid
sequenceDiagram
    participant R as KafkaIngestRoute
    participant S as BatchingSink
    participant J as RejectWriter
    participant X as Flusher (timer)
    participant F as CSV file
    participant C as OffsetCommitter

    R->>S: append(row, offset 5 callback)
    Note over S,X: buffer was empty, timer starts
    R->>S: append(row, offset 6 callback)
    R->>J: write rejects line for offset 7
    R->>S: whenDurable(offset 7 callback)
    Note over S: offset 7 queued behind 5 and 6
    X->>S: batch-interval elapsed, flush
    S->>F: open, write rows 5 and 6, close
    S-->>C: callbacks release offsets 5 to 7
    Note over C,R: the flusher cannot commit
    R->>S: next message arrives, offset 8
    R->>C: commitReleased(): commit offset 7 on the consumer thread
```

## Decision view: when does a write happen?

```mermaid
flowchart TD
    A["Row arrives"] --> D0{"different day from<br/>the waiting rows?"}
    D0 -->|yes| H["flush the older rows first"] --> B
    D0 -->|no| B{"write-mode"}
    B -->|PER_RECORD| C["open file, write, close<br/>release offset"]
    B -->|COUNT| D["add to buffer"] --> E{"buffer size at least batch-size?"}
    B -->|INTERVAL| F["add to buffer<br/>start timer if buffer was empty"]
    E -->|yes| W["flush: write all rows, close,<br/>release offsets"]
    E -->|no| I["wait for next message"]
    F --> I
    T["Timer: oldest row is batch-interval old,<br/>or midnight in file-zone"] --> W
    M["Buffer reaches max-buffered-records"] --> W
    S["Shutdown or end of input file"] --> W
```
