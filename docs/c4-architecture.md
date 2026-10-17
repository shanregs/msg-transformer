# SFTR Mapping Engine: C4 Architecture Diagrams

Diagrams are Mermaid, so they render on GitHub and in most IDEs. See [code-flow-and-classes.md](code-flow-and-classes.md) for the written walkthrough and [design-camel-batch-writer.md](design-camel-batch-writer.md) for why it is built this way.

## Level 1: System context

The engine takes messages from Kafka or an input file, asks the Reference Data Service to enrich each one, and writes the SFTR CSV plus a rejects file for each day. It commits Kafka offsets only after the rows are on disk. One source is used per run; Kafka is the default and the `file` profile switches it off. A separate `publisher` mode of the same application sends generated messages to Kafka for testing.

```mermaid
flowchart LR
    op(["Operator<br/>runs the app"]) -->|starts and configures| engine
    op -->|optional: POST /api/test-message, rest or both profile| engine
    op -->|optional: publisher profile, rate or count or duration| engine
    op -->|optional: POST /api/kafka-message, rest or both profile| engine
    engine -.->|publisher mode and /api/kafka-message: test messages| kafka
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    engine -->|commit offsets after rows are on disk| kafka
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot + Apache Camel<br/>maps messages to rows"] <-->|enrich| ref["Reference Data Service<br/>HTTP, JSON"]
    engine -->|rows| out["Output files<br/>daily SFTR CSV and .rejects"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 2: Containers

One Spring Boot application (`sftr-mapping-engine.jar`) with one Camel Kafka consumer, defined in a YAML route. It reads its mapping config once at startup, then handles messages one at a time. The same jar can run as a message publisher (`publisher` profile).

```mermaid
flowchart LR
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    engine -->|commit offsets after the write| kafka
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    cfg["Config<br/>mapper/sftr-field-mappings.json<br/>routes/kafka-ingest.camel.yaml<br/>application.yml<br/>write-mode, batch-size, batch-interval,<br/>rollover, file-zone, publisher settings"] -.->|loaded at startup| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot application<br/>Camel YAML route, one consumer<br/>maps each message to a row<br/>writes per record or in batches<br/>one file per day<br/>optional publisher mode"]
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
        route["routes/kafka-ingest.camel.yaml<br/>Camel YAML route, one consumer<br/>autoStartup = sftr.kafka.enabled"]
        handler["KafkaMessageHandler<br/>route step 1 (bean)<br/>headers to source ref, calls the pipeline"]
        cproc["OffsetCommitProcessor<br/>route step 2 (process)<br/>commits released offsets"]
        legacy["KafkaRecordListener<br/>previous listener<br/>PER_RECORD only"]
        file["FileInputRunner<br/>reads .txt or .csv lines<br/>flushes at end of file"]
        rest["TestMessageController<br/>POST /api/test-message<br/>rest or both profile"]
        kpub["KafkaPublishController<br/>POST /api/kafka-message<br/>KafkaTemplate, rest or both profile"]
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
        cols["OutputColumns<br/>one constant per output column<br/>lambdas register against these"]
        consts["SftrConstants<br/>values, lists, lookups"]
        ptimer["MessagePublisherRoute<br/>timer to Kafka<br/>publisher profile only"]
        pub["MessagePublisher<br/>next: build message<br/>afterSend: count, apply limits, exit"]
        gen["TradeMessageGenerator<br/>message n from the configured lists"]
        pprops["PublisherProperties<br/>rate-per-minute, max-messages,<br/>duration, validated at startup"]
        pub_topic(["Kafka topic<br/>(outside the engine)"])

        route -->|step 1| handler
        handler -->|handle| pipe
        route -->|step 2| cproc
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
        cproc -->|commit released offsets| commit
        props -.->|selects sink and limits| sink
        loader -.->|compiled| proc
        fns -.->|find by name| loader
        cols -.->|names used for registration| fns
        kpub -.->|publish to the topic, outside the engine| pub_topic
        ptimer -->|each tick| pub
        pub -->|message n| gen
        pprops -.->|limits and period| ptimer
        pprops -.->|limits| pub
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
    class KafkaMessageHandler {
        <<bean called by the YAML route>>
        handle(exchange)
    }
    class OffsetCommitProcessor {
        <<process called by the YAML route>>
        process(exchange)
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
    KafkaMessageHandler ..> RecordPipeline
    KafkaMessageHandler ..> OffsetCommitter
    OffsetCommitProcessor ..> OffsetCommitter
```

### REST endpoints

```mermaid
classDiagram
    class TestMessageController {
        <<RestController>>
        post(body, flush) Result
    }
    class KafkaPublishController {
        <<RestController>>
        publish(body, topic, key) ResponseEntity
    }
    class KafkaTemplate {
        <<Spring Kafka>>
        send(topic, key, value) Future
    }
    class RecordPipeline {
        handle(json, sourceRef, whenDurable)
    }

    TestMessageController ..> RecordPipeline : runs the flow directly
    KafkaPublishController ..> KafkaTemplate : sends, waits for the acknowledgement
```

### Publisher and column names

```mermaid
classDiagram
    class MessagePublisherRoute {
        <<RouteBuilder>>
        timer period = 60000 / rate
        configure()
    }
    class MessagePublisher {
        <<component>>
        next(exchange)
        afterSend(exchange)
        sent() long
        finish() once
    }
    class TradeMessageGenerator {
        message(n) String
    }
    class PublisherProperties {
        <<record>>
        enabled, topic, ratePerMinute
        maxMessages, duration
        tradeTypes, legalEntities, counterpartyIds
        periodMillis()
    }
    class OutputColumns {
        <<constants>>
        CP_REP_CP, CP_CP_SIDE, ...
        all() List
    }
    class SftrMappingFunctions {
        <<component>>
        find(name) Optional
        register(OutputColumns constant, fn)
    }

    MessagePublisherRoute ..> MessagePublisher : bean steps
    MessagePublisherRoute ..> PublisherProperties : period
    MessagePublisher ..> TradeMessageGenerator
    MessagePublisher ..> PublisherProperties : limits
    TradeMessageGenerator ..> PublisherProperties : value lists
    SftrMappingFunctions ..> OutputColumns : registers by constant
```

## Dynamic view 1: `PER_RECORD`, one message

```mermaid
sequenceDiagram
    participant In as KafkaMessageHandler (step 1)
    participant P as RecordPipeline
    participant T as MessageTransformer
    participant R as RefDataClient
    participant M as RecordProcessor
    participant S as PerRecordSink
    participant C as OffsetCommitter
    participant OP as OffsetCommitProcessor (step 2)

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
    OP->>C: commitReleased()
```

## Dynamic view 2: `COUNT` mode, batch of 3

Offsets 10 and 11 stay uncommitted while their rows wait in the buffer. The commit happens only after the write.

```mermaid
sequenceDiagram
    participant K as Kafka
    participant R as KafkaMessageHandler
    participant P as RecordPipeline
    participant S as BatchingSink
    participant F as CSV file
    participant C as OffsetCommitter
    participant OP as OffsetCommitProcessor

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
    OP->>C: commitReleased()
    C->>K: commit offset 12
```

## Dynamic view 3: `INTERVAL` mode with a rejected message

The rejected message goes to the rejects file at once, but its offset waits for the batch so earlier buffered rows are not lost on a crash. The timer thread writes the CSV; the commit runs on the consumer thread when the next message arrives.

```mermaid
sequenceDiagram
    participant R as KafkaMessageHandler
    participant S as BatchingSink
    participant J as RejectWriter
    participant X as Flusher (timer)
    participant F as CSV file
    participant C as OffsetCommitter
    participant OP as OffsetCommitProcessor

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
    OP->>C: commitReleased(): commit offset 7 on the consumer thread
```

## Dynamic view 4: publisher run

`rate-per-minute: 60`, `max-messages: 3`. Each tick is one pass through the route. The third message reaches the count, so the run finishes and the application exits.

```mermaid
sequenceDiagram
    participant T as Camel timer
    participant M as MessagePublisher
    participant G as TradeMessageGenerator
    participant K as Kafka topic
    participant D as deadline timer
    participant A as Spring context

    T->>M: next (tick 1)
    Note over M: first message: start time set,<br/>deadline timer started if duration is set
    M->>G: message(1)
    G-->>M: JSON
    T->>K: send
    T->>M: afterSend (count 1)
    T->>M: next (tick 2)
    M->>G: message(2)
    T->>K: send
    T->>M: afterSend (count 2)
    T->>M: next (tick 3)
    M->>G: message(3)
    T->>K: send
    T->>M: afterSend (count 3 = max-messages)
    M->>M: finish() once, log summary
    M->>A: exit on a new thread
    Note over D,M: with duration set, the deadline timer<br/>calls finish() on time, even between ticks
```

## Dynamic view 5: REST to Kafka to CSV (`both` mode)

`POST /api/kafka-message` only puts the message on the topic. The consumer then handles it exactly like a message from any producer.

```mermaid
sequenceDiagram
    participant U as Caller (curl, Postman)
    participant KP as KafkaPublishController
    participant K as Kafka topic
    participant R as YAML route + KafkaMessageHandler
    participant P as RecordPipeline
    participant F as CSV file

    U->>KP: POST /api/kafka-message (body, topic?, key?)
    KP->>K: send, wait for the acknowledgement
    K-->>KP: partition, offset
    KP-->>U: 200 {topic, partition, offset}
    K->>R: message (consumer poll)
    R->>P: handle(json, ref, whenDurable)
    P->>F: row written (per the write mode)
    Note over KP,U: no broker or no answer in time: 503<br/>empty body: 400
```

## Dynamic view 6: how the Kafka route starts

```mermaid
sequenceDiagram
    participant S as Spring Boot
    participant C as Camel context
    participant Y as routes/kafka-ingest.camel.yaml
    participant B as beans (handler, processor)

    S->>B: create KafkaMessageHandler, OffsetCommitProcessor
    S->>C: start, routes-include-pattern classpath:routes/*.yaml
    C->>Y: load route sftr-kafka-ingest
    Y->>Y: resolve {{...}} placeholders from application.yml
    alt sftr.kafka.enabled = true (kafka, both)
        C->>C: autoStartup true: start the consumer
    else false (rest, file, publisher)
        C->>C: autoStartup false: route loaded, not started
    end
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

## Decision view: when does the publisher stop?

```mermaid
flowchart TD
    A["Tick: next()"] --> B{"already finished?"}
    B -->|yes| Z["stop this tick, send nothing"]
    B -->|no| C{"duration set and<br/>deadline passed?"}
    C -->|yes| F["finish: log summary, exit"]
    C -->|no| D["build message, send"]
    D --> E["afterSend: count + 1"]
    E --> G{"max-messages set and<br/>count reached it?"}
    G -->|yes| F
    G -->|no| H["wait for the next tick"]
    DL["deadline timer fires"] --> F
```
