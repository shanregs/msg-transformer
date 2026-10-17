# SFTR Mapping Engine: C4 Architecture Diagrams

Diagrams are Mermaid, so they render on GitHub and in most IDEs. See [code-flow-and-classes.md](code-flow-and-classes.md) for the written walkthrough and [design-and-architecture.md](design-and-architecture.md) for why it is built this way.

## Level 1: System context

The engine reads trades from a database table that another application fills, asks the Reference Data Service to enrich each one, and writes the SFTR CSV for each day. An operator starts a run with one REST call. Failed trades are logged; there is no rejects file.

```mermaid
flowchart LR
    op(["Operator<br/>Swagger UI, curl or Postman"]) -->|"POST /api/trades/process"| engine
    other["Trade-capture application<br/>writes the trade table"] -->|"rows: id, created_by, created_on,<br/>event, data_json"| db[("PostgreSQL<br/>database sftr")]
    db -->|"RECEIVED rows, read only"| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot<br/>maps trades to rows"] <-->|enrich| ref["Reference Data Service<br/>HTTP, JSON"]
    engine -->|rows| out["Output file<br/>daily SFTR CSV"]
    engine -.->|failed trades| log["Application log"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 2: Containers

One Spring Boot application (`sftr-mapping-engine.jar`) with a web server on port 9090. It reads its mapping config once at startup, then handles one run at a time.

```mermaid
flowchart LR
    op(["Operator"]) -->|"REST + Swagger UI"| engine
    db[("PostgreSQL<br/>table trade")] -->|"JDBC, read only"| engine
    cfg["Config<br/>mapper/sftr-field-mappings.json<br/>application.yml<br/>database, write-mode, batch-size,<br/>rollover, file-zone"] -.->|loaded at startup| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot application<br/>reads RECEIVED trades in id order<br/>maps each to a row<br/>writes in batches, one file per day"]
    engine <-->|enrich| ref["Reference data<br/>HTTP service, JSON"]
    engine -->|rows| csv["SFTR CSV report<br/>sftr-report-yyyy-mm-dd.csv"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 3: Components

Solid arrows run per record; dashed arrows run at startup, on the timer, or by configuration.

```mermaid
flowchart TB
    subgraph engine["SFTR Mapping Engine (Spring Boot application)"]
        tc["TradeController<br/>POST /api/trades/process<br/>Swagger annotated"]
        mc["TestMessageController<br/>POST /api/test-message"]
        fetch["<b>TradeFetchService</b><br/>pages through RECEIVED rows<br/>counts, logs failures, flushes<br/>one run at a time"]
        repo["TradeRepository<br/>Spring Data JPA, read only<br/>keyset query: id greater than last, limit"]
        ent["Trade (entity)<br/>id, created_by, created_on,<br/>event, data_json"]
        dbp["DbProperties<br/>event, page-size"]
        pipe["<b>RecordPipeline</b><br/>one record at a time<br/>day, transform, append<br/>throws RecordProcessingException"]
        day["ReportDay<br/>one clock reading per record<br/>midnight in Asia/Kolkata"]
        tf["MessageTransformer<br/>parse, enrich, map"]
        ref["RefDataClient<br/>HTTP POST with retries"]
        proc["RecordProcessor<br/>runs the active mappings"]
        sink{{"ReportSink<br/>append, flush, close"}}
        per["PerRecordSink<br/>open, write, close per record"]
        batch["BatchingSink<br/>buffer + lock<br/>flush by count, age, day change, midnight"]
        timer["Flusher<br/>one virtual thread<br/>deadline timer"]
        csv["SftrCsvWriter<br/>openAppend, write rows<br/>knows the columns"]
        files["OutputFileResolver<br/>daily file names"]
        props["OutputProperties<br/>write-mode, batch-size, batch-interval,<br/>rollover, file-zone, validated at startup"]
        loader["MappingConfigLoader<br/>reads and validates JSON<br/>compiles each row"]
        cfgbean["CompiledConfig (bean)<br/>the mapping file compiled once<br/>columns + active mappings"]
        fns["SftrMappingFunctions<br/>named business lambdas<br/>+ RefDataExtractors, DateHelpers"]
        cols["OutputColumns<br/>one constant per output column<br/>lambdas register against these"]
        consts["SftrConstants<br/>values, lists, lookups"]
        api["OpenApiConfig + springdoc<br/>/swagger-ui.html, /v3/api-docs"]

        tc --> fetch
        fetch --> repo
        repo --> ent
        dbp -.->|event, page size| fetch
        fetch -->|"data_json, trade:id"| pipe
        mc -->|"body, rest-n"| pipe
        pipe -->|today| day
        pipe -->|transform| tf
        tf -->|fetch| ref
        tf -->|process| proc
        consts -->|context| proc
        pipe -->|row| sink
        sink -.-> per
        sink -.-> batch
        timer -.->|flush when due| batch
        per --> csv
        batch --> csv
        files -.->|file for the day| per
        files -.->|file for the day| batch
        props -.->|selects sink and limits| sink
        loader -.->|compiled once| cfgbean
        cfgbean -.->|active mappings| proc
        cfgbean -.->|columns, CSV order| csv
        fns -.->|find by name| loader
        cols -.->|names used for registration| fns
        api -.->|documents| tc
        api -.->|documents| mc
    end
    pg[("PostgreSQL")] --> repo

    style pipe fill:#dbeafe,stroke:#2563eb,stroke-width:2px
    style fetch fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 4: Code

### Config compilation (startup)

At startup `MappingConfigLoader.load()` turns every `FieldMapping` into a `Column`, and every active one also into a `CompiledMapping` that wraps a `MappingFunction`. `MappingConfiguration` exposes the result as one `CompiledConfig` bean. `RecordProcessor` takes the active mappings from it and, for each record, creates an `SftrTransactionReport` and fills it by running the compiled mappings. `SftrCsvWriter` takes the `Column` list from the same bean, so the sinks never need the rules engine; `Column.read` pulls the values back out when the CSV row is written.

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
    class MappingConfiguration {
        <<configuration>>
        compiledConfig(loader) CompiledConfig
    }
    class RecordProcessor {
        <<component>>
        process(message, ctx)
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
    MappingConfiguration ..> MappingConfigLoader : load() once at startup
    MappingConfiguration ..> CompiledConfig : exposes as a bean
    RecordProcessor ..> CompiledConfig : active mappings
    SftrCsvWriter ..> CompiledConfig : columns
    CompiledConfig o-- CompiledMapping : active
    CompiledConfig o-- Column : columns
    CompiledMapping --> MappingFunction : fn
    RecordProcessor ..> SftrTransactionReport : creates per record
    Column ..> SftrTransactionReport : reads its field
```

### Database input

```mermaid
classDiagram
    class TradeController {
        <<RestController>>
        process(limit) ResponseEntity
    }
    class TradeFetchService {
        <<service>>
        process(limit) Summary
        one run at a time
    }
    class TradeRepository {
        <<Spring Data, read only>>
        findByEventAndIdGreaterThanOrderByIdAsc(event, afterId, limit) List
    }
    class Trade {
        <<entity, immutable>>
        id, createdBy, createdOn
        event, dataJson
    }
    class TradeEvent {
        <<enum>>
        RECEIVED
        PROCESSED
        PUBLISHED
    }
    class DbProperties {
        <<record>>
        event, pageSize
    }
    class RecordPipeline {
        handle(json, sourceRef)
        flush()
    }
    class RecordProcessingException {
        sourceRef()
        short reason, no content
    }

    TradeController ..> TradeFetchService
    TradeFetchService ..> TradeRepository
    TradeFetchService ..> DbProperties
    TradeFetchService ..> RecordPipeline
    TradeRepository ..> Trade
    Trade --> TradeEvent
    RecordPipeline ..> RecordProcessingException : throws
```

### Output

```mermaid
classDiagram
    class RecordPipeline {
        handle(json, sourceRef)
        flush()
    }
    class MessageTransformer {
        transform(json, day) SftrTransactionReport
    }
    class ReportSink {
        <<interface>>
        append(row, day)
        flush()
        close()
    }
    class PerRecordSink {
        open, write, close per row
    }
    class BatchingSink {
        buffer, lock
        flushIfDue()
        retry after failed write
    }
    class OutputFileResolver {
        report(day) Path
    }
    class ReportDay {
        today() LocalDate
    }
    class OutputProperties {
        <<record>>
        file, fileDate
        rollover, fileZone
        writeMode, batchSize
        batchInterval, maxBufferedRecords
    }
    class SftrCsvWriter {
        columns from CompiledConfig
        openAppend(file) Session
    }

    ReportSink <|.. PerRecordSink
    ReportSink <|.. BatchingSink
    RecordPipeline ..> MessageTransformer
    RecordPipeline ..> ReportSink
    RecordPipeline ..> ReportDay
    PerRecordSink ..> SftrCsvWriter
    BatchingSink ..> SftrCsvWriter
    PerRecordSink ..> OutputFileResolver
    BatchingSink ..> OutputFileResolver
    OutputFileResolver ..> OutputProperties
    ReportDay ..> OutputProperties
```

### REST endpoints and column names

```mermaid
classDiagram
    class TradeController {
        <<RestController>>
        process(limit)
    }
    class TestMessageController {
        <<RestController>>
        post(body, flush) Result
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

    SftrMappingFunctions ..> OutputColumns : registers by constant
    TradeController ..> TradeFetchService : whole table
    TestMessageController ..> RecordPipeline : one message
```

## Dynamic view 1: a table run in `COUNT` mode

Batch size 3 for the picture (the default is 100). Rows 1 to 3 fill the batch and are written together. Rows 4 and 5 fail and are logged. The final flush finds nothing waiting.

```mermaid
sequenceDiagram
    participant U as Caller
    participant TC as TradeController
    participant S as TradeFetchService
    participant R as TradeRepository
    participant P as RecordPipeline
    participant K as BatchingSink
    participant F as CSV file

    U->>TC: POST /api/trades/process
    TC->>S: process(null)
    S->>R: RECEIVED, id greater than lowest, page size
    R-->>S: rows 1, 2, 3, 4, 5
    S->>P: handle(json of row 1, trade:1)
    P->>K: append(row)
    S->>P: handle(row 2)
    P->>K: append(row)
    S->>P: handle(row 3)
    P->>K: append(row)
    Note over K: batch-size 3 reached
    K->>F: open, write 3 rows, close
    S->>P: handle(row 4)
    P-->>S: RecordProcessingException (Reference data HTTP 400)
    Note over S: log: Trade 4 failed, continue
    S->>P: handle(row 5)
    P-->>S: RecordProcessingException (Not a JSON object)
    S->>P: flush()
    P->>K: flush() (nothing waiting)
    S-->>TC: Summary(read 5, written 3, failed 2)
    TC-->>U: 200 + JSON
```

## Dynamic view 2: `PER_RECORD`, one record

```mermaid
sequenceDiagram
    participant In as TradeFetchService
    participant P as RecordPipeline
    participant T as MessageTransformer
    participant R as RefDataClient
    participant M as RecordProcessor
    participant S as PerRecordSink

    In->>P: handle(json, trade:42)
    P->>T: transform(json, day)
    T->>R: fetch(message)
    R-->>T: ref data JSON
    T->>M: process(message, ctx)
    M-->>T: report object
    T-->>P: row
    P->>S: append(row, day)
    S->>S: open file, write, close
```

## Dynamic view 3: paging through the table

Page size 2, `RECEIVED` rows with ids 1, 3, 4, 6, 9. The other application inserts id 10 while the run is busy with the second page, so the third query already sees it.

```mermaid
sequenceDiagram
    participant S as TradeFetchService
    participant R as TradeRepository
    participant DB as trade table

    S->>R: after = lowest id, limit 2
    R->>DB: event = RECEIVED and id greater than after, order by id, limit 2
    DB-->>S: ids 1, 3
    Note over S: process 1, 3; after = 3
    S->>R: after = 3, limit 2
    DB-->>S: ids 4, 6
    Note over S: process 4, 6; after = 6
    S->>R: after = 6, limit 2
    DB-->>S: id 9 (and 10, inserted meanwhile)
    Note over S: a full page of two: process 9, 10; after = 10
    S->>R: after = 10, limit 2
    DB-->>S: no rows
    Note over S: empty page: end of the run, flush
```

## Dynamic view 4: `INTERVAL` mode

The timer thread writes the CSV when the oldest waiting row is old enough, even if no more rows arrive. In a table run the end-of-run flush usually writes the batch first.

```mermaid
sequenceDiagram
    participant S as TradeFetchService
    participant K as BatchingSink
    participant X as Flusher (timer)
    participant F as CSV file

    S->>K: append(row 5)
    Note over K,X: buffer was empty, timer starts
    S->>K: append(row 6)
    X->>K: batch-interval elapsed, flush
    K->>F: open, write rows 5 and 6, close
```

## Decision view: when does a write happen?

```mermaid
flowchart TD
    A["Row arrives"] --> D0{"different day from<br/>the waiting rows?"}
    D0 -->|yes| H["flush the older rows first"] --> B
    D0 -->|no| B{"write-mode"}
    B -->|PER_RECORD| C["open file, write, close"]
    B -->|COUNT| D["add to buffer"] --> E{"buffer size at least batch-size?"}
    B -->|INTERVAL| F["add to buffer<br/>start timer if buffer was empty"]
    E -->|yes| W["flush: write all rows, close"]
    E -->|no| I["wait for next row"]
    F --> I
    T["Timer: oldest row is batch-interval old,<br/>or midnight in file-zone"] --> W
    M["Buffer reaches max-buffered-records"] --> W
    S["End of the run (flush), or shutdown"] --> W
```

## Decision view: what happens to one row of the table?

```mermaid
flowchart TD
    A["Row read from the trade table"] --> B{"data_json present and<br/>a JSON object?"}
    B -->|no| X["failed: log id and reason<br/>(Empty message, Malformed JSON, Not a JSON object)"]
    B -->|yes| C["Reference data call"]
    C -->|"4xx"| X2["failed: Reference data HTTP code"]
    C -->|"5xx or timeout,<br/>after all attempts"| X3["failed: Reference data unavailable"]
    C -->|ok| D["run the mappings"]
    D -->|"a rule throws"| X4["failed: Mapping failed for column X"]
    D -->|row| E["append to the sink"]
    E -->|"cannot be written"| X5["failed: could not be written<br/>(logged)"]
    E -->|accepted| OK["written + 1"]
    X --> NEXT["next row"]
    X2 --> NEXT
    X3 --> NEXT
    X4 --> NEXT
    X5 --> NEXT
    OK --> NEXT
```
