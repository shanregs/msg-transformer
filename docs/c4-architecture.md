# SFTR Mapping Engine: C4 Architecture Diagrams

Diagrams are Mermaid, so they render on GitHub and in most IDEs. See [code-flow-and-classes.md](code-flow-and-classes.md) for the written walkthrough.

## Level 1: System context

The engine takes messages from Kafka or an input file, asks the Reference Data Service to enrich each one, and writes the SFTR CSV plus a rejects file. One source is used per run; Kafka is the default and the `file` profile switches it off.

```mermaid
flowchart LR
    op(["Operator<br/>runs the app"]) -->|starts and configures| engine
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot application<br/>maps messages to rows"] <-->|enrich| ref["Reference Data Service<br/>HTTP, JSON"]
    engine -->|rows| out["Output files<br/>SFTR CSV and .rejects"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 2: Containers

The engine is one Spring Boot application (`sftr-mapping-engine.jar`). It reads its mapping config once at startup, then handles messages sequentially.

```mermaid
flowchart LR
    kafka[("Kafka broker<br/>topic trade-messages")] -->|messages| engine
    file["Input file<br/>.txt JSON lines or .csv"] -->|messages| engine
    cfg["Mapping config<br/>sftr-field-mappings.json<br/>application.yml"] -.->|loaded at startup| engine
    engine["<b>SFTR Mapping Engine</b><br/>Spring Boot application<br/>reads one message at a time<br/>enriches via reference data<br/>maps to an SFTR row<br/>appends the row to the CSV<br/>sends bad messages to rejects"]
    engine <-->|enrich| ref["Reference data<br/>HTTP service, JSON"]
    engine -->|rows| csv["SFTR CSV report<br/>appended per row"]
    engine -->|rejects| rej["Rejects file<br/>source and reason"]

    style engine fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 3: Components

Either input component hands a message to `RecordPipeline`. The pipeline calls `RefDataClient`, then `RecordProcessor`, then `SftrCsvWriter`, and writes the rejects file itself. `RecordProcessor` gets its compiled mappings from `MappingConfigLoader` once at startup, which looks lambdas up in `SftrMappingFunctions`. Solid arrows run per message; dashed arrows run at startup only.

```mermaid
flowchart TB
    subgraph engine["SFTR Mapping Engine (Spring Boot application)"]
        file["FileInputRunner<br/>reads .txt or .csv lines"]
        kafka["KafkaRecordListener<br/>one message at a time"]
        pipe["<b>RecordPipeline</b><br/>parse, enrich, map, write<br/>rejects bad messages<br/>single-threaded"]
        ref["RefDataClient<br/>HTTP POST with retries"]
        csv["SftrCsvWriter<br/>appends rows to the CSV"]
        proc["RecordProcessor<br/>runs the active mappings<br/>builds the report object"]
        loader["MappingConfigLoader<br/>reads and validates JSON<br/>compiles each row"]
        fns["SftrMappingFunctions<br/>named business lambdas<br/>+ RefDataExtractors<br/>+ DateHelpers"]
        consts["SftrConstants<br/>values, lists, lookups<br/>also used by the loader"]

        file -->|handle| pipe
        kafka -->|handle| pipe
        pipe -->|fetch| ref
        pipe -->|write| csv
        pipe -->|process| proc
        consts -->|context| proc
        loader -.->|compiled| proc
        fns -.->|find by name| loader
    end

    style pipe fill:#dbeafe,stroke:#2563eb,stroke-width:2px
```

## Level 4: Code

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

### Dynamic view: one message

The numbered calls are the happy path. Dashed arrows are returns. If parsing, enrichment or mapping throws, the pipeline writes a rejects line and returns `REJECTED`.

```mermaid
sequenceDiagram
    participant In as Input (file or Kafka)
    participant P as RecordPipeline
    participant R as RefDataClient
    participant M as RecordProcessor
    participant W as SftrCsvWriter

    In->>P: 1 handle(json, ref)
    P->>R: 2 fetch(message)
    R-->>P: ref data JSON
    P->>M: 3 process(msg, ctx)
    M->>M: apply mappings
    M-->>P: report object
    P->>W: 4 write(report)
    P-->>In: WRITTEN
```
