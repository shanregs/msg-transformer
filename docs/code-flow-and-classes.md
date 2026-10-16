# SFTR Mapping Engine: Code Flow and Class Guide

Diagrams for this design are in [c4-architecture.md](c4-architecture.md).

## Overview

The app turns trade messages (JSON) into rows of an SFTR CSV report, and every output column is defined by one line of JSON config. It runs in two phases:

1. **Startup (once):** `MappingConfigLoader` reads `sftr-field-mappings.json` and compiles each line into a small function. `RecordProcessor` holds the compiled list.
2. **Per message (repeated):** an input source (file or Kafka) hands a message to `RecordPipeline`, which calls the reference-data service, runs the compiled functions, and appends one CSV row.

A bad message never stops the run. It goes to a `.rejects` file and processing continues with the next one.

## Per-message flow

Both entry points are thin and call the same method, `RecordPipeline.handle(json, sourceRef)`.

- `FileInputRunner` runs only when `sftr.input.file` is set. A `.txt` file is read as one JSON message per line (blank lines skipped). A `.csv` file uses its header row as keys, and each row becomes a flat JSON object before it goes on. It counts rows written vs. rejected, and a bad line never stops the run.
- `KafkaRecordListener` consumes one message at a time on one thread and acknowledges the offset after handling. It is switched off by the `file` profile (`sftr.kafka.enabled: false`).

Inside `RecordPipeline.handle()`:

1. `map(json)` parses the JSON and checks it is an object.
2. `RefDataClient.fetch(message)` POSTs selected message fields to the reference-data service. It retries 5xx and timeouts, never 4xx.
3. `RecordProcessor.process(message, ProcessingContext)` runs every active mapping and returns an `SftrTransactionReport`.
4. `SftrCsvWriter` appends the row. The writer is opened lazily on the first row and flushed per record by default.
5. If steps 1-3 throw, `reject()` writes `sourceRef<TAB>summary` to the `.rejects` file and the outcome is `REJECTED`. The summary never contains message content, because the data is regulatory.

## How the JSON config becomes running logic

`MappingConfigLoader.load()` runs once at startup and reads `sftr-field-mappings.json` into `FieldMapping` records, one per CSV column. Each row produces:

- a `Column` (name + the Java field on `SftrTransactionReport`) for **every** row. This fixes the CSV header and column order.
- a `CompiledMapping` (target field + a `MappingFunction`) only for **active** rows, meaning `status = MAPPED` and `mappingType` is not `NOT_APPLICABLE`. `PENDING` and `IGNORE` rows keep their column but stay blank.

`MappingFunction` is the core abstraction: `Object apply(JsonNode message, ProcessingContext ctx)`. Every config row is compiled into one of these in `compile()`:

| mappingType | Function it becomes |
| --- | --- |
| `DIRECT` | Copies `inputField` from the message as text. |
| `HARDCODE` | Returns a constant (`constantRef`), looked up once at startup. |
| `DIRECT_DATA_TRANSFORM` | Copies `inputField` and converts it to a number or date using `formatter` and the format/zone refs. Blank input gives null. |
| `TRANSFORM` / `ENRICH` | Looks up a hand-written lambda in `SftrMappingFunctions` by `handler` (or by `outputField` if no handler is given). |
| `NOT_APPLICABLE` | Skipped. |

**Startup validation.** The app fails to start, listing every problem at once, if a row has no `outputField` or `mappingType`, has a duplicate `outputField`, or names a field that does not exist on `SftrTransactionReport`. The same happens when no lambda is registered for a handler, or a function asks for a constant that does not exist (checked by `smokeRun`, which runs each function once against an empty message).

**Name rule.** `outputField` maps to a Java property by `propertyName()`: the text before the first `_` is lower-cased and the underscores are removed. `CP_RepCp` becomes `cpRepCp`, and `IM_SubmitterID` becomes `imSubmitterID`.

**Per message.** `RecordProcessor.process()` is a plain loop: create an `SftrTransactionReport`, then for each active mapping set `field = fn.apply(message, ctx)`. A failure is wrapped as `Mapping failed for column <name>`. `CompiledMapping.set()` converts the value to the field type through Spring's `ConversionService` if needed.

## Classes by package

All classes are under `com.bmo.sftr.mapping`.

| Package / class | Role |
| --- | --- |
| `SftrMappingApplication` | Spring Boot entry point. Registers `SftrConstants` and `RefDataClient.Properties` as configuration properties. |
| `input.FileInputRunner` | Batch alternative to Kafka. Reads a `.txt` (JSON lines) or `.csv` file and calls the pipeline per line. |
| `kafka.KafkaRecordListener` | Kafka consumer, one message at a time, acknowledges after handling. |
| `service.RecordPipeline` | Coordinates one message: parse, ref-data fetch, map, write, or reject. Single-threaded by design because it owns one open output file. |
| `engine.MappingConfigLoader` | Startup: reads and validates the JSON config, compiles rows into `Column` and `CompiledMapping`. |
| `engine.RecordProcessor` | Holds the compiled config and runs the active mappings for each message. |
| `functions.SftrMappingFunctions` | Registry of named business-logic lambdas, for example `CP_CpSide` and `CP_BranchOfRepCp`. |
| `functions.RefDataExtractors` | Reads values from the ref-data response by dotted path, for example `find_sftr_party.values.lei`. |
| `functions.DateHelpers` | Date parsing and formatting helpers, including the flexible ISO parser. |
| `model.FieldMapping` | One config row (record) with the enums `Type`, `Status` and `Formatter`. |
| `model.MappingFunction` | The functional interface every mapping compiles to, plus a null-safe `text()` helper. |
| `model.ProcessingContext` | Per-message bundle: `refData`, `constants`, `fileDate`. |
| `model.SftrConstants` | Constants from `application.yml`: single values, lists and lookup tables (for example `cpSide`). |
| `model.SftrTransactionReport` | One output row: a plain object with one field per CSV column. |
| `refdata.RefDataClient` | HTTP POST to the reference-data service with retries (5xx and timeouts only) and timeouts from config. |
| `output.SftrCsvWriter` | Opens the CSV (header from the `Column` list), appends rows, and reads each value back from the report by reflection. |

**Configuration files.** `application.yml` holds Kafka, output paths, ref-data settings, constants and lookups. `application-file.yml` turns Kafka off for the `file` profile. `sftr-field-mappings.json` is the column definitions.

## Worked example: one line end to end

Take the first line of `samples/input.txt`: `{"trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"CP1","group_cd":"G1"}`.

1. `FileInputRunner.readLines` reads the line and calls `handle(line, "input.txt:1")`.
2. `RecordPipeline` parses it, then `RefDataClient` posts `bmoLegalEntity`, `counterpartyId`, `group_cd` and `delegated` (only those present) to the ref-data service and gets a JSON response.
3. `RecordProcessor` loops through the active mappings:
    - `IM_SubmitterID` is a `HARDCODE`, so it gets `BMOLN9000003`.
    - `CP_CpSide` is a `TRANSFORM`. The lambda looks up `PO` in the `cpSide` table and returns `GIVE`.
    - `CP_RepCp` is an `ENRICH`. It reads the LEI from the ref-data response at `find_sftr_party.values.lei`.
    - `CP_BranchOfRepCp` is a `TRANSFORM`. `LDNBB01` starts with the London prefix `LDNBB`, so the result is `GB`.
4. `SftrCsvWriter` writes the filled report as one CSV row, in header order.

The line `this line is not json` in the same file fails at parsing. It is written to the `.rejects` file as `input.txt:5<TAB>Malformed JSON`, and the run continues.

## How to add or change a column

| Goal | What to change |
| --- | --- |
| Copy a field, set a constant, or convert a number/date | Edit only `sftr-field-mappings.json` (`DIRECT`, `HARDCODE`, `DIRECT_DATA_TRANSFORM`). Constants and format patterns live under `sftr.constants` in `application.yml`. |
| Custom business logic | Set `TRANSFORM` or `ENRICH` in the JSON and register a lambda with the same name (or the row's `handler`) in `SftrMappingFunctions`. |
| A new column | Add a field to `SftrTransactionReport` named by the `propertyName()` rule, then add the config row. Column order follows config order. |
| Park a column | Set `status` to `PENDING` or `IGNORE`. The column stays in the CSV, empty. |

A wrong name, a missing constant or an unregistered handler fails at startup with a list of all problems, not at the first message.
