# Running and Testing Locally

How to set up the database, build and start the engine, run it over the `trade` table, test single messages, and run and debug it from IntelliJ. The run over the table was checked against a real Postgres 16 with the sample rows in `samples/db`.

## 1. What you need

| Tool | Needed for | Notes |
| --- | --- | --- |
| JDK 25 | build and run | check with `java -version` |
| Maven 3.9 or later | build | `mvn -v` |
| PostgreSQL | the `trade` table | a local server on port 5432; database `sftr` (section 2) |
| Python 3 | the reference-data stub | the engine calls a Reference Data Service for every row; the stub stands in for it |
| curl, Postman or a browser | calling the REST endpoints | Swagger UI needs only a browser |

## 2. The database

The application reads the table `trade`, which another application fills. Locally you create it yourself.

**Create the database and the table** (psql asks for the `postgres` password; run both from the project root):

```
"C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -h localhost -f samples/db/01-create-database.sql
"C:\Program Files\PostgreSQL\18\bin\psql" -U postgres -h localhost -d sftr -f samples/db/02-trade-table.sql
```

`02-trade-table.sql` creates the table and 7 sample rows: three good `RECEIVED` rows, one `RECEIVED` row that fails in the reference-data call, one `RECEIVED` row that is not a JSON object, and one row each for `PROCESSED` and `PUBLISHED`, which are never read.

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | `bigserial` | primary key; rows are processed in id order |
| `created_by` | `varchar(100)` | who wrote the row |
| `created_on` | `timestamptz` | when |
| `event` | `varchar(20)` | `RECEIVED`, `PROCESSED` or `PUBLISHED`; only `RECEIVED` is read |
| `data_json` | `jsonb` | the trade as JSON; this is what gets mapped |

The column names are mapped in `src/main/java/com/bmo/sftr/mapping/db/Trade.java`. If the real table uses other names, change the `@Column(name = ...)` values there.

**Connection settings** are environment variables, so no password is stored in the project:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SFTR_DB_URL` | `jdbc:postgresql://localhost:5432/sftr` | JDBC URL |
| `SFTR_DB_USER` | `postgres` | user |
| `SFTR_DB_PASSWORD` | empty | password |

Set them in the same terminal before starting the app (PowerShell: `$env:SFTR_DB_PASSWORD = "..."`; cmd: `set SFTR_DB_PASSWORD=...`). In IntelliJ put them in the run configuration's Environment variables (section 7).

## 3. Quick start

**1. Start the reference-data stub** (leave it running in its own terminal):

```
python samples/refdata-stub.py
```

It prints `Reference data stub on http://127.0.0.1:8089/ref-data/sftr`. Section 5 describes what it answers.

**2. Build:**

```
mvn clean package
```

This builds `target/sftr-mapping-engine.jar` and runs the tests (they use an in-memory H2 database, so they need no Postgres).

**3. Start the engine:**

```
java -jar target/sftr-mapping-engine.jar
```

Wait for `Started SftrMappingApplication` and `Tomcat started on port 9090`. The default port is 9090 because 8080 is often taken by other applications; use `--server.port=<port>` to change it. If the database is not reachable the app stops at start-up with a connection error.

**4. Process the table:**

```
curl -X POST "http://localhost:9090/api/trades/process"
```

(In PowerShell use `curl.exe`, or `Invoke-RestMethod -Method Post -Uri http://localhost:9090/api/trades/process`.) The call returns when the run has finished. With the sample rows:

```
{"event":"RECEIVED","read":5,"written":3,"failed":2,"firstId":1,"lastId":5,"durationMs":349,"reportFile":".\\data\\sftr-report-2026-10-17.csv"}
```

| Field | Meaning |
| --- | --- |
| `read` | `RECEIVED` rows fetched (rows 6 and 7 are not `RECEIVED`) |
| `written` | rows handed to the CSV writer |
| `failed` | rows that failed; each is logged and skipped |
| `firstId`, `lastId` | ids of the first and last row read |
| `reportFile` | today's CSV |

Add `?limit=100` to stop after 100 rows.

**What you should see**

| Where | What |
| --- | --- |
| `data/sftr-report-<yyyy-mm-dd>.csv` | a header and 3 rows (T1, T2, T3). The date is today in `Asia/Kolkata`. |
| The log | `Run started...`, `Trade 4 failed: Reference data HTTP 400`, `Trade 5 failed: Not a JSON object`, `Run finished: read 5, written 3, failed 2`. The log never contains the JSON. |
| Rejects | there is no rejects file; failures are in the log only |

The three rows: T1 has `CP_RepCp` `549300CP100000000000`, nature `C`, sector `CDTI`, branch `GB` and side `GIVE`. T2 has an empty branch (`BMOCM01` is not a London entity) and side `TAKE`. T3 has nature `F`, the default used when reference data has none.

**Behaviour to know**

- Rows are written in **batches** (`write-mode: COUNT`, `batch-size: 100`): a batch is written when 100 rows are waiting, and the last, partial batch is written at the end of the run. So the CSV is complete when the call returns.
- A second request while a run is active gets **409**. A run reads only `RECEIVED` rows and never changes the table, so running again processes the same rows again and appends them to the CSV.
- A failed row does not stop the run. If the CSV itself cannot be written, each affected row is logged as `could not be written` and the run carries on.

## 4. Swagger UI and single messages

Open `http://localhost:9090/swagger-ui.html`. It lists both endpoints with example values: press Try it out, then Execute. The OpenAPI document is at `/v3/api-docs` (import it into Postman with Import, then Link).

`POST /api/test-message` runs one message through the same flow, without the database. Use it to try mapping and reference-data cases:

```
curl -X POST http://localhost:9090/api/test-message -H "Content-Type: application/json" -d "{\"trade_ref\":\"1\",\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\",\"counterpartyId\":\"CP1\",\"group_cd\":\"G1\"}"
```

The answer is `{"outcome":"WRITTEN","flushed":true,"reportFile":"...","reason":null}`. A message that cannot be processed still answers 200 with `"outcome":"FAILED"` and a short `reason` (for example `Malformed JSON`); the content is never echoed. With `flush=true` (the default) the row is written at once; `?flush=false` leaves it in the batch.

Two files in `samples/` give ready-made requests: `rest-tests.http` (open it in IntelliJ and click the green arrow beside a request) and `sftr-postman-collection.json` (in Postman choose Import and select it; the `baseUrl` is `http://localhost:9090`).

## 5. The reference-data stub

`samples/refdata-stub.py` takes the `counterpartyId` from the request and answers:

| `counterpartyId` | Stub answer | Engine result |
| --- | --- | --- |
| any normal value | 200 with a LEI `549300` + the id in capitals, padded with `0` to 14 characters, nature `C`, sector `CDTI` | row written |
| `NONATURE` | 200 without the nature | `CP_NatureOfRepCp` falls back to the default `F` |
| `FAIL4XX` | HTTP 400 | row fails, `Reference data HTTP 400`, not retried |
| `FAIL5XX` | HTTP 503 | retried 3 times, then fails with `Reference data unavailable` (under a second) |
| `SLOW` | answers after 8 seconds | the 5 second read timeout hits each of 3 attempts, then fails with `Reference data unavailable` (about 15 seconds) |

The stub prints every request it receives, which shows what the engine posts: `bmoLegalEntity`, `counterpartyId`, `group_cd` and `delegated`, only those present. If you start the stub on another port, run the engine with `--sftr.refdata.base-url=http://localhost:<port>`. With no stub running every row fails with `Reference data unavailable`.

## 6. Test cases and expected results

Send these to `POST /api/test-message` (Swagger, curl, Postman or `rest-tests.http`). Only the fields that change are shown; use the message from section 4 as the base.

### A. Rows that are written

| # | Message (changed fields) | Outcome | What you see in the CSV row |
| --- | --- | --- | --- |
| A1 | `trade_type` PO, `bmoLegalEntity` LDNBB01, `counterpartyId` CP1 | `WRITTEN` | RepCp `549300CP100000000000`, nature `C`, sector `CDTI`, branch `GB`, side `GIVE`, same LEI as `CP_EntResponsibleForRep` |
| A2 | PI, BMOCM01, CP2 | `WRITTEN` | branch **empty** (not a London entity), side `TAKE` |
| A3 | RP, LDN-02, CP3 | `WRITTEN` | branch `GB` (the second London prefix), side `GIVE` |
| A4 | LP, LDNBB01, CP4 | `WRITTEN` | side `TAKE` (LP is in the TAKE list) |
| A5 | `trade_type` XX | `WRITTEN` | side **empty**: a code not in the lookup gives no value, the row is still written |
| A6 | no `trade_type` | `WRITTEN` | side **empty** |
| A7 | `counterpartyId` NONATURE | `WRITTEN` | nature `F` |

### B. Messages that fail

| # | Body | Outcome | Reason |
| --- | --- | --- | --- |
| B1 | `{not json` | `FAILED` | `Malformed JSON` |
| B2 | empty body | `FAILED` | `Empty message` |
| B3 | `[1,2,3]` | `FAILED` | `Not a JSON object` |

Nothing is written for a failed message, and the next one still works.

### C. Reference-data failures

| # | `counterpartyId` | Outcome | Reason | Time |
| --- | --- | --- | --- | --- |
| C1 | FAIL4XX | `FAILED` | `Reference data HTTP 400` | instant |
| C2 | FAIL5XX | `FAILED` | `Reference data unavailable` | under 1 second (3 attempts) |
| C3 | SLOW | `FAILED` | `Reference data unavailable` | about 15 seconds |

### D. Write modes

Start the engine with a small batch to watch the batching: add `--sftr.output.batch-size=3`. Delete the `data` folder first.

| # | Request | Result |
| --- | --- | --- |
| D1 | two `test-message` calls with `?flush=false` | `WRITTEN`, `flushed` false. **No report file yet** |
| D2 | a third call with `?flush=false` | the batch is full: the file now has a header and 3 rows |
| D3 | a fourth call with `?flush=false` | waits in a new batch; the file still has 3 rows |
| D4 | a fifth call with the default `flush=true` | writes everything waiting; the file has 5 rows |

To write each row at once, use `--sftr.output.write-mode=PER_RECORD`.

### E. Other things worth trying

| What | How | Expect |
| --- | --- | --- |
| Pin the date | `--sftr.output.file-date=2026-01-01` | rows and file names use that date: `sftr-report-2026-01-01.csv` |
| Another output folder | `--sftr.output.file=C:/temp/out/report.csv` | the folder is created; the date is added to the name |
| Smaller pages | `--sftr.db.page-size=2` | the log is the same; the table is read two rows at a time |
| A bad rule | edit `mapper/sftr-field-mappings.json`, for example misspell an `outputField`, then restart | the app does not start and names the rule |
| Stub off | stop the stub and run the table | every row fails with `Reference data unavailable`; the run still finishes |

## 7. Run and debug in IntelliJ

**Import and set up**

1. Open the project folder (the one with `pom.xml`) and let Maven import finish.
2. Set the project SDK to JDK 25 (File, Project Structure).
3. Turn on annotation processing (Settings, Build, Execution, Deployment, Compiler, Annotation Processors) and install the Lombok plugin, otherwise getters and setters are not found.

**Run configuration**

1. Run, Edit Configurations, plus, **Spring Boot** (in the Community edition use **Application**).
2. Main class: `com.bmo.sftr.mapping.SftrMappingApplication`.
3. Environment variables: `SFTR_DB_PASSWORD=<your postgres password>` (add `SFTR_DB_USER` or `SFTR_DB_URL` only if they differ from section 2).
4. Program arguments (optional): `--server.port=18080` if 9090 is busy, `--sftr.output.batch-size=3` to watch batching.
5. Working directory: the project root, so `data/` is created there.
6. Start the stub first (section 3) and run the configuration with the green arrow, or debug with the bug icon.

**Where to put breakpoints**

| To see | Put a breakpoint in |
| --- | --- |
| The run loop and paging | `TradeFetchService.process` |
| What the query returns | `TradeRepository.findByEventAndIdGreaterThanOrderByIdAsc` (a method breakpoint) or the line after it in `process` |
| The whole record path | `RecordPipeline.handle` |
| Parsing and the reference-data call | `MessageTransformer.transform`, `RefDataClient.fetch` |
| One rule running | `RecordProcessor.process`, inside the loop. A conditional breakpoint on `m.column().equals("CP_CpSide")` stops on one rule only |
| How rules are compiled at startup | `MappingConfigLoader.load`, `MappingConfigLoader.compile` |
| A lambda | the lambda body in `SftrMappingFunctions`, for example `CP_CpSide` |
| When a row is written | `PerRecordSink.append`, `BatchingSink.append`, `BatchingSink.flushLocked` |
| Why a record failed | `RecordProcessingException` (constructor), or the `catch` blocks in `TradeFetchService.process` |
| A REST request arrives | `TradeController.process`, `TestMessageController.post` |

**Debugging tips**

- Use Evaluate Expression on `trade.getDataJson()` (in `process`), `message`, `ctx.refData()`, `ctx.constants()` and the `report` being filled in `RecordProcessor.process`.
- A breakpoint pauses the HTTP request: curl or Swagger waits until you resume.
- A run holds no database transaction between pages, so pausing is safe. A long pause only delays the response.

## 8. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `Web server failed to start. Port 9090 was already in use.` | Another program uses 9090. Add `--server.port=18080`. |
| App stops at start-up with `Connection to localhost:5432 refused` or `password authentication failed` | The Postgres service is not running, or `SFTR_DB_PASSWORD` / `SFTR_DB_USER` / `SFTR_DB_URL` is wrong. Check with psql (section 2). |
| `database "sftr" does not exist` | Run `01-create-database.sql` (section 2). |
| `relation "trade" does not exist` | Run `02-trade-table.sql` against the `sftr` database. |
| `read: 0` | No row has the event `RECEIVED` (check `select event, count(*) from trade group by event`), or the table is empty. |
| Every row fails with `Reference data unavailable` | The stub is not running or is on another port. Start it, or set `--sftr.refdata.base-url`. |
| `Missing constant: sftr.constants...`, `no field ...`, `no lambda registered ...` at start | A rule or constant is wrong. See [rules-compilation.md](rules-compilation.md), section 9. |
| A column name error such as `column "createdBy" does not exist` | The real table uses other column names. Change the `@Column(name = ...)` values in `Trade.java`. |
| `release version 25 not supported` or compile errors on `record`/`switch` syntax | The JDK is older than 25. Check `java -version` and the project SDK. |
| Getters and setters not found in IntelliJ | Annotation processing or the Lombok plugin is off (section 7, step 3). |
| No CSV after a `test-message` call with `flush=false` | The batch is still waiting. Use the default `flush=true`, send more messages, or run the table. |
| 409 on `/api/trades/process` | A run is still active. Wait for it to finish. |
