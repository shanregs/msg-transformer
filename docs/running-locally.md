# Running and Testing Locally

How to build and start the engine on your machine, test it through the REST endpoint (curl, Postman or IntelliJ), run it from IntelliJ and debug it, and a set of test cases with the results you should see. Everything below was run against the real application.

Background: [design-and-architecture.md](design-and-architecture.md) (what it does and why), [code-flow-and-classes.md](code-flow-and-classes.md) (the flow), [rules-compilation.md](rules-compilation.md) (the mapping rules).

## 1. What you need

| Tool | Needed for | Notes |
| --- | --- | --- |
| JDK 25 | everything | `java -version` must say 25 |
| Maven 3.9 or newer | build from a terminal | IntelliJ has its own Maven |
| Python 3 | the reference-data stub | the engine calls a Reference Data Service; the stub stands in for it |
| curl or Postman | the REST tests | a Postman collection is included |
| Docker | Kafka mode and both mode only | runs a local Kafka broker |

## 2. Run modes (Spring profiles)

Pick the mode with one profile. With no profile you get Kafka mode.

| Profile | Kafka consumer | REST endpoint | Use it for |
| --- | --- | --- | --- |
| `rest` | off | on | Quick tests and debugging with no Kafka at all. **Start here.** |
| `kafka` (or no profile) | on | off | Production behaviour: consume the topic only. No web server is started. |
| `both` | on | on | Consume Kafka and also accept test messages through REST. |
| `file` | off | off | Process a `.txt` or `.csv` file (`--sftr.input.file=<path>`), then exit. |

Use exactly one of `rest`, `kafka`, `both`. The REST endpoint is `POST /api/test-message` on port 8080. If 8080 is busy on your machine (it was on mine), add `--server.port=18080`, or any free port.

Every message, from Kafka, a file or REST, goes through the same pipeline.

## 3. Quick start: REST mode in five minutes

Run these from the project folder.

**1. Start the reference-data stub** (leave it running in its own terminal):

```
python samples/refdata-stub.py
```

It listens on `http://127.0.0.1:8089`, the address the engine expects. It answers with a LEI built from the `counterpartyId`, and a few special counterparty ids trigger failures (section 6).

**2. Build** (once, and again after code changes):

```
mvn package -DskipTests
```

**3. Start the engine in REST mode:**

```
java -jar target/sftr-mapping-engine.jar --spring.profiles.active=rest
```

Wait for `Started SftrMappingApplication` and `Tomcat started on port 8080`. To use another port add `--server.port=18080`.

**4. Send a message:**

```
curl -X POST http://localhost:8080/api/test-message -H "Content-Type: application/json" -d "{\"trade_ref\":\"1\",\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\",\"counterpartyId\":\"CP1\",\"group_cd\":\"G1\"}"
```

(In PowerShell use `curl.exe`, or `Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/test-message -ContentType application/json -Body '{"trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"CP1"}'`.)

You get:

```json
{"outcome":"WRITTEN","flushed":true,"reportFile":".\\data\\sftr-report-2026-10-16.csv","rejectsFile":null}
```

**5. Look at the output** in the `data` folder of the working directory:

| File | Content |
| --- | --- |
| `data/sftr-report-<yyyy-mm-dd>.csv` | The report: a header and one row per good message. The date is today in `Asia/Kolkata`. |
| `data/sftr-report-<yyyy-mm-dd>.csv.rejects` | One line per bad message: `source`, a tab, and the reason. |

To start clean, stop the app and delete the `data` folder. It is git-ignored.

The reply fields: `outcome` is `WRITTEN` or `REJECTED` (both are HTTP 200), `flushed` says whether waiting rows were written at once, `reportFile` is the day's CSV and `rejectsFile` is set only for a rejected message. The reason for a reject is in the rejects file. The source on a rejects line is `rest-<n>`, where `n` counts the requests since the app started.

## 4. Test with Postman or the IntelliJ HTTP client

| File | How to use it |
| --- | --- |
| `samples/sftr-postman-collection.json` | In Postman choose Import and select the file. It has 18 requests in four folders, each with test assertions, and a `baseUrl` variable (default `http://localhost:8080`; change it if you used another port). Run a request, or run the whole collection from the Collection Runner. |
| `samples/rest-tests.http` | Open it in IntelliJ (its HTTP Client) and click the green arrow beside a request. Change `@host` at the top if needed. |

Folders A, B and C work in `rest` or `both` mode with the stub running. Folder D needs the batch start-up in section 7. The Postman tests check the HTTP status, the `outcome`, `flushed` and that the rejects file is named; to see the CSV values and reject reasons, open the files in `data`.

## 5. Kafka mode and both mode

Kafka needs a broker. This starts a throwaway one in Docker on port 19092:

```
docker run -d --name sftr-kafka -p 19092:19092 ^
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller ^
  -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:9093 ^
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 ^
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER ^
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT ^
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 ^
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 ^
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 apache/kafka:latest
```

(`^` continues the line in Windows `cmd`; use `` ` `` in PowerShell or `\` in bash.) Wait about 15 seconds, then create the topic:

```
docker exec sftr-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic trade-messages --partitions 1 --replication-factor 1
```

Start the engine in Kafka mode (or `both`):

```
java -jar target/sftr-mapping-engine.jar --spring.profiles.active=kafka --spring.kafka.bootstrap-servers=localhost:19092
```

Send messages (one JSON per line; press Ctrl+D or Ctrl+Z then Enter to finish):

```
docker exec -i sftr-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic trade-messages
```

See how far the engine has read (the committed offset follows the write):

```
docker exec sftr-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group sftr-mapping-engine
```

What you should see:

| Mode | Check |
| --- | --- |
| `kafka` | The log shows `Routes startup (total:1)` and no `Tomcat started`. A REST call is refused (nothing listens). A message on the topic appears in the CSV. |
| `both` | The log shows both `Tomcat started` and `Routes startup (total:1)`. A Kafka message and a REST message end up in the same CSV, in the order they were handled. A bad Kafka message appears in the rejects file as `trade-messages-0@<offset>`. |
| `rest` | The log shows `Routes startup (total:0)`: no Kafka consumer. |

A new consumer group reads the topic from the start, so an old backlog is processed first. Use `--spring.kafka.consumer.group-id=my-test-1` for a fresh group. Stop the broker with `docker rm -f sftr-kafka`.

## 6. The reference-data stub

`samples/refdata-stub.py` takes the `counterpartyId` from the request and answers:

| `counterpartyId` | Stub answer | Engine result |
| --- | --- | --- |
| any normal value | 200 with a LEI `549300` + the id in capitals, padded with `0` to 14 characters, nature `C`, sector `CDTI` | row written |
| `NONATURE` | 200 without the nature | `CP_NatureOfRepCp` falls back to the default `F` |
| `FAIL4XX` | HTTP 400 | rejected, `Reference data HTTP 400`, not retried |
| `FAIL5XX` | HTTP 503 | retried 3 times, then rejected, `Reference data unavailable` (under a second) |
| `SLOW` | answers after 8 seconds | the 5 second read timeout hits each of 3 attempts, then rejected, `Reference data unavailable` (about 15 seconds) |

The stub prints every request it receives, which shows exactly what the engine posts: `bmoLegalEntity`, `counterpartyId`, `group_cd` and `delegated`, only those present. If you start the stub on another port, run the engine with `--sftr.refdata.base-url=http://localhost:<port>`. With no stub running every message is rejected with `Reference data unavailable`.

## 7. Test cases and expected results

Start in REST mode with the stub running. The CSV columns named below are `CP_RepCp`, `CP_NatureOfRepCp`, `CP_SecOfRepCp`, `CP_BranchOfRepCp`, `CP_CpSide` and `CP_EntResponsibleForRep`; the three `IM_` columns are constants (`BMOLN9000003`, `BMOLN9000003`, `LEI`). The other 7 columns are always empty with the seed config.

### A. Rows that are written

| # | Message (changed fields) | Outcome | What you see in the CSV row |
| --- | --- | --- | --- |
| A1 | `trade_type` PO, `bmoLegalEntity` LDNBB01, `counterpartyId` CP1 | `WRITTEN` | RepCp `549300CP100000000000`, nature `C`, sector `CDTI`, branch `GB`, side `GIVE`, same LEI as `CP_EntResponsibleForRep` |
| A2 | PI, BMOCM01, CP2 | `WRITTEN` | branch **empty** (not a London entity), side `TAKE` |
| A3 | RP, LDN-02, CP3 | `WRITTEN` | branch `GB` (the second London prefix), side `GIVE` |
| A4 | LP, LDNBB01, CP4 | `WRITTEN` | side `TAKE` (LP is in the TAKE list) |
| A5 | `trade_type` XX | `WRITTEN` | side **empty**: a code not in the lookup gives no value, the row is still written |
| A6 | no `trade_type` | `WRITTEN` | side **empty** |
| A7 | `counterpartyId` NONATURE | `WRITTEN` | nature `F` (the default, because reference data has none) |

### B. Rejected messages

| # | Request body | Outcome | Line in the rejects file |
| --- | --- | --- | --- |
| B1 | `{not json` | `REJECTED` | `rest-<n>`, tab, `Malformed JSON` |
| B2 | empty body | `REJECTED` | `Empty message` |
| B3 | `[1, 2]` | `REJECTED` | `Not a JSON object` |

### C. Reference-data failures

| # | `counterpartyId` | Outcome | Rejects reason | Time |
| --- | --- | --- | --- | --- |
| C1 | FAIL4XX | `REJECTED` | `Reference data HTTP 400` | instant |
| C2 | FAIL5XX | `REJECTED` | `Reference data unavailable` | under 1 second (3 attempts) |
| C3 | SLOW | `REJECTED` | `Reference data unavailable` | about 15 seconds |

After every rejected message the next one still works: send an A case right after a C case to see this.

### D. Batch write modes

Stop the app and restart it with a batch of 3. Delete the `data` folder first.

```
java -jar target/sftr-mapping-engine.jar --spring.profiles.active=rest --sftr.output.write-mode=COUNT --sftr.output.batch-size=3
```

Send these with `?flush=false` (the test endpoint writes at once by default, which hides the batching):

| Step | Request | What you see |
| --- | --- | --- |
| D1 | message 1, `?flush=false` | `WRITTEN`, `flushed` false. **No report file yet** |
| D2 | message 2, `?flush=false` | still no file |
| D3 | message 3, `?flush=false` | the batch of 3 is full: the file appears with a header and 3 rows |
| D4 | message 4, `?flush=false` | waits: the file still has 3 rows |
| D5 | message 5, default (`flush=true`) | writes the waiting rows: the file has 5 rows |

For time-based batching use `--sftr.output.write-mode=INTERVAL --sftr.output.batch-interval=10s`, send a message with `?flush=false`, and the file appears about 10 seconds later without another request. `--sftr.output.write-mode=PER_RECORD` is the default: every message is written as it arrives.

### E. Other things worth trying

| Try | How | Expect |
| --- | --- | --- |
| Pin the date | `--sftr.output.file-date=2026-01-01` | rows and file names use that date: `sftr-report-2026-01-01.csv` |
| One file instead of daily files | `--sftr.output.rollover=NONE` | `data/sftr-report.csv` |
| Another output folder | `--sftr.output.file=C:/temp/out/report.csv` | the folder is created; the date is added to the name |
| File mode | `--spring.profiles.active=file --sftr.input.file=samples/input.txt` | the app processes the file and exits; `samples/input.txt` has 4 good rows and 2 bad lines |
| A bad rule | edit `sftr-field-mappings.json`, for example misspell an `outputField`, then restart | the app does not start and names the rule |

## 8. Run and debug in IntelliJ

**Import and set up**

1. Open the project folder (File, Open) and choose the `pom.xml`, or open the folder and let IntelliJ load the Maven project.
2. Set the JDK: File, Project Structure, Project, SDK = 25 (and language level 25). Maven reload if you changed it.
3. Turn on annotation processing (the code uses Lombok and a configuration processor): Settings, Build, Execution, Deployment, Compiler, Annotation Processors, tick **Enable annotation processing**. Make sure the Lombok plugin is enabled (it is bundled in recent versions).
4. Build the project once (Build, Build Project) to check there are no errors.

**Start the stub** from a terminal (`python samples/refdata-stub.py`), or add a Python run configuration for it if you have the Python plugin.

**Create a run configuration**

1. Run, Edit Configurations, plus, **Application** (or **Spring Boot** in IntelliJ Ultimate).
2. Main class: `com.bmo.sftr.mapping.SftrMappingApplication`. Module: the project module (classpath of `sftr-mapping-engine`).
3. Working directory: the project folder, so output goes to `<project>/data`.
4. Program arguments: `--spring.profiles.active=rest` (or `kafka`, `both`). Add `--server.port=18080` if 8080 is busy, and any `--sftr...` option from section 7.
5. Run it with the green arrow, or debug it with the bug icon.

**Where to put breakpoints**

| To see | Breakpoint |
| --- | --- |
| A REST request arrive | `TestMessageController.post` |
| The whole message path | `RecordPipeline.handle` (every entry point goes through it) |
| Parsing and the reference-data call | `MessageTransformer.transform`, `RefDataClient.fetch` |
| One rule running | `RecordProcessor.process`, inside the loop. A conditional breakpoint on `m.column().equals("CP_CpSide")` stops on one rule only |
| How rules are compiled at startup | `MappingConfigLoader.load`, `MappingConfigLoader.compile` (hit once per active rule, before the app is ready) |
| A lambda | the lambda body in `SftrMappingFunctions`, for example `CP_CpSide` |
| When a row is written | `PerRecordSink.append`, `BatchingSink.append`, `BatchingSink.flushLocked` |
| Why a message was rejected | `RejectWriter.write` (the exception is the one just caught in `RecordPipeline.handle`) |
| Kafka commits | `KafkaIngestRoute.handle`, `OffsetCommitter.release`, `OffsetCommitter.commitReleased` |

**Debugging tips**

- Use Evaluate Expression on `message`, `ctx.refData()`, `ctx.constants()` and the `report` being filled in `RecordProcessor.process`.
- For the REST endpoint, a breakpoint pauses the request: Postman or curl waits until you resume. In Kafka mode, pausing the consumer thread for a long time makes Kafka think the consumer died; keep pauses short, or use REST mode for long debugging sessions.
- In `INTERVAL` mode the batch is written by a separate virtual thread named `sftr-batch-flusher`; a breakpoint in `BatchingSink.flushLocked` is hit on that thread (or on the consumer thread for `COUNT`).
- Startup problems (a wrong rule or setting) end with `APPLICATION FAILED TO START` or `Invalid SFTR mapping config`, followed by one line per problem. See the error table in [rules-compilation.md](rules-compilation.md).
- To run the unit tests: right-click `src/test/java` and choose Run Tests, or `mvn test`. Two end-to-end tests need a Kafka broker on `localhost:19092` and run only with `-Dkafka.it=true`.

## 9. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `Web server failed to start. Port 8080 was already in use.` | Another program uses 8080. Add `--server.port=18080` and set the Postman `baseUrl` to match. |
| Every message is `REJECTED` with `Reference data unavailable` | The stub is not running or is on another port. Start it, or set `--sftr.refdata.base-url`. |
| `Missing constant: sftr.constants...`, `no field ...`, `no lambda registered ...` at start | A rule or constant is wrong. See [rules-compilation.md](rules-compilation.md), section 9. |
| The app starts and exits at once in Kafka mode | It should not (the Camel run controller keeps it alive). Check you are running the current build, and that Kafka is reachable. |
| Kafka mode: nothing happens | Check the broker is up, the topic `trade-messages` exists, and `--spring.kafka.bootstrap-servers` points to it. A new consumer group starts from the beginning. |
| REST call is refused in `kafka` mode | By design: `kafka` mode has no web server. Use `rest` or `both`. |
| `release version 25 not supported` or compile errors on `record`/`switch` syntax | The JDK is older than 25. Check `java -version` and the project SDK. |
| Getters and setters not found in IntelliJ | Annotation processing or the Lombok plugin is off (section 8, step 3). |
| No CSV after a `COUNT` or `INTERVAL` request | The batch is still waiting. Use the default `flush=true`, send more messages, or wait for the interval. |
