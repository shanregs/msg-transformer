# SFTR Mapping Engine: Project Specification

Build specification for a new Spring Boot project. It is written to be handed to Claude Code (or any developer) as the single source of truth: read it fully, then build exactly what it describes.

- Project name: `sftr-mapping-engine`
- Stack: JDK 21, Spring Boot 3.5.x, Maven, virtual threads
- Base package: `com.bmo.sftr.mapping`
- Purpose: convert incoming JSON trade messages into SFTR report rows (class `SftrTransactionReport`) and write them to CSV, using a metadata-driven mapping engine

---

## 0. Instructions for the builder

1. Implement the project exactly as specified. The class inventory in section 4 is closed: **do not add classes** beyond it (small private or nested helpers inside those classes are fine).
2. **No business literals in Java.** Codes, country codes, prefixes, default values, code tables and response paths live in `application.yml` under `sftr.constants`. Section 5.2 defines what counts as a business literal.
3. Code in this spec is reference code. Complete the helper methods it leaves out, make it compile, and make every test in section 11 pass. If the spec and the reference code disagree, the numbered requirements in section 2 win.
4. Use only Java 21 final features (records, switch expressions, virtual threads). No preview features.
5. Never log message contents or report values (regulatory data). Log counts, column names and line numbers only.
6. When the spec leaves a choice open, take the default listed in section 14 and mention it in the final summary.
7. Finish by running `mvn verify` and reporting the result.

---

## 1. Problem and scope

### 1.1 Problem
A source system sends JSON messages. For each message the application must produce one `SftrTransactionReport` with 200+ output fields and write it as one CSV row. Each output column has its own rule, one of:

| Type | Meaning |
|---|---|
| `DIRECT` | copy one field from the input message |
| `HARDCODE` | a fixed constant value |
| `TRANSFORM` | derive the value with business logic |
| `ENRICH` | look the value up in reference data |

The rules come from a business spreadsheet (the source of truth), which also gives each column a status (Mapped, Pending, IGNORE, Not applicable). Writing 200+ setters by hand is not maintainable, so the rules are configuration and the logic is a registry of lambdas.

### 1.2 In scope
- The mapping engine, constants handling, reference-data client, CSV writer, a batch service and a command-line runner.
- A seed mapping covering the first 16 columns of the spreadsheet (section 8).
- Full tests (section 11).

### 1.3 Out of scope (do not build)
- A Kafka or other message listener (the engine is source-agnostic; the runner reads a JSON Lines file).
- The real `SftrTransactionReport` with all 200+ fields. Build a placeholder with the 16 seed fields; the owner will replace it.
- A tool that converts the Excel sheet into JSON.
- Different rules per input feed (ICI, SFCM-GMTC, G1). One rule set for all.
- Dependencies between output fields (one field reading another output field).

---

## 2. Requirements

| ID | Requirement |
|---|---|
| R1 | Mapping rules are loaded from `mapper/sftr-field-mappings.json`. Row order in the file is the CSV column order. |
| R2 | Every output column name keeps its spreadsheet form in the CSV header (for example `CP_RepCp`). |
| R3 | The report property for a column is derived: lower-case the text before the first `_`, keep the rest as written, drop remaining `_`. Examples: `CP_RepCp` becomes `cpRepCp`, `IM_SubmitterID` becomes `imSubmitterID`. |
| R4 | All TRANSFORM and ENRICH logic lives in **one** class, `SftrMappingFunctions`, as lambdas stored in a `Map<String, MappingFunction>`. The lambda for a column is found by the column's own name, unless the row sets `handler` (used when columns share a lambda). |
| R5 | Rows with status `PENDING` or `IGNORE`, and rows of type `NOT_APPLICABLE`, are **never executed**: no lambda call, no constant lookup, no handler lookup, no input read. Their report field stays `null` and their CSV cell is empty. Only the column name is validated for these rows. |
| R6 | Every business constant lives in `application.yml` (`sftr.constants`), is bound once at startup into an immutable `SftrConstants`, and reaches lambdas through `ProcessingContext.constants()`. The mapping JSON never contains constant values; HARDCODE rows reference a key (`constantRef`). |
| R7 | One reference-data API call is made per input record, before mapping. The raw JSON response is passed to lambdas as `ProcessingContext.refData()`. Lambdas never make network calls. |
| R8 | Values are pulled out of the reference-data response by functions in `RefDataExtractors`, which take the response (and the constants, for response paths) as arguments. |
| R9 | Startup validation collects **all** configuration errors and fails once with the full list (duplicate column, unknown report property, missing `inputField`, unknown constant key, unknown handler, unknown JSON property). Skipped rows (R5) cannot cause errors other than a bad or duplicate column name. |
| R10 | A startup smoke-run calls every active lambda once with an empty message and empty reference data. It fails startup only when a lambda asks for a constant key that does not exist (`SftrConstants.MissingConstantException`). All lambdas and extractors must therefore tolerate null and missing inputs. |
| R11 | Records are processed concurrently on virtual threads. The number of concurrent reference-data calls is limited by a semaphore. Output order equals input order. |
| R12 | A record that fails (bad JSON, reference-data failure after retries, mapping error) is written to a rejects file and is not written to the CSV. The run continues. |
| R13 | Reference-data calls retry on timeouts and 5xx (up to `max-attempts`, linear backoff) and never retry on 4xx. |
| R14 | Absent input fields read as `null`, never as an empty string. The CSV writes `null` as an empty cell. |
| R15 | The runtime path has no `switch` on mapping type, no registry lookup and no reflection lookup per record. These are resolved at startup. |
| R16 | No `synchronized` blocks. All shared objects (constants, compiled mappings, lambdas) are immutable and stateless. |
| R17 | Changing a constant or a mapping needs a restart. Do not use `@RefreshScope` or hot reload. |

---

## 3. Technology and `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.0</version> <!-- use the latest 3.5.x patch -->
    <relativePath/>
  </parent>

  <groupId>com.bmo.sftr</groupId>
  <artifactId>sftr-mapping-engine</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <name>sftr-mapping-engine</name>

  <properties>
    <java.version>21</java.version>
    <commons-csv.version>1.12.0</commons-csv.version> <!-- or newer -->
  </properties>

  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-json</artifactId></dependency>
    <dependency><groupId>org.springframework</groupId><artifactId>spring-web</artifactId></dependency>
    <dependency><groupId>org.apache.commons</groupId><artifactId>commons-csv</artifactId><version>${commons-csv.version}</version></dependency>
    <dependency><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId><optional>true</optional></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-configuration-processor</artifactId><optional>true</optional></dependency>

    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
  </dependencies>

  <build>
    <finalName>sftr-mapping-engine</finalName>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
        <configuration>
          <excludes><exclude><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId></exclude></excludes>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

---

## 4. Project layout and class inventory (closed list)

```text
sftr-mapping-engine/
├── pom.xml
├── src/main/java/com/bmo/sftr/mapping/
│   ├── SftrMappingApplication.java           main class, enables configuration properties
│   ├── model/
│   │   ├── FieldMapping.java                 record: one config row (+ nested enums Type, Status)
│   │   ├── MappingFunction.java              functional interface (+ static null-safe JSON helper)
│   │   ├── ProcessingContext.java            record: (refData, constants, fileDate)
│   │   ├── SftrConstants.java                @ConfigurationProperties: every constant from YAML
│   │   └── SftrTransactionReport.java        placeholder report (16 String fields)
│   ├── functions/
│   │   ├── SftrMappingFunctions.java         ALL TRANSFORM/ENRICH lambdas + their name registry
│   │   └── RefDataExtractors.java            static pure functions over the reference-data response
│   ├── refdata/
│   │   └── RefDataClient.java                one API call per record, retry, nested Properties record
│   ├── engine/
│   │   ├── MappingConfigLoader.java          load JSON, validate, compile (+ nested Column, CompiledMapping, CompiledConfig)
│   │   └── RecordProcessor.java              the runtime loop
│   ├── output/
│   │   └── SftrCsvWriter.java                CSV output (+ nested Session)
│   ├── service/
│   │   └── SftrReportService.java            windowed, concurrent batch run
│   └── runner/
│       └── SftrBatchRunner.java              command-line adapter (off by default)
└── src/main/resources/
    ├── application.yml
    └── sftr-field-mappings.json
```

Tests are listed in section 11. Total: 12 main classes.

---

## 5. Configuration

### 5.1 `application.yml` (complete)

```yaml
spring:
  application:
    name: sftr-mapping-engine
  main:
    web-application-type: none        # batch application, no web server
  threads:
    virtual:
      enabled: true

logging:
  level:
    com.bmo.sftr: INFO

sftr:
  mapping:
    config: classpath:mapper/sftr-field-mappings.json

  runner:
    enabled: false                    # set true (command line) to run the batch
    input: ./data/input.jsonl         # JSON Lines: one JSON object per line
    output: ./data/sftr-report.csv
    file-date:                        # optional ISO date; default is today

  processing:
    window-size: 1000                 # records in flight per window
    max-concurrent-ref-data-calls: 20

  csv:
    delimiter: ","

  refdata:
    base-url: http://localhost:8089
    path: /ref-data/sftr
    connect-timeout: 2s
    read-timeout: 5s
    max-attempts: 3
    backoff: 200ms                    # attempt n waits n * backoff
    request-fields:                   # request JSON key -> input message field
      bmoLegalEntity: bmoLegalEntity
      counterpartyId: counterpartyId
      group_cd: group_cd
      delegated: delegated

  constants:
    values:                           # single literals
      submitterId: BMOLN9000003
      participantId: BMOLN9000003
      repCpCodeType: LEI
      natureOfRepCpDefault: F
      londonBranchCountry: GB
      refPathPartyLei: find_sftr_party.values.lei
      refPathPartyNature: find_sftr_party_sector.values.natureOfReportingCounterparty
      refPathPartySector: find_sftr_party_sector.values.sector
    lists:                            # lists of literals
      londonLegalEntityPrefixes: [LDNBB, LDN-]
    lookups:                          # code tables: result value -> codes that produce it
      cpSide:
        GIVE: [PO, DP, RP, TP, BP, BC, CLD, B, RPO]
        TAKE: [PI, DV, RV, TV, LP, LC, CLR, L, RPI]
```

YAML notes: quote any code that YAML could read as a boolean or number (`'NO'`, `'ON'`, `'Y'`, `'01'`). Per-environment overrides go in `application-<profile>.yml`, a ConfigMap or environment variables.

### 5.2 What counts as a business literal
Must be in YAML: codes and code tables, country codes, legal-entity prefixes, default values, fixed values written to the report, and reference-data response paths.
May stay in Java: input JSON field names used inside lambdas (for example `trade_type`, `bmoLegalEntity`), constant **keys** used to read from `SftrConstants` (for example `"cpSide"`), and technical literals (charset, `"\n"`).

### 5.3 Constant shapes
`SftrConstants` offers three shapes. New constants of these shapes need only YAML:

| Shape | YAML section | Accessor |
|---|---|---|
| single value | `values` | `value(key)` |
| list | `lists` | `list(key)`, `startsWithAny(listKey, text)` |
| code table | `lookups` | `lookup(table, code)` |

---

## 6. Mapping file `mapper/sftr-field-mappings.json`

### 6.1 Row schema

| Property | Required for | Meaning |
|---|---|---|
| `outputField` | all | Spreadsheet column name, also the CSV header. Unique. |
| `mappingType` | all | `DIRECT`, `HARDCODE`, `TRANSFORM`, `ENRICH`, `NOT_APPLICABLE` |
| `inputField` | DIRECT | Field read from the input message |
| `constantRef` | HARDCODE | Key in `sftr.constants.values` |
| `handler` | optional, TRANSFORM/ENRICH | Lambda name; defaults to `outputField` |
| `status` | optional | `MAPPED` (default), `PENDING`, `IGNORE` |
| `description` | optional | Short note for readers. **Never** a constant value or code list. |

Unknown properties in the file are an error (so a stale `constantValue` is caught).

### 6.2 Seed file (16 rows, spreadsheet order)

```json
[
  { "outputField": "IM_SubmitterID",   "mappingType": "HARDCODE", "constantRef": "submitterId" },
  { "outputField": "IM_ParticipantID", "mappingType": "HARDCODE", "constantRef": "participantId" },
  { "outputField": "IM_RepCpCodeType", "mappingType": "HARDCODE", "constantRef": "repCpCodeType" },
  { "outputField": "CP_RepCp",         "mappingType": "ENRICH" },
  { "outputField": "CP_NatureOfRepCp", "mappingType": "ENRICH" },
  { "outputField": "IM_NatureOfOtherCp", "mappingType": "ENRICH", "status": "PENDING" },
  { "outputField": "CP_SecOfRepCp",    "mappingType": "ENRICH" },
  { "outputField": "CP_AdditionalSecClass", "mappingType": "NOT_APPLICABLE" },
  { "outputField": "IM_BranchOfRepCpType", "mappingType": "TRANSFORM", "status": "PENDING" },
  { "outputField": "CP_BranchOfRepCp", "mappingType": "TRANSFORM" },
  { "outputField": "IM_BranchOfOtherCpType", "mappingType": "TRANSFORM", "status": "IGNORE" },
  { "outputField": "CP_BranchOfOtherCp", "mappingType": "DIRECT", "inputField": "cparty_branch_cd", "status": "IGNORE" },
  { "outputField": "CP_CpSide",        "mappingType": "TRANSFORM" },
  { "outputField": "CP_EntResponsibleForRep", "mappingType": "ENRICH", "handler": "CP_RepCp" },
  { "outputField": "IM_OtherCptyEntResponsibleForRep", "mappingType": "ENRICH", "status": "PENDING" },
  { "outputField": "IM_OtherCpType",   "mappingType": "TRANSFORM", "status": "PENDING" }
]
```

Result: 9 rows run (3 HARDCODE and 6 lambda-based, listed in section 8) and the other 7 rows are skipped. The `PENDING` TRANSFORM/ENRICH rows have **no lambda registered on purpose**: startup must still succeed (requirement R5).

---

## 7. Class specifications

### 7.1 `SftrMappingApplication`

```java
@SpringBootApplication
@EnableConfigurationProperties({SftrConstants.class, RefDataClient.Properties.class})
public class SftrMappingApplication {
    public static void main(String[] args) { SpringApplication.run(SftrMappingApplication.class, args); }
}
```

### 7.2 `model.FieldMapping`

```java
public record FieldMapping(
        String outputField,
        Type mappingType,
        String inputField,
        String constantRef,
        String handler,
        Status status,
        String description) {

    public enum Type   { DIRECT, HARDCODE, TRANSFORM, ENRICH, NOT_APPLICABLE }
    public enum Status { MAPPED, PENDING, IGNORE }

    public FieldMapping {
        if (status == null) status = Status.MAPPED;
    }

    public String handlerName() { return handler != null ? handler : outputField; }
}
```

### 7.3 `model.MappingFunction`

```java
@FunctionalInterface
public interface MappingFunction {

    Object apply(JsonNode message, ProcessingContext ctx);

    /** Null-safe text read: a missing field or JSON null returns null (never ""). */
    static String text(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }
}
```

### 7.4 `model.ProcessingContext`

```java
public record ProcessingContext(JsonNode refData, SftrConstants constants, String fileDate) {}
```
Immutable, created once per record. `refData` is the raw response of the single reference-data call (an empty object if the response had no body).

### 7.5 `model.SftrConstants`

```java
@ConfigurationProperties(prefix = "sftr.constants")
public final class SftrConstants {

    private final Map<String, String> values;
    private final Map<String, List<String>> lists;
    private final Map<String, Map<String, String>> codeToValue;   // lookups inverted to code -> result

    public SftrConstants(Map<String, String> values,
                         Map<String, List<String>> lists,
                         Map<String, Map<String, List<String>>> lookups) {
        this.values = Map.copyOf(values == null ? Map.of() : values);
        this.lists = Map.copyOf(lists == null ? Map.of() : lists);
        this.codeToValue = invert(lookups == null ? Map.of() : lookups);
    }

    public String value(String key)        { return required(values, "values", key); }
    public List<String> list(String key)   { return required(lists, "lists", key); }

    /** The table is checked first, so a missing table fails even when code is null. */
    public String lookup(String table, String code) {
        Map<String, String> t = required(codeToValue, "lookups", table);
        return code == null ? null : t.get(code);
    }

    public boolean startsWithAny(String listKey, String text) {
        List<String> prefixes = list(listKey);
        return text != null && prefixes.stream().anyMatch(text::startsWith);
    }

    public static class MissingConstantException extends IllegalStateException {
        MissingConstantException(String path) { super("Missing constant: sftr.constants." + path); }
    }

    private static <T> T required(Map<String, T> map, String section, String key) {
        T v = map.get(key);
        if (v == null) throw new MissingConstantException(section + "." + key);
        return v;
    }

    /** GIVE: [PO, DP]  becomes  PO -> GIVE, DP -> GIVE. A code under two different results fails startup. */
    private static Map<String, Map<String, String>> invert(Map<String, Map<String, List<String>>> lookups) {
        Map<String, Map<String, String>> out = new HashMap<>();
        lookups.forEach((table, byResult) -> {
            Map<String, String> codes = new HashMap<>();
            byResult.forEach((result, list) -> list.forEach(code -> {
                String previous = codes.put(code, result);
                if (previous != null && !previous.equals(result)) {
                    throw new IllegalStateException("lookups." + table + ": code '" + code
                            + "' appears under both '" + previous + "' and '" + result + "'");
                }
            }));
            out.put(table, Map.copyOf(codes));
        });
        return Map.copyOf(out);
    }
}
```

### 7.6 `model.SftrTransactionReport` (placeholder)

Plain class with Lombok `@Getter @Setter`, one `private String` field per seed column, named by rule R3:

`imSubmitterID, imParticipantID, imRepCpCodeType, cpRepCp, cpNatureOfRepCp, imNatureOfOtherCp, cpSecOfRepCp, cpAdditionalSecClass, imBranchOfRepCpType, cpBranchOfRepCp, imBranchOfOtherCpType, cpBranchOfOtherCp, cpCpSide, cpEntResponsibleForRep, imOtherCptyEntResponsibleForRep, imOtherCpType`

No `toString()` (it would expose report data in logs). The owner replaces this class later with the real one; nothing else may depend on its field list.

### 7.7 `functions.RefDataExtractors`

```java
public final class RefDataExtractors {
    private RefDataExtractors() {}

    /**
     * Walks a dotted path such as "find_sftr_party.values.lei" through the response.
     * Returns null if any step is missing, JSON null, not a value node, or the text is blank.
     */
    public static String at(JsonNode refData, String dottedPath) {
        if (refData == null || dottedPath == null) return null;
        JsonNode node = refData;
        for (String part : dottedPath.split("\\.")) {
            node = node.get(part);
            if (node == null || node.isNull()) return null;
        }
        if (!node.isValueNode()) return null;
        String text = node.asText();
        return text.isBlank() ? null : text;
    }

    public static String partyLei(JsonNode refData, SftrConstants c)     { return at(refData, c.value("refPathPartyLei")); }
    public static String partyNature(JsonNode refData, SftrConstants c)  { return at(refData, c.value("refPathPartyNature")); }
    public static String partySector(JsonNode refData, SftrConstants c)  { return at(refData, c.value("refPathPartySector")); }
}
```
The response paths come from YAML (`refPath...` values). Each extractor reads its path **before** touching the response, so a missing constant fails the startup smoke-run even with empty reference data. Adding an enrichment means: add a path constant, add one extractor line, use it in a lambda.

### 7.8 `functions.SftrMappingFunctions`

```java
@Component
public class SftrMappingFunctions {

    private final Map<String, MappingFunction> byName = new LinkedHashMap<>();

    public SftrMappingFunctions() {

        // ===== Trade =====
        register("CP_CpSide", (msg, ctx) ->
                ctx.constants().lookup("cpSide", MappingFunction.text(msg, "trade_type")));

        // ===== Reporting counterparty (reference-data enrichment) =====
        register("CP_RepCp", (msg, ctx) ->
                RefDataExtractors.partyLei(ctx.refData(), ctx.constants()));

        register("CP_NatureOfRepCp", (msg, ctx) -> firstNonBlank(
                RefDataExtractors.partyNature(ctx.refData(), ctx.constants()),
                ctx.constants().value("natureOfRepCpDefault")));

        register("CP_SecOfRepCp", (msg, ctx) ->
                RefDataExtractors.partySector(ctx.refData(), ctx.constants()));

        // ===== Branch logic =====
        register("CP_BranchOfRepCp", (msg, ctx) -> {
            String legalEntity = MappingFunction.text(msg, "bmoLegalEntity");
            return ctx.constants().startsWithAny("londonLegalEntityPrefixes", legalEntity)
                    ? ctx.constants().value("londonBranchCountry")
                    : null;
        });

        // add new lambdas here, grouped by business area, in spreadsheet order
    }

    public Optional<MappingFunction> find(String name) { return Optional.ofNullable(byName.get(name)); }

    /** protected so tests can register extra lambdas in a subclass. */
    protected void register(String name, MappingFunction fn) {
        if (byName.putIfAbsent(name, fn) != null) {
            throw new IllegalStateException("Duplicate mapping function: " + name);
        }
    }

    private static String firstNonBlank(String... candidates) {
        for (String c : candidates) if (c != null && !c.isBlank()) return c;
        return null;
    }
}
```

Rules for every lambda:
- Pure function of `(msg, ctx)`; no state, no I/O.
- Tolerates a missing message field and empty reference data (returns `null` or a documented default).
- Contains no business literal (section 5.2). Review rule: if a lambda contains a quoted code, country, prefix or default, move it to YAML.
- Stays short. Longer logic becomes a private method in this class or a helper in `RefDataExtractors`.
- If this file passes about 1,500 lines, the registry may be split by business area later; lookup by name makes that a mechanical change.

### 7.9 `refdata.RefDataClient`

```java
@Component
public class RefDataClient {

    @ConfigurationProperties(prefix = "sftr.refdata")
    public record Properties(
            String baseUrl,
            String path,
            @DefaultValue("2s") Duration connectTimeout,
            @DefaultValue("5s") Duration readTimeout,
            @DefaultValue("3") int maxAttempts,
            @DefaultValue("200ms") Duration backoff,
            @DefaultValue Map<String, String> requestFields) {}

    private final Properties p;
    private final RestClient http;

    public RefDataClient(Properties p) {
        this.p = p;
        HttpClient jdk = HttpClient.newBuilder().connectTimeout(p.connectTimeout()).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdk);
        factory.setReadTimeout(p.readTimeout());
        this.http = RestClient.builder().baseUrl(p.baseUrl()).requestFactory(factory).build();
    }

    public JsonNode fetch(JsonNode message) {
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        p.requestFields().forEach((requestKey, messageField) -> {
            String v = MappingFunction.text(message, messageField);
            if (v != null) body.put(requestKey, v);              // null values are omitted
        });

        RuntimeException last = null;
        for (int attempt = 1; attempt <= p.maxAttempts(); attempt++) {
            try {
                JsonNode r = http.post().uri(p.path())
                        .contentType(MediaType.APPLICATION_JSON).body(body)
                        .retrieve().body(JsonNode.class);
                return r != null ? r : JsonNodeFactory.instance.objectNode();
            } catch (HttpClientErrorException e) {
                throw e;                                          // 4xx: never retried
            } catch (HttpServerErrorException | ResourceAccessException e) {
                last = e;                                         // 5xx and I/O timeouts: retried
                if (attempt < p.maxAttempts()) pause(p.backoff().toMillis() * attempt);
            }
        }
        throw new IllegalStateException("Reference data call failed after " + p.maxAttempts() + " attempts", last);
    }

    private static void pause(long millis) {
        try { Thread.sleep(millis); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
```
Validate at construction that `maxAttempts >= 1`, `baseUrl` and `path` are non-blank. The request keys and the message fields they come from are configuration (`request-fields`), not Java.

**Response shape assumed by the seed extractors:** an object whose top-level keys are dataset names, each holding a `values` object (for example `{"find_sftr_party":{"values":{"lei":"..."}}}`), already scoped to the record that was sent. If the real response differs, change only the `refPath...` constants (or `RefDataExtractors.at`).

### 7.10 `engine.MappingConfigLoader`

Responsibilities: read the JSON, validate every row, build `Column` for every row and `CompiledMapping` for active rows only, run the smoke-run, fail once with all errors.

```java
@Component
public class MappingConfigLoader {

    /** One CSV column. Every config row has one, active or skipped. */
    public record Column(String name, Field field) {
        public Object read(SftrTransactionReport report) {
            try { return field.get(report); }
            catch (IllegalAccessException e) { throw new IllegalStateException(e); }
        }
    }

    /** One executable step. Exists only for active rows. */
    public record CompiledMapping(String column, Field target, MappingFunction fn, ConversionService conversion) {
        public Object value(JsonNode msg, ProcessingContext ctx) { return fn.apply(msg, ctx); }
        public void set(SftrTransactionReport report, Object value) throws IllegalAccessException {
            Object v = (value == null || target.getType().isInstance(value))
                    ? value : conversion.convert(value, target.getType());
            target.set(report, v);
        }
    }

    public record CompiledConfig(List<Column> columns, List<CompiledMapping> active, int skipped) {}

    private static final JsonNode EMPTY = JsonNodeFactory.instance.objectNode();

    private final ObjectMapper mapper;
    private final SftrMappingFunctions functions;
    private final SftrConstants constants;
    private final Resource configResource;
    private final ConversionService conversion = DefaultConversionService.getSharedInstance();

    public MappingConfigLoader(ObjectMapper mapper, SftrMappingFunctions functions, SftrConstants constants,
                               @Value("${sftr.mapping.config}") Resource configResource) { /* assign */ }

    public CompiledConfig load() {
        List<FieldMapping> rows = read();
        List<String> errors = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        List<CompiledMapping> active = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (FieldMapping m : rows) {
            try {
                if (!seen.add(m.outputField())) throw new IllegalArgumentException("duplicate outputField");
                Field target = reportField(m.outputField());           // validated for EVERY row
                columns.add(new Column(m.outputField(), target));
                if (isActive(m)) {
                    CompiledMapping compiled = compile(m, target);     // skipped rows are never compiled
                    smokeRun(compiled);
                    active.add(compiled);
                }
            } catch (RuntimeException e) {
                errors.add(m.outputField() + ": " + e.getMessage());
            }
        }
        if (!errors.isEmpty()) {
            throw new IllegalStateException("Invalid SFTR mapping config:\n" + String.join("\n", errors));
        }
        return new CompiledConfig(List.copyOf(columns), List.copyOf(active), columns.size() - active.size());
    }

    /** Only MAPPED rows that do real work run. PENDING, IGNORE and NOT_APPLICABLE are skipped. */
    private static boolean isActive(FieldMapping m) {
        return m.status() == FieldMapping.Status.MAPPED && m.mappingType() != FieldMapping.Type.NOT_APPLICABLE;
    }

    private CompiledMapping compile(FieldMapping m, Field target) {
        MappingFunction fn = switch (m.mappingType()) {
            case DIRECT -> {
                String in = require(m.inputField(), "inputField");
                yield (msg, ctx) -> MappingFunction.text(msg, in);
            }
            case HARDCODE -> {
                String c = constants.value(require(m.constantRef(), "constantRef"));   // resolved once, at startup
                yield (msg, ctx) -> c;
            }
            case TRANSFORM, ENRICH -> functions.find(m.handlerName())
                    .orElseThrow(() -> new IllegalArgumentException("no lambda registered as '" + m.handlerName() + "'"));
            case NOT_APPLICABLE -> throw new IllegalStateException("skipped rows are never compiled");
        };
        return new CompiledMapping(m.outputField(), target, fn, conversion);
    }

    /** Fails startup only if a lambda asks for a constant that does not exist. */
    private void smokeRun(CompiledMapping m) {
        try {
            m.fn().apply(EMPTY, new ProcessingContext(EMPTY, constants, null));
        } catch (SftrConstants.MissingConstantException e) {
            throw e;
        } catch (RuntimeException ignored) {
            // other failures are not what this check is for
        }
    }

    static String propertyName(String outputField) {                    // rule R3
        int i = outputField.indexOf('_');
        if (i < 0) return Character.toLowerCase(outputField.charAt(0)) + outputField.substring(1);
        return outputField.substring(0, i).toLowerCase(Locale.ROOT) + outputField.substring(i + 1).replace("_", "");
    }

    private Field reportField(String outputField) {
        String name = propertyName(outputField);
        try {
            Field f = SftrTransactionReport.class.getDeclaredField(name);
            f.setAccessible(true);                                       // once, at startup
            return f;
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("no field '" + name + "' on SftrTransactionReport");
        }
    }

    // read(): mapper.copy().enable(FAIL_ON_UNKNOWN_PROPERTIES).readValue(stream, new TypeReference<List<FieldMapping>>() {})
    //         wrap IOException in IllegalStateException
    // require(value, propertyName): throws IllegalArgumentException("<propertyName> is required") if null or blank
}
```

### 7.11 `engine.RecordProcessor`

```java
@Component
public class RecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

    private final List<MappingConfigLoader.Column> columns;                // every row, config order
    private final List<MappingConfigLoader.CompiledMapping> mappings;      // active rows only

    public RecordProcessor(MappingConfigLoader loader) {
        MappingConfigLoader.CompiledConfig cfg = loader.load();
        this.columns = cfg.columns();
        this.mappings = cfg.active();
        log.info("SFTR mappings: {} columns, {} active, {} skipped (PENDING/IGNORE/NOT_APPLICABLE)",
                columns.size(), mappings.size(), cfg.skipped());
    }

    public SftrTransactionReport process(JsonNode message, ProcessingContext ctx) {
        SftrTransactionReport report = new SftrTransactionReport();
        for (MappingConfigLoader.CompiledMapping m : mappings) {
            try {
                m.set(report, m.value(message, ctx));
            } catch (Exception e) {
                throw new IllegalStateException("Mapping failed for column " + m.column(), e);
            }
        }
        return report;
    }

    public List<MappingConfigLoader.Column> columnDefs() { return columns; }
}
```
`process` loops over active mappings only. Skipped columns are never touched, so their fields keep the default `null`.

### 7.12 `output.SftrCsvWriter`

- Uses Apache Commons CSV.
- Format: delimiter from `sftr.csv.delimiter` (default `,`), record separator `\n`, UTF-8, header = column names in config order, quoting by the library (never string concatenation).
- `Session open(Path file, List<Column> columns)` writes the header and returns a `Session` (nested static class implementing `AutoCloseable`) with `write(SftrTransactionReport)` and `close()`.
- A row is built by `column.read(report)` for **every** column; `null` becomes an empty cell; other values use `String.valueOf`.

```java
@Component
public class SftrCsvWriter {
    private final char delimiter;
    public SftrCsvWriter(@Value("${sftr.csv.delimiter:,}") char delimiter) { this.delimiter = delimiter; }

    public Session open(Path file, List<MappingConfigLoader.Column> columns) throws IOException { /* header + printer */ }

    public static final class Session implements AutoCloseable {
        public void write(SftrTransactionReport report) throws IOException { /* printRecord */ }
        @Override public void close() throws IOException { /* flush and close */ }
    }
}
```

### 7.13 `service.SftrReportService`

Behaviour of `RunResult run(Path input, Path output, String fileDate)`:
1. Read the input file line by line (UTF-8), keeping the 1-based line number. Skip blank lines. Count each non-blank line as read.
2. For each line submit one task to a virtual-thread executor. The task: parse the JSON (`ObjectMapper.readTree`); acquire a permit from the reference-data semaphore; call `RefDataClient.fetch`; release the permit; build `ProcessingContext(refData, constants, fileDate)`; call `RecordProcessor.process`.
3. Collect tasks in windows of `window-size`. When a window is full (and at end of file), take results **in submission order**: on success write to the CSV; on failure write a rejects line.
4. Rejects file: `<output>.rejects`. Each line is `<lineNo><TAB><error summary>`. It never contains the input record. Delete the file at the end if no record was rejected.
5. Return `RunResult(read, written, rejected)` and log the three counts.
6. If the thread is interrupted while waiting for a result, restore the interrupt flag and fail the run.

```java
@Service
public class SftrReportService {

    public record RunResult(long read, long written, long rejected) {}

    private record Pending(long lineNo, Future<SftrTransactionReport> future) {}

    // fields: RefDataClient, RecordProcessor, SftrCsvWriter, SftrConstants, ObjectMapper, windowSize, Semaphore apiPermits
    // constructor: @Value("${sftr.processing.window-size:1000}") int windowSize,
    //              @Value("${sftr.processing.max-concurrent-ref-data-calls:20}") int maxConcurrentCalls

    public RunResult run(Path input, Path output, String fileDate) throws IOException { /* as described above */ }

    private JsonNode fetchLimited(JsonNode message) throws InterruptedException {
        apiPermits.acquire();
        try { return refData.fetch(message); } finally { apiPermits.release(); }
    }
}
```
The semaphore wraps **only** the reference-data call. Virtual threads make it easy to overload a downstream service, so this limit, not a thread count, is the throttle.

### 7.14 `runner.SftrBatchRunner`

```java
@Component
@ConditionalOnProperty(name = "sftr.runner.enabled", havingValue = "true")
public class SftrBatchRunner implements ApplicationRunner {
    // injects SftrReportService and @Value paths: sftr.runner.input, sftr.runner.output, sftr.runner.file-date
    @Override public void run(ApplicationArguments args) throws Exception {
        String fileDate = (configured date is blank) ? LocalDate.now().toString() : configured date;
        RunResult r = service.run(Path.of(input), Path.of(output), fileDate);
        log.info("SFTR run finished: read={}, written={}, rejected={}", r.read(), r.written(), r.rejected());
    }
}
```
Any exception makes the process exit non-zero. A run with rejected records still exits 0; the rejects file is the signal.

---

## 8. Seed mapping: where each row comes from

Source: the first 16 rows of the business spreadsheet (read from screenshots, so verify against the file).

| Output column | Rule in the spreadsheet | Status in spec | Implementation |
|---|---|---|---|
| IM_SubmitterID | Hardcode | MAPPED | HARDCODE `submitterId` |
| IM_ParticipantID | Hardcode | MAPPED | HARDCODE `participantId` |
| IM_RepCpCodeType | Hardcode to LEI | MAPPED | HARDCODE `repCpCodeType` |
| CP_RepCp | Enrich-CDER: LEI from party lookup | MAPPED | lambda `CP_RepCp` |
| CP_NatureOfRepCp | Enrich/Hardcode: nature from party-sector lookup, otherwise the default | MAPPED | lambda `CP_NatureOfRepCp` (default `natureOfRepCpDefault`) |
| IM_NatureOfOtherCp | Enrich-CDER by counterparty or group, depends on delegation | PENDING (no DEV status) | skipped |
| CP_SecOfRepCp | Enrich-CDER: sector | MAPPED | lambda `CP_SecOfRepCp` |
| CP_AdditionalSecClass | Not applicable | NOT_APPLICABLE | skipped |
| IM_BranchOfRepCpType | Logic: country code if another field is populated | PENDING | skipped |
| CP_BranchOfRepCp | Logic: GB if legal entity is LDNBB or LDN-, else null | MAPPED | lambda `CP_BranchOfRepCp` |
| IM_BranchOfOtherCpType | Logic | IGNORE | skipped |
| CP_BranchOfOtherCp | Direct from `cparty_branch_cd` | IGNORE | skipped |
| CP_CpSide | Logic from `trade_type` (GIVE/TAKE code tables) | MAPPED | lambda `CP_CpSide` |
| CP_EntResponsibleForRep | Same value as the reporting counterparty | MAPPED | shares lambda `CP_RepCp` (`handler`) |
| IM_OtherCptyEntResponsibleForRep | Enrich-CDER by counterparty or group | PENDING (no DEV status) | skipped |
| IM_OtherCpType | Logic: LEI or internal depending on enrichment | PENDING | skipped |

Active rows: 9 (IM_SubmitterID, IM_ParticipantID, IM_RepCpCodeType, CP_RepCp, CP_NatureOfRepCp, CP_SecOfRepCp, CP_BranchOfRepCp, CP_CpSide, CP_EntResponsibleForRep). Skipped rows: 7. Startup log must read: `16 columns, 9 active, 7 skipped`.

The DIRECT path is not active in the seed (its only row is IGNORE), so it is covered by test configurations (section 11).

Interpretations to confirm are listed in section 14.

---

## 9. Processing flow

```text
Input line (JSON)
   |
   |  [virtual thread, one per record, windowed]
   v
parse JSON -> RefDataClient.fetch(message)   (semaphore, retry on 5xx/timeout)
   |
   v
ProcessingContext(refData, constants, fileDate)
   |
   v
RecordProcessor.process: for each ACTIVE mapping -> lambda -> set report field
   |
   v
SftrTransactionReport  ->  window result list (in input order)  ->  SftrCsvWriter (single writer)
   \--> on any failure: rejects file line (line number + error summary)
```

Startup (once):
```text
sftr-field-mappings.json + SftrConstants + SftrMappingFunctions
   -> MappingConfigLoader: validate all rows, compile active rows, smoke-run
   -> RecordProcessor (immutable lists)
```

Concurrency rules:
- Tasks are independent; no shared mutable state.
- Output order equals input order because results of a window are consumed in submission order by one writer thread.
- Memory is bounded by `window-size` reports at a time.

---

## 10. Error handling

| Situation | Behaviour |
|---|---|
| Invalid config at startup | Application fails once with all errors listed (R9, R10) |
| Malformed JSON line | Record rejected, run continues |
| Reference data timeout or 5xx | Retry up to `max-attempts`, then reject record |
| Reference data 4xx | Reject record immediately |
| Lookup finds nothing | Not an error: the extractor returns `null`, the lambda decides the fallback |
| Lambda throws | Record rejected; the error names the column (`Mapping failed for column X`) |
| Report field type differs from the value type | Value converted with Spring `ConversionService`; failure rejects the record |
| Input file missing | Run fails with a clear exception |

Never emit a CSV row with silently missing enrichment caused by a failed call.

---

## 11. Tests

Use JUnit 5, AssertJ and Mockito from `spring-boot-starter-test`. Use `@MockitoBean` (not the removed `@MockBean`).

| Test class | What it proves |
|---|---|
| `SftrConstantsTest` | Binds from YAML; `lookup("cpSide","PO")` is `GIVE`, `"LC"` is `TAKE`, unknown or null code is `null`; missing table, value or list throws `MissingConstantException`; a code under two results fails construction; `startsWithAny` true for `LDNBB01` and `LDN-X`, false for `NYC1` and null |
| `RefDataExtractorsTest` | `at` returns a value; returns `null` for a missing step, JSON null, an object node, blank text; extractors use the path from constants |
| `SftrMappingFunctionsTest` | Table-driven for each seed lambda: side for several trade types (GIVE, TAKE, unknown, missing); LEI/nature/sector from sample reference data; nature falls back to the default when absent; branch is `GB` for `LDNBB01` and `LDN-1`, `null` for others and for missing; every lambda tolerates an empty message and empty reference data |
| `MappingConfigLoaderTest` | Valid config loads; errors are all reported together for: duplicate column, unknown report property, DIRECT without `inputField`, HARDCODE without `constantRef`, unknown `constantRef`, unknown handler, unknown JSON property; a `PENDING` row with a non-existent handler and a non-existent constant does **not** fail; a skipped row with an unknown report property **does** fail; smoke-run fails when a test subclass of `SftrMappingFunctions` registers a lambda using a constant key that does not exist; `propertyName("CP_RepCp")` is `cpRepCp`, `"IM_SubmitterID"` is `imSubmitterID` |
| `RecordProcessorTest` | With the seed config and sample inputs, the 9 active fields are set; the 7 skipped fields are `null`; with a test config containing a DIRECT row, the value is copied and an absent input field gives `null`; a test subclass registers a counting lambda bound to a `PENDING` row and the count stays 0; a lambda that throws produces an error naming the column |
| `SftrCsvWriterTest` | Header equals config column order; skipped columns present and empty; values containing the delimiter, quotes and newlines are quoted correctly; delimiter `;` is honoured; file is UTF-8 |
| `RefDataClientTest` | Uses `com.sun.net.httpserver.HttpServer` on an ephemeral port: 503 twice then 200 succeeds after 3 attempts; 400 is not retried (exactly one request); persistent 503 fails after `max-attempts`; null message fields are omitted from the request body; request keys follow `request-fields`; read timeout triggers retry |
| `SftrReportServiceIT` | `@SpringBootTest` with `@MockitoBean RefDataClient`. 50 input lines including one malformed JSON line and one record whose reference-data call fails: CSV has 48 data rows in input order, rejects file lists exactly those two line numbers and contains no record data, `RunResult` is (50, 48, 2). Concurrency check: with the mock recording the maximum number of simultaneous calls, that maximum never exceeds `max-concurrent-ref-data-calls`. Determinism check: runs with limit 1 and limit 50 produce byte-identical CSV files |
| `GoldenFileTest` | Sample input file through the whole pipeline equals `src/test/resources/golden/expected.csv` byte for byte |
| `ConfigConsistencyTest` | Real `mapper/sftr-field-mappings.json` and real `application.yml` load without error against the real `SftrTransactionReport`; every `constantRef` exists; prints the report fields that have no row. Fails on missing rows only when run with `-Dsftr.strict=true` (so the report can be replaced and filled in gradually) |

### 11.1 Test fixtures

Input line (`src/test/resources/golden/input.jsonl`):
```json
{"trade_ref":"T1","trade_type":"PO","bmoLegalEntity":"LDNBB01","counterpartyId":"CP1","group_cd":"G1"}
```

Reference-data response used by the golden test (`RefDataClient` mocked to return it):
```json
{"find_sftr_party":{"values":{"lei":"549300ABCDEFGHIJ0001"}},
 "find_sftr_party_sector":{"values":{"natureOfReportingCounterparty":"C","sector":"CDTI"}}}
```

Expected `golden/expected.csv` (header plus one row, `\n` line endings, trailing newline):
```csv
IM_SubmitterID,IM_ParticipantID,IM_RepCpCodeType,CP_RepCp,CP_NatureOfRepCp,IM_NatureOfOtherCp,CP_SecOfRepCp,CP_AdditionalSecClass,IM_BranchOfRepCpType,CP_BranchOfRepCp,IM_BranchOfOtherCpType,CP_BranchOfOtherCp,CP_CpSide,CP_EntResponsibleForRep,IM_OtherCptyEntResponsibleForRep,IM_OtherCpType
BMOLN9000003,BMOLN9000003,LEI,549300ABCDEFGHIJ0001,C,,CDTI,,,GB,,,GIVE,549300ABCDEFGHIJ0001,,
```

Test configuration files go in `src/test/resources` (for example `mappings/valid.json`, `mappings/invalid-all.json`, `mappings/direct.json`). The test constants YAML must be a copy of the real constants so lambda tests exercise real values; do not hand-write maps in tests.

---

## 12. Build and run

```bash
mvn -N wrapper:wrapper          # optional, creates mvnw
mvn verify                      # compile, unit and integration tests
java -jar target/sftr-mapping-engine.jar \
     --sftr.runner.enabled=true \
     --sftr.runner.input=./data/input.jsonl \
     --sftr.runner.output=./data/sftr-report.csv \
     --sftr.refdata.base-url=http://localhost:8089
```

Add a `.gitignore` (target, IDE files, `data/`).

---

## 13. Definition of done

1. `mvn verify` passes with no skipped tests.
2. Application starts with the seed config and logs `16 columns, 9 active, 7 skipped`.
3. The golden test passes.
4. Changing a `PENDING` row to a non-existent handler still starts; changing an active row to a non-existent handler fails startup with that row listed.
5. Removing `sftr.constants.lookups.cpSide` from the YAML fails startup (smoke-run) naming the missing constant.
6. `grep` over `src/main/java` finds none of the seed business literals: `GIVE`, `TAKE`, `LDNBB`, `LDN-`, `BMOLN9000003`, `LEI` (as a value), `"GB"`. Input field names and constant keys are allowed.
7. No class exists outside the closed list in section 4 (nested records, enums, static classes and private helpers are fine).
8. No `synchronized`, no logging of message or report content.
9. Output CSV is identical for concurrency limits 1 and 50.

---

## 14. Assumptions and defaults (confirm later, none blocks the build)

| # | Assumption / default taken | If wrong |
|---|---|---|
| 1 | Reference-data response shape: top-level dataset keys, each with a `values` object, scoped to the record sent | Change `refPath...` constants or `RefDataExtractors.at` |
| 2 | The three party lookups use the paths in the seed YAML; the spreadsheet shows two variants for sector (`...values.sector` and `...values.sectorOfReportingCounterparty`) | Change `refPathPartySector` |
| 3 | `CP_NatureOfRepCp`: use the looked-up nature, otherwise default `F` | Adjust the lambda and `natureOfRepCpDefault` |
| 4 | `CP_BranchOfRepCp`: "LDNBB or LDN-" means starts-with | For exact match, add an accessor `containsExact(listKey, text)` to `SftrConstants` |
| 5 | `CP_EntResponsibleForRep` equals `CP_RepCp` | Give it its own lambda |
| 6 | `CP_CpSide` uses one table combining ICI and G1 codes (no overlap) | Split into two tables and choose in the lambda |
| 7 | All report fields are `String` | The engine converts at set time if a field is another type; lambdas must return convertible values |
| 8 | Property naming: ID suffixes keep their case (`imSubmitterID`) | Adjust `propertyName` and R3 |
| 9 | Input field names inside lambdas stay in Java | If they must be configurable, add an `inputFields` list per row later |
| 10 | Rejects file holds line number and error only, no record data | Add an opt-in property if operations need the record |
| 11 | Input is JSON Lines | The service takes a path today; a listener can call `RecordProcessor` directly |
| 12 | Delimiter `,`, UTF-8, `\n` line endings | Set in `application.yml` / `SftrCsvWriter` |

---

## 15. How to extend (the daily workflow)

| To add or change | Do this |
|---|---|
| A DIRECT column | Add the report field; add a JSON row with `inputField` |
| A HARDCODE column | Add the report field; add the value to `sftr.constants.values`; add a JSON row with `constantRef` |
| A TRANSFORM column | Add the report field; add constants to YAML if needed; add a lambda in `SftrMappingFunctions`; add a JSON row; add a table-driven test |
| An ENRICH column | Add the report field; add a `refPath...` constant; add one line in `RefDataExtractors`; add a lambda; add the request key to `request-fields` if the call needs a new key; add a JSON row and a test with a saved response |
| Switch on a pending column | Make sure its lambda and constants exist, then change `status` to `MAPPED` (or remove it) |
| Skip a column | Set `status` to `PENDING` or `IGNORE`; nothing else changes |
| Share a lambda between two columns | Set `handler` on the second row |

Order of work for the 200+ columns: HARDCODE and DIRECT rows first (no Java), then TRANSFORM, then ENRICH.

---

## 16. Possible later work (not part of this build)

- A Kafka or REST listener that calls `RecordProcessor` per message.
- A script that generates JSON rows and YAML constants from the spreadsheet, copying its status column.
- A short-lived cache for reference-data responses keyed by request fields (only if measurements show too many calls).
- Per-feed rule sets, by adding a `feed` property to `FieldMapping` and one compiled list per feed.
