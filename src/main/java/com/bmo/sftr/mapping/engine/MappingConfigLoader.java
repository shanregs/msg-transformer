package com.bmo.sftr.mapping.engine;

import com.bmo.sftr.mapping.functions.DateHelpers;
import com.bmo.sftr.mapping.functions.SftrMappingFunctions;
import com.bmo.sftr.mapping.model.FieldMapping;
import com.bmo.sftr.mapping.model.MappingFunction;
import com.bmo.sftr.mapping.model.ProcessingContext;
import com.bmo.sftr.mapping.model.SftrConstants;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.JsonNodeFactory;

@Component
public class MappingConfigLoader {

    /** One CSV column. Every config row has one, active or skipped. */
    public record Column(String name, Field field) {
        public Object read(SftrTransactionReport report) {
            try {
                return field.get(report);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** One executable step. Exists only for active rows. */
    public record CompiledMapping(String column, Field target, MappingFunction fn, ConversionService conversion) {
        public Object value(JsonNode msg, ProcessingContext ctx) {
            return fn.apply(msg, ctx);
        }

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
                               @Value("${sftr.mapping.config}") Resource configResource) {
        this.mapper = mapper;
        this.functions = functions;
        this.constants = constants;
        this.configResource = configResource;
    }

    public CompiledConfig load() {
        List<FieldMapping> rows = read();
        List<String> errors = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        List<CompiledMapping> active = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (FieldMapping m : rows) {
            try {
                if (m.outputField() == null || m.outputField().isBlank()) {
                    throw new IllegalArgumentException("outputField is required");
                }
                if (m.mappingType() == null) throw new IllegalArgumentException("mappingType is required");
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
        return m.status() == FieldMapping.Status.MAPPED && m.mappingType() != FieldMapping.MappingType.NOT_APPLICABLE;
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
            case DIRECT_DATA_TRANSFORM -> formatter(m);
            case TRANSFORM, ENRICH, LOGIC -> functions.find(m.handlerName())
                    .orElseThrow(() -> new IllegalArgumentException("no lambda registered as '" + m.handlerName() + "'"));
            case NOT_APPLICABLE -> throw new IllegalStateException("skipped rows are never compiled");
        };
        return new CompiledMapping(m.outputField(), target, fn, conversion);
    }

    /**
     * Builds the conversion for a DIRECT_DATA_TRANSFORM row once, at startup. Patterns and zones are constants
     * (resolved through the *Ref properties). Blank or missing input gives null; a value that cannot be
     * converted throws, without echoing the value (regulatory data).
     */
    private MappingFunction formatter(FieldMapping m) {
        String in = require(m.inputField(), "inputField");
        if (m.formatter() == null) throw new IllegalArgumentException("formatter is required");
        String inPattern = optionalValue(m.inputFormatRef());
        String outPattern = optionalValue(m.outputFormatRef());

        return switch (m.formatter()) {
            case INTEGER, LONG, DECIMAL, DOUBLE -> numberFormatter(in, m.formatter(), inPattern, outPattern);
            case DATE, TIMESTAMP -> {
                ZoneId inZone = optionalZone(m.inputZoneRef());
                ZoneId outZone = optionalZone(m.outputZoneRef());
                DateTimeFormatter inFmt = inPattern == null
                        ? DateHelpers.ISO_FLEXIBLE : DateTimeFormatter.ofPattern(inPattern);
                DateTimeFormatter outFmt = outPattern == null
                        ? (m.formatter() == FieldMapping.DataFormatter.DATE
                                ? DateTimeFormatter.ISO_LOCAL_DATE : DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        : DateTimeFormatter.ofPattern(outPattern);
                yield dateFormatter(in, inFmt, outFmt, inZone, outZone);
            }
        };
    }

    private static MappingFunction numberFormatter(String in, FieldMapping.DataFormatter type,
                                                   String inPattern, String outPattern) {
        if (inPattern != null) new DecimalFormat(inPattern);          // validate the pattern now
        if (outPattern != null) new DecimalFormat(outPattern);
        return (msg, ctx) -> {
            String text = MappingFunction.text(msg, in);
            if (text == null || text.isBlank()) return null;
            try {
                BigDecimal number = parseNumber(text.trim(), inPattern);
                if (outPattern != null) return decimalFormat(outPattern).format(number);
                return switch (type) {
                    case INTEGER -> (Object) number.intValueExact();
                    case LONG -> number.longValueExact();
                    case DECIMAL -> number;
                    default -> number.doubleValue();
                };
            } catch (ArithmeticException | ParseException | NumberFormatException e) {
                throw new IllegalArgumentException("value is not a valid " + type + " number");
            }
        };
    }

    private static BigDecimal parseNumber(String text, String pattern) throws ParseException {
        if (pattern == null) return new BigDecimal(text);
        ParsePosition pos = new ParsePosition(0);
        Number n = decimalFormat(pattern).parse(text, pos);
        if (n == null || pos.getIndex() != text.length()) throw new ParseException("unparseable", pos.getIndex());
        return (BigDecimal) n;
    }

    /** DecimalFormat is not thread-safe, so one instance per call. Fixed symbols: '.' decimal, ',' grouping. */
    private static DecimalFormat decimalFormat(String pattern) {
        DecimalFormat df = new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(Locale.ROOT));
        df.setParseBigDecimal(true);
        return df;
    }

    private static MappingFunction dateFormatter(String in, DateTimeFormatter inFmt, DateTimeFormatter outFmt,
                                                 ZoneId inZone, ZoneId outZone) {
        return (msg, ctx) -> {
            String text = MappingFunction.text(msg, in);
            if (text == null || text.isBlank()) return null;
            try {
                TemporalAccessor parsed = inFmt.parseBest(text.trim(),
                        ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
                ZoneId assumed = inZone != null ? inZone : ZoneOffset.UTC;
                ZonedDateTime zoned = switch (parsed) {
                    case ZonedDateTime z -> z;
                    case LocalDateTime l -> l.atZone(assumed);
                    default -> ((LocalDate) parsed).atStartOfDay(assumed);
                };
                if (outZone != null) zoned = zoned.withZoneSameInstant(outZone);
                return outFmt.format(zoned);
            } catch (DateTimeException e) {
                throw new IllegalArgumentException("value is not a valid date/time for the configured format");
            }
        };
    }

    private String optionalValue(String ref) {
        return ref == null ? null : constants.value(ref);
    }

    private ZoneId optionalZone(String ref) {
        return ref == null ? null : ZoneId.of(constants.value(ref));
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

    private List<FieldMapping> read() {
        ObjectMapper strict = mapper.rebuild().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
        List<FieldMapping> fieldMappings = null;
        try (InputStream in = configResource.getInputStream()) {
            fieldMappings= strict.readValue(in, new TypeReference<List<FieldMapping>>() {});
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read SFTR mapping config " + configResource, e);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Invalid SFTR mapping config " + configResource + ": " + e.getMessage(), e);
        }
        return fieldMappings;
    }

    private static String require(String value, String propertyName) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(propertyName + " is required");
        return value;
    }
}
