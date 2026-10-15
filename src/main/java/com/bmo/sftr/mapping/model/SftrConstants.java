package com.bmo.sftr.mapping.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

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

    public String value(String key) {
        return required(values, "values", key);
    }

    public List<String> list(String key) {
        return required(lists, "lists", key);
    }

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
        MissingConstantException(String path) {
            super("Missing constant: sftr.constants." + path);
        }
    }

    private static <T> T required(Map<String, T> map, String section, String key) {
        T v = map.get(key);
        if (v == null) throw new MissingConstantException(section + "." + key);
        return v;
    }

    /** GIVE: [PO, DP] becomes PO -> GIVE, DP -> GIVE. A code under two different results fails startup. */
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
