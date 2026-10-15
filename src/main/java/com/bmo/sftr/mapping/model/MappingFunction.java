package com.bmo.sftr.mapping.model;

import tools.jackson.databind.JsonNode;

@FunctionalInterface
public interface MappingFunction {

    Object apply(JsonNode message, ProcessingContext ctx);

    /** Null-safe text read: a missing field or JSON null returns null (never an empty string). */
    static String text(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }
}
