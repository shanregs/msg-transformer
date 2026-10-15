package com.bmo.sftr.mapping.functions;

import com.bmo.sftr.mapping.model.SftrConstants;
import tools.jackson.databind.JsonNode;

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

    // Each extractor reads its path BEFORE touching the response, so a missing constant
    // fails the startup smoke-run even with empty reference data.

    public static String partyLei(JsonNode refData, SftrConstants c) {
        return at(refData, c.value("refPathPartyLei"));
    }

    public static String partyNature(JsonNode refData, SftrConstants c) {
        return at(refData, c.value("refPathPartyNature"));
    }

    public static String partySector(JsonNode refData, SftrConstants c) {
        return at(refData, c.value("refPathPartySector"));
    }
}
