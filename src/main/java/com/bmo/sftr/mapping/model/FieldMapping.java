package com.bmo.sftr.mapping.model;

public record FieldMapping(
        String outputField,
        Type mappingType,
        String inputField,
        String constantRef,
        String handler,
        Status status,
        String description,
        Formatter formatter,
        String inputFormatRef,
        String outputFormatRef,
        String inputZoneRef,
        String outputZoneRef) {

    public enum Type { DIRECT, HARDCODE, TRANSFORM, ENRICH, DIRECT_DATA_TRANSFORM, NOT_APPLICABLE }

    /** Target type of a DIRECT_DATA_TRANSFORM row. */
    public enum Formatter { INTEGER, LONG, DECIMAL, DOUBLE, DATE, TIMESTAMP }

    public enum Status { MAPPED, PENDING, IGNORE }

    public FieldMapping {
        if (status == null) status = Status.MAPPED;
    }

    public String handlerName() {
        return handler != null ? handler : outputField;
    }
}
