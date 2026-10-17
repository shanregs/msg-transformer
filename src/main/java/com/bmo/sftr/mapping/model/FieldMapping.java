package com.bmo.sftr.mapping.model;

public record FieldMapping(
        String outputField,
        MappingType mappingType,
        String inputField,
        String constantRef,
        String handler,
        Status status,
        String description,
        DataFormatter formatter,
        String inputFormatRef,
        String outputFormatRef,
        String inputZoneRef,
        String outputZoneRef) {

    public enum MappingType { DIRECT, HARDCODE, TRANSFORM, ENRICH, LOGIC, DIRECT_DATA_TRANSFORM, NOT_APPLICABLE }

    /** Target type of a DIRECT_DATA_TRANSFORM row. */
    public enum DataFormatter { INTEGER, LONG, DECIMAL, DOUBLE, DATE, TIMESTAMP }

    public enum Status { MAPPED, PENDING, IGNORE }

    public FieldMapping {
        if (status == null) status = Status.MAPPED;
    }

    public String handlerName() {
        return handler != null ? handler : outputField;
    }
}
