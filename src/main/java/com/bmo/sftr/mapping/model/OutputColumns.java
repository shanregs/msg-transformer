package com.bmo.sftr.mapping.model;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

/**
 * The single list of SFTR output column names, spelled exactly as in sftr-field-mappings.json
 * ({@code outputField}). Lambdas in SftrMappingFunctions register against these constants, so a column is
 * never tied to a function through a string literal. Add a constant here when a column is added to the
 * mapping file, in spreadsheet order.
 */
public final class OutputColumns {

    private OutputColumns() {}

    // ===== Submission / message =====
    public static final String IM_SUBMITTER_ID = "IM_SubmitterID";
    public static final String IM_PARTICIPANT_ID = "IM_ParticipantID";
    public static final String IM_REP_CP_CODE_TYPE = "IM_RepCpCodeType";
    public static final String IM_NATURE_OF_OTHER_CP = "IM_NatureOfOtherCp";
    public static final String IM_BRANCH_OF_REP_CP_TYPE = "IM_BranchOfRepCpType";
    public static final String IM_BRANCH_OF_OTHER_CP_TYPE = "IM_BranchOfOtherCpType";
    public static final String IM_OTHER_CPTY_ENT_RESPONSIBLE_FOR_REP = "IM_OtherCptyEntResponsibleForRep";
    public static final String IM_OTHER_CP_TYPE = "IM_OtherCpType";

    // ===== Counterparty =====
    public static final String CP_REP_CP = "CP_RepCp";
    public static final String CP_NATURE_OF_REP_CP = "CP_NatureOfRepCp";
    public static final String CP_SEC_OF_REP_CP = "CP_SecOfRepCp";
    public static final String CP_ADDITIONAL_SEC_CLASS = "CP_AdditionalSecClass";
    public static final String CP_BRANCH_OF_REP_CP = "CP_BranchOfRepCp";
    public static final String CP_BRANCH_OF_OTHER_CP = "CP_BranchOfOtherCp";
    public static final String CP_CP_SIDE = "CP_CpSide";
    public static final String CP_ENT_RESPONSIBLE_FOR_REP = "CP_EntResponsibleForRep";

    /** Every constant above, in declaration order. */
    public static List<String> all() {
        return Arrays.stream(OutputColumns.class.getDeclaredFields())
                .filter(f -> Modifier.isPublic(f.getModifiers()) && Modifier.isStatic(f.getModifiers())
                        && f.getType() == String.class)
                .map(f -> {
                    try {
                        return (String) f.get(null);
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .toList();
    }
}
