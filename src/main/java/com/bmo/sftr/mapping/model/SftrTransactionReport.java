package com.bmo.sftr.mapping.model;

import lombok.Getter;
import lombok.Setter;

/** Placeholder report with the 16 seed columns. No toString(): it would expose report data in logs. */
@Getter
@Setter
public class SftrTransactionReport {
    private String imSubmitterID;
    private String imParticipantID;
    private String imRepCpCodeType;
    private String cpRepCp;
    private String cpNatureOfRepCp;
    private String imNatureOfOtherCp;
    private String cpSecOfRepCp;
    private String cpAdditionalSecClass;
    private String imBranchOfRepCpType;
    private String cpBranchOfRepCp;
    private String imBranchOfOtherCpType;
    private String cpBranchOfOtherCp;
    private String cpCpSide;
    private String cpEntResponsibleForRep;
    private String imOtherCptyEntResponsibleForRep;
    private String imOtherCpType;
}
