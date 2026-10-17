package com.bmo.sftr.mapping.output;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.engine.MappingConfigLoader;
import com.bmo.sftr.mapping.engine.MappingConfigLoaderTest;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SftrCsvWriterTest {

    @TempDir
    Path dir;

    private List<MappingConfigLoader.Column> columns() {
        return MappingConfigLoaderTest.loader("mapper/sftr-field-mappings.json").load().columns();
    }

    private String write(char delimiter, SftrTransactionReport... reports) throws Exception {
        Path file = dir.resolve("out.csv");
        try (SftrCsvWriter.Session s = new SftrCsvWriter(delimiter, columns()).open(file)) {
            for (SftrTransactionReport r : reports) s.write(r);
        }
        return Files.readString(file, StandardCharsets.UTF_8);
    }

    @Test
    void headerFollowsConfigOrderAndSkippedColumnsAreEmpty() throws Exception {
        SftrTransactionReport r = new SftrTransactionReport();
        r.setImSubmitterID("A");
        String csv = write(',', r);
        String[] lines = csv.split("\n");
        assertThat(lines[0]).startsWith("IM_SubmitterID,IM_ParticipantID,IM_RepCpCodeType,CP_RepCp")
                .endsWith("IM_OtherCptyEntResponsibleForRep,IM_OtherCpType");
        assertThat(lines[1]).isEqualTo("A" + ",".repeat(15));
        assertThat(csv).endsWith("\n").doesNotContain("\r");
    }

    @Test
    void specialCharactersAreQuoted() throws Exception {
        SftrTransactionReport r = new SftrTransactionReport();
        r.setImSubmitterID("a,b");
        r.setImParticipantID("say \"hi\"");
        r.setImRepCpCodeType("line1\nline2");
        String csv = write(',', r);
        assertThat(csv).contains("\"a,b\",\"say \"\"hi\"\"\",\"line1\nline2\"");
    }

    @Test
    void delimiterIsHonouredAndFileIsUtf8() throws Exception {
        SftrTransactionReport r = new SftrTransactionReport();
        r.setImSubmitterID("a,b");
        r.setImParticipantID("é€");
        String csv = write(';', r);
        assertThat(csv.split("\n")[0]).contains("IM_SubmitterID;IM_ParticipantID;");
        assertThat(csv).contains("a,b;é€;");
        assertThat(Files.readAllBytes(dir.resolve("out.csv")))
                .contains("é€".getBytes(StandardCharsets.UTF_8));
    }
}
