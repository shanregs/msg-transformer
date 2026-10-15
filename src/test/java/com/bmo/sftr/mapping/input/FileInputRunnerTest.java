package com.bmo.sftr.mapping.input;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bmo.sftr.mapping.service.RecordPipeline;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

class FileInputRunnerTest {

    private final RecordPipeline pipeline = mock(RecordPipeline.class);

    private FileInputRunner runner(Path file) {
        return new FileInputRunner(pipeline, JsonMapper.builder().build(), file.toString(), ',');
    }

    @Test
    void csvRowsBecomeJsonObjectsAndABadRowDoesNotStopTheRun(@TempDir Path dir) throws Exception {
        Path csv = Files.writeString(dir.resolve("in.csv"), "id,side\n1,PO\n2,PI\n3,PO\n");
        when(pipeline.handle(any(), eq("in.csv:3"))).thenThrow(new IllegalStateException("boom"));

        runner(csv).run(null);

        verify(pipeline).handle("{\"id\":\"1\",\"side\":\"PO\"}", "in.csv:2");
        verify(pipeline).handle("{\"id\":\"2\",\"side\":\"PI\"}", "in.csv:3");
        verify(pipeline).handle("{\"id\":\"3\",\"side\":\"PO\"}", "in.csv:4");
    }

    @Test
    void txtFileIsOneJsonMessagePerLineBlankLinesSkipped(@TempDir Path dir) throws Exception {
        Path txt = Files.writeString(dir.resolve("in.txt"), "{\"a\":1}\n\n{\"a\":2}\n");

        runner(txt).run(null);

        verify(pipeline).handle("{\"a\":1}", "in.txt:1");
        verify(pipeline).handle("{\"a\":2}", "in.txt:3");
    }
}
