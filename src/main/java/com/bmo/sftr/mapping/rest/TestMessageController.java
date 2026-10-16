package com.bmo.sftr.mapping.rest;

import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.ReportDay;
import com.bmo.sftr.mapping.service.RecordPipeline;
import com.bmo.sftr.mapping.service.RecordPipeline.Outcome;
import java.io.IOException;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * A quick way to push one message through the same flow as Kafka and the file input: reference-data call,
 * mapping, write to the CSV. Only present when {@code sftr.rest.enabled=true} (the {@code rest} profile).
 *
 * <pre>
 * curl -X POST localhost:8080/api/test-message -H "Content-Type: application/json" -d '{"trade_type":"PO",...}'
 * </pre>
 *
 * A rejected message still answers 200; the {@code outcome} field says {@code REJECTED} and the reason is
 * in the rejects file. With {@code flush=true} (the default) a row waiting in a batch is written at once, so
 * it is in the CSV when the call returns. Use {@code flush=false} to watch the batch modes behave as in production.
 */
@RestController
@RequestMapping("/api")
@ConditionalOnProperty(name = "sftr.rest.enabled", havingValue = "true")
public class TestMessageController {

    /** @param reportFile the day's CSV file; @param rejectsFile the rejects file, only when rejected */
    public record Result(String outcome, boolean flushed, String reportFile, String rejectsFile) {}

    private final RecordPipeline pipeline;
    private final ReportDay reportDay;
    private final OutputFileResolver files;
    private final AtomicLong sequence = new AtomicLong();

    public TestMessageController(RecordPipeline pipeline, ReportDay reportDay, OutputFileResolver files) {
        this.pipeline = pipeline;
        this.reportDay = reportDay;
        this.files = files;
    }

    @PostMapping(path = "/test-message", produces = MediaType.APPLICATION_JSON_VALUE)
    public Result post(@RequestBody(required = false) String body,
                       @RequestParam(defaultValue = "true") boolean flush) throws IOException {
        Outcome outcome = pipeline.handle(body, "rest-" + sequence.incrementAndGet());
        if (flush) pipeline.flush();
        LocalDate day = reportDay.today();
        return new Result(outcome.name(), flush, files.report(day).toString(),
                outcome == Outcome.REJECTED ? files.rejects(day).toString() : null);
    }
}
