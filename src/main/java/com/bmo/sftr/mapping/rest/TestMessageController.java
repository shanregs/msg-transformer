package com.bmo.sftr.mapping.rest;

import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.ReportDay;
import com.bmo.sftr.mapping.service.RecordPipeline;
import com.bmo.sftr.mapping.service.RecordProcessingException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * A quick way to push one message through the same flow as the database run: reference-data call, mapping,
 * write to the CSV.
 *
 * <pre>
 * curl -X POST localhost:9090/api/test-message -H "Content-Type: application/json" -d '{"trade_type":"PO",...}'
 * </pre>
 *
 * A message that cannot be processed still answers 200; {@code outcome} is {@code FAILED} and {@code reason}
 * says why (it never contains the message). With {@code flush=true} (the default) a row waiting in a batch is
 * written at once, so it is in the CSV when the call returns. Use {@code flush=false} to watch the batch
 * write mode behave as in a real run.
 */
@RestController
@RequestMapping("/api")
@Tag(name = "Test message", description = "Run one message through the same flow as the database run")
public class TestMessageController {

    /** @param reportFile the day's CSV file; @param reason only when FAILED */
    public record Result(String outcome, boolean flushed, String reportFile, String reason) {}

    private static final Logger log = LoggerFactory.getLogger(TestMessageController.class);

    private final RecordPipeline pipeline;
    private final ReportDay reportDay;
    private final OutputFileResolver files;
    private final AtomicLong sequence = new AtomicLong();

    public TestMessageController(RecordPipeline pipeline, ReportDay reportDay, OutputFileResolver files) {
        this.pipeline = pipeline;
        this.reportDay = reportDay;
        this.files = files;
    }

    @Operation(summary = "Run one message through the whole flow",
            description = "Reference-data call, mapping, write to the CSV. A message that cannot be processed still answers 200; outcome is FAILED and reason says why.",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(content = @Content(
                    mediaType = "application/json",
                    examples = @ExampleObject(name = "London entity, PO", value = "{\"trade_ref\":\"T1\",\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\",\"counterpartyId\":\"CP1\",\"group_cd\":\"G1\"}"))))
    @ApiResponse(responseCode = "200", description = "outcome WRITTEN or FAILED")
    @PostMapping(path = "/test-message", produces = MediaType.APPLICATION_JSON_VALUE)
    public Result post(@RequestBody(required = false) String body,
                       @io.swagger.v3.oas.annotations.Parameter(description = "write a row waiting in a batch at once")
                       @RequestParam(defaultValue = "true") boolean flush) throws IOException {
        String ref = "rest-" + sequence.incrementAndGet();
        String file = files.report(reportDay.today()).toString();
        try {
            pipeline.handle(body, ref);
        } catch (RecordProcessingException e) {
            log.error("{} failed: {}", ref, e.getMessage());
            return new Result("FAILED", false, file, e.getMessage());
        }
        if (flush) pipeline.flush();
        return new Result("WRITTEN", flush, file, null);
    }
}
