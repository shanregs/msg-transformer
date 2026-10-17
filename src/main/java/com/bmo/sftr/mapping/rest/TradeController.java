package com.bmo.sftr.mapping.rest;

import com.bmo.sftr.mapping.db.TradeEvent;
import com.bmo.sftr.mapping.service.TradeFetchService;
import com.bmo.sftr.mapping.service.TradeFetchService.RunInProgressException;
import com.bmo.sftr.mapping.output.OutputFileResolver;
import com.bmo.sftr.mapping.output.ReportDay;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Starts a run over the trade table and answers when it has finished.
 *
 * <pre>
 * curl -X POST "localhost:9090/api/trades/process"
 * curl -X POST "localhost:9090/api/trades/process?limit=100"
 * </pre>
 */
@RestController
@RequestMapping("/api/trades")
@Tag(name = "Trades", description = "Process the RECEIVED rows of the trade table into the SFTR CSV")
public class TradeController {

    /** @param reportFile today's CSV file; @param firstId and @param lastId the ids of the first and last row read */
    public record Result(TradeEvent event, long read, long written, long failed, Long firstId, Long lastId,
                         long durationMs, String reportFile) {}

    public record Error(String error) {}

    private static final Logger log = LoggerFactory.getLogger(TradeController.class);

    private final TradeFetchService service;
    private final ReportDay reportDay;
    private final OutputFileResolver files;

    public TradeController(TradeFetchService service, ReportDay reportDay, OutputFileResolver files) {
        this.service = service;
        this.reportDay = reportDay;
        this.files = files;
    }

    @Operation(summary = "Process the RECEIVED trades",
            description = "Reads the RECEIVED rows of the trade table in id order, runs each row's JSON through the "
                    + "reference-data call and the mappings, and appends the rows to the day's CSV file. A row that "
                    + "fails is logged and skipped. The call returns when the run has finished.")
    @ApiResponse(responseCode = "200", description = "run finished: counts, first and last id, duration, CSV file")
    @ApiResponse(responseCode = "400", description = "limit is not a positive number")
    @ApiResponse(responseCode = "409", description = "another run is still active")
    @ApiResponse(responseCode = "500", description = "the database or the CSV file failed")
    @PostMapping(path = "/process", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> process(
            @Parameter(description = "stop after this many rows; omit for all") @RequestParam(required = false) Integer limit) {
        try {
            TradeFetchService.Summary s = service.process(limit);
            return ResponseEntity.ok(new Result(s.event(), s.read(), s.written(), s.failed(), s.firstId(), s.lastId(),
                    s.durationMs(), files.report(reportDay.today()).toString()));
        } catch (RunInProgressException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(new Error(e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new Error(e.getMessage()));
        } catch (IOException | RuntimeException e) {
            log.error("Run failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new Error("Run failed: " + e.getClass().getSimpleName()));
        }
    }
}
