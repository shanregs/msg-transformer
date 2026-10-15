package com.bmo.sftr.mapping.runner;

import com.bmo.sftr.mapping.service.SftrReportService;
import java.nio.file.Path;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "sftr.runner.enabled", havingValue = "true")
public class SftrBatchRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SftrBatchRunner.class);

    private final SftrReportService service;
    private final String input;
    private final String output;
    private final String configuredFileDate;

    public SftrBatchRunner(SftrReportService service,
                           @Value("${sftr.runner.input}") String input,
                           @Value("${sftr.runner.output}") String output,
                           @Value("${sftr.runner.file-date:}") String configuredFileDate) {
        this.service = service;
        this.input = input;
        this.output = output;
        this.configuredFileDate = configuredFileDate;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String fileDate = (configuredFileDate == null || configuredFileDate.isBlank())
                ? LocalDate.now().toString() : configuredFileDate;
        SftrReportService.RunResult r = service.run(Path.of(input), Path.of(output), fileDate);
        log.info("SFTR runner done: read={}, written={}, rejected={}", r.read(), r.written(), r.rejected());
    }
}
