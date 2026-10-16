package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import java.nio.file.Path;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Chooses the {@link ReportSink} from {@code sftr.output.write-mode}. */
@Configuration(proxyBeanMethods = false)
public class ReportSinkConfig {

    @Bean
    public ReportSink reportSink(OutputProperties props, SftrCsvWriter csvWriter, RecordProcessor processor) {
        return switch (props.writeMode()) {
            case PER_RECORD -> new PerRecordSink(csvWriter, processor, Path.of(props.file()));
            case COUNT, INTERVAL -> throw new IllegalStateException(
                    "sftr.output.write-mode " + props.writeMode() + " is not available yet");
        };
    }
}
