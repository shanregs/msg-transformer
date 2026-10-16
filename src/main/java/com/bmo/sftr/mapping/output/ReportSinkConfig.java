package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.RecordProcessor;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Chooses the {@link ReportSink} from {@code sftr.output.write-mode} and provides the clock for the file day. */
@Configuration(proxyBeanMethods = false)
public class ReportSinkConfig {

    @Bean
    @ConditionalOnMissingBean
    public Clock clock(OutputProperties props) {
        return Clock.system(props.fileZone());
    }

    @Bean
    public ReportSink reportSink(OutputProperties props, SftrCsvWriter csvWriter, RecordProcessor processor,
                                 OutputFileResolver files) {
        return switch (props.writeMode()) {
            case PER_RECORD -> new PerRecordSink(csvWriter, processor, files);
            case COUNT, INTERVAL -> throw new IllegalStateException(
                    "sftr.output.write-mode " + props.writeMode() + " is not available yet");
        };
    }
}
