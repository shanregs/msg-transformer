package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.output.OutputProperties.Rollover;
import java.nio.file.Path;
import java.time.LocalDate;
import org.springframework.stereotype.Component;

/**
 * Works out which file a day's rows and rejects go to.
 * With {@code DAILY} rollover the date is added before the extension:
 * {@code report.csv} becomes {@code report-2026-10-16.csv}. With {@code NONE} the configured names are used as they are.
 */
@Component
public class OutputFileResolver {

    private final Path report;
    private final Path rejects;       // null: derive from the report file of that day
    private final boolean daily;

    public OutputFileResolver(OutputProperties props) {
        this.report = Path.of(props.file());
        this.rejects = props.rejectsFile() == null || props.rejectsFile().isBlank() ? null : Path.of(props.rejectsFile());
        this.daily = props.rollover() == Rollover.DAILY;
    }

    public Path report(LocalDate day) {
        return daily ? withDate(report, day) : report;
    }

    public Path rejects(LocalDate day) {
        if (rejects == null) {
            Path r = report(day);
            return r.resolveSibling(r.getFileName() + ".rejects");
        }
        return daily ? withDate(rejects, day) : rejects;
    }

    static Path withDate(Path file, LocalDate day) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String dated = dot > 0 ? name.substring(0, dot) + "-" + day + name.substring(dot) : name + "-" + day;
        return file.resolveSibling(dated);
    }
}
