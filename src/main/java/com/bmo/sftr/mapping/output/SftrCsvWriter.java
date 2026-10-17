package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.MappingConfigLoader;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SftrCsvWriter {

    private final char delimiter;
    private final List<MappingConfigLoader.Column> columns;

    /** The columns, in CSV order, come from the compiled mapping file. */
    @Autowired
    public SftrCsvWriter(@Value("${sftr.csv.delimiter:,}") char delimiter, MappingConfigLoader.CompiledConfig config) {
        this(delimiter, config.columns());
    }

    public SftrCsvWriter(char delimiter, List<MappingConfigLoader.Column> columns) {
        this.delimiter = delimiter;
        this.columns = List.copyOf(columns);
    }

    /** Creates or truncates the file and writes the header. */
    public Session open(Path file) throws IOException {
        return open(file, false);
    }

    /** Appends to an existing file (the header is written only when the file is new or empty). */
    public Session openAppend(Path file) throws IOException {
        return open(file, true);
    }

    private Session open(Path file, boolean append) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        boolean header = !append || !Files.exists(file) || Files.size(file) == 0;
        String[] names = columns.stream().map(MappingConfigLoader.Column::name).toArray(String[]::new);
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setRecordSeparator("\n")
                .setHeader(names)
                .setSkipHeaderRecord(!header)
                .get();
        BufferedWriter out = append
                ? Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
                : Files.newBufferedWriter(file, StandardCharsets.UTF_8);
        try {
            return new Session(new CSVPrinter(out, format), columns);   // writes the header
        } catch (IOException | RuntimeException e) {
            out.close();
            throw e;
        }
    }

    public static final class Session implements AutoCloseable {
        private final CSVPrinter printer;
        private final List<MappingConfigLoader.Column> columns;

        private Session(CSVPrinter printer, List<MappingConfigLoader.Column> columns) {
            this.printer = printer;
            this.columns = columns;
        }

        public void write(SftrTransactionReport report) throws IOException {
            Object[] row = new Object[columns.size()];
            for (int i = 0; i < row.length; i++) {
                Object v = columns.get(i).read(report);
                row[i] = v == null ? null : String.valueOf(v);
            }
            printer.printRecord(row);
        }

        public void flush() throws IOException {
            printer.flush();
        }

        @Override
        public void close() throws IOException {
            printer.flush();
            printer.close(true);
        }
    }
}
