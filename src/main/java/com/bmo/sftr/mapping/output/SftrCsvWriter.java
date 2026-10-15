package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.engine.MappingConfigLoader;
import com.bmo.sftr.mapping.model.SftrTransactionReport;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SftrCsvWriter {

    private final char delimiter;

    public SftrCsvWriter(@Value("${sftr.csv.delimiter:,}") char delimiter) {
        this.delimiter = delimiter;
    }

    public Session open(Path file, List<MappingConfigLoader.Column> columns) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        String[] header = columns.stream().map(MappingConfigLoader.Column::name).toArray(String[]::new);
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setRecordSeparator("\n")
                .setHeader(header)
                .get();
        BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
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

        @Override
        public void close() throws IOException {
            printer.flush();
            printer.close(true);
        }
    }
}
