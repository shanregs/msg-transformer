package com.bmo.sftr.mapping.output;

import com.bmo.sftr.mapping.refdata.RefDataClient;
import jakarta.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import tools.jackson.core.JacksonException;

/**
 * Appends one line per rejected message: source reference, a tab, and a short reason. Flushed at once.
 * When daily files are on, the first reject of a new day closes the previous day's file and starts a new one.
 */
@Component
public class RejectWriter {

    private static final Logger log = LoggerFactory.getLogger(RejectWriter.class);

    private final OutputFileResolver files;
    private Path openFile;
    private BufferedWriter rejects;

    public RejectWriter(OutputFileResolver files) {
        this.files = files;
    }

    public void write(String sourceRef, String summary, LocalDate day) throws IOException {
        Path file = files.rejects(day);
        if (rejects == null || !file.equals(openFile)) {
            close();
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            rejects = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            openFile = file;
        }
        rejects.write(sourceRef + "\t" + summary + "\n");
        rejects.flush();
        log.warn("Rejected {}: {}", sourceRef, summary);
    }

    /** Error summary for the rejects file. Never includes message content (parser messages quote the input). */
    public static String summarize(Throwable t) {
        String text = switch (t) {
            case JacksonException _ -> "Malformed JSON";
            case HttpStatusCodeException h -> "Reference data HTTP " + h.getStatusCode().value();
            case RefDataClient.UnavailableException _ -> "Reference data unavailable";
            case IllegalStateException e when e.getMessage() != null -> e.getMessage();
            case IllegalArgumentException e when e.getMessage() != null
                    && (e.getMessage().startsWith("Empty") || e.getMessage().startsWith("Not a JSON")) -> e.getMessage();
            default -> t.getClass().getSimpleName();
        };
        return text.replaceAll("[\\t\\r\\n]+", " ");
    }

    @PreDestroy
    public void close() throws IOException {
        if (rejects != null) {
            rejects.close();
            rejects = null;
            openFile = null;
        }
    }
}
