package com.bmo.sftr.mapping.service;

import com.bmo.sftr.mapping.refdata.RefDataClient;
import org.springframework.web.client.HttpStatusCodeException;
import tools.jackson.core.JacksonException;

/**
 * One record could not be turned into a row (malformed JSON, reference data failure, mapping error).
 * The message is a short reason that never contains record content, so it is safe to log.
 */
public class RecordProcessingException extends RuntimeException {

    private final String sourceRef;

    public RecordProcessingException(String sourceRef, Throwable cause) {
        super(summarize(cause), cause);
        this.sourceRef = sourceRef;
    }

    /** Where the record came from, for example {@code trade:42}. */
    public String sourceRef() {
        return sourceRef;
    }

    /** Never includes message content (parser messages quote the input). */
    static String summarize(Throwable t) {
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
}
