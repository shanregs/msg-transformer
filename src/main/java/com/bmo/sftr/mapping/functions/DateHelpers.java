package com.bmo.sftr.mapping.functions;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

/**
 * Date helpers for mapping functions. Inputs may be a date only ("2025-12-31") or a timestamp with a
 * "T" ("2025-12-31T10:30:00", "...T10:30:00.123Z", "...T10:30:00-05:00"). Only the DATE is used:
 * the date part is taken as written, with no time-zone conversion.
 *
 * <p>Pure, stateless, null-safe: a null or blank input gives null from the extractors and false from the
 * comparisons, so lambdas stay tolerant of missing fields. A value that is present but not a date throws
 * {@link IllegalArgumentException} without echoing the value (regulatory data).
 */
public final class DateHelpers {
    private DateHelpers() {}

    /**
     * Date, optional "T" time, optional offset. Used as the default input format of DIRECT_DATA_TRANSFORM,
     * so a DATE or TIMESTAMP row accepts both shapes. Resolves to a date, a local date-time or an offset date-time.
     */
    public static final DateTimeFormatter ISO_FLEXIBLE = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .optionalStart().appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).optionalEnd()
            .optionalStart().appendOffsetId().optionalEnd()
            .toFormatter();

    // ---------- extraction ----------

    /** The date of a date-only or "T" timestamp string, or null if the input is null or blank. */
    public static LocalDate dateOf(String text) {
        if (text == null || text.isBlank()) return null;
        String t = text.strip();
        int cut = t.indexOf('T');
        if (cut < 0) cut = t.indexOf(' ');                       // "2025-12-31 10:30:00"
        String datePart = cut < 0 ? t : t.substring(0, cut);
        try {
            return LocalDate.parse(datePart, DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("value is not a valid ISO date or timestamp");
        }
    }

    /** Same, for a custom input pattern (the pattern itself is a constant). Time parts of the pattern are ignored. */
    public static LocalDate dateOf(String text, String pattern) {
        if (text == null || text.isBlank()) return null;
        try {
            return LocalDate.from(DateTimeFormatter.ofPattern(pattern).parse(text.strip()));
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("value does not match the configured date format");
        }
    }

    /** The date part as an ISO string (yyyy-MM-dd), or null. */
    public static String dateOnly(String text) {
        LocalDate d = dateOf(text);
        return d == null ? null : d.toString();
    }

    // ---------- comparison (inputs are strings in either shape; false when either side is null) ----------

    public static boolean isAfter(String a, String b) {
        return isAfter(dateOf(a), dateOf(b));
    }

    public static boolean isBefore(String a, String b) {
        return isBefore(dateOf(a), dateOf(b));
    }

    public static boolean isEqual(String a, String b) {
        return isEqual(dateOf(a), dateOf(b));
    }

    public static boolean isOnOrAfter(String a, String b) {
        return isOnOrAfter(dateOf(a), dateOf(b));
    }

    public static boolean isOnOrBefore(String a, String b) {
        return isOnOrBefore(dateOf(a), dateOf(b));
    }

    /** True when from <= date <= to (both ends inclusive). */
    public static boolean isBetween(String date, String from, String to) {
        return isBetween(dateOf(date), dateOf(from), dateOf(to));
    }

    /** Whole days from a to b (negative if b is earlier), or null if either is missing. */
    public static Long daysBetween(String a, String b) {
        return daysBetween(dateOf(a), dateOf(b));
    }

    // ---------- the same on LocalDate ----------

    public static boolean isAfter(LocalDate a, LocalDate b) {
        return a != null && b != null && a.isAfter(b);
    }

    public static boolean isBefore(LocalDate a, LocalDate b) {
        return a != null && b != null && a.isBefore(b);
    }

    public static boolean isEqual(LocalDate a, LocalDate b) {
        return a != null && b != null && a.isEqual(b);
    }

    public static boolean isOnOrAfter(LocalDate a, LocalDate b) {
        return a != null && b != null && !a.isBefore(b);
    }

    public static boolean isOnOrBefore(LocalDate a, LocalDate b) {
        return a != null && b != null && !a.isAfter(b);
    }

    public static boolean isBetween(LocalDate d, LocalDate from, LocalDate to) {
        return isOnOrAfter(d, from) && isOnOrBefore(d, to);
    }

    public static Long daysBetween(LocalDate a, LocalDate b) {
        return a == null || b == null ? null : ChronoUnit.DAYS.between(a, b);
    }
}
