package telegram.files.security;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public final class RedactingFormatter extends Formatter {

    private final SimpleFormatter delegate = new SimpleFormatter();
    private final boolean redactNonCritical;

    public RedactingFormatter() {
        this(true);
    }

    public RedactingFormatter(boolean redactNonCritical) {
        this.redactNonCritical = redactNonCritical;
    }

    @Override
    public String format(LogRecord record) {
        return SensitiveDataRedactor.redact(delegate.format(record), redactNonCritical);
    }
}
