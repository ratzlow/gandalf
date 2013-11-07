package net.gandalf.journal.api;

/**
 * Thrown if something goes wrong with the journal.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-05
 */
public class JournalException extends RuntimeException {
    public JournalException(String message) {
        super(message);
    }

    public JournalException(String message, Throwable e ) {
        super(message, e);
    }

    public JournalException(Throwable cause) {
        super(cause);
    }
}
