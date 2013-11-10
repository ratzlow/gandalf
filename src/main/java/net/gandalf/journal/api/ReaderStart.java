package net.gandalf.journal.api;

/**
 * Specifiy how the reader should work if attached attached to a journal. Immutable and thread safe.
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-03
 */
// TODO: remove generics from class
public class ReaderStart<E extends EventBatch> {
    private final JournalUpdateListener<E> listener;
    private final int timeout;
    private final long startIndex;

    //
    // constructor
    //

    public ReaderStart(JournalUpdateListener<E> listener) {
        this.listener = listener;
        this.timeout = 0;
        this.startIndex = 0;
    }

    public ReaderStart(JournalUpdateListener<E> listener, int timeout, long startIndex) {
        this.listener = listener;
        this.timeout = timeout;
        this.startIndex = startIndex;
    }

    //
    // accessors
    //

    public JournalUpdateListener<E> getListener() {
        return listener;
    }

    public int getTimeout() {
        return timeout;
    }

    public long getStartIndex() {
        return startIndex;
    }
}
