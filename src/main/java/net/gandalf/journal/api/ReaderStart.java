package net.gandalf.journal.api;

/**
 * Specifiy how the reader should work if attached attached to a journal. Immutable and thread safe.
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-03
 */
// TODO: remove generics from class
public class ReaderStart {
    private final JournalUpdateListener listener;
    private final int timeout;
    private final long startIndex;

    //
    // constructor
    //

    /**
     * Start a reader from the beginning of the journal, so processing everything. The consumer thread will not pause
     * but busuy spin. This will produce the least latency but contribute to higher CPU consumption.
     *
     * @param listener callback invoked on the occurence of a new batch read from the journal.
     */
    public ReaderStart(JournalUpdateListener listener) {
        this.listener = listener;
        this.timeout = 0;
        this.startIndex = 0;
    }


    /**
     * Flexible strategy to modify the listening behaviour in terms of when to start and how eager to look for new
     * entries.
     * @param listener invoked on new event
     * @param timeout time to wait if no new event could be fetched from journal
     * @param startIndex from what index on invoke listener. Needed e.g. for replay/recovery scenarios.
     */
    public ReaderStart(JournalUpdateListener listener, int timeout, long startIndex) {
        this.listener = listener;
        this.timeout = timeout;
        this.startIndex = startIndex;
    }

    //
    // accessors
    //

    public JournalUpdateListener getListener() {
        return listener;
    }

    public int getTimeout() {
        return timeout;
    }

    public long getStartIndex() {
        return startIndex;
    }
}
