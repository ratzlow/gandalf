package net.gandalf.journal.api;

/**
 * Reader process attached to a journal. Multiple readers can access same journal concurrently and operate
 * on their own speed.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
public interface Reader {

    /**
     * Start reading at the first entry of the journal.
     */
    void start();

    /**
     * Stop reading from journal and dispatching to {@link JournalUpdateListener}
     */
    void stop();
}
