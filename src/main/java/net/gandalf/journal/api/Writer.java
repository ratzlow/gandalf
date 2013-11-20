package net.gandalf.journal.api;

/**
 * Process adding new entries to a given journal. Preferably only one process writes to the journal
 * to avoid locking needs. If you intend to write from multiple process to same journal make sure a
 * synchronized journal is created.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
public interface Writer<T extends EventBatch> {

    /**
     * Add a new item in an atomic operation to the journal.
     *
     * @param batch encapsulating all items that need to be safed as one TX
     * @return index ID generated for this add operation. Does not need to be monoton rising and will restart at zero if
     * a new journal is created.
     */
    long add(T batch);


    /**
     * Turn the writer on. Otherwise it will throw an exception stating it is stopped.
     */
    void start();


    /**
     * No more entries are added via this writer instance to the journal.
     */
    void stop();
}
