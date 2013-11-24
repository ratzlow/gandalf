package net.gandalf.journal.api;

/**
 * Expose statistics of the journal.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-05
 */
public interface JournalStatistics {

    /**
     * Number of entries in the journal. This doesn't need to match the index, since gaps in the indexes are allowed.
     * @return number of entries
     */
    long getLength();
}
