package net.gandalf.journal.api;

/**
 * Expose statistics of the journal
 */
public interface JournalStatistics {

    /**
     * Number of entries in the journal. This doesn't need to match the index, since gaps in the indexes are allowed.
     * @return number of entries
     */
    long getLength();
}
