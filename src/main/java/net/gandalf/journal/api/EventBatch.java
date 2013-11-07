package net.gandalf.journal.api;

import java.util.List;

/**
 * This resembles a transaction spanning multiple items - here entries. This supports the semantic
 * of storing 1-n records in 2 phase commit within a journal.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
public interface EventBatch<T extends Entry> {

    /**
     * @return payload
     */
    List<T> getEntries();

    /**
     * @return index of it's storage in the journal
     */
    long getIndex();
}
