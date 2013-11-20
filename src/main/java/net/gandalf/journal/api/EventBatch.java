package net.gandalf.journal.api;

import java.util.List;

/**
 * This resembles a transaction spanning multiple items - here entries. It supports the semantic
 * of storing 1-n records in 2 phase commit within a journal.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
public interface EventBatch<T extends Entry> {

    /**
     * @return payload or events that have to be treated as a whole.
     */
    List<T> getEntries();

    /**
     * @return index of it's storage in the journal. The number needs to rise but not neccessarily monoton.
     */
    long getIndex();

    // TODO: return Class<T> to recognize different event types in same journal? How far need mix and match go?
}
