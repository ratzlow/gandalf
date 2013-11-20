package net.gandalf.journal.api;

/**
 * Callback to fire if a new event was read from the journal. This will delegate to some real event handler that does
 * things with it.
 *
 * @author ratzlow@gmail.com
 * @since 2013-09-28
 */
public interface JournalUpdateListener<T extends EventBatch> {

    /**
     * Will be invoked for each event read from the journal.
     * @param batch the item read from the journal.
     */
    void onEvent( T batch );
}
