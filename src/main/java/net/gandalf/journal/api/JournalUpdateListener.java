package net.gandalf.journal.api;

/**
 * Callback to fire if a new event was read from the journal.
 *
 * @author ratzlow@gmail.com
 * @since 2013-09-28
 */
public interface JournalUpdateListener<T> {
    void onEvent( T batch );
}
