package net.gandalf.journal.api;

/**
 * Process adding new entries to a given journal. Preferably only one process writes to the journal
 * to avoid locking needs. If you intend to write from multiple process to same journal make sure a
 * synchronized journal is created.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
public interface Writer {

    <T> long add(T entry);

    void start();

    void stop();
}
