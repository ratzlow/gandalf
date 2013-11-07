package net.gandalf.journal.api;

/**
 * Controller interface to get access to a producer or consumer handle)
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
public interface Journal {

    <T extends EventBatch> Reader createReader(ReaderStart<T> strategy);

    <T extends EventBatch> Writer<T> createWriter();

    void stop();
}
