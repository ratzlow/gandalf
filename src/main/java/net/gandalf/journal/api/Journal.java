package net.gandalf.journal.api;

/**
 * Controller interface to get access to a producer or consumer handle. This is the entry point (SPI) to return specific
 * journal implementations. A journal is analog to a queue with multiple readers and usually one writer.
 *
 * In a perfect world we stick to single producer principle and just create one writer at a time. Usually you want to
 * create readers and writers in different threads.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
public interface Journal {

    /**
     * Create a reader handle to the undelying storage. It sequentially reads from the source and handles the read
     * entries based on the supplied strategy.
     *
     * @param strategy determines when the listener should be activated, pooling behaviour of the journal and to what
     *                 specifics the listener will respond.
     * @param <T> the concrete batch type the reader will work on.
     * @return handle that reads/consumes all entries in the journal
     */
    <T extends EventBatch> Reader createReader(ReaderStart<T> strategy);


    /**
     * Handle to add more entries/batches to the journal.
     *
     * @param <T> the type we put into the journal by our writer
     * @return handle to the journal on the producer side.
     */
    <T extends EventBatch> Writer<T> createWriter();


    /**
     * Stop all readers and writers by first shutting done writers and then readers.
     */
    void stop();


    /**
     * Statistics returned will be only approximative values at a time since multiple processes can influence
     * the metrics.
     * @return current usage of the journal
     */
    JournalStatistics getStatistics();
}
