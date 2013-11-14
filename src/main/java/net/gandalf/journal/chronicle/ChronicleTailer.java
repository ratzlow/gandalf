package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalException;
import net.gandalf.journal.api.JournalUpdateListener;
import net.gandalf.journal.api.Reader;
import net.gandalf.journal.api.ReaderStart;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.serialization.BytesMarshaller;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chronicle Reader process that starts at specified index to inform the listener about events. Works sequentiatlly
 * through the journal. Multiple Readers reading same file maintain their own progress/index.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
class ChronicleTailer extends AbstractChronicleJournal implements Reader {
    private static final Logger LOGGER = Logger.getLogger(ChronicleTailer.class);

    private final AtomicLong entriesDispatched = new AtomicLong(0);

    private final ExcerptTailer tailer;
    private final JournalUpdateListener<ChronicleBatch> listener;
    private final int timeout;
    private final long startIndex;


    //
    // constructors
    //

    /**
     * Construct a new reader. Not keeping any state.
     *
     * @param fileName the chronicle journal file
     * @param strategy specifies how to read the journal
     */
    ChronicleTailer(String fileName, ReaderStart<ChronicleBatch> strategy) {
        super(fileName);

        if (strategy.getListener() == null) {
            throw new IllegalArgumentException("No listener specified!");
        }

        this.listener = strategy.getListener();
        this.timeout = strategy.getTimeout();
        this.startIndex = strategy.getStartIndex();
        try {
            tailer = chronicle.createTailer();
        } catch (IOException e) {
            throw new JournalException( e );
        }
    }

    private void listen() {

        final BytesMarshaller<ChronicleBatch> marshaller =
                tailer.bytesMarshallerFactory().acquireMarshaller(ChronicleBatch.class, true);

        while (started.get()) {

            boolean validExcerptFound = tailer.nextIndex();
            if (validExcerptFound) {
                long currentIndex = tailer.index();
                if (currentIndex >= startIndex) {
                    ChronicleBatch eventBatch = marshaller.read(tailer);
                    tailer.finish();
                    if ( LOGGER.isDebugEnabled() ) {
                        LOGGER.debug("Process batch.index=" + eventBatch.getIndex() );
                    }
                    listener.onEvent(eventBatch);
                    entriesDispatched.incrementAndGet();
                }

            } else {

                if (timeout > 0) {
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        throw new JournalException(e);
                    }
                }
            }
        }
    }

    @Override
    public void start() {
        super.start();
        listen();
    }

    @Override
    public void stop() {
        LOGGER.info("Entries dispatched = " + entriesDispatched.get() );
        super.stop();
    }

    @Override
    protected Logger getLogger() { return LOGGER; }
}

