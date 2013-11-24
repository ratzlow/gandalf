package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalException;
import net.gandalf.journal.api.Writer;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.io.serialization.BytesMarshaller;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write to a chronicle file.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
// TODO (FRa) : (FRa) : write better algo to derive byte size of batch
class ChronicleAppender extends AbstractChronicleJournal implements Writer {

    private static final Logger LOGGER = Logger.getLogger(ChronicleAppender.class);

    private final BatchDecoratorRegistry decoratorRegistry;
    private final ExcerptAppender appender;

    private final AtomicLong currentIndex = new AtomicLong(0);

    //
    // constructors
    //

    /**
     * Create new appender. Only single producer supported by now.
     *
     * @param fileName of the journal to connect to.
     * @param decoratorRegistry with references to all handlers to deal with different batch event types.
     */
    ChronicleAppender( String fileName, BatchDecoratorRegistry decoratorRegistry ) {
        super(fileName);
        try {
            this.decoratorRegistry = decoratorRegistry;
            this.appender = chronicle.createAppender();
            this.currentIndex.set(appender.lastWrittenIndex());
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    //
    // public API
    //

    @Override
    public <T> long add( T batch ) {
        if (!started.get())
            throw new IllegalStateException("Journal not started. Event will not be added.");

        long index = -1;
        try {
            index = addInternal(batch);
        } catch (Exception e) {
            LOGGER.error( "Failed to add batch to journal!", e );
        }

        return index;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    protected Logger getLogger() { return LOGGER; }

    //
    // internal implementation
    //

    private <T> long addInternal(T batch) {
        final ChronicleBatchDecorator<T> batchDecorator = decoratorRegistry.getDecorator(batch.getClass());
        long size = batchDecorator.getSize( batch );
        long excerptSize = size * 4;

        // TODO (FRa) : (FRa) : add rollover behaviour
        // start TX: reserve space in file and add to it
        appender.startExcerpt(excerptSize);
        if ( excerptSize > Integer.MAX_VALUE || appender.remaining() < excerptSize ) {
            throw new JournalException("Content of batch to big! Available bytes = " +
                    appender.remaining() + " needed size = " + excerptSize);
        }

        long index = appender.index();

        batchDecorator.setIndex(batch, index);

        Class<T> marshallable = batchDecorator.getMarshallableClass();
        BytesMarshaller<T> marshaller = appender.bytesMarshallerFactory().acquireMarshaller(marshallable, true);
        appender.writeUTF( batch.getClass().getCanonicalName() );
        marshaller.write(appender, batch);

        // end TX: data written to disk by sync
        appender.finish();
        return index;
    }
}