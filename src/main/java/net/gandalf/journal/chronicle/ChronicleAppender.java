package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalException;
import net.gandalf.journal.api.Writer;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.io.serialization.BytesMarshallable;
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
class ChronicleAppender extends AbstractChronicleJournal implements Writer<ChronicleBatch> {

    private static final Logger LOGGER = Logger.getLogger(ChronicleAppender.class);

    // TODO: expose it for statistics
    private final AtomicLong currentIndex = new AtomicLong(0);
    private final ExcerptAppender appender;

    /**
     * Writer to the chronicle.
     *
     * @param fileName where to write to
     * @param marshallables classes to be registered as being suitable for chronicles streaming
     */
    ChronicleAppender(String fileName, Class<? extends BytesMarshallable> ... marshallables ) {
        super(fileName);
        try {
            appender = chronicle.createAppender();
            currentIndex.set( appender.lastWrittenIndex() );
            appender.bytesMarshallerFactory().acquireMarshaller( ChronicleBatch.class, true);
            for (Class<? extends BytesMarshallable> marshallable : marshallables) {
                appender.bytesMarshallerFactory().acquireMarshaller( marshallable, true);
            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    //
    // public API
    //

    @Override
    public long add(ChronicleBatch batch) {
        if (!started.get())
            throw new IllegalStateException("Journal not started. Event will not be added.");

        // start TX: reserve space in file and add to it
        long size = batch.getSize();
        long excerptSize = size * 4;

        // TODO (FRa) : (FRa) : add rollover behaviour
        appender.startExcerpt(excerptSize);
        if ( excerptSize > Integer.MAX_VALUE || appender.remaining() < excerptSize ) {
            throw new JournalException("Content of batch to big! Available bytes = " +
                    appender.remaining() + " needed size = " + excerptSize);
        }

        long index = appender.index();

        batch.setIndex( index );
        BytesMarshaller<ChronicleBatch> marshaller =
                appender.bytesMarshallerFactory().acquireMarshaller(ChronicleBatch.class, true);
        marshaller.write(appender, batch);

        // end TX: data written to disk by sync
        appender.finish();

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
}