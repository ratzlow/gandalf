package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.Journal;
import net.gandalf.journal.api.Reader;
import net.gandalf.journal.api.ReaderStart;
import net.gandalf.journal.api.Writer;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Journal implemented based on Chronicle as a backing storage.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
// TODO (FRa) : (FRa) : fix typing
// TODO (FRa) : (FRa) : impl non-busyspin listener, sizing, replication
public class ChronicleJournal implements Journal {

    private Logger LOGGER = Logger.getLogger( ChronicleJournal.class );

    private final String fileName;
    private final List<Reader> readers = Collections.synchronizedList( new ArrayList<Reader>() );
    private final List<Writer> writers = Collections.synchronizedList( new ArrayList<Writer>() );
    private final Class<? extends BytesMarshallable>[] marshallables;

    public ChronicleJournal(String fileName, Class<? extends BytesMarshallable> ... marshallables ) {
        this.fileName = fileName;
        this.marshallables = marshallables;
    }

    @Override
    public Reader createReader(ReaderStart strategy) {
        ChronicleTailer tailer = new ChronicleTailer( fileName, strategy);
        readers.add( tailer );
        return tailer;
    }

    @Override
    public Writer createWriter() {
        ChronicleAppender writer = new ChronicleAppender( fileName, marshallables );
        writers.add( writer );
        return writer;
    }

    /**
     * First close all writers to prevent adding more work. Then close the readers.
     */
    @Override
    public void stop() {
        for (Writer writer : writers) {
            writer.stop();
        }
        LOGGER.info( "Stopped journal writers!" );

        for (Reader reader : readers) {
            reader.stop();
        }
        LOGGER.info( "Stopped journal readers!" );
    }
}