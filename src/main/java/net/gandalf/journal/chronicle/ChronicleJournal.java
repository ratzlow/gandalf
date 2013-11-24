package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.*;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.IndexedChronicle;
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
    private final BatchDecoratorRegistry decoratorRegistry;
    private final ChronicleStatistics statistics;

    public ChronicleJournal(String fileName, BatchDecoratorRegistry decoratorRegistry ) {
        this.fileName = fileName;
        this.decoratorRegistry = decoratorRegistry;
        statistics = new ChronicleStatistics(fileName);
        statistics.start();
    }

    @Override
    public Reader createReader(ReaderStart strategy) {
        ChronicleTailer tailer = new ChronicleTailer( fileName, decoratorRegistry, strategy);
        readers.add( tailer );
        return tailer;
    }

    @Override
    public Writer createWriter() {
        ChronicleAppender writer = new ChronicleAppender( fileName, decoratorRegistry );
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

        statistics.stop();
        LOGGER.info( "Stopped statistics!" );
    }


    @Override
    public JournalStatistics getStatistics() {
        return statistics.update();
    }
}