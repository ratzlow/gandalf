package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalException;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.IndexedChronicle;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class controlling life cycle of processes accessing a chronicle journal. It opens and closes access to it, no
 * matter if it the reader/writer side.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
// TODO (FRa) : (FRa) : do we need to take control of the sequence?
// TODO (FRa) : (FRa) : delegate logger creation to factory
abstract class AbstractChronicleJournal {
    protected final AtomicBoolean started = new AtomicBoolean(false);
    protected IndexedChronicle chronicle;

    //
    // constructors
    //

    /**
     * Construct access to the underlying journal file(s). If the files exist reuse them and keep writing/reading to them
     * in an append mode. If the file does not exist yet, crete new chronicle journals.
     *
     * @param fileName the file name to be used for the underlying chronicle data/index/... files
     */
    protected AbstractChronicleJournal(String fileName) {
        try {
            // TODO (FRa) : (FRa) : create proper config per env
            chronicle = new IndexedChronicle(fileName, ChronicleConfig.SMALL);

        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    //
    // public lifecycle API
    //

    /**
     * After called no more entries can be read/written to the chronicle files.
     */
    public void stop() {
        try {
            started.set(false);
            chronicle.close();
            getLogger().info("Chronicle journal stopped!");

        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * Only mark the chronicle to be ready for processing.
     */
    public void start() {
        started.set(true);
        getLogger().info("Chronicle journal started!");
    }

    public boolean isStarted() { return started.get(); }

    protected abstract Logger getLogger();
}