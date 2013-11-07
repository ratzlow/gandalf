package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalException;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.IndexedChronicle;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class controlling life cycle of processes accessing a chronicle journal.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */

// TODO (FRa) : (FRa) : is the assumption correct that the index for journals can be restart from zero?
// TODO (FRa) : (FRa) : or do we have to support (monoton) increasing sequence?
// TODO (FRa) : (FRa) : do we need to take control of the sequence?
abstract class AbstractChronicleJournal {
    protected final AtomicBoolean started = new AtomicBoolean(false);
    protected IndexedChronicle chronicle;

    protected AbstractChronicleJournal(String fileName) {
        try {
            // TODO (FRa) : (FRa) : create proper config per env
            chronicle = new IndexedChronicle(fileName, ChronicleConfig.SMALL);

        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    public void stop() {
        try {
            started.set(false);
            chronicle.close();
            getLogger().info("Chronicle journal stopped!");

        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    public void start() {
        started.set(true);
        getLogger().info("Chronicle journal started!");
    }

    protected abstract Logger getLogger();
}