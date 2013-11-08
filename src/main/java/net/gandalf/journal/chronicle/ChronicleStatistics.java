package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalStatistics;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * // Only used to access read only statistics to our chronicle file
 */
public class ChronicleStatistics extends AbstractChronicleJournal implements JournalStatistics {
    private static Logger LOGGER = Logger.getLogger( ChronicleStatistics.class );

    // exposed metrics
    private AtomicLong length = new AtomicLong(0);

    //
    // constructors
    //

    ChronicleStatistics(String fileName) {
        super(fileName);
    }

    /**
     * If the monitoring Chronicle is already started by a controlling Journal update only numbers. Otherwise temporary start it
     * refresh numbers and stop it immediately.
     */
    JournalStatistics update() {
        if ( isStarted() ) {
            refreshMetrics();
        } else {
            start();
            refreshMetrics();
            stop();
        }

        return this;
    }

    private void refreshMetrics() {
        long newValue = chronicle.findTheLastIndex() + 1;
        length.set(newValue);
    }

    @Override
    public void stop() { super.stop(); }

    @Override
    public void start() { super.start(); }

    @Override
    protected Logger getLogger() { return LOGGER; }

    /**
     * TODO approximative value?!
     * @return
     */
    @Override
    public long getLength() {
        return length.get();
    }
}
