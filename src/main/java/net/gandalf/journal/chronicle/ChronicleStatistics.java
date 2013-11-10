package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalStatistics;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A journal instance neither reading nor writing entries. It only servers as a handle to the content to retrieve
 * statistics. Make sure update() is called before you access any metrics, since the underlying journal can change.
 *
 * @author fratzlow
 * @since 2013-11-10
 */
public class ChronicleStatistics extends AbstractChronicleJournal implements JournalStatistics {
    private static Logger LOGGER = Logger.getLogger( ChronicleStatistics.class );

    // exposed metrics
    private AtomicLong length = new AtomicLong(0);

    //
    // constructors
    //

    public ChronicleStatistics(String fileName) {
        super(fileName);
    }

    /**
     * If the monitoring Chronicle is already started by a controlling Journal update only numbers. Otherwise temporary start it
     * refresh numbers and stop it immediately.
     */
    public JournalStatistics update() {
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
     * @return chronicle as the underlying journal implementation will not return the exact value, since it returns the
     * highest index found. Since the index is not strictly monoton rising there might be gaps leading to a higher index
     * than entries contained in the journal. If actually counting the entries (e.g.) looping over it the correct number
     * is returned.
     */
    @Override
    public long getLength() {
        return length.get();
    }
}
