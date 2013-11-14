package net.gandalf.journal;

import net.gandalf.journal.api.*;
import net.gandalf.journal.chronicle.ChronicleBatch;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.chronicle.ChronicleStatistics;
import net.gandalf.journal.common.JournalTestUtil;
import net.gandalf.journal.sample.mapevent.DmlType;
import net.gandalf.journal.sample.mapevent.SimpleModelEvent;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Test file usage mimic of Journal. E.g.
 * - no file must be overriden/deleted
 * - data within a file must only be added and not overriden
 * - if journal is full it should throw an exception
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-04
 */
public class JournalLifeCycleTest {

    private final static int NO_WRITES = 1000;

    /**
     * Writing to same journal subsequently should be happen in sequence and not causing overrides of entries.
     * Reading from this journal should return all the entries.
     */
    @Test
    public void testFindJournalFileIfAlreadyExists() throws InterruptedException {
        String fileNmae = JournalTestUtil.createLogFileNameRandom("restart");
        Assert.assertFalse("before creation of " + fileNmae, chronicleFilesExist(fileNmae));

        // start application, no chronicle file is there yet so a new gets created
        writeBatchesToJournal(fileNmae, 0, NO_WRITES);

        // restart application with same file, so something gets appended in same file
        Assert.assertTrue("before creation of " + fileNmae, chronicleFilesExist(fileNmae));
        writeBatchesToJournal(fileNmae, NO_WRITES, 2 * NO_WRITES);

        // check if it is in sync  with what we retrieve as stats
        ChronicleStatistics stats = new ChronicleStatistics( fileNmae );
        Assert.assertEquals( 2* NO_WRITES, stats.update().getLength() );

        // read all entries of the journal written in 2 sequences
        readBatchesFromJournal(fileNmae);
    }

    private void readBatchesFromJournal(String fileNmae) throws InterruptedException {

        Journal journal = new ChronicleJournal( fileNmae, SimpleModelEvent.class );
        final CountDownLatch latch = new CountDownLatch(2* NO_WRITES);
        JournalUpdateListener<EventBatch<SimpleModelEvent>> listener = new JournalUpdateListener<EventBatch<SimpleModelEvent>>() {
            Logger LOGGER = Logger.getLogger( getClass() );
            @Override
            public void onEvent(EventBatch<SimpleModelEvent> batch) {
                latch.countDown();
                Assert.assertFalse(batch.getEntries().isEmpty());
                long index = batch.getIndex();
                if ( index > 2*NO_WRITES ) {
                    LOGGER.error("Found to high index = " + index );
                }
            }
        };
        final Reader reader = journal.createReader(new ReaderStart<EventBatch<SimpleModelEvent>>(listener));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                reader.start();
            }
        });

        Assert.assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
        journal.stop();
    }

    private void writeBatchesToJournal(String fileNmae, long entriesBefore, long entriesAfter) {
        Journal journal = new ChronicleJournal(fileNmae, SimpleModelEvent.class);
        Assert.assertEquals( entriesBefore, journal.getStatistics().getLength() );
        Writer<EventBatch<SimpleModelEvent>> writer = journal.createWriter();
        EventBatch<SimpleModelEvent> batch = createEventBatch();
        writer.start();
        for ( int i=0; i< entriesAfter - entriesBefore; i++) {
            writer.add( batch );
        }
        Assert.assertEquals(entriesAfter, journal.getStatistics().getLength());
        journal.stop();
    }

    /**
     * @return true ... if index and data file exist
     */
    // TODO (FRa) : (FRa) : rewrite this file filter -> ugly
    private boolean chronicleFilesExist( final String pathPrefix) {
        File[] files = new File("C:/Temp").listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                String dataName = pathPrefix + ".data";
                String indexName = pathPrefix + ".index";
                return name.contains("delete") && dataName.endsWith(name) || indexName.endsWith(name);
            }
        });

        return files.length == 2;
    }

    @Before
    public void init() {
        JournalTestUtil.deleteFiles();
    }

    @AfterClass
    public static void staticAfter() {
        JournalTestUtil.deleteFiles();
    }

    private EventBatch<SimpleModelEvent> createEventBatch() {
        List<SimpleModelEvent> entries = new ArrayList<SimpleModelEvent>();
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("ouse", "Ofip");
        attributes.put("gaotag", "ZR77:131021:12");
        entries.add( new SimpleModelEvent(DmlType.INSERT, "hub_order", attributes ) );
        return new ChronicleBatch<SimpleModelEvent>( entries, SimpleModelEvent.class );
    }
}
