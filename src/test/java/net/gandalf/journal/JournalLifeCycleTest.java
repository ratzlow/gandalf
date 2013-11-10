package net.gandalf.journal;

import net.gandalf.journal.api.*;
import net.gandalf.journal.chronicle.ChronicleBatch;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.chronicle.ChronicleStatistics;
import net.gandalf.journal.common.JournalTestUtil;
import net.gandalf.sampleclient.oh.event.DmlType;
import net.gandalf.sampleclient.oh.event.ModelEvent;
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

        Journal journal = new ChronicleJournal( fileNmae, ModelEvent.class );
        final CountDownLatch latch = new CountDownLatch(2* NO_WRITES);
        JournalUpdateListener<EventBatch<ModelEvent>> listener = new JournalUpdateListener<EventBatch<ModelEvent>>() {
            Logger LOGGER = Logger.getLogger( getClass() );
            @Override
            public void onEvent(EventBatch<ModelEvent> batch) {
                latch.countDown();
                Assert.assertFalse(batch.getEntries().isEmpty());
                long index = batch.getIndex();
                if ( index > 2*NO_WRITES ) {
                    LOGGER.error("Found to high index = " + index );
                }
            }
        };
        final Reader reader = journal.createReader(new ReaderStart<EventBatch<ModelEvent>>(listener));

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
        Journal journal = new ChronicleJournal(fileNmae, ModelEvent.class);
        Assert.assertEquals( entriesBefore, journal.getStatistics().getLength() );
        Writer<EventBatch<ModelEvent>> writer = journal.createWriter();
        EventBatch<ModelEvent> batch = createEventBatch();
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
    private boolean chronicleFilesExist( final String pathPrefix) {
        File[] files = new File("C:/Temp").listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                String dataName = pathPrefix + ".data";
                String indexName = pathPrefix + ".index";
                return dataName.endsWith(name) || indexName.contains(name);
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

    private EventBatch<ModelEvent> createEventBatch() {
        List<ModelEvent> entries = new ArrayList<ModelEvent>();
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("ouse", "Ofip");
        attributes.put("gaotag", "ZR77:131021:12");
        entries.add( new ModelEvent(DmlType.INSERT, "hub_order", attributes ) );
        return new ChronicleBatch<ModelEvent>( entries, ModelEvent.class );
    }
}
