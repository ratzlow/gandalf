package net.gandalf.journal;

import net.gandalf.journal.api.*;
import net.gandalf.journal.chronicle.ChronicleBatch;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.common.JournalTestUtil;
import net.gandalf.sampleclient.oh.event.DmlType;
import net.gandalf.sampleclient.oh.event.ModelEvent;
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


    @Test
    public void testFindJournalFileIfAlreadyExists() {
        String fileNmae = JournalTestUtil.createLogFileNameRandom("restart");
        Assert.assertFalse( "before creation of " + fileNmae, chronicleFilesExist( fileNmae ) );

        // start application, no chronicle file is there yet so a new gets created
        int noWrites = 100;
        writeBatchToJournal(fileNmae, 0 * noWrites, 1 * noWrites );

        // restart application with same file, so something gets appended in same file
        Assert.assertTrue( "before creation of " + fileNmae, chronicleFilesExist( fileNmae ) );
        writeBatchToJournal(fileNmae, 1*noWrites, 2*noWrites);
    }

    private void writeBatchToJournal(String fileNmae, long entriesBefore, long entriesAfter ) {
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
     * @param pathPrefix
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

    private ReaderStart<EventBatch<ModelEvent>> createReaderStarter() {

        JournalUpdateListener<EventBatch<ModelEvent>> listener =
                new JournalUpdateListener<EventBatch<ModelEvent>>() {
            @Override
            public void onEvent(EventBatch<ModelEvent> batch) {
                Assert.assertNotNull( batch );
                Assert.assertTrue(batch.getEntries().isEmpty());
            }
        };
        return new ReaderStart<EventBatch<ModelEvent>>(listener);
    }
}
