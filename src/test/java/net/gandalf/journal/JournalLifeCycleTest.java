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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-04
 */
public class JournalLifeCycleTest {


    @Test
    public void testFindJournalFileIfAlreadyExists() {
        String fileNmae = JournalTestUtil.createLogFileNameRandom("restart");
        Journal journal = new ChronicleJournal(fileNmae, ModelEvent.class);
        Writer<EventBatch<ModelEvent>> writer = journal.createWriter();
        EventBatch<ModelEvent> batch = createEventBatch();
        writer.start();
        writer.add( batch );
//
//        ReaderStart<EventBatch<ModelEvent>> strategy = createReaderStarter();
//        journal.createReader(strategy);
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
