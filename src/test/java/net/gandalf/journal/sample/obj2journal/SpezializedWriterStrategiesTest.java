package net.gandalf.journal.sample.obj2journal;

import net.gandalf.journal.api.Journal;
import net.gandalf.journal.api.Writer;
import net.gandalf.journal.common.DefaultChronicleBatch;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.common.JournalTestUtil;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.gandalf.journal.common.JournalTestUtil.createDefaultChronicleBatchRegistry;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-12
 */
public class SpezializedWriterStrategiesTest {
    private static Logger LOGGER = Logger.getLogger(SpezializedWriterStrategiesTest.class);
    private final int batchCount = 1000;
    private final int eventsCount = 1000;

    @Test
    public void testWriteAsObjects() {
        String fileName = JournalTestUtil.createLogFileNameRandom("specialObjWrite");
        Journal journal = new ChronicleJournal(fileName, createDefaultChronicleBatchRegistry());
        Writer writer = journal.createWriter();
        writer.start();

        long writerStart = System.currentTimeMillis();
        List<DefaultChronicleBatch> batches = createBatches();
        for (DefaultChronicleBatch batch : batches) {
            writer.add(batch);
        }
        LOGGER.info("Finished writing batches to chronicle (ms) =" + (System.currentTimeMillis() - writerStart));
        journal.stop();
    }

    private List<DefaultChronicleBatch> createBatches() {
        long start = System.currentTimeMillis();
        LOGGER.info("Start creating batches ...");
        List<DefaultChronicleBatch> batches = new ArrayList<DefaultChronicleBatch>();
        for ( int i=0; i < batchCount; i++) {
            List<FastModelEvent> entries = createEntries();
            DefaultChronicleBatch<FastModelEvent> batch = JournalTestUtil.createChronicleBatch(entries, FastModelEvent.class);
            batches.add( batch );
        }

        LOGGER.info("Finished batch creation in (ms) " + (System.currentTimeMillis() - start));
        return batches;
    }

    private List<FastModelEvent> createEntries() {

        List<FastModelEvent> entries = new ArrayList<FastModelEvent>();
        for (int i = 0; i < eventsCount; i++) {
            Map<ModelDD, Object> values = new HashMap<ModelDD, Object>();
            values.put(ModelDD.ID, System.nanoTime());
            values.put(ModelDD.SIZE, 99);
            values.put(ModelDD.LUSER, "FrankTheWinner");
            values.put(ModelDD.OUSER, "OFIP");
            values.put(ModelDD.STEXT, "RV:asomewhatlonger text showing an error");
            values.put(ModelDD.DELAYED_ASK, BigDecimal.valueOf(898.44));
            values.put(ModelDD.DELAYED_BID, BigDecimal.valueOf(125.564));
            values.put(ModelDD.FQOPEN, 399);
            values.put(ModelDD.FQDONE, null);
            values.put(ModelDD.STATE, OrderState.CANCELED);
            values.put(ModelDD.FSIZE, 999);
            values.put(ModelDD.OSTAMP, new Timestamp(System.nanoTime()));
            values.put(ModelDD.CSTAMP, new Timestamp(System.nanoTime()));
            values.put(ModelDD.ASK_PRICE, BigDecimal.valueOf(83834.383));

            entries.add(new FastModelEvent(values));
        }
        return entries;
    }
}
