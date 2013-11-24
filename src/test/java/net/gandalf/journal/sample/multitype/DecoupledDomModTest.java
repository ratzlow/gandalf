package net.gandalf.journal.sample.multitype;

import net.gandalf.journal.api.Journal;
import net.gandalf.journal.api.Writer;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.common.JournalTestUtil;
import net.gandalf.journal.sample.mapevent.DmlType;
import net.gandalf.journal.sample.multitype.appl.EntryUpdateEvent;
import net.gandalf.journal.sample.multitype.appl.ModelUpdateEvent;
import net.gandalf.journal.sample.obj2journal.Attribute;
import net.gandalf.journal.sample.obj2journal.ModelDD;
import net.gandalf.journal.sample.obj2journal.OrderState;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.gandalf.journal.common.JournalTestUtil.createDefaultChronicleBatchRegistry;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-22
 */
public class DecoupledDomModTest {

    @Test
    public void testReadWriteOneBatchType() {
        ModelUpdateEvent modelUpdateEvent = new ModelUpdateEvent( createEntryUpdates() );
        String fileName = JournalTestUtil.createLogFileNameRandom("multiBatchType");
        Journal journal = new ChronicleJournal(fileName, createDefaultChronicleBatchRegistry());
        writeToJournal( journal, modelUpdateEvent );
    }

    private void writeToJournal(final Journal journal, final ModelUpdateEvent modelUpdateEvent) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit( new Runnable() {
            @Override
            public void run() {
                Writer writer = journal.createWriter();
                writer.add( modelUpdateEvent );
            }
        });
    }

    private List<EntryUpdateEvent> createEntryUpdates() {
        List<EntryUpdateEvent> events = new ArrayList<EntryUpdateEvent>();
        Map<Attribute, Object> values = createValues();
        EntryUpdateEvent event = new EntryUpdateEvent( DmlType.INSERT, "hub_order", values );
        events.add( event );

        return events;
    }

    private Map<Attribute, Object> createValues() {
        Map<Attribute, Object> values = new HashMap<Attribute, Object>();
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
        return values;
    }
}
