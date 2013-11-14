package net.gandalf.journal.common;

import net.gandalf.journal.chronicle.ChronicleBatch;
import net.gandalf.journal.sample.mapevent.DmlType;
import net.gandalf.journal.sample.mapevent.SimpleModelEvent;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-04
 */
public abstract class JournalTestUtil {
    private static final Logger LOGGER = Logger.getLogger(JournalTestUtil.class);
    private static final boolean deleteFiles = true;
    private static final String basePath = "C:/temp/delete.";

    public static List<ChronicleBatch> createEventBatches( int batchCount, int eventsCount) {
        LOGGER.info( "Start creating batches ...");
        long start = System.currentTimeMillis();
        List<ChronicleBatch> batches = new ArrayList<ChronicleBatch>();
        for (int i=0; i<batchCount; i++) {
            List<SimpleModelEvent> events = new ArrayList<SimpleModelEvent>(createEvents(eventsCount));
            batches.add( new ChronicleBatch<SimpleModelEvent>(events, SimpleModelEvent.class) );
        }
        LOGGER.info("Created " + batchCount + " batches in ms = " + (System.currentTimeMillis() - start));
        return batches;
    }


    public static String createLogFileNameRandom(String name) {
        return createLogFileNameFixed(UUID.randomUUID().toString() + "." + name);
    }

    public static String createLogFileNameFixed(String name) {
        return basePath + name;
    }

    public static void deleteFiles() {
        if (deleteFiles) {
            File[] deleteCandidates = new File("C:\\temp").listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().startsWith("delete.");
                }
            });
            for (File file : deleteCandidates) {
                file.deleteOnExit();
            }
        }
    }


    private static Collection<SimpleModelEvent> createEvents( int eventsCount ) {
        Collection<SimpleModelEvent> events = new ArrayList<SimpleModelEvent>();
        long start = System.nanoTime();
        for (int i = 0; i < eventsCount; i++) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put("id", toString(i) );
            attributes.put("size", toString(87) );
            attributes.put("luser", "frank");
            attributes.put("stext", "RV:invalidPrice");
            attributes.put("delayed_ask", toString( BigDecimal.valueOf(99.585)) );
            attributes.put("delayed_bid", toString(BigDecimal.valueOf(2299.989)) );
            attributes.put("fqopen", toString(15) );
            attributes.put("fqdone", toString(100) );
            attributes.put("state", "PCXL");
            attributes.put("ouser", "ofip");
            attributes.put("anothertext", "RV:invalidPrice");
            attributes.put("fsize", toString(1000) );
            attributes.put("ostamp", toString(new Timestamp(System.currentTimeMillis())) );
            attributes.put("ask_price", toString(BigDecimal.valueOf(87.8)) );
            attributes.put("bid_price", toString(BigDecimal.valueOf(105.8)) );
            attributes.put("null_attribute", null );

            SimpleModelEvent event = new SimpleModelEvent( DmlType.INSERT, "HubOrder", attributes );
            events.add( event);
        }
//        LOGGER.info("Creating " + eventsCount + " events took ms = " + (System.nanoTime() - start) / 1e6);
        return events;
    }


    private static String toString(int value) { return Integer.valueOf(value).toString(); }
    private static String toString(BigDecimal value) { return Double.valueOf(value.doubleValue()).toString(); }
    private static String toString(Timestamp value) { return Long.valueOf( value.getTime()).toString(); }

}
