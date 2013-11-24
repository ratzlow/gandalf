package net.gandalf.journal;

import net.gandalf.journal.api.*;
import net.gandalf.journal.common.DefaultChronicleBatch;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.common.AbstractJournalTest;
import net.gandalf.journal.common.JournalTestUtil;
import net.gandalf.journal.sample.mapevent.SimpleModelEvent;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.gandalf.journal.common.JournalTestUtil.createDefaultChronicleBatchRegistry;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-14
 */
// TODO: add test case for pause on producer side of n sec and pickup of events after this delay; consumer idles for a while
public class ChronicleBatchPayloadTest extends AbstractJournalTest {

    private static final Logger LOGGER = Logger.getLogger( ChronicleBatchPayloadTest.class );

    @Test
    public void testPayloadReadWrite() throws InterruptedException {
        final Journal journal = new ChronicleJournal(
                JournalTestUtil.createLogFileNameRandom("payloadCompare"), createDefaultChronicleBatchRegistry() );
        Assert.assertFalse(producedBatches.isEmpty());
        producerDuration = writeToJournal(journal);

        final CountDownLatch latch = new CountDownLatch(producedBatches.size());
        PayloadCompareListener listener = new PayloadCompareListener( latch );
        final ReaderStart start = new ReaderStart(listener);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Reader reader = journal.createReader(start);
                reader.start();
            }}
        );
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        consumerDurations.put("Consumer_1", (double) listener.duration.get());
        executorService.shutdownNow();
    }



    private class PayloadCompareListener implements JournalUpdateListener<DefaultChronicleBatch> {
        final AtomicLong duration = new AtomicLong();
        final long start = System.currentTimeMillis();
        final CountDownLatch latch;

        private PayloadCompareListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onEvent(DefaultChronicleBatch batch) {
            int index = (int) (producedBatches.size() - latch.getCount());
            DefaultChronicleBatch expected = producedBatches.get(index);
            compareBatches(expected, batch);
            latch.countDown();
            if ( latch.getCount() == 0 ) {
                duration.set( System.currentTimeMillis() - start );
            }
        }

        void compareBatches(DefaultChronicleBatch<SimpleModelEvent> expected, DefaultChronicleBatch<SimpleModelEvent> actual) {
            Assert.assertTrue( actual.getIndex() > -1 );
            Assert.assertFalse(actual.getEntries().isEmpty());
            Assert.assertEquals(expected.getSize(), actual.getSize());
            for ( int i=0; i < expected.getEntries().size(); i++ ) {
                SimpleModelEvent expectedEvent = expected.getEntries().get(i);
                SimpleModelEvent actualEvent = actual.getEntries().get(i);
                compareEvent( expectedEvent, actualEvent );
            }
        }

        void compareEvent(SimpleModelEvent expected, SimpleModelEvent actual) {
            Assert.assertEquals( expected.getDmlType(), actual.getDmlType() );
            Assert.assertEquals( expected.getEntityName(), actual.getEntityName() );
            Assert.assertEquals( expected.getSize(), actual.getSize() );
            Assert.assertEquals( expected.getAttributes().size(), actual.getAttributes().size() );
            for (String key : actual.getAttributes().keySet()) {
                String actualValue = actual.getAttributes().get(key);
                String expectedValue = expected.getAttributes().get(key);
                try {
                    if ( actualValue != null || expectedValue != null ) {
                        Assert.assertEquals( key + " -> ", expectedValue, actualValue );
                    }
                } catch (Throwable e) {
                    String stackTrace = ExceptionUtils.getStackTrace(e);
                    LOGGER.error(stackTrace);
                    throw new RuntimeException( e );
                }
            }
        }
    }
}
