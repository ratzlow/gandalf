package net.gandalf.journal;

import net.gandalf.journal.api.*;
import net.gandalf.journal.chronicle.ChronicleBatch;
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
                JournalTestUtil.createLogFileNameRandom("payloadCompare"), SimpleModelEvent.class );
        Assert.assertFalse(producedBatches.isEmpty());
        final ChronicleBatch chronicleBatch = producedBatches.get(0);
        producerDuration = writeToJournal(journal);

        final CountDownLatch latch = new CountDownLatch(producedBatches.size());
        PayloadCompareListener listener = new PayloadCompareListener( chronicleBatch, latch );
        final ReaderStart<ChronicleBatch> start = new ReaderStart<ChronicleBatch>(listener);
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



    private class PayloadCompareListener implements JournalUpdateListener<ChronicleBatch> {
        final AtomicLong duration = new AtomicLong();
        final long start = System.currentTimeMillis();
        final ChronicleBatch expected;
        final CountDownLatch latch;

        private PayloadCompareListener(ChronicleBatch expected, CountDownLatch latch) {
            this.expected = expected;
            this.latch = latch;
        }

        @Override
        public void onEvent(ChronicleBatch batch) {
            latch.countDown();
            compareBatches(expected, batch);
            if ( latch.getCount() == 0 ) {
                duration.set( System.currentTimeMillis() - start );
            }
        }

        void compareBatches(ChronicleBatch<SimpleModelEvent> expected, ChronicleBatch<SimpleModelEvent> actual) {
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
