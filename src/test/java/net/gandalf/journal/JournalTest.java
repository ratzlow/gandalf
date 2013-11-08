package net.gandalf.journal;

import net.gandalf.journal.api.Journal;
import net.gandalf.journal.api.JournalUpdateListener;
import net.gandalf.journal.api.Reader;
import net.gandalf.journal.api.ReaderStart;
import net.gandalf.journal.chronicle.ChronicleBatch;
import net.gandalf.journal.chronicle.ChronicleJournal;
import net.gandalf.journal.common.AbstractJournalTest;
import net.gandalf.journal.common.JournalTestUtil;
import net.gandalf.sampleclient.oh.event.ModelEvent;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Check basic funcationality of reading/writing to a journal.
 * TODO: crash,  mix different event types, restart multiple listeners
 *
 * @author ratzlow@gmail.com
 * @since 2013-09-28
 */
public class JournalTest extends AbstractJournalTest {

    private static final Logger LOGGER = Logger.getLogger(JournalTest.class);
    private final String fileName = "AsyncReadWrite";

    @Test
    public void testSingleProducerSingleConsumer() throws InterruptedException {

        final Journal journal = new ChronicleJournal( JournalTestUtil.createLogFileNameRandom(fileName), ModelEvent.class);
        producerDuration = writeToJournal(journal);
        runConsumer(journal, 1);

        journal.stop();
    }


    @Test
    public void testSingleProducerManyConsumer() throws InterruptedException {
        final Journal journal = new ChronicleJournal( JournalTestUtil.createLogFileNameRandom(fileName), ModelEvent.class );

        producerDuration = writeToJournal(journal);
        runConsumer(journal, 1 );
        runConsumer(journal, 2 );
        runConsumer(journal, 3);

        journal.stop();
    }

    @Test
    public void testReplayConsumerFromGivenIndex() throws InterruptedException {
        final Journal journal = new ChronicleJournal( JournalTestUtil.createLogFileNameRandom(fileName) );

        producerDuration = writeToJournal(journal);

        final long startIndex = (long) (0.95 * batchCount);

        RunFromIndexListener listener = new RunFromIndexListener(startIndex);
        final Reader reader = journal.createReader( new ReaderStart<ChronicleBatch>(listener, 0, startIndex) );
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        consumer.submit( new Runnable() {
            @Override
            public void run() {
                reader.start();
            }
        });

        listener.assertState();
        journal.stop();
    }

    private class RunFromIndexListener implements JournalUpdateListener<ChronicleBatch> {
        final AtomicLong nanos = new AtomicLong(0);
        final CountDownLatch latch;
        final long startIndex;
        long expectedInvokedIndex;

        private RunFromIndexListener(long startIndex) {
            this.latch = new CountDownLatch((int) (batchCount - startIndex));
            this.startIndex = startIndex;
            expectedInvokedIndex = startIndex;
        }

        @Override
        public void onEvent(ChronicleBatch batch) {
            long start = System.nanoTime();
            latch.countDown();
            long index = batch.getIndex();
            // check we start at given index
            if ( startIndex == expectedInvokedIndex ) {
                Assert.assertEquals( startIndex, index);
            }
            Assert.assertEquals( expectedInvokedIndex, index );
            expectedInvokedIndex++;
            LOGGER.info("listener called for index " + index);
            nanos.getAndAdd( System.nanoTime() - start);
        }

        void assertState() throws InterruptedException {
            Assert.assertTrue("Missed to record events! reached = " + latch.getCount(),
                    latch.await(6, TimeUnit.SECONDS));
            consumerDurations.put("Consumer_1", nanos.get() / 1e6);

            // check we were notified up to the last event of the created batches
            Assert.assertEquals( batchCount, expectedInvokedIndex);
        }
    }

    private void runConsumer( final Journal journal, int consumerIndex ) throws InterruptedException {
        long duration = readFromJournal( journal );
        consumerDurations.put( "Consumer_" + consumerIndex, (double)duration );
    }
}
