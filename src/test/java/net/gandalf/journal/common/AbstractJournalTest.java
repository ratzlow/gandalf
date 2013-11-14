package net.gandalf.journal.common;

import net.gandalf.journal.api.*;
import net.gandalf.journal.chronicle.ChronicleBatch;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-19
 */
public abstract class AbstractJournalTest {

    private static final Logger LOGGER = Logger.getLogger(AbstractJournalTest.class);

    public final static int batchCount = 1000;
    public final static int eventsCount = 100;
    protected static List<ChronicleBatch> producedBatches = new ArrayList<ChronicleBatch>();
    protected List<ChronicleBatch> consumedBatches = new ArrayList<ChronicleBatch>();

    protected Map<String, Double> consumerDurations = new HashMap<String, Double>();
    protected long  producerDuration = 0;


    /**
     * Producer side of the queue
     */
    protected long writeToJournal(Journal journal) throws InterruptedException {
        LOGGER.info("Start sending messages to chronicle ...");
        final AtomicLong producerDuration = new AtomicLong(0);
        final CountDownLatch producerLatch = new CountDownLatch(batchCount);
        ExecutorService producer = Executors.newSingleThreadExecutor();
        final Writer<ChronicleBatch> writer = journal.createWriter();
        writer.start();
        producer.submit(new Runnable() {
            @Override
            public void run() {
                long startProducer = System.nanoTime();

                for (ChronicleBatch batch : producedBatches) {
                    long index = writer.add(batch);
                    if (index > batchCount)
                        LOGGER.error("found too high index");//Assert.assertTrue( index < batchCount );
                    producerLatch.countDown();
                }
                producerDuration.set((long) ((System.nanoTime() - startProducer) / 1e6));
            }
        });

        producerLatch.await(5, TimeUnit.SECONDS);
        return producerDuration.get();
    }


    protected long readFromJournal( final Journal journal ) throws InterruptedException {
        return readFromJournal( journal, new MockJournalUpdateListener() );
    }

    protected long readFromJournal( final Journal journal, MockJournalUpdateListener listener )
            throws InterruptedException {

        LOGGER.info("Start reading messages from chronicle journal ...");
        //-------------------------------------------------------------
        // Consumer side of the queue
        //-------------------------------------------------------------

        final ExecutorService consumer = Executors.newSingleThreadExecutor();

        final Reader reader = journal.createReader(new ReaderStart<ChronicleBatch>(listener));
        consumer.submit( new Runnable() {
            @Override
            public void run() {
                reader.start();
            }
        });

        listener.latch.await(12, TimeUnit.SECONDS);
        consumer.shutdown();
        long timeMS = listener.getDuration();
        Assert.assertTrue( "Could not find duration for reading the journal!", timeMS > 0 );
        return timeMS;
    }



    @AfterClass
    public static void cleanup() {
        JournalTestUtil.deleteFiles();
    }


    @BeforeClass
    public static void initStatic() {
        producedBatches = JournalTestUtil.createEventBatches(batchCount, eventsCount);
    }

    @Before
    public void init() {
        JournalTestUtil.deleteFiles();
        consumedBatches.clear();
        consumerDurations.clear();
        producerDuration = 0;
    }

    @After
    public void after() {
        LOGGER.info("Batch metrics: no. batches = " + batchCount + " with " + eventsCount+
                " events with total size in MB " + producedBatches.get(0).getSize() * producedBatches.size() / 1024 / 1024);
        LOGGER.info("Producer finished after ms = " + producerDuration);

        StringBuilder sb = new StringBuilder("Consumer finished after ms = ");
        for (Map.Entry<String, Double> entry : consumerDurations.entrySet()) {
            Double value = entry.getValue();
            String consumerMsg = "[" + entry.getKey() + " = " + value + "] ";
            sb.append(consumerMsg);
            Assert.assertTrue( "Consumer duration must be longer than '0'! " + consumerMsg, value > 0 );
        }
        LOGGER.info(sb.toString());
    }
}
