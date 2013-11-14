package net.gandalf.journal.common;

import net.gandalf.journal.api.JournalUpdateListener;
import net.gandalf.journal.chronicle.ChronicleBatch;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;

/**
 * // TODO: comment
 */
public class MockJournalUpdateListener implements JournalUpdateListener<ChronicleBatch> {
    final CountDownLatch latch = new CountDownLatch(AbstractJournalTest.batchCount);
    // todo: how fixing this visibility issue?
    private volatile long duration = 0;
    final long startNS = System.nanoTime();
    private ChronicleBatch firstBatch;
    private ChronicleBatch lastBatch;



    @Override
    public void onEvent(ChronicleBatch batch) {
        latch.countDown();
        if (firstBatch == null) firstBatch = batch;
        lastBatch = batch;

        if (latch.getCount() == 0) {
            long durationMS = (long) ((System.nanoTime() - startNS) / 1e6);
            duration = durationMS;
        }
        Assert.assertEquals(AbstractJournalTest.eventsCount, batch.getEntries().size());
    }

    synchronized long getDuration() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return duration;
    }
}