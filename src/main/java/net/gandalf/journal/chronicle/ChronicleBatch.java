package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.EventBatch;
import net.gandalf.journal.api.JournalException;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;

import java.util.ArrayList;
import java.util.List;

/**
 * Collector object containing all entries that need to be written to a chronicle data file in one TX.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-03
 */
public class ChronicleBatch<E extends ChronicleEntry>
        implements EventBatch<E>, BytesMarshallable {

    private List<E> entries;
    private Class<E> clazz;
    private long index;

    //
    // constructors
    //

    /**
     * Create a new item that can be written to the journal.
     *
     * @param entries the actual payload we will write
     * @param clazz type token to differentiate batches containing different payload e.g.
     *              - xml msg to sent
     *              - persistence entries containing the deltas to write to the DB
     */
    public ChronicleBatch(List<E> entries, Class<E> clazz) {
        this.entries = entries;
        this.clazz = clazz;
    }

    //
    // API
    //

    /**
     * Reestate a batch from provided stream.
     *
     * @param in handle to the stream to read this batch from
     */
    @Override
    public void readMarshallable( Bytes in ) {
        try {
            index = in.readLong();

            String clazzName = in.readUTF();
            this.clazz = (Class<E>) Class.forName(clazzName);

            entries = new ArrayList<E>();
            in.readList(entries, clazz);
        } catch (Exception e) {
            // TODO: improve exception handling/logging. Just log and skip?
            throw new JournalException("Could not read batch from chronicle!", e );
        }
    }

    /**
     * Write the current instance of the batch into the stream.
     *
     * @param out stream to write to.
     */
    @Override
    public void writeMarshallable( Bytes out ) {
        try {
            out.writeLong(index);
            out.writeUTF(clazz.getCanonicalName());
            out.writeList(entries);
        } catch (Exception e) {
        // TODO: improve exception handling/logging. Just log and skip?
            throw new JournalException("Could not write batch to chronicle!", e );
        }
    }

    @Override
    public List<E> getEntries() {
        return entries;
    }

    @Override
    public long getIndex() {
        return index;
    }

    /**
     * @return size in bytes as approximative value
     */
    public long getSize() {
        // TODO: can this be done more efficiently?
        int size = getClass().getCanonicalName().length();
        for (E event : entries) {
            size += event.getSize();
        }

        return size;
    }

    /**
     * The index of this batch when written to the journal. It cannot be used for next journal.
     * @param index of this batch in the current journal.
     */
    // TODO: check how we can float a system unique sequence can be floated through the system as this one is
    // assigned on per journal base.
    public void setIndex(long index) {
        this.index = index;
    }
}
