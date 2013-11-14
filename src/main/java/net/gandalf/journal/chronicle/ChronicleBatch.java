package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.EventBatch;
import net.gandalf.journal.api.JournalException;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO: comment
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

    public ChronicleBatch(List<E> entries, Class<E> clazz) {
        this.entries = entries;
        this.clazz = clazz;
    }

    //
    // API
    //

    @Override
    public void readMarshallable( Bytes in ) {
        try {
            index = in.readLong();

            String clazzName = in.readUTF();
            this.clazz = (Class<E>) Class.forName(clazzName);

            entries = new ArrayList<E>();
            in.readList(entries, clazz);
        } catch (Exception e) {
            throw new JournalException("Could not read batch from chronicle!", e );
        }
    }

    @Override
    public void writeMarshallable( Bytes out ) {
        try {
            out.writeLong(index);
            out.writeUTF(clazz.getCanonicalName());
            out.writeList(entries);
        } catch (Exception e) {
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

    public long getSize() {
        int size = getClass().getCanonicalName().length();
        for (E event : entries) {
            size += event.getSize();
        }

        return size;
    }

    public void setIndex(long index) {
        this.index = index;
    }
}
