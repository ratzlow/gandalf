package net.gandalf.journal.chronicle;

/**
 * Introspect given batch type. A batch corresponds to chronicle excerpt.
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-23
 */
public interface ChronicleBatchDecorator<T> {

    /**
     * @param batch to read or write#
     * @return size in bytes
     */
    long getSize(T batch);


    /**
     * @param batch to read/write
     * @param index in the journal
     */
    void setIndex(T batch, long index);

    long getIndex(T batch);

    Class<T> getMarshallableClass();
}
