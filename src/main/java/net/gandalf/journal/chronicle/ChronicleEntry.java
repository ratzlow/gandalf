package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.Entry;


/**
 * Multiple entries make up a batch. This supports the mimic to execute a list of something, where something is here
 * the ChronicleEntry.
 * Every ChronicleEntry needs to implement {@link net.openhft.lang.io.serialization.BytesMarshallable}.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
// todo: put interface straight on this class
public interface ChronicleEntry extends Entry {
    int getSize();
}
