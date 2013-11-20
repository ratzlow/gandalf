package net.gandalf.journal.sample.obj2journal;

import net.gandalf.journal.api.JournalException;
import net.gandalf.journal.chronicle.ChronicleEntry;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-12
 */
public class FastModelEvent implements ChronicleEntry, BytesMarshallable {

    private Map<ModelDD, Object> entries;

    public FastModelEvent(Map<ModelDD, Object> entries) {
        this.entries = entries;
    }

    @Override
    public void readMarshallable(Bytes in) throws IllegalStateException {
        int entriesCount = in.readInt();
        if ( entriesCount % 2 != 0 ) throw new JournalException("Invalid number of key/value pairs!");

        entries = new HashMap<ModelDD, Object>();
        for ( int i=0; i < entriesCount; i++) {
            entries = SerializerRegistry.getInstance().readValue( in, entries );
        }

    }

    @Override
    public void writeMarshallable( Bytes out ) {
        out.writeInt( entries.size() );

        for ( Map.Entry<ModelDD, Object> mapEntry : entries.entrySet() ) {
            SerializerRegistry.getInstance().writeValue( out, mapEntry.getKey(), mapEntry.getValue() );
        }
    }

    @Override
    public int getSize() {
        return getClass().getCanonicalName().length();
    }

}
