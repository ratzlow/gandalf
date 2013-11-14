package net.gandalf.journal.sample.obj2journal;

import net.gandalf.journal.api.JournalException;
import net.openhft.lang.io.Bytes;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-12
 */
public class SerializerRegistry {
    private static SerializerRegistry INSTANCE = null;

    private final Map<ModelDD, ValueSerializer> valueReaders = new HashMap<ModelDD, ValueSerializer>();

    private SerializerRegistry() {
        ValueSerializer stringReader = new ValueSerializer() {
            @Override
            public Object read(Bytes source) {
                return source.readUTF();
            }

            @Override
            public void write(Bytes target, Object value) {
                target.writeUTF( (String) value );
            }
        };

        ValueSerializer intReader = new ValueSerializer() {
            @Override
            public Object read(Bytes source) {
                boolean isNull = source.readBoolean();
                return !isNull ? source.readInt() : null;
            }

            @Override
            public void write(Bytes target, Object value) {
                boolean isNull = value == null;
                target.writeBoolean( isNull );
                if ( !isNull ) {
                    target.writeInt((Integer) value);
                }
            }
        };

        ValueSerializer longReader = new ValueSerializer() {
            @Override
            public Object read(Bytes source) {
                return source.readLong();
            }

            @Override
            public void write(Bytes target, Object value) {
                target.writeLong((Long) value);
            }
        };

        ValueSerializer bigDecReader = new ValueSerializer() {
            @Override
            public Object read(Bytes source) {
                return BigDecimal.valueOf( source.readDouble() );
            }

            @Override
            public void write(Bytes target, Object value) {
                target.writeDouble( ((BigDecimal) value).doubleValue() );
            }
        };

        ValueSerializer timestampReader = new ValueSerializer() {
            @Override
            public Object read(Bytes source) {
                return new Timestamp(source.readLong());
            }

            @Override
            public void write(Bytes target, Object value) {
                target.writeLong( ((Timestamp)value).getTime() ) ;
            }
        };

        ValueSerializer orderStateReader = new ValueSerializer() {
            Class<?> enumClazz = OrderState.class;
            @Override
            public Object read(Bytes source) {
                return source.readEnum( enumClazz);
            }

            @Override
            public void write(Bytes target, Object value) {
                target.writeEnum( value );
            }
        };

        for ( ModelDD attribute : ModelDD.values()) {
            if      ( attribute.getClazz().equals(Integer.class)) valueReaders.put( attribute, intReader );
            else if ( attribute.getClazz().equals(Long.class)) valueReaders.put( attribute, longReader );
            else if ( attribute.getClazz().equals(String.class)) valueReaders.put( attribute, stringReader );
            else if ( attribute.getClazz().equals(BigDecimal.class)) valueReaders.put( attribute, bigDecReader );
            else if ( attribute.getClazz().equals(Timestamp.class)) valueReaders.put( attribute, timestampReader );
            else if ( attribute.getClazz().equals(OrderState.class)) valueReaders.put( attribute, orderStateReader );
            else throw new JournalException("No mapping for " + attribute + " -> " + attribute.getClazz());
        }
    }


    public static SerializerRegistry getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SerializerRegistry();
        }

        return INSTANCE;
    }

    public Map<ModelDD, Object> readValue( final Bytes source, final Map<ModelDD, Object> target ) {
        ModelDD attribute = source.readEnum( ModelDD.class );
        Object value = valueReaders.get( attribute ).read( source );
        target.put( attribute, value );
        return target;
    }

    public void writeValue(Bytes target, ModelDD key, Object value) {
        ValueSerializer valueSerializer = valueReaders.get(key);
        if ( valueSerializer == null ) {
            throw new JournalException("No de/serializer found for " + key + " : " + value);
        }

        try {
            valueSerializer.write(target, value);
        } catch (Exception e) {
            throw new JournalException("Cannot write " + key + " : " + value, e );
        }
    }
}
