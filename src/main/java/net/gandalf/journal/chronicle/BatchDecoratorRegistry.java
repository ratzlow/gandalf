package net.gandalf.journal.chronicle;

import net.gandalf.journal.api.JournalException;

import java.util.HashMap;
import java.util.Map;

/**
 * Keep all mappings to decorators that tell chronicle how to deal with a particular batch type.
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-23
 */
public class BatchDecoratorRegistry {
    private Map<Class<?>, ChronicleBatchDecorator<?>> registry =
            new HashMap<Class<?>, ChronicleBatchDecorator<?>>();

    //
    // public API
    //

    public <T> void add(Class<T> batchClazz, ChronicleBatchDecorator<T> decorator ) {
        registry.put( batchClazz, decorator);
    }

    public <T> ChronicleBatchDecorator<T> getDecorator( Class batchClazz ) {
        return getDecoratorInternal( batchClazz );
    }

    public <T> ChronicleBatchDecorator<T> getDecorator( String batchClazz ) {
        Class<T> clazz = clazzFromString( batchClazz );
        return getDecoratorInternal( clazz );
    }

    //
    // internal impl
    //

    private Class clazzFromString(String batchClazz) {
        if ( batchClazz == null || batchClazz.isEmpty() ) {
            throw new IllegalArgumentException("No valid batchClazz name specified! clazz = " + batchClazz );
        }

        try {
            return Class.forName(batchClazz);
        } catch (Exception e) {
            throw new JournalException( "Clazz cannot be used as key to lookup decorator! Used = " + batchClazz, e );
        }
    }

    private <T> ChronicleBatchDecorator<T> getDecoratorInternal( Class<T> batchClazz ) {
        final ChronicleBatchDecorator<?> decorator = registry.get(batchClazz);
        if ( decorator == null ) {
            throw new JournalException("Could not find batch decorator for class = " + batchClazz );
        }

        return (ChronicleBatchDecorator<T>) decorator;
    }
}
