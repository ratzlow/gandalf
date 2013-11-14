package net.gandalf.journal.sample.obj2journal;

import net.openhft.lang.io.Bytes;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-12
 */
public interface ValueSerializer {
    Object read( Bytes source );
    void write( Bytes target, Object value );
}
