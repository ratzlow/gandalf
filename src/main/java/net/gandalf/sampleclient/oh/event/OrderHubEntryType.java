package net.gandalf.sampleclient.oh.event;

import net.gandalf.journal.api.Entry;
import net.gandalf.journal.api.EntryType;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-20
 */
public enum OrderHubEntryType implements EntryType {
    MODEL_UPDATE(ModelEvent.class);

    //
    // private attributes
    //

    private Class<? extends Entry> clazz;

    //
    // constructors
    //

    private OrderHubEntryType(Class<? extends Entry> clazz) {
        this.clazz = clazz;
    }
}
