package net.gandalf.journal.sample.multitype.appl;

import java.util.List;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-22
 */
public class ModelUpdateEvent {

    private final List<EntryUpdateEvent> entryUpdates;

    public ModelUpdateEvent(List<EntryUpdateEvent> entryUpdates) {
        this.entryUpdates = entryUpdates;
    }
}
