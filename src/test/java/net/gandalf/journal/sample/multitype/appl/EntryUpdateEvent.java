package net.gandalf.journal.sample.multitype.appl;

import net.gandalf.journal.sample.mapevent.DmlType;
import net.gandalf.journal.sample.obj2journal.Attribute;

import java.util.Map;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-22
 */
public class EntryUpdateEvent {
    private final DmlType dmlType;
    private final String tableName;
    private final Map<Attribute, Object> values;

    public EntryUpdateEvent(DmlType dmlType, String tableName, Map<Attribute, Object> values) {
        this.dmlType = dmlType;
        this.tableName = tableName;
        this.values = values;
    }

    public DmlType getDmlType() {
        return dmlType;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<Attribute, Object> getValues() {
        return values;
    }
}
