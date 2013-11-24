package net.gandalf.journal.sample.mapevent;


import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-09-28
 */
public class SimpleModelEvent implements Sizeable, BytesMarshallable {

    private DmlType dmlType;
    private String entityName;
    private Map<String, String> attributes;
    private int size = 0;

    public SimpleModelEvent(DmlType dmlType, String entityName, Map<String, String> attributes) {
        this.dmlType = dmlType;
        this.entityName = entityName;
        this.attributes = attributes;
        size = initSize();
    }

    public DmlType getDmlType() {
        return dmlType;
    }

    public String getEntityName() {
        return entityName;
    }

    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleModelEvent{");
        sb.append("dmlType=").append(dmlType);
        sb.append(", entityName='").append(entityName).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void readMarshallable( Bytes in ) throws IllegalStateException {
        attributes = new HashMap<String, String>();
        try {
            dmlType = in.readEnum(DmlType.class);
            entityName = in.readUTF();
            in.readMap(attributes, String.class, String.class);
            size = initSize();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeMarshallable( Bytes out ) {
        out.writeEnum(dmlType);
        out.writeUTF(entityName);
        out.writeMap(attributes);
    }

    /**
     * @return approximative value
     */
    @Override
    public int getSize() {
        return size;
    }

    private int initSize() {
        int size = 0;
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            size += entry.getKey().length() + entry.getKey().length();
        }

        size += entityName.length();
        return size;
    }
}
