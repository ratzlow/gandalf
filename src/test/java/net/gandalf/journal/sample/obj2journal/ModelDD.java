package net.gandalf.journal.sample.obj2journal;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * TODO: comment
 *
 * @author ratzlow@gmail.com
 * @since 2013-11-12
 */
public enum ModelDD implements Attribute {

    ID(Long.class),
    SIZE(Integer.class),
    LUSER(String.class),
    OUSER(String.class),
    STEXT(String.class),
    DELAYED_ASK(BigDecimal.class),
    DELAYED_BID(BigDecimal.class),
    FQOPEN(Integer.class),
    FQDONE(Integer.class),
    STATE(OrderState.class),
    FSIZE(Integer.class),
    OSTAMP(Timestamp.class),
    CSTAMP(Timestamp.class),
    ASK_PRICE(BigDecimal.class);

    private Class<?> clazz;

    ModelDD(Class<?> clazz) {
        this.clazz = clazz;
    }

    public Class<?> getValueClazz() {
        return clazz;
    }
}
