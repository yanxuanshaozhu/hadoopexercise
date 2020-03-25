package pers.yanxuanshaozhu.Nu20_flumecustomclass;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class ASink extends AbstractSink implements Configurable {
    private String prefix;
    private String suffix;

    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            storeSomeData(event);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error) t;
            }
        }
        return status;
    }

    private void storeSomeData(Event event) {
        System.out.println(prefix);
        System.out.println(event.getBody());
        System.out.println(suffix);
    }

    public void configure(Context context) {
        prefix = context.getString("prefix");
        suffix = context.getString("suffix");

    }
}
