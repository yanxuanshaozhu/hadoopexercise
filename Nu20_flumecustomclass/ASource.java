package pers.yanxuanshaozhu.Nu20_flumecustomclass;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class ASource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;

    public Status process() throws EventDeliveryException {
        Status status = null;
        try {
            Event event = getSomeData();
            getChannelProcessor().processEvent(event);
            status = Status.READY;
        } catch (Throwable throwable) {
            status = Status.BACKOFF;
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
        }
        return status;
    }

    private Event getSomeData() {
        //get some data from data source
        String data = Double.toString(Math.random() * 1000).substring(0, 3);
        SimpleEvent event = new SimpleEvent();
        event.setBody((prefix + data).getBytes());
        return event;
    }

    public long getBackOffSleepIncrement() {
        return 1000;
    }

    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    public void configure(Context context) {
        prefix = context.getString("prefix");
    }
}
