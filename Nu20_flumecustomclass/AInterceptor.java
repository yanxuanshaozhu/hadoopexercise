package pers.yanxuanshaozhu.Nu20_flumecustomclass;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class AInterceptor implements Interceptor {

    public void initialize() {

    }

    //Use this interceptor to seperates the  events by digits and characters.

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String line = new String(body);
        char first = line.charAt(0);
        if ((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
            headers.put("type", "character");
        } else {
            headers.put("type", "digit");
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new AInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
