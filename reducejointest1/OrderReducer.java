package pers.yanxuanshaozhu.reducejointest1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator iter = values.iterator();
        iter.next();
        String pname = key.getPname();
        while (iter.hasNext()) {
            iter.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }

    }
}
