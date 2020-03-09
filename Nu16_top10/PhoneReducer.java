package pers.yanxuanshaozhu.Nu16_top10;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class PhoneReducer extends Reducer<PhoneBean, NullWritable, PhoneBean, NullWritable> {
    PhoneBean phoneBean = new PhoneBean();

    @Override
    protected void reduce(PhoneBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iter = values.iterator();
        for (int i = 0; i < 10; i++) {
            if (iter.hasNext()) {
                context.write(key,iter.next());
            }
        }
    }
}
