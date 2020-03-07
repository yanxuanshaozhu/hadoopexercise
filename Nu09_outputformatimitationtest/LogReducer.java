package pers.yanxuanshaozhu.Nu09_outputformatimitationtest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<LogBean, NullWritable, LogBean, NullWritable> {


    @Override
    protected void reduce(LogBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key,value);
        }
    }
}
