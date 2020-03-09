package pers.yanxuanshaozhu.Nu15_reverseindextest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable sum = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        sum.set(count);
        context.write(key, sum);
    }
}
