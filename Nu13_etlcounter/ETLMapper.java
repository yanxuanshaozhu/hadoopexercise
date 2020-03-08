package pers.yanxuanshaozhu.Nu13_etlcounter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text text = new Text();
    private Counter fail;
    private Counter success;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fail = context.getCounter("ETL", "FAIL");
        success = context.getCounter("ETL", "SUCCESS");

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        if (words.length >= 11) {
            text.set(value);
            context.write(text, NullWritable.get());
            success.increment(1);
        } else {
            fail.increment(1);
        }
    }
}
