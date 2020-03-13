package pers.yanxuanshaozhu.Nu18_commonfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        v.set(split[0]);
        String[] persons = split[1].split(",");
        for (String person : persons) {
            k.set(person);
            context.write(k, v);
        }
    }
}
