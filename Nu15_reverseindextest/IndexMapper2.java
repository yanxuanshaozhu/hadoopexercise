package pers.yanxuanshaozhu.Nu15_reverseindextest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IndexMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    Text text1 = new Text();
    Text text2 = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        String val = words[2];
        String file = words[1];
        String name = words[0];
        text1.set(name);
        text2.set(file + "-->" + val);
        context.write(text1, text2);
    }
}
