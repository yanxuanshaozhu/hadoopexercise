package pers.yanxuanshaozhu.Nu15_reverseindextest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IndexMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text text = new Text();
    private IntWritable val = new IntWritable(1);
    FileSplit fileSplit;
    String title;

    @Override
    protected void setup(Context context) {
        fileSplit = (FileSplit) context.getInputSplit();
        title = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            text.set(word + "\t" + title);
            context.write(text, val);
        }
    }
}
