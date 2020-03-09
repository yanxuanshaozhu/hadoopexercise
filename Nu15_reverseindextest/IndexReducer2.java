package pers.yanxuanshaozhu.Nu15_reverseindextest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexReducer2 extends Reducer<Text, Text, Text, Text> {
    Text text = new Text();
    StringBuilder builder = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        builder.setLength(0);
        for (Text value : values) {
            builder.append(value + "\t");
        }
        text.set(builder.toString());
        context.write(key, text);
    }
}
