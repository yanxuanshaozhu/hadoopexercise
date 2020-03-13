package pers.yanxuanshaozhu.Nu18_commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FFReducer1 extends Reducer<Text, Text, Text, Text> {

    private Text v = new Text();
    private StringBuilder sb = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.setLength(0);
        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }
        v.set(sb.substring(0, sb.length() - 1));
        context.write(key, v);
    }
}
