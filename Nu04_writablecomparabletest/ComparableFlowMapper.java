package pers.yanxuanshaozhu.Nu04_writablecomparabletest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComparableFlowMapper extends Mapper<LongWritable, Text, ComparableFlowBean, Text> {

    private ComparableFlowBean comparableFlowBean = new ComparableFlowBean();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        String phone = words[1];
        text.set(phone);

        long upFlow = Long.parseLong(words[words.length - 3]);
        long downFlow = Long.parseLong(words[words.length - 2]);
        long sumFlow = upFlow + downFlow;

        comparableFlowBean.setUpFlow(upFlow);
        comparableFlowBean.setDownFlow(downFlow);
        comparableFlowBean.setSumFlow(sumFlow);

        context.write(comparableFlowBean, text);
    }
}
