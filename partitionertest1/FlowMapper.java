package pers.yanxuanshaozhu.partitionertest1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split("\t");
        String phone = values[1];
        text.set(phone);
        long upFlow = Long.parseLong(values[values.length - 3]);
        long downFlow = Long.parseLong(values[values.length - 2]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(upFlow + downFlow);
        context.write(text, flowBean);

    }
}
