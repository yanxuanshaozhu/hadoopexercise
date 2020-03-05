package pers.yanxuanshaozhu.mapreducetest2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, Text, PhoneBean> {
    private Text word = new Text();
    private PhoneBean phoneBean = new PhoneBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split("\t");
        String phone = values[1];
        word.set(phone);
        long upFlow = Long.parseLong(values[values.length - 3]);
        long downFlow = Long.parseLong(values[values.length - 2]);
        phoneBean.setUpFlow(upFlow);
        phoneBean.setDownFlow(downFlow);
        phoneBean.setSumFlow(upFlow + downFlow);
        context.write(word, phoneBean);

    }
}
