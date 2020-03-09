package pers.yanxuanshaozhu.Nu16_top10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable, Text, PhoneBean, NullWritable> {
    PhoneBean phoneBean = new PhoneBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        phoneBean.setPhone(words[0]);
        phoneBean.setUpFlow(Long.parseLong(words[1]));
        phoneBean.setDownFlow(Long.parseLong(words[2]));
        phoneBean.setSumFlow(Long.parseLong(words[3]));
        context.write(phoneBean, NullWritable.get());
    }
}
