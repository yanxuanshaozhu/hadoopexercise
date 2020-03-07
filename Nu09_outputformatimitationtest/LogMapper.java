package pers.yanxuanshaozhu.Nu09_outputformatimitationtest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, LogBean, NullWritable> {

    private LogBean logBean = new LogBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String location = value.toString();

        logBean.setLocation(location);

        context.write(logBean, NullWritable.get());

    }
}
