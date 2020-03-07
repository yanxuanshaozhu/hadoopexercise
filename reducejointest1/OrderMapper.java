package pers.yanxuanshaozhu.reducejointest1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();
    private FileSplit fileSplit;
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split("\t");

        if ("order.txt".equals(filename)) {
            String id = words[0];
            String pid = words[1];
            int amount = Integer.parseInt(words[2]);

            orderBean.setId(id);
            orderBean.setPid(pid);
            orderBean.setAmount(amount);
            orderBean.setPname("");
        } else   {
            String pid = words[0];
            String pname = words[1];
            orderBean.setId("");
            orderBean.setPid(pid);
            orderBean.setAmount(0);
            orderBean.setPname(pname);
        }
        context.write(orderBean, NullWritable.get());
    }
}
