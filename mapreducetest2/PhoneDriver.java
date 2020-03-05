package pers.yanxuanshaozhu.mapreducetest2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration(), "PhoneJob");

        job.setJarByClass(PhoneDriver.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(PhoneBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
