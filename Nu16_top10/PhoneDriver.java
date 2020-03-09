package pers.yanxuanshaozhu.Nu16_top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

        job.setGroupingComparatorClass(AComparator.class);

        job.setMapOutputKeyClass(PhoneBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(PhoneBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
