package pers.yanxuanshaozhu.Nu10_outputformattest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration(), "OutputFormat Job");

        job.setJarByClass(LogDriver.class);

        job.setOutputFormatClass(AOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
