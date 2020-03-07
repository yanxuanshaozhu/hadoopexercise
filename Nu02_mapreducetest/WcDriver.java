package pers.yanxuanshaozhu.Nu02_mapreducetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. Get a new job
        Job job = Job.getInstance(new Configuration(), "MyWordCount");

        //2. Set the jar package for the job
        job.setJarByClass(WcDriver.class);

        //3. Set the usr-defined mapper and reducer for the job
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //4. Set the output format of mapper and reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5. Set input path and output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. Submit the job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
