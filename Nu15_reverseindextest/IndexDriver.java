package pers.yanxuanshaozhu.Nu15_reverseindextest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "IndexJob");

        job.setJarByClass(IndexDriver.class);
        job.setMapperClass(IndexMapper1.class);
        job.setReducerClass(IndexReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        boolean b = job.waitForCompletion(true);

        while (b) {

            Job job2 = Job.getInstance(new Configuration(), "IndexJob2");

            job2.setJarByClass(IndexDriver.class);
            job2.setMapperClass(IndexMapper2.class);
            job2.setReducerClass(IndexReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2, new Path("D:\\hadoopdata\\output"));
            FileOutputFormat.setOutputPath(job2, new Path("D:\\hadoopdata\\output2"));

            boolean b2 = job2.waitForCompletion(true);

            System.exit(b2 ? 0 : 1);
        }
    }
}
