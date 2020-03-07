package pers.yanxuanshaozhu.Nu08_groupingcomparatortest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProductDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(new Configuration(), "TestJob");

        job.setJarByClass(ProductDriver.class);
        job.setMapperClass(ProductMapper.class);
        job.setReducerClass(ProductReducer.class);

        job.setGroupingComparatorClass(ProductComparator.class);

        job.setMapOutputKeyClass(ProductBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(ProductBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
