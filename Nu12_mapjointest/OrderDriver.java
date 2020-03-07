package pers.yanxuanshaozhu.Nu12_mapjointest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "MapJoinTest");

        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.addCacheFile(URI.create("file:///d:/hadoopdata/input/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
