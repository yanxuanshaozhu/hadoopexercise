package pers.yanxuanshaozhu.Nu07_partitionwritablecomparabletest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComparableFlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "PartitionComparableJob");

        job.setJarByClass(ComparableFlowDriver.class);
        job.setMapperClass(ComparableFlowMapper.class);
        job.setReducerClass(ComparableFlowReducer.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(APartitioner.class);

        job.setMapOutputKeyClass(ComparableFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ComparableFlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
