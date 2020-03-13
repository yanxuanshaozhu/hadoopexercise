package pers.yanxuanshaozhu.Nu18_commonfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class FFDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FFDriver.class);

        job.setMapperClass(FFMapper1.class);
        job.setReducerClass(FFReducer1.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopdata\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopdata\\output1"));

        boolean b = job.waitForCompletion(true);

        if (b) {
            Job job2 = Job.getInstance(new Configuration());

            job2.setJarByClass(FFDriver.class);

            job2.setMapperClass(FFMapper2.class);
            job2.setReducerClass(FFReducer2.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2, new Path("D:\\hadoopdata\\output1"));
            FileOutputFormat.setOutputPath(job2, new Path("D:\\hadoopdata\\output2"));

            boolean b2 = job2.waitForCompletion(true);
            System.exit(b2 ? 0 : 1);
        }
    }
}
