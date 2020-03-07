package pers.yanxuanshaozhu.Nu10_outputformattest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AOutputFormat extends FileOutputFormat<LongWritable, Text> {
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        ARecordWriter recordWriter = new ARecordWriter();
        recordWriter.initialize(taskAttemptContext);
        return  recordWriter;
    }
}
