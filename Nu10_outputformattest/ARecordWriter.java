package pers.yanxuanshaozhu.Nu10_outputformattest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ARecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream outputStream1;
    private FSDataOutputStream outputStream2;

    public void initialize(TaskAttemptContext taskAttemptContext) throws IOException {

        Configuration configuration = taskAttemptContext.getConfiguration();
        String outDir = configuration.get(FileOutputFormat.OUTDIR);

        FileSystem fileSystem = FileSystem.get(configuration);
        outputStream1 = fileSystem.create(new Path(outDir + "\\google.log"));
        outputStream2 = fileSystem.create(new Path(outDir + "\\other.log"));
    }

    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {
        String line = text.toString() + "\n";
        if (line.contains("google")) {
            outputStream1.write(line.getBytes());
        } else {
            outputStream2.write(line.getBytes());
        }

    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outputStream1.close();
        outputStream2.close();
    }
}
