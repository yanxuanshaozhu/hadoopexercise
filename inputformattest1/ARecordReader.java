package pers.yanxuanshaozhu.inputformattest1;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ARecordReader extends RecordReader<Text, BytesWritable> {

    private boolean isRead = false;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    FileSplit fs;
    FSDataInputStream open;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fs = (FileSplit) inputSplit;
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        open = fileSystem.open(fs.getPath());
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isRead) {
            key.set(fs.getPath().toString());
            byte[] buff = new byte[(int) fs.getLength()];
            open.read(buff);
            value.set(buff, 0, buff.length);

            isRead = true;
            return true;
        } else {
            return false;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    public void close() throws IOException {
        IOUtils.closeStream(open);
    }
}
