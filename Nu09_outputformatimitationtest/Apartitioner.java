package pers.yanxuanshaozhu.Nu09_outputformatimitationtest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Apartitioner extends Partitioner<LogBean, NullWritable> {
    public int getPartition(LogBean logBean, NullWritable nullWritable, int i) {
        String location = logBean.getLocation();
        if (location.contains("google")) {
            return 0;
        } else {
            return 1;
        }
    }
}
