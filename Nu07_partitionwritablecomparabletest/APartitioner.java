package pers.yanxuanshaozhu.Nu07_partitionwritablecomparabletest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class APartitioner extends Partitioner<ComparableFlowBean, Text> {

    public int getPartition(ComparableFlowBean comparableFlowBean, Text text, int i) {
        String phone = text.toString().substring(0, 3);

        if ("136".equals(phone)) {
            return 0;
        } else if ("137".equals(phone)) {
            return 1;
        } else if ("138".equals(phone)) {
            return 2;
        } else  if ("139".equals(phone)) {
            return 3;
        } else {
            return 4;
        }
    }
}
