package pers.yanxuanshaozhu.partitionertest1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class APartitioner extends Partitioner<Text, FlowBean> {
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String substring = text.toString().substring(0, 3);
        if ("136".equals(substring)) {
            return 0;
        } else if ("137".equals(substring)) {
            return 1;
        } else if ("138".equals(substring)) {
            return 2;
        } else if ("139".equals(substring)) {
            return 3;
        } else {
            return 4;
        }

    }
}
