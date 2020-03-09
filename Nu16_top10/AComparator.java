package pers.yanxuanshaozhu.Nu16_top10;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }

    protected AComparator() {
        super(PhoneBean.class,true);
    }
}
