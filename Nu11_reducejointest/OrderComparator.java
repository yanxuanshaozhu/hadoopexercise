package pers.yanxuanshaozhu.Nu11_reducejointest;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator {

    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((OrderBean) a).getPid().compareTo(((OrderBean) b).getPid());
    }
}
