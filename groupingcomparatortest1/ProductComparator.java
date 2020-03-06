package pers.yanxuanshaozhu.groupingcomparatortest1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ProductComparator extends WritableComparator {
    protected ProductComparator() {
        super(ProductBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((ProductBean) a).getProductId().compareTo(((ProductBean) b).getProductId());
    }
}
