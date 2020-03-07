package pers.yanxuanshaozhu.Nu08_groupingcomparatortest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ProductReducer extends Reducer<ProductBean, NullWritable, ProductBean, NullWritable> {

    @Override
    protected void reduce(ProductBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
        Iterator iter = values.iterator();
        context.write(key, (NullWritable) iter.next());

    }
}
