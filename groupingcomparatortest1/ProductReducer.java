package pers.yanxuanshaozhu.groupingcomparatortest1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductReducer extends Reducer<ProductBean, NullWritable, ProductBean, NullWritable> {

    @Override
    protected void reduce(ProductBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
