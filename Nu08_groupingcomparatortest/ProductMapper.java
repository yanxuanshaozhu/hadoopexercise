package pers.yanxuanshaozhu.Nu08_groupingcomparatortest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text, ProductBean, NullWritable> {


    private ProductBean productBean = new ProductBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        String productId = words[0];
        String productName = words[1];
        double price = Double.parseDouble(words[2]);

        productBean.setProductId(productId);
        productBean.setProductName(productName);
        productBean.setPrice(price);

        context.write(productBean, NullWritable.get());

    }
}
