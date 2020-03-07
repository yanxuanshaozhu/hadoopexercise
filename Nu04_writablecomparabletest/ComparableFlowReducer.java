package pers.yanxuanshaozhu.Nu04_writablecomparabletest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ComparableFlowReducer extends Reducer<ComparableFlowBean, Text, Text, ComparableFlowBean> {

    private ComparableFlowBean comparableFlowBean = new ComparableFlowBean();

    @Override
    protected void reduce(ComparableFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}

