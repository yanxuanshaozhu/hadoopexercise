package pers.yanxuanshaozhu.Nu03_mapreducetest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class PhoneReducer extends Reducer<Text, PhoneBean, Text, PhoneBean> {
    private PhoneBean total = new PhoneBean();

    @Override
    protected void reduce(Text key, Iterable<PhoneBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        long sumFlow = 0;

        for (PhoneBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            sumFlow += value.getSumFlow();
        }
        total.setUpFlow(upFlow);
        total.setDownFlow(downFlow);
        total.setSumFlow(sumFlow);
        context.write(key, total);
    }
}
