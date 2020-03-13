package pers.yanxuanshaozhu.Nu18_commonfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper2 extends Mapper<Text, Text, Text, Text> {

    private Text k = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] persons = value.toString().split(",");

        for (int i = 0; i < persons.length; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                if (persons[i].compareTo(persons[j]) < 0)
                    k.set(persons[i] + "-" + persons[j]);
                else
                    k.set(persons[j] + "-" + persons[i]);
                context.write(k, key);
            }
        }

    }
}
