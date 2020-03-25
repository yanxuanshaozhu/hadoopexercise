package pers.yanxuanshaozhu.Nu19_hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFDemo extends UDF {

    //reverse a string
    public String evaluate(String input) {
        if (input == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder(input);
        return builder.reverse().toString();
    }
}
