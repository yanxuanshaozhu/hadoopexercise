package pers.yanxuanshaozhu.Nu09_outputformatimitationtest;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogBean implements WritableComparable {
    private String location;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "location='" + location + '\'' +
                '}';
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(location);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.location = dataInput.readUTF();
    }

    public int compareTo(Object o) {
        return  ((LogBean)o).getLocation().compareTo(((LogBean)o).getLocation());
    }

}
