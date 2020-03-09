package pers.yanxuanshaozhu.Nu16_top10;


import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneBean implements WritableComparable<PhoneBean> {
    private String phone;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getPhone() {
        return phone;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }


    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return phone + '\t' + upFlow + '\t' + downFlow + '\t' + sumFlow;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public int compareTo(PhoneBean o) {
        return (int) (o.getSumFlow() - this.getSumFlow());
    }
}
