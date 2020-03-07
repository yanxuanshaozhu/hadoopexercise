package pers.yanxuanshaozhu.reducejointest1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private String pid;
    private int amount;
    private String pname;

    public String getId() {
        return id;
    }

    public String getPid() {
        return pid;
    }

    public int getAmount() {
        return amount;
    }

    public String getPname() {
        return pname;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id=" + id +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
    }

    public int compareTo(OrderBean o) {
        int order = this.getPid().compareTo(o.getPid());
        if (order == 0) {
            return o.getPname().compareTo(this.getPname());
        } else {

            return order;
        }
    }
}
