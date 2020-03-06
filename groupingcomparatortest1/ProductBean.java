package pers.yanxuanshaozhu.groupingcomparatortest1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProductBean implements WritableComparable<ProductBean> {
    private String productId;
    private String productName;
    private double price;

    @Override
    public String toString() {
        return "ProductBean{" +
                "productId=" + productId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public double getPrice() {
        return price;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(productName);
        dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.productId = dataInput.readUTF();
        this.productName = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    public int compareTo(ProductBean o) {
        long compare = (this.getProductId()).compareTo(o.getProductId());
        if ( compare == 0) {
            return Double.compare(o.getPrice(),this.getPrice());
        } else {
            return (int) compare;
        }
    }
}
