package pers.yanxuanshaozhu.Nu01_hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HDFSClient1 {
    public static void main(String[] args) throws IOException, InterruptedException {
        //1. Create an abstract object for the cluster
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "yanxuanshaozhu");
        //2. use the abstract object to manipulate the cluster
        fileSystem.mkdirs(new Path("/testAPT"));
        fileSystem.rename(new Path("/testAPT"),new Path("/testAPI"));
        //3. close the resource
        fileSystem.close();
    }
}
