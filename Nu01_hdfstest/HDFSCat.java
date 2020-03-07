package pers.yanxuanshaozhu.Nu01_hdfstest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSCat {
    private FileSystem fileSystem;
    private Configuration configuration = new Configuration();

    @Before
    public void before() throws IOException, InterruptedException {
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"yanxuanshaozhu");
    }

    @Test
    public void cat() throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/phone.txt"));
        IOUtils.copyBytes(inputStream, System.out,1024);
        System.out.println();
    }

    @After
    public void after() throws IOException {
        fileSystem.close();
    }
}
