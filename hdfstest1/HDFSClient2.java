package pers.yanxuanshaozhu.hdfstest1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSClient2 {
    private FileSystem fileSystem;
    private Configuration configuration = new Configuration();

    @Before
    public void before() throws IOException, InterruptedException {
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "yanxuanshaozhu");
    }

    @Test
    public void cpl() throws IOException {
        fileSystem.copyToLocalFile(new Path("/phone.txt"), new Path("d:/"));
    }

    @After
    public void after() throws IOException {
        fileSystem.close();
    }
}
