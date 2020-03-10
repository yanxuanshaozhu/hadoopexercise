package pers.yanxuanshaozhu.Nu17_zookeeperapitest;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class ZookeeperClient {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 2000;
        ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("Default Callback Function.");
            }
        });


        zooKeeper.create("/testAPI", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        List<String> children = zooKeeper.getChildren("/testAPI", true);
        for (String child : children) {
            System.out.println(child);
        }


        Stat stat = new Stat();
        zooKeeper.getData("/testAPI", new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("Usrdefined Callback Function");
            }
        }, stat);
        zooKeeper.close();

        Stat exists = zooKeeper.exists("/testAPI", true);
        if (exists != null) {
            System.out.println("Znode exists.");
        } else {
            System.out.println("Znode doesn't exist.");
        }

        Stat exist = zooKeeper.exists("/testAPI", true);
        if (exist == null) {
            System.out.println("Znode doesn't exist");
        } else {
            zooKeeper.setData("/testAPI", "API".getBytes(), exist.getVersion());
        }

        Stat ifexist = zooKeeper.exists("/testAPI", true);
        if (ifexist == null) {
            System.out.println("Znode doesn't exist");
        } else {
            zooKeeper.delete("/testAPI", ifexist.getVersion());
        }

    }
}
