package com.ustb.ly;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author LinYue
 * @email ustb2021@126.com
 * @create 2021-02-20 15:49
 */
public class ZookeeperClient {
    private static String connectString =
            "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("默认回调函数");
            }
        });
    }

    /**
     * 测试ls方法
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_ls() throws KeeperException, InterruptedException {
        //第二个参数为true调用默认的回调函数
        //List<String> children = zkClient.getChildren("/", true);
        List<String> children = zkClient.getChildren("/", (e) -> {
            System.out.println("自定义回调函数");
        });
        System.out.println("=============================");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("=============================");
    }

    /**
     * 测试create方法
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_create() throws KeeperException, InterruptedException {
        String newNode = zkClient.create("/yuege", "Idea".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(newNode);
    }

    /**
     * 测试get方法
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_get() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/yuege", true, new Stat());

        String s = new String(data);

        System.out.println(s);
    }

    /**
     * 测试set方法
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_set() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/yuege", true);
        Stat stat = zkClient.setData("/yuege", "linyue".getBytes(), exists.getVersion());

        //获取数据长度
        System.out.println(stat.getDataLength());
    }

    /**
     * 测试stat方法
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_stat() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/yuege", true);

        if (exists == null) {
            System.out.println("没有此节点");
        } else {
            System.out.println("此节点的数据长度为：" + exists.getDataLength());
        }
    }

    /**
     * 测试delete方法
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test_delete() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/yuege", false);
        if (exists != null) {
            zkClient.delete("/yuege",exists.getVersion());
        }
    }
}
