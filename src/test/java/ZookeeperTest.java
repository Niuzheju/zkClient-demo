import com.niuzj.util.ZookeeperUtil;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class ZookeeperTest {

    private ZkClient zkClient;

    private Logger logger = Logger.getLogger(ZookeeperTest.class);

    @Before
    public void before() {
        zkClient = ZookeeperUtil.getZkClient();
        logger.info("zookeeper 连接成功");
    }

    /**
     * 创建节点
     */
    @Test
    public void test01() throws InterruptedException {
        /*
            path:节点路径, 以/开头
            data:存储在节点的数据
            mode:节点的类型分别有如下取值
            PERSISTENT:持久,无顺序
            PERSISTENT_SEQUENTIAL:持久,有顺序
            EPHEMERAL:临时,无顺序
            EPHEMERAL_SEQUENTIAL:临时有顺序
         */
        String result = zkClient.create("/zk-test/child02", "jsr", CreateMode.PERSISTENT);
        //递归创建
//        zkClient.createPersistent("/zk-temp/temp01", true);
        System.out.println(result);
//        Thread.sleep(20000L);
    }

    /**
     * 删除节点
     */
    @Test
    public void test02() {
//        boolean b = zkClient.delete("/zk-test0000000005");
        //具有子节点的节点必须调用此方法
        boolean b = zkClient.deleteRecursive("/zk-temp");
        System.out.println(b);
    }

    /**
     * 查询节点
     */
    @Test
    public void test03() {
        //获取一个节点的所有的子节点
        String parentPath = "/zk-test";
        List<String> children = zkClient.getChildren(parentPath);
        for (String child : children) {
            //获取节点数据,只能获取zkClient保存的数据,获取zkCli命令行保存的数据会报错
            System.out.println("子节点, path=" + child + "data=" + zkClient.readData(parentPath + "/" + child));
        }
    }

    /**
     * 更新数据
     */
    @Test
    public void test04() {
        zkClient.writeData("/zk-test", "delete");
    }


    @Test
    public void test05() {
        ZookeeperUtil.create_p("/zyq", Arrays.asList(new String[]{"/child01", "/child02", "/child03"}), CreateMode.PERSISTENT);
    }

    @Test
    public void test06() {
        ZookeeperUtil.delete("/zyq");
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void test07() {
        boolean b = zkClient.exists("/zyq");
        System.out.println(b);
    }

    /**
     * 监听一个节点的子节点的变化, 不监听数据变化
     * 新增子节点
     * 删除子节点
     * 直接删除该节点
     */
    @Test
    public void test08() throws InterruptedException {
        zkClient.subscribeChildChanges("/nzj", (path, currentChilds) -> {
            System.out.println("parent path is " + path);
            System.out.println("current childs is " + currentChilds);
        });

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 订阅一个节点的数据变化
     */
    @Test
    public void test09() throws InterruptedException {
        zkClient.subscribeDataChanges("/zyq", new IZkDataListener() {

            //该节点数据变化时调用
            @Override
            public void handleDataChange(String s, Object data) throws Exception {
                System.out.println(s + "节点数据发生变化, 变更后为" + data);
            }

            //该节点被删除时调用
            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println(s + "节点被删除");
            }
        });
        Thread.sleep(3000L);
        zkClient.writeData("/zyq", "nzj");
        Thread.sleep(3000L);
        ZookeeperUtil.delete("/zyq");
        Thread.sleep(Long.MAX_VALUE);

    }


}
