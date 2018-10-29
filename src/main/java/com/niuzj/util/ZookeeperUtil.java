package com.niuzj.util;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.ResourceBundle;

public class ZookeeperUtil {

    private static ZkClient zkClient;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("zookeeper");
        String host = bundle.getString("host");
        if ("".equals(host)){
            throw new RuntimeException("host is null");
        }
        zkClient = new ZkClient(new ZkConnection(host), 10000);
    }

    public static ZkClient getZkClient(){
        return zkClient;
    }

    /**
     * 创建一个父节点并创建它的子节点,data默认为""
     * @param parent 父节点path
     * @param childs 子节点pathList
     */
    public static void create_p(String parent, List<String> childs, CreateMode createMode){
        if (parent == null || "".equals(parent)){
            return;
        }
        if (createMode == null){
            createMode = CreateMode.PERSISTENT;
        }

        zkClient.create(parent, "", createMode);

        if (childs == null || childs.size() == 0){
            return;
        }
        for (String child : childs) {
            String path = parent + child;
            zkClient.create(path, "", createMode);
        }
    }

    /**
     * 递归删除节点
     */
    public static void delete(String path){
        if (path == null || "".equals(path)){
            return;
        }
        zkClient.deleteRecursive(path);
    }


}
