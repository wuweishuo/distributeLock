package com.wws.impl;

import com.wws.DistributeLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZookeeperLock implements DistributeLock {

    private ZooKeeper zk;
    private String lockName;
    private String root = "/locks";
    private String myZNode;

    public ZookeeperLock(ZooKeeper zk, String lockName) throws IOException {
        this.zk = zk;
        this.lockName = lockName;
    }

    public boolean tryLock(long waitTime){
        try {
            //1.创建临时有序节点
            myZNode = zk.create(root + "/" + lockName + "-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            return waitForLock(waitTime);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean waitForLock(long watiTime) {
        long start = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - start < watiTime) {
                //2.获取子节点
                List<String> children = zk.getChildren(root, false);
                List<String> lockNodes = new ArrayList<>();
                for (String s : children) {
                    if (s.startsWith(lockName)) {
                        lockNodes.add(s);
                    }
                }
                Collections.sort(lockNodes);
                //3.判断是否最小节点
                if (myZNode.equals(root + "/" + lockNodes.get(0))) {
                    return true;
                }
                //4.监听前一个节点删除事件
                String seq = myZNode.substring(myZNode.lastIndexOf('/') + 1);
                String waitNode = lockNodes.get(Collections.binarySearch(lockNodes, seq) - 1);
                CountDownLatch latch = new CountDownLatch(1);
                Stat stat = zk.exists(root + "/" + waitNode, watchedEvent -> {
                    if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted) {
                        latch.countDown();
                    }
                });
                if (stat != null) {
                    latch.await(watiTime - System.currentTimeMillis() + start, TimeUnit.MILLISECONDS);
                }
            }
        }catch (Exception e){
            deleteNode();
            e.printStackTrace();
        }
        deleteNode();
        return false;
    }

    public boolean unlock() {
        return deleteNode();
    }

    private boolean deleteNode(){
        try {
            zk.delete(myZNode, -1);
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }
}
