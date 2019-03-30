import com.wws.DistributeLock;
import com.wws.impl.SingletonRedisLock;
import com.wws.impl.ZookeeperLock;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.RedissonRedLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DistributeLockTest {

    @Test
    public void lockTest() {
        ExecutorService threadPool = Executors.newFixedThreadPool(20);
        CyclicBarrier barrier = new CyclicBarrier(20);
        for (int i = 0; i < 20; i++) {
            threadPool.execute(new CuratorLockTest(barrier));
        }
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void redLockTest() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.9.150:7000");
        RedissonClient client = Redisson.create(config);
        Config config1 = new Config();
        config1.useSingleServer().setAddress("redis://192.168.9.150:7001");
        RedissonClient client1 = Redisson.create(config1);
        Config config2 = new Config();
        config2.useSingleServer().setAddress("redis://192.168.9.150:7002");
        RedissonClient client2 = Redisson.create(config2);
        try {
            RLock lock = client.getLock("lock");
            RLock lock1 = client1.getLock("lock");
            RLock lock2 = client2.getLock("lock");
            RedissonRedLock redLock = new RedissonRedLock(lock, lock1, lock2);
            redLock.lock();
            try {
                System.out.println(Thread.currentThread().getName() + "get lock");
                Thread.sleep(100);
                System.out.println(Thread.currentThread().getName() + "get unlock");
            } finally {
                redLock.unlock();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.shutdown();
            client1.shutdown();
            client2.shutdown();
        }
    }


    private static class RedisLockTest implements Runnable {

        private CyclicBarrier barrier;

        RedisLockTest(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        @Override
        public void run() {
            JedisPool jedisPool = new JedisPool("192.168.9.150", 6379);
            try {
                DistributeLock lock = new SingletonRedisLock(jedisPool, "lock", 300);
                barrier.await();
                boolean flag = lock.tryLock(Integer.MAX_VALUE);
                try {
                    System.out.println(Thread.currentThread().getName() + "get lock:flag=" + flag);
                    Thread.sleep(100);
                    System.out.println(Thread.currentThread().getName() + "get unlock");
                } finally {
                    lock.unlock();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                jedisPool.close();
            }
        }
    }

    private static class RedissonLockTest implements Runnable {

        private CyclicBarrier barrier;

        RedissonLockTest(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        @Override
        public void run() {
            Config config = new Config();
            config.useSingleServer().setAddress("redis://192.168.9.150:6379");
            RedissonClient client = Redisson.create(config);
            try {
                RLock lock = client.getLock("lock");
                barrier.await();
                lock.lock();
                try {
                    System.out.println(Thread.currentThread().getName() + "get lock");
                    Thread.sleep(100);
                    System.out.println(Thread.currentThread().getName() + "get unlock");
                } finally {
                    lock.unlock();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                client.shutdown();
            }
        }
    }

    private static class ZoookeeperLockTest implements Runnable {

        private CyclicBarrier barrier;

        ZoookeeperLockTest(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                CountDownLatch latch = new CountDownLatch(1);
                ZooKeeper zk = new ZooKeeper("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", 5000, WatchedEvent -> {
                    latch.countDown();
                });
                latch.await();
                try {
                    DistributeLock lock = new ZookeeperLock(zk, "lock");
                    barrier.await();
                    lock.tryLock(Integer.MAX_VALUE);
                    try {
                        System.out.println(Thread.currentThread().getName() + "get lock");
                        Thread.sleep(100);
                        System.out.println(Thread.currentThread().getName() + "get unlock");
                    } finally {
                        lock.unlock();
                    }
                } finally {
                    zk.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class CuratorLockTest implements Runnable {

        private CyclicBarrier barrier;

        CuratorLockTest(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        @Override
        public void run() {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
                    .sessionTimeoutMs(5000)
                    .retryPolicy(retryPolicy)
                    .build();
            client.start();
            InterProcessLock lock = new InterProcessMutex(client, "/locks/curator-lock");
            try{
                lock.acquire();
                try {
                    System.out.println(Thread.currentThread().getName() + "get lock");
                    Thread.sleep(100);
                    System.out.println(Thread.currentThread().getName() + "get unlock");
                }finally {
                    lock.release();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                client.close();
            }
        }
    }

}
