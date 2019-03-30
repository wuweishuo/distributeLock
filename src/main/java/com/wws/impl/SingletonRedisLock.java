package com.wws.impl;

import com.wws.DistributeLock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Calendar;
import java.util.Collections;
import java.util.UUID;

public class SingletonRedisLock implements DistributeLock {

    private JedisPool jedisPool;
    private String lockName;
    private String clientId;
    private long leaseTime;

    public SingletonRedisLock(JedisPool jedisPool, String lockName, long leaseTime){
        this.jedisPool = jedisPool;
        this.lockName = lockName;
        this.clientId = UUID.randomUUID().toString();
        this.leaseTime = leaseTime;
    }

    public boolean tryLock(long waitTime) {
        long end = Calendar.getInstance().getTimeInMillis() + waitTime;
        Jedis jedis = jedisPool.getResource();
        try {
            do {
                String result = jedis.set(lockName, getClientId(), "NX", "EX", leaseTime);
                if ("OK".equals(result)) {
                    return true;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return false;
                }
            } while (Calendar.getInstance().getTimeInMillis() < end);
        }finally {
            if(jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    public boolean unlock() {
        String lua = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
                    "    return redis.call(\"del\",KEYS[1])\n" +
                    "else\n" +
                    "    return 0\n" +
                    "end";
        Jedis jedis = jedisPool.getResource();
        try {
            Object obj = jedis.eval(lua, Collections.singletonList(lockName), Collections.singletonList(getClientId()));
            if (obj.equals(1)) {
                return true;
            }
        }finally {
            if(jedis != null){
                jedis.close();
            }
        }
        return false;
    }

    private String getClientId(){
        long threadId = Thread.currentThread().getId();
        return clientId + threadId;
    }
}
