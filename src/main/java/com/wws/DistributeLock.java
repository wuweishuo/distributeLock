package com.wws;

public interface DistributeLock {

    boolean tryLock(long waitTime);

    boolean unlock();

}
