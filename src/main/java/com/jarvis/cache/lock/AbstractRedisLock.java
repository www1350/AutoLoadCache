package com.jarvis.cache.lock;

import java.util.HashMap;
import java.util.Map;

import com.jarvis.cache.to.RedisLockInfo;

/**
 * 基于Redis实现分布式锁
 * @author jiayu.qiu
 */
public abstract class AbstractRedisLock implements ILock {

    private static final ThreadLocal<Map<String, RedisLockInfo>> LOCK_START_TIME=new ThreadLocal<Map<String, RedisLockInfo>>();

    /**
     * 
     * SETNX
     * @param key
     * @param val
     * @return
     */
    protected abstract Long setnx(String key, String val);

    /**
     * 
     * EXPIRE
     * @param key
     * @param expire
     */
    protected abstract void expire(String key, int expire);

    /**
     * 
     * GET
     * @param key
     * @return
     */
    protected abstract String get(String key);

    /**
     * 
     * GETSET
     * @param key
     * @param newVal
     * @return
     */
    protected abstract String getSet(String key, String newVal);

    private long serverTimeMillis() {
        return System.currentTimeMillis();
    }

    private boolean isTimeExpired(String value) {
        return serverTimeMillis() > Long.parseLong(value);
    }

    /**
     * 
     * DEL
     * @param key
     */
    protected abstract void del(String key);

    @Override
    public boolean tryLock(String key, int lockExpire) {
        boolean locked=getLock(key, lockExpire);

        if(locked) {
            Map<String, RedisLockInfo> startTimeMap=LOCK_START_TIME.get();
            if(null == startTimeMap) {
                startTimeMap=new HashMap<String, RedisLockInfo>(8);
                LOCK_START_TIME.set(startTimeMap);
            }
            RedisLockInfo info=new RedisLockInfo();
            info.setLeaseTime(lockExpire * 1000);
            info.setStartTime(System.currentTimeMillis());
            startTimeMap.put(key, info);
        }
        return locked;
    }

    /***
     * 1.setnx(lockkey, 当前时间+过期超时时间) ，如果返回1，则获取锁成功,expire(lockkey,过期超时时间)，返回；如果返回0则没有获取到锁，转向2
     * 2.get(lockkey)获取值oldExpireTime ，并将这个value值与当前的系统时间进行比较，如果小于当前系统时间，则认为这个锁已经超时，可以允许别的请求重新获取，转向3
     * 3.计算newExpireTime=当前时间+过期超时时间，然后getset(lockkey, newExpireTime) 会返回当前lockkey的值currentExpireTime。
     * 4.判断currentExpireTime与oldExpireTime 是否相等，如果相等，说明当前getset设置成功，获取到了锁。
     * 如果不相等，说明这个锁又被别的请求获取走了，那么当前请求可以直接返回失败，或者继续重试。
     * @param key
     * @param lockExpire
     * @return
     */
    private boolean getLock(String key, int lockExpire) {
        long lockExpireTime=serverTimeMillis() + (lockExpire * 1000) + 1;// 锁超时时间
        String lockExpireTimeStr=String.valueOf(lockExpireTime);
        if(setnx(key, lockExpireTimeStr).intValue() == 1) {// 获取到锁
            try {
                expire(key, lockExpire);
            } catch(Throwable e) {
            }
            return true;
        }
        String oldValue=get(key);
        if(oldValue != null && isTimeExpired(oldValue)) { // lock is expired
            String oldValue2=getSet(key, lockExpireTimeStr); // getset is atomic
            // 但是走到这里时每个线程拿到的oldValue肯定不可能一样(因为getset是原子性的)
            // 假如拿到的oldValue依然是expired的，那么就说明拿到锁了
            if(oldValue2 != null && isTimeExpired(oldValue2)) {
                return true;
            }
        }
        return false;
    }

    /***
     *
     * 比较自己的处理时间和对于锁设置的超时时间，如果小于锁设置的超时时间，则直接执行delete释放锁；
     * 如果大于锁设置的超时时间，则不需要再锁进行处理。
     * @param key 锁Key
     */
    @Override
    public void unlock(String key) {
        Map<String, RedisLockInfo> startTimeMap=LOCK_START_TIME.get();
        RedisLockInfo info=null;
        if(null != startTimeMap) {
            info=startTimeMap.remove(key);
        }
        if(null != info && (System.currentTimeMillis() - info.getStartTime()) >= info.getLeaseTime()) {
            return;
        }
        try {
            del(key);
        } catch(Throwable e) {
        }
    }
}
