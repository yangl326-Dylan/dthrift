package com.example.dylan.thrift.common;

import com.facebook.fb303.fb_status;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 链接对象，包含对象池及对象池状态数据
 *
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-23
 */
public class DThriftConnEntity {
    private ConcurrentHashMap<String, Object> status = new ConcurrentHashMap<String, Object>();

    private long lastGetTime = 0;
    private int cachePriority = 5;

    private GenericObjectPool genericObjectPool;

    public Object getStatusValue(String key) {
        return status.get(key);
    }

    public Object setStatusValue(String key, Object o) {
        return status.put(key, o);
    }

    public GenericObjectPool<DPoolWrapper> getGenericObjectPool() {
        return genericObjectPool;
    }

    public void setGenericObjectPool(GenericObjectPool genericObjectPool) {
        this.genericObjectPool = genericObjectPool;
    }

    public void clear() {
        this.genericObjectPool.clear();
    }

    public void close() {
        this.genericObjectPool.close();
    }

    public boolean isActive() {
        try {
            if (getGenericObjectPool().borrowObject().getClient().getStatus() == fb_status.ALIVE) {
                return true;
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return false;
    }

    public int getPriority() {
        try {
            String priorityStr = getGenericObjectPool().borrowObject().getClient().getOption("priority");
            if(priorityStr == null){
                return cachePriority;
            }
            long currentTime = System.currentTimeMillis();
            if ((currentTime - lastGetTime) > 60 * 1000 || lastGetTime == 0) {
                lastGetTime = currentTime;
                cachePriority = Integer.parseInt(priorityStr);
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return cachePriority;
    }
}
