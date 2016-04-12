package com.example.dylan.thrift.policy;

import com.example.dylan.thrift.common.DThriftConnEntity;

import java.util.Collection;

/**
 * 加权轮询策略
 * @author zhoudylan
 * @version 1.0
 * @created 16-3-14
 */
public class WeightedRoundRobin implements ClientPolicyI {
    private int currentNo = -1;
    private int priority = 0;
    private DThriftConnEntity[] array;

    @Override
    public synchronized DThriftConnEntity next(Collection<DThriftConnEntity> clients) {
        int size = clients.size();
        if (size <= 0) {
            return null;
        }
        array = clients.toArray(new DThriftConnEntity[size]);
        while (true) {
            currentNo = (currentNo + 1) % size;
            if (currentNo == 0) {
                priority = priority - 1;//priority = priority - gcd(...)
                if (priority <= 0) {
                    priority = maxPriority(clients);
                }
            }
            if (array[currentNo].getPriority() >= priority) {
                return array[currentNo];
            }
        }
    }

    /**
     * 获取最大权重值
     *
     * @param clients
     * @return 最大权重值
     */
    private int maxPriority(Collection<DThriftConnEntity> clients) {
        int i = 1;
        for (DThriftConnEntity entity : clients) {
            int priority = entity.getPriority();
            if (i < priority) {
                i = priority;
            }
        }
        return i;
    }
}
