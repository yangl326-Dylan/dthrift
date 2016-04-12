package com.example.dylan.thrift.policy;

import com.example.dylan.thrift.common.DThriftConnEntity;

import java.util.Collection;

/**
 * 均衡轮询
 * @author zhoudylan
 * @version 1.0
 * @created 16-3-7
 */
public class DefaultRoundRobin implements ClientPolicyI {

    private int currentNo = 0;
    private DThriftConnEntity[] array = null;

    @Override
    public synchronized DThriftConnEntity next(Collection<DThriftConnEntity> clients) {
        int size = clients.size();
        array = clients.toArray(new DThriftConnEntity[size]);
        if (array.length <= 0) {
            return null;
        }
        currentNo = (currentNo + 1) % size;
        return array[currentNo];
    }
}
