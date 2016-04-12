package com.example.dylan.thrift.policy;

import com.example.dylan.thrift.common.DThriftConnEntity;

import java.util.Collection;

/**
 * @author zhoudylan
 * @version 1.0
 * @created 16-3-7
 */
public interface ClientPolicyI {

    /**
     * 注意实现策略方法的线程安全性
     *
     * @param clients 连接对象集合
     * @return 链接实体
     */
    DThriftConnEntity next(Collection<DThriftConnEntity> clients);
}
