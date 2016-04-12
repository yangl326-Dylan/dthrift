package com.example.dylan.thrift.common;

import com.example.dylan.thrift.policy.ClientPolicyI;
import com.example.dylan.thrift.policy.DefaultRoundRobin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 代理类的invoke handler
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-22
 */
public class DThriftInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(DThriftInvocationHandler.class);

    private ConcurrentSkipListMap<String, DThriftConnEntity> clients;
    private String currentHost = null;
    private int timeout;
    private ClientPolicyI clientPolicyI;

    public DThriftInvocationHandler(ConcurrentSkipListMap<String, DThriftConnEntity> clients, int timeout) {
        this.clients = clients;
        this.timeout = timeout;
    }

    public void setClientPolicyI(ClientPolicyI clientPolicyI) {
        if (clientPolicyI != null) {
            this.clientPolicyI = clientPolicyI;
        } else {
            this.clientPolicyI = new DefaultRoundRobin();
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        DThriftConnEntity currentEntity;
        long start = System.currentTimeMillis();
        String methodName = method.getName();

        // client 选择策略
        DThriftConnEntity entry = clientPolicyI.next(clients.values());
        if (entry == null) {
            logger.warn("can not find server list!");
            return null;
        }

        currentEntity = entry;

        DPoolWrapper obj = null;
        try {
            // 真实client对象wrapper类
            obj = currentEntity.getGenericObjectPool().borrowObject(timeout);
            return method.invoke(obj.getClient(), args);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("client process error");
        } finally {
            if (logger.isDebugEnabled()) {
                long delta = System.currentTimeMillis() - start;
                logger.debug("invoke method={}, fromHost={}, priority={}, cost={} ms", new Object[]{methodName, currentEntity.getStatusValue("connStr"),
                        currentEntity.getPriority(),
                        delta});
            }
            if (obj != null) {
                currentEntity.getGenericObjectPool().returnObject(obj);
            }
        }
    }
}
