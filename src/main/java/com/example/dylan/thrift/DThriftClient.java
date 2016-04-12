package com.example.dylan.thrift;

import com.example.dylan.thrift.common.DThriftInvocationHandler;
import com.example.dylan.thrift.policy.ClientPolicyI;
import com.example.dylan.thrift.register.ServiceDiscovery;
import com.facebook.fb303.FacebookService;
import com.example.dylan.thrift.common.DThriftConnEntity;
import com.example.dylan.thrift.common.DThriftClientWrapperFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * iface sync, 同步client
 * 连接层为TSocket，传输模式为TFastFramedTransport
 *
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-18
 */
public class DThriftClient {
    private static final Logger logger = LoggerFactory.getLogger(DThriftClient.class);

    // client cache list
    private ConcurrentSkipListMap<String, DThriftConnEntity> clientMap = new ConcurrentSkipListMap<String, DThriftConnEntity>();
    private String[] hosts;
    private Class service;
    private String clientMode = Constants.DEFAULT_MODE;// 目前默认配置列表直连和zk

    private int initBuffer = 32 * 1024;// initbuffer 大小
    private int minIdlePerHost = 1;// 最小空闲client数量
    private int maxIdlePerHost = 5;// 最大空闲client数量
    private int maxTotalPerHost = 100;// 最大client个数
    private int timeoutMillis = 1000;// 超时时间 socket超时和pool获取对象超时

    //zk
    private String zkConnStr = null;
    private String zkServiceName = null;
    private String zkGroup = null;
    private int zkConnTimeout = 2000;// zk链接超时
    private int zkSessionTimeout = 2000;// zk session超时时间设置
    private ClientPolicyI clientPolicyI;// client 请求策略

    /**
     * 构建直连模式client
     *
     * @param service thrfit service class
     * @param hosts   server hosts列表
     */
    public DThriftClient(Class service, String[] hosts) {
        this.hosts = hosts;
        this.service = service;
        this.clientMode = Constants.DEFAULT_MODE;
    }

    /**
     * 构建zk模式client
     *
     * @param service       thrfit service class
     * @param zkConnStr     zookeeper connection str
     * @param zkGroup       服务分组
     * @param zkServiceName 服务名称
     */
    public DThriftClient(Class service, String zkConnStr, String zkGroup, String zkServiceName) {
        this.service = service;
        this.zkConnStr = zkConnStr;
        this.zkGroup = zkGroup;
        this.zkServiceName = zkServiceName;
        this.clientMode = Constants.ZK_MODE;
    }

    /**
     * 设置一个server连接的最大空闲client数量
     *
     * @param maxIdlePerHost default 5
     * @return this
     */
    public DThriftClient setMaxIdlePerHost(int maxIdlePerHost) {
        this.maxIdlePerHost = maxIdlePerHost;
        return this;
    }

    /**
     * 设置一个server连接client的最大数量
     *
     * @param maxTotalPerHost default 100
     * @return this
     */
    public DThriftClient setMaxTotalPerHost(int maxTotalPerHost) {
        if (maxTotalPerHost > 0 && maxTotalPerHost < 1000) {
            this.maxTotalPerHost = maxTotalPerHost;
        } else {
            throw new IllegalArgumentException("illegal maxTotalPerHost");
        }
        return this;
    }

    /**
     * 设置一个server连接client的最小空闲数
     *
     * @param minIdlePerHost 缺省1 最小1
     * @return this
     */
    public DThriftClient setMinIdlePerHost(int minIdlePerHost) {
        if (minIdlePerHost < 1) {
            throw new IllegalArgumentException("illegal minIdlePerHost");
        }
        this.minIdlePerHost = minIdlePerHost;
        return this;
    }

    /**
     * 超时时间 socket超时和pool获取对象超时
     *
     * @param timeoutMillis 超时时间 毫秒
     * @return this
     */
    public DThriftClient setTimeout(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    /**
     * 设置传输模式初始化buffer大小
     *
     * @param initBuffer
     * @return this
     */
    public DThriftClient setInitBuffer(int initBuffer) {
        this.initBuffer = initBuffer;
        return this;
    }

    /**
     * 设置client请求策略，默认为server可用列表轮询
     *
     * @param clientPolicyI
     * @return this
     */
    public DThriftClient setClientPolicyI(ClientPolicyI clientPolicyI) {
        this.clientPolicyI = clientPolicyI;
        return this;
    }

    /**
     * 设置连接zk超时时间
     *
     * @param timeoutMillis
     * @return this
     */
    public DThriftClient setZKConnTimeout(int timeoutMillis) {
        checkForZK();
        this.zkConnTimeout = timeoutMillis;
        return this;
    }

    /**
     * 设置zk模式连接zk超时时间
     *
     * @param timeoutMillis
     * @return this
     */
    public DThriftClient setZkSessionTimeout(int timeoutMillis) {
        checkForZK();
        this.zkSessionTimeout = timeoutMillis;
        return this;
    }

    private void checkForZK() {
        if (clientMode != Constants.ZK_MODE) {
            throw new IllegalArgumentException("no need to set zk params");
        }
    }

    /**
     * client proxy 构建
     *
     * @return Iface
     * @throws Exception
     */
    public Object build() throws Exception {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(maxIdlePerHost);
        config.setMaxTotal(maxTotalPerHost);
        config.setMinIdle(minIdlePerHost);
        clientMap.clear();
        if (clientMode == Constants.ZK_MODE) {
            ServiceDiscovery sd = new ServiceDiscovery(this, service, clientMap, zkConnStr, zkGroup, zkServiceName, zkConnTimeout, zkSessionTimeout,
                    config, initBuffer, timeoutMillis);
            List<String> hostList = sd.watchGetChildren();
            for (String host : hostList) {
                sd.addServer(host);
            }
            logger.info("init connect to servers={}", new Object[]{Arrays.toString(hostList.toArray())});
        } else {
            for (String host : hosts) {
                addServer(host, config);
            }
            logger.info("init connect to servers={}", new Object[]{Arrays.toString(hosts)});
        }

        //实现Iface接口自身的代理类
        DThriftInvocationHandler handler = new DThriftInvocationHandler(clientMap, timeoutMillis);
        handler.setClientPolicyI(clientPolicyI);
        Object obj = Proxy.newProxyInstance(FacebookService.Client.class.getClassLoader(), new Class[]{getIfaceInterface()},
                handler);
        logger.info("client proxy created successfully");
        return obj;
    }


    private void addServer(String connStr, GenericObjectPoolConfig config) { // 添加server到cachelist
        DThriftConnEntity entity = new DThriftConnEntity();
        entity.setStatusValue("connStr",connStr);
        entity.setGenericObjectPool(new GenericObjectPool(new DThriftClientWrapperFactory(connStr, service, initBuffer, timeoutMillis), config));
        DThriftConnEntity current;
        if ((current = clientMap.get(connStr)) != null) {
            if (!current.isActive()) {
                current = clientMap.remove(connStr);
                if (current != null) {
                    current.close();
                }
            } else {
                logger.info("ignore add a existed server={}", new Object[]{connStr});
            }
        } else {
            clientMap.put(connStr, entity);
        }
    }

    private Class getIfaceInterface() { // 获取iface接口
        Class[] classes = service.getClasses();
        for (Class c : classes)
            if (c.isInterface() && "Iface".equals(c.getSimpleName())) {
                return c;
            }
        throw new IllegalArgumentException("service must contain legal subInterface Iface");
    }

}
