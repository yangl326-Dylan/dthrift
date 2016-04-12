package com.example.dylan.thrift.register;

import com.example.dylan.thrift.Constants;
import com.example.dylan.thrift.DThriftClient;
import com.example.dylan.thrift.common.DThriftConnEntity;
import com.example.dylan.thrift.common.DThriftClientWrapperFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 服务发现
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-25
 */
public class ServiceDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

    private final CuratorFramework client;
    private final String zkGroup;
    private final String serviceName;
    private final DThriftClient dThriftClient;
    private final Class service;
    private final ConcurrentSkipListMap<String, DThriftConnEntity> clients;
    private final GenericObjectPoolConfig config;
    private final int initBuffer;
    private final int timeout;

    public ServiceDiscovery(DThriftClient dThriftClient, Class service, ConcurrentSkipListMap<String, DThriftConnEntity> clientMap, String zkConnStr, String
            zkGroup, String serviceName, int zkConnTimeout, int zkSessionTimeout, GenericObjectPoolConfig config, int initBuffer, int timeout) {
        this.zkGroup = zkGroup;
        this.serviceName = serviceName;
        this.dThriftClient = dThriftClient;
        this.service = service;
        this.clients = clientMap;
        this.config = config;
        this.initBuffer = initBuffer;
        this.timeout = timeout;
        client = CuratorFrameworkFactory.builder()
                .sessionTimeoutMs(zkSessionTimeout)
                .connectionTimeoutMs(zkConnTimeout)
                .connectString(zkConnStr)
                .retryPolicy(new ExponentialBackoffRetry(1000, 8))
                .defaultData(null)
                .namespace(Constants.BASE_ROOT)
                .build();
        client.start();

    }

    public List<String> watchGetChildren() throws Exception {
        if (client.checkExists().forPath(getPath()) == null) {
            createPath(client, getPath());
        }
        PathChildrenCache childrenCache = new PathChildrenCache(client, getPath(), false);
        ServiceListener listener = new ServiceListener(dThriftClient);
        childrenCache.getListenable().addListener(listener);
        childrenCache.start();
        return client.getChildren().forPath(getPath());

    }

    private String getPath() {
        return "/" + zkGroup + "/" + serviceName;
    }

    private void createPath(CuratorFramework client, String path) throws Exception {
        String[] paths = path.split("/");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < paths.length; i++) {
            sb.append("/").append(paths[i]);
            if (client.checkExists().forPath(sb.toString()) == null) {
                client.create().withMode(CreateMode.PERSISTENT).forPath(sb.toString());
            }

        }
    }

    public void addServer(String host) {
        DThriftConnEntity entity = new DThriftConnEntity();
        entity.setStatusValue("connStr",host);
        entity.setGenericObjectPool(new GenericObjectPool(new DThriftClientWrapperFactory(host, service, initBuffer, timeout), config));
        DThriftConnEntity current;
        if ((current = clients.get(host))!=null) {
            if (!current.isActive()) {
                current = clients.remove(host);
                if (current != null) {
                    current.close();
                    clients.put(host, entity);
                    logger.info("update server host={}", new Object[]{host});
                }
            } else {
                logger.info("ignore add server host={}", new Object[]{host});
            }
        } else {
            clients.put(host, entity);
            logger.info("add server host={}", new Object[]{host});
        }
    }

    /**
     * 移除无效client
     * @param connStr
     */
    public void removeInvalidServer(String connStr) {
        DThriftConnEntity current = clients.get(connStr);
        if (!current.isActive()) {
            current = clients.remove(connStr);
            if (current != null) {
                current.close();
                logger.info("remove successfully, host={}", new Object[]{connStr});
            }
        } else {
            logger.info("server is active, ignore remove, host={}", new Object[]{connStr});
        }
    }

    /**
     * 强制移除
     * @param connStr 连接串
     */
    public void forceRemoveServer(String connStr) {
        DThriftConnEntity current = clients.remove(connStr);
        if (current != null) {
            current.close();
            logger.info("force remove, host={}", new Object[]{connStr});
        }
    }

    class ServiceListener implements PathChildrenCacheListener {
        DThriftClient dThriftClient;

        ServiceListener(DThriftClient dThriftClient) {
            this.dThriftClient = dThriftClient;
        }

        private void processNodeDeleted(String path) {
            logger.info("prepare to remove server host={}", new Object[]{path});
            removeInvalidServer(path);
        }

        private void processNodeCreated(String path) {
            logger.info("prepare to add server host={}", new Object[]{path});
            addServer(path);
        }

        @Override
        public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
            switch (pathChildrenCacheEvent.getType()) {
                case CHILD_ADDED:
                    processNodeCreated(getChildNode(pathChildrenCacheEvent.getData().getPath()));
                    break;
                case CHILD_REMOVED:
                    processNodeDeleted(getChildNode(pathChildrenCacheEvent.getData().getPath()));
                    break;
                default:
                    break;
            }
        }

        private String getChildNode(String path) {
            String[] paths = path.split("/");
            return paths[paths.length - 1];
        }
    }
}
