package com.example.dylan.thrift.register;

import com.example.dylan.thrift.Constants;
import com.example.dylan.thrift.utils.LocalUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 服务注册
 *
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-24
 */
public class ServiceRegister {
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegister.class);

    private final CuratorFramework client;
    private final String zkGroup;
    private final String serviceName;
    private final int port;
    private byte[] data = null;

    public ServiceRegister(String zkConnStr, String zkGroup, String serviceName, int zkConnTimeout, int zkSessionTimeout, int port) {
        this.zkGroup = zkGroup;
        this.serviceName = serviceName;
        this.port = port;
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

    public void register(boolean keeping, int interval) {
        if (interval < 10 || interval > 60 * 10) {
            interval = 30;
        }
        final String path = getPath();
        createPath(client, path);
        if (keeping) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    createPath(client, path);
                }
            }, interval, interval, TimeUnit.SECONDS);
        }
    }

    private String getPath() {
        return "/" + zkGroup + "/" + serviceName + "/" + LocalUtil.getLocalIpV4() + ":" + port;
    }

    private void createPath(CuratorFramework client, String path) {
        String[] paths = path.split("/");
        StringBuilder sb = new StringBuilder();
        try {
            if (client.checkExists().forPath(path) == null) {
                for (int i = 1; i < paths.length; i++) {
                    sb.append("/").append(paths[i]);
                    if (client.checkExists().forPath(sb.toString()) == null) {
                        if (i >= (paths.length - 1)) {
                            client.create().withMode(CreateMode.EPHEMERAL).forPath(sb.toString(), data);
                        } else {
                            client.create().withMode(CreateMode.PERSISTENT).forPath(sb.toString());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void unRegister() {
        try {
            String path = getPath();
            if (client.checkExists().forPath(path) != null) {
                client.delete().guaranteed().forPath(path);
            }
            logger.info("unregister self service, host={}", new Object[]{LocalUtil.getLocalIpV4()});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        client.close();
    }

    public void setData(byte[] data) {
        this.data = data;
        String path = getPath();
        try {
            if (client.checkExists().forPath(path) != null) {
                client.setData().forPath(path, data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
