package com.example.dylan.thrift.common;

import com.facebook.fb303.FacebookService;
import org.apache.thrift.transport.TTransport;

/**
 * 对象池包装类，暴露链接和client对象
 *
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-23
 */
public class DPoolWrapper {
    private TTransport transport;// 传输层链接对象，复用
    private String connStr;// 连接串
    private FacebookService.Client client;// client对象

    public DPoolWrapper(TTransport transport, FacebookService.Client client, String connStr) {
        this.transport = transport;
        this.client = client;
        this.connStr = connStr;
    }

    public FacebookService.Client getClient() {
        return client;
    }

    public TTransport getTransport() {
        return transport;
    }

    public String getConnStr() {
        return connStr;
    }
}
