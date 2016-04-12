package com.example.dylan.thrift.common;

import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对象池构建类
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-22
 */
public class DThriftClientWrapperFactory extends BasePooledObjectFactory {
    private static final Logger logger = LoggerFactory.getLogger(DThriftClientWrapperFactory.class);

    private String connStr;
    private Class service;
    private int initBuffer;
    private int timeout;

    public DThriftClientWrapperFactory(String connStr, Class service, int initBuffer, int timeout) {
        this.connStr = connStr;
        this.service = service;
        this.initBuffer = initBuffer;
        this.timeout = timeout;
    }

    /**
     * 创建池中对象
     *
     * @return
     * @throws Exception
     */
    @Override
    public Object create() throws Exception {
        String[] host = connStr.split(":");
        if (host.length != 2) {
            throw new IllegalArgumentException("connStr illegal");
        }
        TTransport transport;
        TSocket tSocket = new TSocket(host[0], Integer.parseInt(host[1]));
        tSocket.setTimeout(timeout);
        transport = new TFastFramedTransport(tSocket, initBuffer);
        try {
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
            throw new RuntimeException("connect to server error");
        }
        TProtocol protocol = new TBinaryProtocol(transport);
        Class clientClazz = null;
        for (Class c : service.getClasses()) {
            if (c.isMemberClass() && "Client".equals(c.getSimpleName())) {
                clientClazz = c;
            }
        }
        if (clientClazz == null) {
            throw new IllegalArgumentException("service must contain legal subClass Client");
        }
        Object client = clientClazz.getConstructor(TProtocol.class).newInstance(protocol);
        DPoolWrapper dPoolObject = new DPoolWrapper(transport, (FacebookService.Client) client, connStr);
        logger.debug("create poolObject of thriftClient to connect {}", new Object[]{connStr});
        return dPoolObject;
    }

    @Override
    public PooledObject wrap(Object o) {
        return new DefaultPooledObject(o);
    }

    @Override
    public PooledObject makeObject() throws Exception {
        return super.makeObject();
    }

    @Override
    public void destroyObject(PooledObject p) throws Exception {
        DPoolWrapper dPoolObject = (DPoolWrapper) p.getObject();
        dPoolObject.getTransport().close();
        super.destroyObject(p);
    }

    @Override
    public boolean validateObject(PooledObject p) {
        FacebookService.Client client = ((DPoolWrapper) p.getObject()).getClient();
        try {
            logger.debug("prepare validate client={} from={}", new Object[]{client.getName(), connStr});
            if (client.getStatus() != fb_status.ALIVE) {
                logger.info("client={} from={} is not alive", new Object[]{client.getName(), connStr});
                return false;
            }
        } catch (TException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
