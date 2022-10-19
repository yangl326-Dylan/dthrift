package com.example.dylan.thrift;

import com.example.dylan.thrift.register.ServiceRegister;
import com.example.dylan.thrift.utils.DThriftOptions;
import com.example.dylan.thrift.utils.LocalUtil;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * server服务，缺省非阻塞异步方式
 *
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-18
 */
public class DThriftServer {
    private static final Logger logger = LoggerFactory.getLogger(DThriftServer.class);

    private TServerTransport serverTransport;
    private TProcessor tProcessor;
    private TServer tServer;
    private int port = 7070;

    // 默认TThreadedSelectorServer模型的缺省参数
    private int maxReadBufferBytes = 10 * 1024 * 1024; // 最大read buffer大小
    private int workerThreads = 50; // worker线程数
    private int selectorThreads = 5; // selector线程数
    private int acceptQueueSizePerThread = 200; // 每个线程接收队列大小
    private int stopTimeoutMilliseconds = 500; // 超时时间 单位ms

    private String serverMode = Constants.DEFAULT_MODE;
    private int startTimeout = 10000;// 启动超时
    private DThriftOptions serverOptions;

    private String zkConnStr = null;
    private String zkServiceName = null;
    private String zkGroup = null;
    private int zkConnTimeout = 2000;// ms
    private int zkSessionTimeout = 2000;// ms


    /**
     * 直连构造器
     *
     * @param service      service class
     * @param dBaseService service实现类
     */
    public DThriftServer(Class service, DBaseService dBaseService) {
        this.tProcessor = getTProcess(service, dBaseService);
        this.serverMode = Constants.DEFAULT_MODE;
        if (serverOptions == null) {
            serverOptions = new DThriftOptions();
            dBaseService.setDThriftOptions(serverOptions);
        }
    }

    /**
     * zk模式构造器
     *
     * @param service       service class
     * @param dBaseService  service实现类
     * @param zkConnStr     zk连接串
     * @param zkGroup       组名称
     * @param zkServiceName 服务名
     */
    public DThriftServer(Class service, DBaseService dBaseService, String zkConnStr, String zkGroup, String zkServiceName) {
        this.tProcessor = getTProcess(service, dBaseService);
        this.serverMode = Constants.ZK_MODE;
        this.zkConnStr = zkConnStr;
        this.zkServiceName = zkServiceName;
        this.zkGroup = zkGroup;
        if (serverOptions == null) {
            serverOptions = new DThriftOptions();
            dBaseService.setDThriftOptions(serverOptions);
        }
    }

    /**
     * 设置端口
     *
     * @param port
     * @return this
     */
    public DThriftServer setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * 设置zk模式连接zk超时时间
     *
     * @param timeoutMillis 超时时间ms
     * @return this
     */
    public DThriftServer setZKConnTimeout(int timeoutMillis) {
        checkForZK();
        this.zkConnTimeout = timeoutMillis;
        return this;
    }

    /**
     * 设置zk模式session超时时间
     *
     * @param timeoutMillis 超时时间ms
     * @return this
     */
    public DThriftServer setZkSessionTimeout(int timeoutMillis) {
        checkForZK();
        this.zkSessionTimeout = timeoutMillis;
        return this;
    }

    private void checkForZK() {
        if (serverMode != Constants.ZK_MODE) {
            throw new IllegalArgumentException("no need to set zk params");
        }
    }

    /**
     * 设置传输模式,默认为TNonblockingServerSocket
     *
     * @param tServerTransport
     * @return this
     */
    public DThriftServer setTServerTransport(TServerTransport tServerTransport) {
        this.serverTransport = tServerTransport;
        return this;
    }

    /**
     * 设置server模型，默认为TThreadedSelectorServer
     *
     * @param tServer
     * @return this
     */
    public DThriftServer setTServer(TServer tServer) {
        this.tServer = tServer;
        return this;
    }

    /**
     * 设置启动超时时间。1~60s 之间
     *
     * @param timeoutMills 缺省10s启动超时,毫秒
     * @return this
     */
    public DThriftServer setStartTimeout(int timeoutMills) {
        if (timeoutMills < 1000 || timeoutMills > 60000) {
            timeoutMills = 10000;
        }
        this.startTimeout = timeoutMills;
        return this;
    }

    /**
     * 设置启动server权重。[1:10] 之间
     * 注意：依赖客户端选用的策略， 如使用轮询那么权重值无效。适用于加权轮询策略的客户端
     *
     * @param priority 缺省权重5
     * @return this
     */
    public DThriftServer setPriority(int priority) {
        if (priority < 1 || priority > 10) {
            logger.warn("illegal value priority={}, set to default 5", new Object[]{priority});
            priority = 5;
        }
        serverOptions.setOption("priority", String.valueOf(priority));
        return this;
    }

    /**
     * 线程的等待队列数，缺省200
     *
     * @param acceptQueueSizePerThread
     * @return this
     */
    public DThriftServer setAcceptQueueSizePerThread(int acceptQueueSizePerThread) {
        this.acceptQueueSizePerThread = acceptQueueSizePerThread;
        return this;
    }

    public DThriftServer setMaxReadBufferBytes(int maxReadBufferBytes) {
        this.maxReadBufferBytes = maxReadBufferBytes;
        return this;
    }

    /**
     * 设置selector线程数
     *
     * @param selectorThreads 默认5
     * @return this
     */
    public DThriftServer setSelectorThreads(int selectorThreads) {
        this.selectorThreads = selectorThreads;
        return this;
    }

    public DThriftServer setStopTimeoutMilliseconds(int stopTimeoutMilliseconds) {
        this.stopTimeoutMilliseconds = stopTimeoutMilliseconds;
        return this;
    }

    /**
     * 设置worker线程数
     *
     * @param workerThreads 缺省50
     * @return this
     */
    public DThriftServer setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
        return this;
    }

    /**
     * server启动
     *
     */
    public void start() {
        if (serverTransport == null) {
            try {
                serverTransport = new TNonblockingServerSocket(port);
            } catch (TTransportException e) {
                e.printStackTrace();
                logger.warn("server init error! on ip={}, port={}", new Object[]{LocalUtil.getLocalIpV4(), port});
            }
        }
        if (tServer == null) {
            TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args((TNonblockingServerSocket) serverTransport);
            args.maxReadBufferBytes = maxReadBufferBytes;
            args.selectorThreads(selectorThreads);
            args.workerThreads(workerThreads);
            args.stopTimeoutUnit(TimeUnit.MILLISECONDS);
            args.stopTimeoutVal(stopTimeoutMilliseconds);
            args.acceptQueueSizePerThread(acceptQueueSizePerThread);
            args.processor(tProcessor);
            args.transportFactory(new TFramedTransport.Factory());
            tServer = new TThreadedSelectorServer(args);
        }

        long prepareStartTime = System.currentTimeMillis();
        new ServerThread(tServer).start();
        while (!tServer.isServing()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if ((System.currentTimeMillis() - prepareStartTime) >= startTimeout) {
                tServer.stop();
                throw new RuntimeException("server start timeout!!");
            }
        }
        if (serverMode == Constants.ZK_MODE) {
            //register service
            final ServiceRegister sr = new ServiceRegister(zkConnStr, zkGroup, zkServiceName, zkConnTimeout, zkSessionTimeout, port);
            sr.register(true, 10);
            sr.setData(LocalUtil.generatorServerInfo());
            Runtime.getRuntime().addShutdownHook(new HookThread(sr));
        }
        logger.info("server started on port=" + port);
        serverOptions.setStartTimestamp(System.currentTimeMillis());
    }

    /**
     * 启动线程
     */
    class ServerThread extends Thread {
        private TServer server;

        public ServerThread(TServer server) {
            this(server, false);
        }

        public ServerThread(TServer server, boolean daemon) {
            this.server = server;
            if (daemon) {
                setDaemon(true);
            }
        }

        public void run() {
            server.serve();
        }
    }

    /**
     * shutdown hook线程
     */
    class HookThread extends Thread {
        private ServiceRegister serviceRegister;

        public HookThread(ServiceRegister serviceRegister) {
            this.serviceRegister = serviceRegister;
        }

        public void run() {
            serviceRegister.unRegister();
        }
    }

    private TProcessor getTProcess(Class clazz, DBaseService dService) {
        Class iFace = null;
        Class process = null;

        for (Class c : clazz.getClasses()) {
            if ("Iface".equals(c.getSimpleName())) {
                iFace = c;
            }
            if ("Processor".equals(c.getSimpleName())) {
                process = c;
            }
        }
        if (process == null) {
            throw new IllegalArgumentException("service must contain legal subClass TProcessor");
        }
        if (iFace == null) {
            throw new IllegalArgumentException("service must contain legal subInterface Iface");
        }
        try {
            return (TProcessor) process.getConstructor(iFace).newInstance(dService);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("illegal serviceClass");
        }
    }
}
