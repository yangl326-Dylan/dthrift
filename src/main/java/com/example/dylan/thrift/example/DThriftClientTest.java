package com.example.dylan.thrift.example;

import com.example.dylan.thrift.policy.DefaultRoundRobin;
import com.facebook.fb303.FacebookService;
import com.example.dylan.thrift.DThriftClient;
import org.apache.thrift.TException;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-23
 */
public class DThriftClientTest {

    public FacebookService.Iface buildClient() throws Exception {
        return (TestService.Iface) new DThriftClient(TestService.class, new String[]{"localhost:9090"})
                .build();
    }

    public FacebookService.Iface buildZKClient() throws Exception {
        return (TestService.Iface) new DThriftClient(TestService.class, "localhost:2181", "group", "service1")
//                .setClientPolicyI(new WeightedRoundRobin())
                .setClientPolicyI(new DefaultRoundRobin())
                .setMaxTotalPerHost(10)
                .build();
    }

    public static void main(String[] args) throws Exception {
        DThriftClientTest test = new DThriftClientTest();
        final TestService.Iface testClient = (TestService.Iface) test.buildZKClient();
        Executor pool = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 550; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 600; i++) {
                        try {
                            long start = System.currentTimeMillis();
                            long result = testClient.add(1, 2);
                            System.out.println("sum="+(System.currentTimeMillis() - start));
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

    }
}

