package com.example.dylan.thrift.example;

import com.example.dylan.thrift.DThriftServer;

/**
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-23
 */
public class DThriftServerTest {

    public void startServer(){
        new DThriftServer(TestService.class, new ServiceImpl())
                .setPort(9090)
                .start();
    }

    public void startServerForZK(){
        new DThriftServer(TestService.class, new ServiceImpl(),"localhost:2181","group","service1")
                .setPort(9093)
                .setPriority(1)
                .start();
    }
    public void startServerForZK2(){
        new DThriftServer(TestService.class, new ServiceImpl(),"localhost:2181","group","service1")
                .setPort(9094)
                .setPriority(6)
                .start();
    }

    public void startServerForZK3(){
        new DThriftServer(TestService.class, new ServiceImpl(),"localhost:2181","group","service1")
                .setPort(9095)
                .start();
    }
    public static void main(String[] args) {
        DThriftServerTest test = new DThriftServerTest();
        test.startServerForZK();
        test.startServerForZK2();
        test.startServerForZK3();
    }
}
