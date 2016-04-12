package com.example.dylan.thrift.example;

import com.example.dylan.thrift.DBaseService;

/**
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-18
 */
public class ServiceImpl extends DBaseService implements TestService.Iface {

    @Override
    public long add(int a, int b) throws org.apache.thrift.TException {
        return a + b;
    }
}
