package com.example.dylan.thrift.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * server信息
 *
 * @author zhoudylan
 * @version 1.0
 * @created 16-3-9
 */
public class DThriftOptions {
    //启动时间戳
    public long startTimestamp = 0;

    //服务端选项
    private ConcurrentHashMap<String, String> options = new ConcurrentHashMap<String, String>();


    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public void setOption(String key, String value) {
        options.put(key, value);
    }

    public String getOption(String key) {
        return options.get(key);
    }

    public Map<String, String> getOptions() {
        return options;
    }
}
