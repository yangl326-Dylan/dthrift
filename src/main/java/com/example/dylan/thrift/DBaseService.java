package com.example.dylan.thrift;

import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import com.example.dylan.thrift.utils.DThriftOptions;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-18
 */
public class DBaseService implements FacebookService.Iface {
    private DThriftOptions dThriftOptions = new DThriftOptions();

    protected void setDThriftOptions(DThriftOptions dThriftOptions) {
        this.dThriftOptions = dThriftOptions;
    }

    @Override
    public String getName() throws TException {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getVersion() throws TException {
        return null;
    }

    @Override
    public fb_status getStatus() throws TException {
        return fb_status.ALIVE;
    }

    @Override
    public String getStatusDetails() throws TException {
        return null;
    }

    @Override
    public Map<String, Long> getCounters() throws TException {
        return null;
    }

    @Override
    public long getCounter(String s) throws TException {
        return 0;
    }

    @Override
    public void setOption(String s, String s1) throws TException {
        dThriftOptions.setOption(s, s1);
    }

    @Override
    public String getOption(String s) throws TException {
        return dThriftOptions.getOption(s);
    }

    @Override
    public Map<String, String> getOptions() throws TException {
        return dThriftOptions.getOptions();
    }

    @Override
    public String getCpuProfile(int i) throws TException {
        return null;
    }

    @Override
    public long aliveSince() throws TException {
        return dThriftOptions.getStartTimestamp();
    }

    @Override
    public void reinitialize() throws TException {
        //TODO
    }

    @Override
    public void shutdown() throws TException {
        //TODO
    }
}
