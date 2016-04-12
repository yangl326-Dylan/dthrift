package com.example.dylan.thrift.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * @author zhoudylan
 * @version 1.0
 * @created 16-2-24
 */
public class LocalUtil {

    private static String cacheIp = null;

    public static void main(String[] args) {
        System.out.println(getLocalIpV4(null));
    }


    public static String getLocalIpV4() {
        return getLocalIpV4(null);
    }

    /**
     *
     * @param tryIp
     * @return 获取的ip
     */
    public static String getLocalIpV4(String tryIp) {
        Enumeration<NetworkInterface> networkInterface;
        try {
            networkInterface = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new IllegalStateException(e);
        }
        String ip = null;
        while (networkInterface.hasMoreElements()) {
            NetworkInterface ni = networkInterface.nextElement();
            Enumeration<InetAddress> inetAddress = ni.getInetAddresses();
            while (inetAddress.hasMoreElements()) {
                InetAddress ia = inetAddress.nextElement();
                if (ia instanceof Inet6Address)
                    continue; // ignore ipv6
                String thisIp = ia.getHostAddress();
                if (!ia.isLoopbackAddress() && !thisIp.contains(":") && !"127.0.0.1".equals(thisIp) && ip == null) {
                    ip = thisIp;
                    if (ip.equals(tryIp))
                        return tryIp;
                }
            }
        }

        if (cacheIp == null) {// cache ip
            cacheIp = ip;
        }
        return ip;
    }

    /**
     * 生成基本信息 ip：serverip  rts：zk上注册的起始时间。“\n”分割
     *
     * @return bytes
     */
    public static byte[] generatorServerInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("ip:").append(cacheIp == null ? getLocalIpV4() : cacheIp).append("\n");
        sb.append("rts:").append(System.currentTimeMillis()).append("\n");
        return sb.toString().getBytes();
    }
}
