# dthrift 封装java版本thrift RPC的链接和客户端池
### 直连模式
参照example 直连版本，server启动，给定server的ip:port列表。 客户端调用
### zk模式
参照example zk版本。修改zk集群服务地址。serve启动注册。客户端调用
### 包含内容  
1、client池封装

2、client描述文件反射封装

3、zk模式封装

4、客户端策略接口：轮询和加权轮询

