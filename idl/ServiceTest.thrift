namespace java com.example.dylan.thrift.example

include 'fb303.thrift'

service TestService extends fb303.FacebookService{
    i64 add(1:i32 a, 2: i32 b);
}