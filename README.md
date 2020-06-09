##### bd-hermes_collector-fm: Hermes数据收集端Flume解析

- 编译： mvn clean install -Dmaven.test.skip=true

- 上线运行：nohup sh ./bin/flume-ng agent -c /opt/apache-flume-1.9.0-bin/conf -f /opt/apache-flume-1.9.0-bin/conf/multi_accesslog_kafka.conf -n a1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 -Dflume_log_file=user_track2kafka >/dev/null 2>&1 &
-  广告 inc   nohup sh ./bin/flume-ng agent -c /opt/apache-flume-1.9.0-bin/conf -f /opt/apache-flume-1.9.0-bin/conf/multi_inc_kafka.conf  -n a1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34548 -Dflume_log_file=inc  > /dev/null 2>&1  &  
- 更新记录
    - 1.0.0-RELEASE / 20191001
    ```java
    基础功能上线
    ```
    - 1.1.0-RELEASE  / 20191113
    ```java
    添加Offset降级,在出现异常时从远端拉取offset
    添加独立输出error日志的打印功能
    ```
    - 1.1.1-RELEASE  / 20191223
    ```java
    修复数据降级后重复消费问题
    各应用日志间隔离
    ```
     - 1.1.2-RELEASE  / 20200408
    ```java
    添加Event限制，超出20KB将抛弃数据，并记录日志
    ```
     - 1.1.3-RELEASE  / 20200421
     ```java
    数据根据 partner tbl 读取业务方数据
     ```
    
- 注意
    ```java
    kill -TERM pid 一定要用这个，不然会丢失offset问题 
    ```
