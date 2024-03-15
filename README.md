# 项目说明

## 包结构

## 启动flink集群 & 上传task
    ./Users/zyf/software/flink-1.17.0/bin/start-cluster.sh 
    上传FlinkService-1.0-SNAPSHOT-jar-with-dependencies.jar到flink集群

## 打包
  打包命令：mvn package assembly:assembly

   上传FlinkService-1.0-SNAPSHOT-jar-with-dependencies.jar到flink集群


## mysql
  安装于mac本地，开机自动启动，账号为root，密码为12345678
  登陆：mysql -uroot -p
  查看binlog:  show variables like 'log_bin';


## kafka

  启动zk：
    cd /Users/zyf/software/kafka_2.13-2.8.2
    nohup ./bin/zookeeper-server-start.sh config/zookeeper.properties &

  启动kafka:
   cd /Users/zyf/software/kafka_2.13-2.8.2
     nohup ./bin/kafka-server-start.sh config/server.properties &

  查看topic:
     ./bin/kafka-topics.sh --list --zookeeper localhost:2181
    
  创建topic:
     ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

  发送消息：
  ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

  消费消息：
  ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning --topic test

  启动管理页面：
    cd /Users/zyf/software/kafka-manager-2.0.0.0
    nohup ./bin/kafka-manager -Dhttp.port=9922 &

  访问管理页面：
    http://localhost:9922/
    
## HBase
   安装目录：/Users/zyf/software/hbase-2.2.2

   同时启动Master和RegionServer：./bin/start-hbase.sh     

   同时停止Master和RegionServer：./bin/stop-hbase.sh

   进入客户端命令行（可操作如表创建等等）:   ./bin/hbase  shell

   访问管理节面：http://localhost:16010/master-status#baseStats






















