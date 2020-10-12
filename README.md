# Consumption and writing to Hudi based on multiple topic
## 功能介绍
  
基于 `spring-cloud` 和 `consul` 整合 `hudi` 服务，目的是可以利用服务发现来处理同一批次的多个spark任务之间的调度。实现的场景是启动1至多个spark任务，共享多个 `topic` 的消费任务，并且每个任务之间的实际执行是互斥的（分组策略是利用zookeeper做负载均衡策略，尽量让每个任务节点都平均负载从而避免某一个节点任务堆积）。基于 `spring-consul` 做动态任务配置调整，从而应对任务的实时控制和调整。

整体功能基于 `spark-yarn-client` 模式的服务，基于任务执行历史做db存储，以提供后续审计使用。当前版本暂时不考虑 `yarn-cluster` 模式。在基于配置文件的提交策略的基础上新增基于 db/其他存储 的提交策略，方便实时更新每个任务的配置。

项目测试主要使用了 [Debezium](https://debezium.io/) 做为CDC服务的提供方

## 部署方式
### 本地开发
```shell script
# 启动 dev
./consul agent -dev
# 然后在编译工具里面启动 spring 项目
```

### 服务器部署
考虑到 `spring` 以及相关依赖和 `spark` / `hudi` 可能存在的jar包重复和问题，所以本身项目代码单独部署在 `run-base` 下，打包依赖的 `spring` 相关代码放在 `spring-base` 下。
`multistream-run-base-VERSION-SNAPSHOT.jar` 会作为基准的执行jar包，依赖的jar包放在了 `spring-base/target/libs` 下面，可按照如下脚本参考执行任务。
```shell script
spark-submit \
    --master yarn \
    --jars $(echo /xxx/*.jar | tr ' ' ','),/xxx/spark-avro_2.11-2.4.3.jar,/xxx/hudi-utilities-bundle_2.11-0.6.1-SNAPSHOT.jar \
    --driver-java-options "-Dspring.profiles.active=bigdata" \
    --class org.apache.hudi.multistream.HudiMultistreamApplication \
    --conf spark.driver.userClassPathFirst=true \
    multistream-run-base-1.0.0-SNAPSHOT.jar
```
* 注：删除了 slf4j-api-1.7.30.jar，避免jar包冲突

### 部署脚本
放在 shell/start.sh 下面

## Consul Config
### 配置 K/V
路径：
```
http://localhost:8500/ui/dc1/kv/config/multistream-bigdata/data/edit
```
命名路径举例：
```
config/multistream-bigdata/data
config/multistream-xxxx/data
```

## zookeeper
### 领导选举
目前我们选择的是用 `LeaderSelector` 进行多节点选举，每次的master可能不会在一个节点上，这样尽量做到资源平均分配
