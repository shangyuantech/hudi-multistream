package org.apache.hudi.multistream.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperConfig {

    public static String zkPath;

    @Value("${hudi.zookeeper.path:hudi}")
    public void setZkPath(String zkPath) {
        ZookeeperConfig.zkPath = zkPath;
    }

    public static String serviceName;

    @Value("${spring.cloud.consul.discovery.service-name}")
    public void setServiceName(String serviceName) {
        ZookeeperConfig.serviceName = serviceName;
    }

    public static String getServiceName() {
        return serviceName;
    }

    @Value("${hudi.zookeeper.connect}")
    private String zkConnPath;

    @Value("${hudi.zookeeper.client.session-timeout:5000}")
    private Integer sessionTimeout;

    @Value("${hudi.zookeeper.client.connection-timeout:5000}")
    private Integer connectionTimeout;

    @Value("${hudi.zookeeper.client.base-sleep-timeout:1000}")
    private Integer baseSleepTimeout;

    @Value("${hudi.zookeeper.client.max-retries:3}")
    private Integer maxRetries;

    @Bean("curatorFramework")
    public CuratorFramework createZkClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeout, maxRetries);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(zkConnPath)
                .sessionTimeoutMs(sessionTimeout)
                .connectionTimeoutMs(connectionTimeout)
                .retryPolicy(retryPolicy)
                .namespace(zkPath)
                .build();
        client.start();
        return client;
    }
}
