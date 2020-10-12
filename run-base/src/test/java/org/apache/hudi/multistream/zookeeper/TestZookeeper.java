package org.apache.hudi.multistream.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestZookeeper {

    @Test
    public void testLeader() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .namespace("test")
                .build();
        client.start();

        LeaderLatchListener listener = new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("is leader!");
            }

            @Override
            public void notLeader() {
                System.out.println("not leader!");
            }
        };

        LeaderLatch leaderLatch = new LeaderLatch(client, "/test_leader");
        leaderLatch.addListener(listener);
        leaderLatch.start();

        TimeUnit.SECONDS.sleep(100);
    }
}
