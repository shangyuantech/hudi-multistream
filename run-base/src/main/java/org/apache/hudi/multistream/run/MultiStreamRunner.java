package org.apache.hudi.multistream.run;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.hudi.multistream.config.BuildConfig;
import org.apache.hudi.multistream.config.HudiConfig;
import org.apache.hudi.multistream.config.ZookeeperConfig;
import org.apache.hudi.multistream.hudi.zookeeper.LeaderService;
import org.apache.hudi.multistream.hudi.zookeeper.ListenerService;
import org.apache.hudi.multistream.utils.HudiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;


@Component
@Profile("!test")
public class MultiStreamRunner implements ApplicationRunner {

    private final static Logger log = LoggerFactory.getLogger(MultiStreamRunner.class);

    @Autowired
    @Qualifier("curatorFramework")
    private CuratorFramework client;

    @Autowired
    private LeaderService leaderService;

    @Autowired
    private ListenerService listenerService;

    @Autowired
    private HudiConfig hudiConfig;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {

        // register
        registerListener();

        // leader
        startLeader();

        // listener
        startListener();
    }

    private void startLeader() {

        LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
            public void takeLeadership(CuratorFramework client) throws Exception {
                try {
                    leaderService.scanTopic();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                Thread.sleep(TimeUnit.SECONDS.toMillis(hudiConfig.getKafka().getLoopInterval()));
            }
        };

        String leaderPath = String.format("/%s/leader", ZookeeperConfig.getServiceName());
        LeaderSelector selector = new LeaderSelector(client, leaderPath, listener);
        selector.autoRequeue();
        selector.start();
    }

    private void registerListener() throws Exception {
        listenerService.registerListener();
    }

    private void startListener() throws Exception {
        TreeCache treeCache = new TreeCache(client, HudiUtil.getListenerPath(BuildConfig.serverPort));

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                listenerService.action(event);
            }
        });

        //开始监听
        treeCache.start();
    }
}
