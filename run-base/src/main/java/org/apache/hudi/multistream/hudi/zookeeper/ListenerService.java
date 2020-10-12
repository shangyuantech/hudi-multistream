package org.apache.hudi.multistream.hudi.zookeeper;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hudi.multistream.config.BuildConfig;
import org.apache.hudi.multistream.config.HudiConfig;
import org.apache.hudi.multistream.hudi.DeltaStreamService;
import org.apache.hudi.multistream.utils.HudiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;

@Service
public class ListenerService {

    private final static Logger log = LoggerFactory.getLogger(ListenerService.class);

    @Autowired
    private ZookeeperService zookeeper;

    @Autowired
    private DeltaStreamService deltaStream;

    public void registerListener() throws Exception {
        zookeeper.createNode(HudiUtil.getListenerPath(BuildConfig.serverPort), null, EPHEMERAL);
    }

    public void action(TreeCacheEvent event) throws Exception {
        ChildData data = event.getData();
        if (data != null && data.getData() != null) {
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    // start task
                    String props = new String(data.getData());
                    log.info("[listener {}] start a hudi task with props: {}", BuildConfig.ipAddress, props);
                    try {
                        deltaStream.startTask(HudiUtil.getTypedProps(props));
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        // remove task data
                        zookeeper.setData(data.getPath(), null);
                    }

                    break;
                default:
                    break;
            }
        }
    }
}
