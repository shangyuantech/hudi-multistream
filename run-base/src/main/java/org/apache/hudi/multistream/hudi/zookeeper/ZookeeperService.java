package org.apache.hudi.multistream.hudi.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ZookeeperService {

    private static Logger log = LoggerFactory.getLogger(ZookeeperService.class);

    @Autowired
    @Qualifier("curatorFramework")
    private CuratorFramework client;

    public List<String> getChildes(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    public Stat dataExists(String path) throws Exception {
        return dataExists(path, false);
    }

    public Stat dataExists(String path, boolean watch) throws Exception {
        return watch ? client.checkExists().watched().forPath(path)
                : client.checkExists().forPath(path);
    }

    public void createNode(String path, String data, CreateMode createMode) throws Exception {
        log.debug("create node " + path);
        client.create()
                .creatingParentsIfNeeded()
                .withMode(createMode)
                .forPath(path, data == null ? null : data.getBytes());
    }

    public void createNode(String path, String data) throws Exception {
        createNode(path, data, CreateMode.PERSISTENT);
    }

    public void createNode(String path) throws Exception {
        createNode(path, null);
    }

    public void delete(String path) throws Exception {
        delete(path, null);
    }

    public void delete(String path, Integer version) throws Exception {
        if (version != null) {
            client.delete()
                    .deletingChildrenIfNeeded()
                    .withVersion(version)
                    .forPath(path);
        } else {
            client.delete()
                    .deletingChildrenIfNeeded()
                    .forPath(path);
        }
    }

    public String getData(String path) throws Exception {
        if (dataExists(path, false) == null) {
            return null;
        }
        byte[] data = client.getData().forPath(path);
        return data == null ? "" : new String(data);
    }

    public void setData(String path, String data, Integer version) throws Exception {
        log.debug("set node {}, data {}, version {}", path, data, version);
        client.setData()
                .withVersion(version)
                .forPath(path, data == null ? null : data.getBytes());
    }

    public void setData(String path, String data) throws Exception {
        log.debug("set node {}, data {}", path, data);
        client.setData().forPath(path, data == null ? null : data.getBytes());
    }
}
