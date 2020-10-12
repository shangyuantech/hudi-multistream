package org.apache.hudi.multistream.hudi.db.dao.taskconfig;

import org.apache.hudi.multistream.common.pojo.StreamTaskConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("streamTaskConfigService")
public class StreamTaskConfigService {

    @Autowired
    private StreamTaskConfigDao dao;

    public void saveAll(List<StreamTaskConfig> streamTaskConfigs) {
        dao.saveAll(streamTaskConfigs);
    }

    public void deleteByTaskId(Long taskId) {
        if (!dao.findByTaskId(taskId).isEmpty()) {
            dao.deleteByTaskId(taskId);
        }
    }

    public List<StreamTaskConfig> findByTaskId(Long taskId) {
        return dao.findByTaskId(taskId);
    }
}
