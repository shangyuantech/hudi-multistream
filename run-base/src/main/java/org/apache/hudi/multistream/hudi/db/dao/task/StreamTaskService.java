package org.apache.hudi.multistream.hudi.db.dao.task;

import org.apache.hudi.multistream.common.pojo.StreamTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service("streamTaskService")
public class StreamTaskService {

    @Autowired
    private StreamTaskDao dao;

    public List<StreamTask> findByTopic(String topic) {
        return dao.findByTopic(topic);
    }

    public List<StreamTask> findByTopics(String ... topics) {
        return dao.findByTopics(Arrays.asList(topics));
    }

    public List<StreamTask> findByTopics(List<String> topics) {
        return dao.findByTopics(topics);
    }

    public List<StreamTask> findByName(String name) {
        return dao.findByName(name);
    }

    public void save(StreamTask streamTask) {
        dao.save(streamTask);
    }

    public void deleteById(Long id) {
        if (dao.findById(id).isPresent()) {
            dao.deleteById(id);
        }
    }

    public void deleteByName(String name) {
        dao.deleteByName(name);
    }
}
