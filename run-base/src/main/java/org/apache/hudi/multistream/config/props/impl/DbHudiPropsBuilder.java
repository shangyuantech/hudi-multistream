package org.apache.hudi.multistream.config.props.impl;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.multistream.common.pojo.StreamTask;
import org.apache.hudi.multistream.common.pojo.StreamTaskConfig;
import org.apache.hudi.multistream.config.props.IHudiPropsBuilder;
import org.apache.hudi.multistream.hudi.db.dao.task.StreamTaskService;
import org.apache.hudi.multistream.hudi.db.dao.taskconfig.StreamTaskConfigService;
import org.apache.hudi.multistream.utils.SpringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DbHudiPropsBuilder implements IHudiPropsBuilder {

    private final HashMap<String, Long> streamTaskTopics = new HashMap<>();

    private static final int LOOP_SIZE = 10;

    public DbHudiPropsBuilder(List<String> topics) {
        StreamTaskService taskService = SpringUtil.getBean("streamTaskService", StreamTaskService.class);
        ArrayList<String> tmpList = new ArrayList<>();

        for (String topic : topics) {
            tmpList.add(topic);
            if (tmpList.size() == LOOP_SIZE) {
                List<StreamTask> streamTasks = taskService.findByTopics(tmpList);
                streamTasks.forEach(streamTask -> streamTaskTopics.put(streamTask.getTopic(), streamTask.getId()));
                tmpList.clear();
            }
        }

        if (!tmpList.isEmpty()) {
            List<StreamTask> streamTasks = taskService.findByTopics(tmpList);
            streamTasks.forEach(streamTask -> streamTaskTopics.put(streamTask.getTopic(), streamTask.getId()));
        }
    }

    @Override
    public TypedProperties getTypedProperties(String topic) {
        TypedProperties config = new TypedProperties();
        StreamTaskConfigService configService = SpringUtil.getBean("streamTaskConfigService", StreamTaskConfigService.class);

        List<StreamTaskConfig> taskConfigs = configService.findByTaskId(streamTaskTopics.get(topic));
        taskConfigs.forEach(streamTaskConfig -> config.put(streamTaskConfig.getPropKey(), streamTaskConfig.getPropValue()));

        return config;
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>(streamTaskTopics.keySet());
    }
}
