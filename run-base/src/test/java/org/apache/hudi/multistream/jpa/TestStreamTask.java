package org.apache.hudi.multistream.jpa;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.multistream.common.pojo.StreamTask;
import org.apache.hudi.multistream.common.pojo.StreamTaskConfig;
import org.apache.hudi.multistream.config.HudiConfig;
import org.apache.hudi.multistream.hudi.db.dao.task.StreamTaskService;
import org.apache.hudi.multistream.hudi.db.dao.taskconfig.StreamTaskConfigService;
import org.apache.hudi.multistream.utils.HudiUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles(profiles = "test")
public class TestStreamTask {

    @Autowired
    StreamTaskService service;

    @Test
    public void testFindByTopic() {
        List<StreamTask> streamTasks = service.findByTopic("bigdata-mysql.bigdata.test_time");
        System.out.println(streamTasks);
        Assert.assertEquals(1, streamTasks.size());
    }

    @Test
    public void testFindByTopics() {
        List<StreamTask> streamTasks = service.findByTopics("bigdata-mysql.bigdata.test_time");
        System.out.println(streamTasks);
        Assert.assertEquals(1, streamTasks.size());
    }

    @Autowired
    private HudiConfig hudiConfig;

    @Autowired
    private StreamTaskConfigService configService;

    @Test
    public void testImportProps() throws Exception {
        Map<String, TypedProperties> configMap = HudiUtil.getHudiTopicsByConfigFolder(hudiConfig.getDelta().getConfigFolder());
        long id = 1L;
        for (Map.Entry<String, TypedProperties> entry : configMap.entrySet()) {

            String name = "test" + id;
            for (StreamTask tmp : service.findByName(name)) {
                service.deleteById(tmp.getId());
                configService.deleteByTaskId(tmp.getId());
            }

            // stream_task
            StreamTask task = new StreamTask();
            task.setName(name);
            task.setTopic(entry.getKey());
            task.setCreateTime(new Date());
            task.setDelFlag(Boolean.FALSE);
            service.save(task);

            // stream_task_config
            ArrayList<StreamTaskConfig> streamTaskConfigs = new ArrayList<>();
            TypedProperties typedProperties = entry.getValue();
            for (String key : typedProperties.stringPropertyNames()) {
                StreamTaskConfig config = new StreamTaskConfig();
                config.setTaskId(task.getId());
                config.setType("02");
                config.setPropKey(key);
                config.setPropValue(typedProperties.getString(key));
                config.setCreateTime(new Date());
                config.setDelFlag(Boolean.FALSE);
                streamTaskConfigs.add(config);
            }
            configService.saveAll(streamTaskConfigs);

            id ++;
        }
    }
}
