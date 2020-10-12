package org.apache.hudi.multistream.jpa;

import org.apache.hudi.multistream.common.pojo.StreamTaskLog;
import org.apache.hudi.multistream.hudi.callback.dao.StreamTaskLogService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles(profiles = "test")
public class TestStreamTaskLog {

    @Autowired
    StreamTaskLogService streamTaskLogService;

    @Test
    public void testInsert() {
        StreamTaskLog t = new StreamTaskLog();

        t.setFromTopic("topic_test");
        t.setToSource("test");
        t.setToTable("test");
        t.setCommitTime("20200824123456");
        t.setBasePath("/hive/warehouse/test.db/test/");
        t.setHudiTable("test");
        t.setInsertRows(100L);
        t.setDeleteRows(0L);
        t.setUpdateRows(0L);
        t.setCreateTime(new Date());

        streamTaskLogService.save(t);
    }
}
