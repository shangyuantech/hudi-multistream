package org.apache.hudi.multistream.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles(profiles = "test")
public class TestKafkaAdmin {

    @Autowired
    private TopicService topicService;
    
    @Test
    public void testTopic() throws Exception {
        System.out.println(topicService.listTopics("bigdata-mysql.bigdata.(.*)"));
    }

    @Test
    public void testOffset() throws Exception {
        System.out.println(topicService.consumerOffset("bigdata-mysql.bigdata.test_time"));
    }
}
