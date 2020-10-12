package org.apache.hudi.multistream.hudi;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.apache.hudi.multistream.utils.HudiUtil;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {"spring.cloud.consul.config.enabled=false"})
public class TestHudiUtils {

    @Test
    public void testFileList() throws IOException {
        System.out.println(HudiUtil.getHudiTopicsByConfigFolder("file://" + System.getProperty("user.dir") + "/../delta_props/"));
    }
}
