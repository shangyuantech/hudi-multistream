package org.apache.hudi.multistream.hudi;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.apache.hudi.multistream.config.HudiConfig;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles(profiles = "test")
public class TestHudiConfig {

    @Autowired
    private HudiConfig hudiConfig;

    @Test
    public void testConfig() {
        System.out.println(hudiConfig.toString());
        assertEquals(hudiConfig.getDelta().getTableType(), "MERGE_ON_READ");
    }
}
