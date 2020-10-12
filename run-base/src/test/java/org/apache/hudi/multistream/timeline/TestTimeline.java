package org.apache.hudi.multistream.timeline;

import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.apache.hudi.multistream.hudi.TimelineService;

import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles(profiles = "test")
public class TestTimeline {

    @Autowired
    private TimelineService timelineService;

    @Test
    public void testTimeline() {
        try {
            Option<RemoteHoodieTableFileSystemView> fsView = timelineService.getTimeline("/hive/warehouse/bigdata.db/test_time/");
            System.out.println(fsView.get().getLatestBaseFiles().collect(Collectors.toList()));
            System.out.println(fsView.get().getTimeline().lastInstant().get().getState());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
