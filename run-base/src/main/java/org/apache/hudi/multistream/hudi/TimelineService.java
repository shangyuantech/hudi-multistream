package org.apache.hudi.multistream.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.hudi.multistream.config.HudiConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.common.table.view.FileSystemViewStorageConfig.FILESYSTEM_VIEW_REMOTE_HOST;
import static org.apache.hudi.common.table.view.FileSystemViewStorageConfig.FILESYSTEM_VIEW_REMOTE_PORT;

@Service
public class TimelineService {

    private final static Logger log = LoggerFactory.getLogger(TimelineService.class);

    @Autowired
    private HudiConfig hudiConfig;

    public Option<RemoteHoodieTableFileSystemView> getTimeline(String basePath) throws IOException {

        Option<RemoteHoodieTableFileSystemView> fsView = Option.empty();
        Configuration configuration = FSUtils.prepareHadoopConf(new Configuration());
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(new Path(basePath))) {
            HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fileSystem.getConf(), basePath, true);
            fsView = Option.of(
                    new RemoteHoodieTableFileSystemView(hudiConfig.getPropertiesValue(FILESYSTEM_VIEW_REMOTE_HOST),
                            Integer.parseInt(hudiConfig.getPropertiesValue(FILESYSTEM_VIEW_REMOTE_PORT)), metaClient)
            );
        }

        return fsView;
    }

    public Map<TopicPartition, Long> getLastCommitCheckpoint(String basePath) throws IOException {
        return getLastCommitCheckpoint(basePath, false);
    }

    public Map<TopicPartition, Long> getLastCommitCheckpoint(String basePath, boolean refresh) throws IOException, HoodieException {
        Option<RemoteHoodieTableFileSystemView> fsView = getTimeline(basePath);
        Option<String> lastCheckpointStr = Option.empty();

        if (fsView.isPresent()) {
            RemoteHoodieTableFileSystemView remoteFsView = fsView.get();
            if (refresh) fsView.get().refresh();

            Option<HoodieInstant> lastCommit = remoteFsView.getLastInstant();
            HoodieTimeline commitTimelineOpt = remoteFsView.getTimeline();

            if (lastCommit.isPresent() && lastCommit.get().isCompleted()) {
                HoodieCommitMetadata commitMetadata;
                try {
                    commitMetadata = HoodieCommitMetadata.fromBytes(commitTimelineOpt.getInstantDetails(lastCommit.get()).get(),
                            HoodieCommitMetadata.class);
                } catch (Exception e) {
                    if (!refresh) {
                        log.warn("there is something wrong in get HoodieCommitMetadata, maybe need to refresh remoteFsView");
                         return getLastCommitCheckpoint(basePath, true);
                    } else {
                        throw new HoodieException(e.getMessage(), e);
                    }
                }

                if (commitMetadata.getMetadata(HoodieDeltaStreamer.CHECKPOINT_KEY) != null) {
                    lastCheckpointStr = Option.of(commitMetadata.getMetadata(HoodieDeltaStreamer.CHECKPOINT_KEY));
                }
            }
        }

        if (lastCheckpointStr.isPresent() && !lastCheckpointStr.get().isEmpty()) {
            Map<TopicPartition, Long> lastCheckpoint = KafkaOffsetGen.CheckpointUtils.strToOffsets(lastCheckpointStr.get());
            log.debug("get hudi checkpoint = {}", lastCheckpoint);
            return lastCheckpoint;
        }

        return Collections.emptyMap();
    }
}
