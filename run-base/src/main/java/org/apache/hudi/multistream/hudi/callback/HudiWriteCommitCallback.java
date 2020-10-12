package org.apache.hudi.multistream.hudi.callback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.multistream.common.pojo.StreamTaskLog;
import org.apache.hudi.multistream.hudi.callback.dao.StreamTaskLogService;
import org.apache.hudi.multistream.utils.SpringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class HudiWriteCommitCallback implements HoodieWriteCommitCallback {

    private final static Logger LOG = LoggerFactory.getLogger(HudiWriteCommitCallback.class);

    private final HoodieWriteConfig client;

    public HudiWriteCommitCallback(HoodieWriteConfig config) {
        this.client = config;
    }

    private static final String CREATE_USER = "machine";

    private static final String KAFKA_TOPIC_NAME = "hoodie.deltastreamer.source.kafka.topic";

    @Override
    public void call(HoodieWriteCommitCallbackMessage callbackMessage) {

        Properties props = client.getProps();
        LOG.debug("hudi call back for table: {}, commit_time: {}, config: {}",
                callbackMessage.getTableName(), callbackMessage.getCommitTime(), props);

        HoodieTableMetaClient htmc = new HoodieTableMetaClient(new Configuration(), callbackMessage.getBasePath(),
                false, ConsistencyGuardConfig.newBuilder().build(), Option.of(new TimelineLayoutVersion(1)));
        HoodieActiveTimeline activeTimeline = htmc.getActiveTimeline();
        HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
        HoodieInstant commitInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION,
                callbackMessage.getCommitTime());

        if (!timeline.containsInstant(commitInstant)) {
            LOG.warn("Commit {} not found in Commits {}", callbackMessage.getCommitTime(), timeline);
            return;
        }

        try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitInstant).get(),
                    HoodieCommitMetadata.class);
            long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
            long totalUpdateRecordsWritten = commitMetadata.fetchTotalUpdateRecordsWritten();
            long totalInsertRecordsWritten = commitMetadata.fetchTotalInsertRecordsWritten();
            long totalDeleteRecordsWritten = 0;
            for (List<HoodieWriteStat> stats : commitMetadata.getPartitionToWriteStats().values()) {
                for (HoodieWriteStat stat : stats) {
                    if (stat.getPrevCommit() != null) {
                        totalDeleteRecordsWritten += stat.getNumDeletes();
                    }
                }
            }

            LOG.debug("totalRecordsWritten = {}, totalUpdateRecordsWritten = {}, totalInsertRecordsWritten = {}, totalDeleteRecordsWritten = {}",
                    totalRecordsWritten, totalUpdateRecordsWritten, totalInsertRecordsWritten, totalDeleteRecordsWritten);

            StreamTaskLog streamTaskLog = new StreamTaskLog();
            streamTaskLog.setFromTopic(props.getProperty(KAFKA_TOPIC_NAME));
            streamTaskLog.setToSource(props.getProperty(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY()));
            streamTaskLog.setToTable(props.getProperty(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY()));
            streamTaskLog.setCommitTime(callbackMessage.getCommitTime());
            streamTaskLog.setBasePath(callbackMessage.getBasePath());
            streamTaskLog.setHudiTable(callbackMessage.getTableName());
            streamTaskLog.setInsertRows(totalInsertRecordsWritten);
            streamTaskLog.setDeleteRows(totalDeleteRecordsWritten);
            streamTaskLog.setUpdateRows(totalUpdateRecordsWritten);
            streamTaskLog.setCreateTime(new Date());
            streamTaskLog.setCreateUser(CREATE_USER);
            streamTaskLog.setDelFlag(Boolean.FALSE);

            saveTaskLog(streamTaskLog);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveTaskLog(StreamTaskLog streamTaskLog) {
        StreamTaskLogService service = SpringUtil.getBean("streamTaskLogService", StreamTaskLogService.class);
        service.save(streamTaskLog);
    }
}
