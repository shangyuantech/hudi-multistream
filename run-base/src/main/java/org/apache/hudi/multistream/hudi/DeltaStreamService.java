package org.apache.hudi.multistream.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.multistream.common.Accumulator;
import org.apache.hudi.multistream.common.HudiTable;
import org.apache.hudi.multistream.config.BuildConfig;
import org.apache.hudi.multistream.config.HudiConfig;
import org.apache.hudi.multistream.utils.HudiUtil;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DeltaStreamService {

    private final static Logger log = LoggerFactory.getLogger(DeltaStreamService.class);

    @Autowired
    private HudiConfig hudiConfig;

    @Autowired
    private JavaSparkContext jssc;

    public void startTask(TypedProperties typedProperties) {
        try {
            String topic = typedProperties.getString(HudiConfig.KAFKA_TOPIC_NAME);
            HudiTable hudiTable = HudiUtil.rebuildPathToDbTable(topic);
            String targetBasePath = HudiUtil.getHudiPath(hudiConfig.getBasePath(), hudiTable.getDb(), hudiTable.getTable());

            HoodieDeltaStreamer.Config deltaConfig = hudiConfig.getDeltaConfig();
            deltaConfig.targetBasePath = targetBasePath;
            deltaConfig.targetTableName = hudiTable.getTable();

            Accumulator.INSTANCE.rebuildCurrentTopic(topic);
            Configuration configuration = jssc.hadoopConfiguration();
            log.info("[listener {}] start a hudi task to {} with delta: {}", BuildConfig.ipAddress, targetBasePath, deltaConfig);
            new HoodieDeltaStreamer(deltaConfig, jssc, FSUtils.getFs(targetBasePath, configuration), configuration,
                    typedProperties).sync();
            Accumulator.INSTANCE.success();
        } catch (Exception e) {
            e.printStackTrace();
            Accumulator.INSTANCE.failed();
        } finally {
            Accumulator.INSTANCE.rebuildCurrentTopic("");
        }
    }
}
