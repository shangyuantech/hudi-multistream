package org.apache.hudi.multistream.config;

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.JsonDFSSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@Component
@ConfigurationProperties(prefix = "hudi")
public class HudiConfig {

    public static final String KAFKA_TOPIC_NAME = "hoodie.deltastreamer.source.kafka.topic";

    private String basePath;

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    private Map<String, String> properties;

    public Map<String, String> getProperties() {
        return properties;
    }

    public Properties getHudiProperties() {
        Properties props = new Properties();
        props.putAll(properties);
        return props;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getPropertiesValue(String key) {
        return properties.get(key);
    }

    public static class Delta {

        private String configFolder;
        private String tableType;
        private String sourceClass = JsonDFSSource.class.getName();
        private String sourceOrderingField = "ts";
        private String payloadClass = OverwriteWithLatestAvroPayload.class.getName();
        private String schemaproviderClass;
        private List<String> transformerClass;
        private Long sourceLimit = Long.MAX_VALUE;
        private HoodieDeltaStreamer.Operation op = HoodieDeltaStreamer.Operation.UPSERT;
        private Boolean filterDupes = false;
        private Boolean enableHiveSync = false;
        private Integer maxPendingCompactions = 5;
        private Integer minSyncIntervalSeconds = 0;
        private String sparkMaster = "local[2]";
        private Boolean commitOnErrors = false;
        private Integer deltaSyncSchedulingWeight = 1;
        private Integer compactSchedulingWeight = 1;
        private Integer deltaSyncSchedulingMinshare = 0;
        private Integer compactSchedulingMinshare = 0;
        private Boolean disableCompaction = false;

        public String getConfigFolder() {
            return configFolder;
        }

        public void setConfigFolder(String configFolder) {
            this.configFolder = configFolder;
        }

        public String getTableType() {
            return tableType;
        }

        public void setTableType(String tableType) {
            this.tableType = tableType;
        }

        public String getSourceClass() {
            return sourceClass;
        }

        public void setSourceClass(String sourceClass) {
            this.sourceClass = sourceClass;
        }

        public String getSourceOrderingField() {
            return sourceOrderingField;
        }

        public void setSourceOrderingField(String sourceOrderingField) {
            this.sourceOrderingField = sourceOrderingField;
        }

        public String getPayloadClass() {
            return payloadClass;
        }

        public void setPayloadClass(String payloadClass) {
            this.payloadClass = payloadClass;
        }

        public String getSchemaproviderClass() {
            return schemaproviderClass;
        }

        public void setSchemaproviderClass(String schemaproviderClass) {
            this.schemaproviderClass = schemaproviderClass;
        }

        public List<String> getTransformerClass() {
            return transformerClass;
        }

        public void setTransformerClass(List<String> transformerClass) {
            this.transformerClass = transformerClass;
        }

        public Long getSourceLimit() {
            return sourceLimit;
        }

        public void setSourceLimit(Long sourceLimit) {
            this.sourceLimit = sourceLimit;
        }

        public HoodieDeltaStreamer.Operation getOp() {
            return op;
        }

        public void setOp(HoodieDeltaStreamer.Operation op) {
            this.op = op;
        }

        public Boolean getFilterDupes() {
            return filterDupes;
        }

        public void setFilterDupes(Boolean filterDupes) {
            this.filterDupes = filterDupes;
        }

        public Boolean getEnableHiveSync() {
            return enableHiveSync;
        }

        public void setEnableHiveSync(Boolean enableHiveSync) {
            this.enableHiveSync = enableHiveSync;
        }

        public Integer getMaxPendingCompactions() {
            return maxPendingCompactions;
        }

        public void setMaxPendingCompactions(Integer maxPendingCompactions) {
            this.maxPendingCompactions = maxPendingCompactions;
        }

        public Integer getMinSyncIntervalSeconds() {
            return minSyncIntervalSeconds;
        }

        public void setMinSyncIntervalSeconds(Integer minSyncIntervalSeconds) {
            this.minSyncIntervalSeconds = minSyncIntervalSeconds;
        }

        public String getSparkMaster() {
            return sparkMaster;
        }

        public void setSparkMaster(String sparkMaster) {
            this.sparkMaster = sparkMaster;
        }

        public Boolean getCommitOnErrors() {
            return commitOnErrors;
        }

        public void setCommitOnErrors(Boolean commitOnErrors) {
            this.commitOnErrors = commitOnErrors;
        }

        public Integer getDeltaSyncSchedulingWeight() {
            return deltaSyncSchedulingWeight;
        }

        public void setDeltaSyncSchedulingWeight(Integer deltaSyncSchedulingWeight) {
            this.deltaSyncSchedulingWeight = deltaSyncSchedulingWeight;
        }

        public Integer getCompactSchedulingWeight() {
            return compactSchedulingWeight;
        }

        public void setCompactSchedulingWeight(Integer compactSchedulingWeight) {
            this.compactSchedulingWeight = compactSchedulingWeight;
        }

        public Integer getDeltaSyncSchedulingMinshare() {
            return deltaSyncSchedulingMinshare;
        }

        public void setDeltaSyncSchedulingMinshare(Integer deltaSyncSchedulingMinshare) {
            this.deltaSyncSchedulingMinshare = deltaSyncSchedulingMinshare;
        }

        public Integer getCompactSchedulingMinshare() {
            return compactSchedulingMinshare;
        }

        public void setCompactSchedulingMinshare(Integer compactSchedulingMinshare) {
            this.compactSchedulingMinshare = compactSchedulingMinshare;
        }

        public Boolean getDisableCompaction() {
            return disableCompaction;
        }

        public void setDisableCompaction(Boolean disableCompaction) {
            this.disableCompaction = disableCompaction;
        }

        @Override
        public String toString() {
            return "Delta{" +
                    "configFolder='" + configFolder + '\'' +
                    ", tableType='" + tableType + '\'' +
                    ", sourceClass='" + sourceClass + '\'' +
                    ", sourceOrderingField='" + sourceOrderingField + '\'' +
                    ", payloadClass='" + payloadClass + '\'' +
                    ", schemaproviderClass='" + schemaproviderClass + '\'' +
                    ", transformerClass='" + transformerClass + '\'' +
                    ", sourceLimit='" + sourceLimit + '\'' +
                    ", op=" + op +
                    ", filterDupes=" + filterDupes +
                    ", enableHiveSync=" + enableHiveSync +
                    ", maxPendingCompactions=" + maxPendingCompactions +
                    ", minSyncIntervalSeconds=" + minSyncIntervalSeconds +
                    ", sparkMaster='" + sparkMaster + '\'' +
                    ", commitOnErrors=" + commitOnErrors +
                    ", deltaSyncSchedulingWeight=" + deltaSyncSchedulingWeight +
                    ", compactSchedulingWeight=" + compactSchedulingWeight +
                    ", deltaSyncSchedulingMinshare=" + deltaSyncSchedulingMinshare +
                    ", compactSchedulingMinshare=" + compactSchedulingMinshare +
                    ", disableCompaction=" + disableCompaction +
                    '}';
        }
    }

    private Delta delta = new Delta();

    public Delta getDelta() {
        return delta;
    }

    public void setDelta(Delta delta) {
        this.delta = delta;
    }

    public HoodieDeltaStreamer.Config getDeltaConfig() {
        HoodieDeltaStreamer.Config config = new HoodieDeltaStreamer.Config();
        config.tableType = delta.getTableType();
        config.sourceClassName = delta.getSourceClass();
        config.sourceOrderingField = delta.getSourceOrderingField();
        config.payloadClassName = delta.getPayloadClass();
        config.schemaProviderClassName = delta.getSchemaproviderClass();
        config.transformerClassNames = delta.getTransformerClass();
        config.sourceLimit = delta.getSourceLimit();
        config.operation = delta.getOp();
        config.filterDupes = delta.getFilterDupes();
        config.enableHiveSync = delta.getEnableHiveSync();
        config.maxPendingCompactions = delta.getMaxPendingCompactions();
        config.minSyncIntervalSeconds = delta.getMinSyncIntervalSeconds();
        config.sparkMaster = delta.getSparkMaster();
        config.commitOnErrors = delta.getCommitOnErrors();
        config.compactSchedulingMinShare = delta.getCompactSchedulingMinshare();
        config.compactSchedulingWeight = delta.getCompactSchedulingWeight();
        config.deltaSyncSchedulingMinShare = delta.getDeltaSyncSchedulingMinshare();
        config.deltaSyncSchedulingWeight = delta.getDeltaSyncSchedulingWeight();
        config.continuousMode = false;
        return config;
    }

    public static class Kafka {

        public enum BASEON {
            FILE,
            DB,
            TOPIC
        }

        private String topic;
        private BASEON baseOn = BASEON.TOPIC;
        private Integer loopInterval = 5;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public BASEON getBaseOn() {
            return baseOn;
        }

        public void setBaseOn(BASEON baseOn) {
            this.baseOn = baseOn;
        }

        public Integer getLoopInterval() {
            return loopInterval;
        }

        public void setLoopInterval(Integer loopInterval) {
            this.loopInterval = loopInterval;
        }

        @Override
        public String toString() {
            return "Kafka{" +
                    "topic='" + topic + '\'' +
                    ", baseOn=" + baseOn +
                    ", loopInterval=" + loopInterval +
                    '}';
        }
    }

    private Kafka kafka = new Kafka();

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    @Override
    public String toString() {
        return "HudiConfig{" +
                "properties=" + properties +
                ", kafka=" + kafka +
                ", delta=" + delta +
                '}';
    }
}
