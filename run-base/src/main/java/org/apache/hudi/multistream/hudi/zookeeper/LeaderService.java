package org.apache.hudi.multistream.hudi.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.multistream.common.HudiTable;
import org.apache.hudi.multistream.config.HudiConfig;
import org.apache.hudi.multistream.config.ZookeeperConfig;
import org.apache.hudi.multistream.config.props.IHudiPropsBuilder;
import org.apache.hudi.multistream.config.props.impl.BaseHudiPropsBuilder;
import org.apache.hudi.multistream.config.props.impl.DbHudiPropsBuilder;
import org.apache.hudi.multistream.config.props.impl.FileHudiPropsBuilder;
import org.apache.hudi.multistream.hudi.TimelineService;
import org.apache.hudi.multistream.kafka.TopicService;
import org.apache.hudi.multistream.utils.HudiUtil;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class LeaderService {

    private final static Logger log = LoggerFactory.getLogger(LeaderService.class);

    @Autowired
    private HudiConfig hudiConfig;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private TopicService topicService;

    @Autowired
    private TimelineService timeline;

    @Autowired
    private ZookeeperService zookeeper;

    public void scanTopic() throws Exception {
        List<String> topics = topicService.listTopics(hudiConfig.getKafka().getTopic());
        IHudiPropsBuilder props;
        switch (hudiConfig.getKafka().getBaseOn()) {
            case FILE:
                props = new FileHudiPropsBuilder(topics, hudiConfig.getDelta().getConfigFolder());
                break;
            case DB:
                props = new DbHudiPropsBuilder(topics);
                break;
            default:
                props = new BaseHudiPropsBuilder(topics);
                break;
        }

        List<String> runTopics = props.getTopics();
        log.debug("[leader] get {} topic(s) to run:\n{}", runTopics.size(), runTopics);

        for (String topic : runTopics) {
            // hudi current checkpoint
            Map<TopicPartition, Long> offsets = topicService.consumerOffset(topic);
            HudiTable hudiTable = HudiUtil.rebuildPathToDbTable(topic);
            String targetBasePath = HudiUtil.getHudiPath(hudiConfig.getBasePath(), hudiTable.getDb(), hudiTable.getTable());
            Map<TopicPartition, Long> latest = timeline.getLastCommitCheckpoint(targetBasePath);

            if (HudiUtil.checkOffset(offsets, latest)) {
                Properties hudiProps = hudiConfig.getHudiProperties();
                hudiProps.putAll(kafkaProperties.getProperties());
                hudiProps.putAll(kafkaProperties.buildConsumerProperties());

                TypedProperties overrideProps = HudiUtil.combineProps(hudiProps, props.getTypedProperties(topic));
                log.info("[leader] need to publish a delta streamer task with topic = {} and targetBasePath = {} and hudiProps = {}"
                        , topic, targetBasePath, overrideProps);

                // fillback a task to zookeeper listener
                while (!backfillTask(topic, overrideProps)) {
                    log.debug("there is not listener free, wait for topic {} to start...", topic);
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }
            }
        }
    }

    private boolean backfillTask(String topic, TypedProperties properties) throws Exception {
        String listenersPath = String.format("/%s/listener", ZookeeperConfig.getServiceName());
        List<String> listeners = zookeeper.getChildes(listenersPath);
        List<String> freeListeners = new ArrayList<>();

        for (String listener : listeners) {
            String listenerPath = String.format("%s/%s", listenersPath, listener);;
            String data = zookeeper.getData(listenerPath);
            if (StringUtils.isBlank(data)) {
                freeListeners.add(listenerPath);
            } else {
                String runningTopic = HudiUtil.getTypedProps(data).getString(HudiConfig.KAFKA_TOPIC_NAME);
                if (runningTopic.equals(topic)) {
                    return true;
                }
            }
        }

        JSONObject jsonProps = new JSONObject(properties);
        String hudiPropStr = jsonProps.toString();
        for (String listenerPath : freeListeners) {
            try {
                zookeeper.setData(listenerPath, hudiPropStr);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return false;
    }
}
