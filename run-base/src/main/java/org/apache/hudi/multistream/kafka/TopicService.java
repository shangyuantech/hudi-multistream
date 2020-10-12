package org.apache.hudi.multistream.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.hudi.multistream.config.HudiConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class TopicService {

    private final static Logger log = LoggerFactory.getLogger(TopicService.class);

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private KafkaAdmin admin;

    @Value("${spring.kafka.consumer.key-deserializer}")
    private String keyDeserializer;

    @Value("${spring.kafka.consumer.value-deserializer}")
    private String valueDeserializer;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    public List<String> listTopics(String grep) throws InterruptedException, ExecutionException {

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topics = listTopicsResult.names().get();

        Pattern p = Pattern.compile(grep);
        List<String> filterTopics = topics.stream().filter(topic -> p.matcher(topic).find()).collect(Collectors.toList());
        Collections.sort(filterTopics, new Comparator<String>(){
            public int compare(String left, String right) {
                return left.compareTo(right);
            }
        });

        return filterTopics;
    }

    public Map<TopicPartition, Long> consumerOffset(String topicName) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, admin.getConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                admin.getConfig().get(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG));

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topicName);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitions.forEach(partition -> {
            TopicPartition topicPartition = new TopicPartition(topicName, partition.partition());
            topicPartitions.add(topicPartition);
        });

        Map<TopicPartition, Long> offsets = kafkaConsumer.endOffsets(topicPartitions);
        log.debug("get current offsets = {}", offsets);
        return offsets;
    }

    public List<String> listTopicsByIndex(List<String> filterTopics, int index, int total) {
        for (int i = filterTopics.size() - 1 ; i >= 0 ; i --) {
            if (i % total != index) {
                filterTopics.remove(i);
            }
        }
        return filterTopics;
    }

    public List<String> listTopicsByBalancer(String grep, HudiConfig.Kafka.BASEON baseOn, int index, int total)
            throws InterruptedException, ExecutionException, IOException {
        // TODO 利用负载均衡来处理，可以平均分配
        List<String> filterTopics = listTopics(grep);
        return filterTopics;
    }
}
