package org.apache.hudi.multistream.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.multistream.config.BuildConfig;
import org.apache.hudi.multistream.config.ZookeeperConfig;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.kafka.common.TopicPartition;
import org.apache.hudi.multistream.common.HudiTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class HudiUtil {

    private final static Logger log = LoggerFactory.getLogger(HudiUtil.class);

    private static final String KAFKA_TOPIC_NAME = "hoodie.deltastreamer.source.kafka.topic";

    //public static final String IP = getInnetIp();

    public static HudiTable rebuildPathToDbTable(String topic) {
        String[] paths = topic.split("\\.");
        //return Arrays.copyOfRange(paths, paths.length - 2, paths.length);
        return new HudiTable(paths[paths.length - 2], paths[paths.length - 1]);
    }

    public static boolean checkOffset(Map<TopicPartition, Long> topicOffsets, Map<TopicPartition, Long> hudiOffsets) {
        return hudiOffsets.isEmpty() ? true : topicOffsets.entrySet().stream()
                .anyMatch(offset -> {
                    TopicPartition key = offset.getKey();
                    if (!hudiOffsets.containsKey(key)) {
                        return true;
                    }
                    return offset.getValue() > hudiOffsets.get(key);
                });
    }

    public static List<String> filterTopicByFile(List<String> topics, List<String> fileTopics) {
        return topics.stream().filter(topic -> fileTopics.contains(topic)).collect(Collectors.toList());
    }

    public static Map<String, TypedProperties> getHudiTopicsByConfigFolder(String configFolder) throws IOException {
        configFolder = configFolder.charAt(configFolder.length() - 1) == '/' ? configFolder.substring(0, configFolder.length() - 1) : configFolder;
        Path folderPath = new Path(configFolder);
        FileSystem fs = folderPath.getFileSystem(FSUtils.prepareHadoopConf(new Configuration()));

        FileStatus[] folder = fs.listStatus(folderPath);
        log.debug("scan config folder files = {}", Arrays.stream(folder));
        Map<String, TypedProperties> fileTopics = new HashMap<>();
        for (FileStatus child : folder) {
            TypedProperties properties = UtilHelpers.readConfig(fs, child.getPath(), Collections.emptyList()).getConfig();
            fileTopics.put(properties.getString(KAFKA_TOPIC_NAME), properties);
        }

        return fileTopics;
    }

    public static TypedProperties combineProps(Properties hudiProps, TypedProperties overrideProps) {
        TypedProperties combineProps = new TypedProperties();
        combineProps.putAll(hudiProps);

        if (overrideProps != null && !overrideProps.isEmpty()) {
            combineProps.putAll(overrideProps);
        }

        return combineProps;
    }

    public static String getInnetIp() {
        String localip = null;
        String netip = null;

        try {
            Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            boolean finded = false;// 是否找到外网IP
            while (netInterfaces.hasMoreElements() && !finded) {
                NetworkInterface ni = netInterfaces.nextElement();
                Enumeration<InetAddress> address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                    ip = address.nextElement();
                    if (!ip.isSiteLocalAddress()
                            && !ip.isLoopbackAddress()
                            && ip.getHostAddress().indexOf(":") == -1) {// 外网IP
                        netip = ip.getHostAddress();
                        finded = true;
                        break;
                    } else if (ip.isSiteLocalAddress()
                            && !ip.isLoopbackAddress()
                            && ip.getHostAddress().indexOf(":") == -1) {// 内网IP
                        localip = ip.getHostAddress();
                    }
                }
            }
        } catch (SocketException e1) {
            e1.printStackTrace();
            
            try {
                localip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e2) {
                e2.printStackTrace();
            }
        }

        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }

    public static String getListenerPath(Integer serverPort) {
        return String.format("/%s/listener/%s:%s", ZookeeperConfig.getServiceName(), BuildConfig.ipAddress, serverPort);
    }

    public static String getHudiPath(String path, String db, String table) {
        return String.format(path, db, table);
    }

    public static TypedProperties getTypedProps(String propsStr) throws JsonProcessingException {
        Properties props = new ObjectMapper().readValue(propsStr, Properties.class);
        TypedProperties typedProperties = new TypedProperties();
        typedProperties.putAll(props);
        return typedProperties;
    }
}
