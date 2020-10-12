package org.apache.hudi.multistream.config.props.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.multistream.config.props.IHudiPropsBuilder;
import org.apache.hudi.utilities.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.hudi.multistream.config.HudiConfig.KAFKA_TOPIC_NAME;

public class FileHudiPropsBuilder implements IHudiPropsBuilder {

    private final static Logger log = LoggerFactory.getLogger(FileHudiPropsBuilder.class);

    private  Map<String, TypedProperties> fileTopics = new HashMap<>();

    public FileHudiPropsBuilder(List<String> topics, String configFolder) {
        configFolder = configFolder.charAt(configFolder.length() - 1) == '/' ? configFolder.substring(0, configFolder.length() - 1) : configFolder;
        Path folderPath = new Path(configFolder);

        try {
            FileSystem fs = folderPath.getFileSystem(FSUtils.prepareHadoopConf(new Configuration()));
            FileStatus[] folder = fs.listStatus(folderPath);
            log.debug("scan config folder files = {}", Arrays.stream(folder));

            for (FileStatus child : folder) {
                TypedProperties properties = UtilHelpers.readConfig(fs, child.getPath(), Collections.emptyList()).getConfig();
                String kafkaTopic = properties.getString(KAFKA_TOPIC_NAME);
                if (topics.contains(kafkaTopic)) {
                    fileTopics.put(properties.getString(KAFKA_TOPIC_NAME), properties);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TypedProperties getTypedProperties(String topic) {
        return fileTopics.get(topic);
    }

    @Override
    public List<String> getTopics() {
        return new ArrayList<>(fileTopics.keySet());
    }
}
