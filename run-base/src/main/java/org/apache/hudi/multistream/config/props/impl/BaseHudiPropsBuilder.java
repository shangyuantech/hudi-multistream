package org.apache.hudi.multistream.config.props.impl;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.multistream.config.props.IHudiPropsBuilder;

import java.util.List;

public class BaseHudiPropsBuilder implements IHudiPropsBuilder {

    private List<String> topics;

    public BaseHudiPropsBuilder(List<String> topics) {
        this.topics = topics;
    }

    @Override
    public TypedProperties getTypedProperties(String topic) {
        return new TypedProperties();
    }

    @Override
    public List<String> getTopics() {
        return topics;
    }
}
