package org.apache.hudi.multistream.config.props;

import org.apache.hudi.common.config.TypedProperties;

import java.util.List;

public interface IHudiPropsBuilder {

    TypedProperties getTypedProperties(String topic);

    List<String> getTopics();
}
