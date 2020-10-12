package org.apache.hudi.multistream.rest;

import org.apache.hudi.multistream.common.Accumulator;
import org.springframework.stereotype.Service;

@Service
public class HudiMultistreamService {

    public String getCurrentTopic() {
        return Accumulator.INSTANCE.getCurrentTopic();
    }

    public long getSuccessTasks() {
        return Accumulator.INSTANCE.getSuccessTasks();
    }

    public long getFailedTasks() {
        return Accumulator.INSTANCE.getFailedTasks();
    }
}
