package org.apache.hudi.multistream.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class TaskInfo {

    @JsonProperty(value = "current_topic")
    private String currentTopic;


    @JsonProperty(value = "success_task")
    private long successTask;

    @JsonProperty(value = "failed_task")
    private long failedTask;

    public TaskInfo(String currentTopic, long successTask, long failedTask) {
        this.currentTopic = currentTopic;
        this.successTask = successTask;
        this.failedTask = failedTask;
    }

    public String getCurrentTopic() {
        return currentTopic;
    }

    public void setCurrentTopic(String currentTopic) {
        this.currentTopic = currentTopic;
    }

    public long getSuccessTask() {
        return successTask;
    }

    public void setSuccessTask(long successTask) {
        this.successTask = successTask;
    }

    public long getFailedTask() {
        return failedTask;
    }

    public void setFailedTask(long failedTask) {
        this.failedTask = failedTask;
    }

    @Override
    public String toString() {
        return "TaskInfo{" +
                "currentTopic='" + currentTopic + '\'' +
                ", successTask=" + successTask +
                ", failedTask=" + failedTask +
                '}';
    }
}
